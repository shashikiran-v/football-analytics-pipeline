"""
Integration tests for src.gold.run.

End-to-end tests that run Bronze + Silver first, then Gold via the
runner. These exercise the full pipeline as a reviewer would on first
inspection of the repo.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from src.bronze.run import run_bronze
from src.gold.run import GoldRunSummary, run_gold
from src.metadata.db import init_db
from src.silver.run import run_silver
from src.utils.config import get_config


SAMPLES_DIR = Path(__file__).resolve().parents[1] / "data" / "sample"


@pytest.fixture
def fresh_db():
    init_db()


@pytest.fixture
def silver_seeded(fresh_db):
    """Bronze + Silver populated; Gold can now run."""
    run_bronze(batch_id="2024-12-01", raw_root=SAMPLES_DIR)
    run_silver(batch_id="2024-12-01")


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


class TestRunGoldHappyPath:
    def test_full_run_builds_all_five_artifacts(self, silver_seeded):
        summary = run_gold(batch_id="2024-12-01")
        assert isinstance(summary, GoldRunSummary)
        assert summary.layer_status == "success"
        assert summary.skipped_layer is False
        assert len(summary.results) == 5
        names = {r.artifact_name for r in summary.results}
        assert names == {
            "top_scorers_by_season",
            "club_season_summary",
            "top_players_all_time",
            "player_valuation_rolling_avg",
            "club_performance_metrics",
        }

    def test_all_artifacts_marked_written(self, silver_seeded):
        summary = run_gold(batch_id="2024-12-01")
        assert all(r.status == "written" for r in summary.results)

    def test_expected_row_counts(self, silver_seeded):
        summary = run_gold(batch_id="2024-12-01")
        rows_by_artifact = {r.artifact_name: r.rows_written for r in summary.results}
        assert rows_by_artifact["top_scorers_by_season"] == 12
        assert rows_by_artifact["club_season_summary"] == 5
        assert rows_by_artifact["top_players_all_time"] == 12
        assert rows_by_artifact["player_valuation_rolling_avg"] == 18
        assert rows_by_artifact["club_performance_metrics"] == 5

    def test_partitions_written_on_disk(self, silver_seeded):
        run_gold(batch_id="2024-12-01")
        gold_root = get_config().paths.gold
        for artifact in [
            "top_scorers_by_season",
            "club_season_summary",
            "top_players_all_time",
            "player_valuation_rolling_avg",
            "club_performance_metrics",
        ]:
            partition_dir = gold_root / artifact / "batch_id=2024-12-01"
            assert partition_dir.is_dir(), (
                f"Missing partition directory: {partition_dir}"
            )
            assert list(partition_dir.glob("*.parquet")), (
                f"No parquet files in {partition_dir}"
            )

    def test_primary_source_attribution_recorded(self, silver_seeded):
        """Each artifact's outcome must report its primary_source."""
        summary = run_gold(batch_id="2024-12-01")
        by_name = {r.artifact_name: r for r in summary.results}
        assert by_name["top_scorers_by_season"].primary_source == "appearances"
        assert by_name["top_players_all_time"].primary_source == "appearances"
        assert by_name["club_season_summary"].primary_source == "games"
        assert by_name["club_performance_metrics"].primary_source == "games"
        assert by_name["player_valuation_rolling_avg"].primary_source == "player_valuations"


# ---------------------------------------------------------------------------
# Layer-grain idempotency
# ---------------------------------------------------------------------------


class TestLayerIdempotency:
    def test_repeat_batch_is_skipped(self, silver_seeded):
        first = run_gold(batch_id="2024-12-01")
        assert first.layer_status == "success"
        second = run_gold(batch_id="2024-12-01")
        assert second.layer_status == "skipped"
        assert second.skipped_layer is True
        assert len(second.results) == 0


# ---------------------------------------------------------------------------
# Audit DAO integration
# ---------------------------------------------------------------------------


class TestAuditIntegration:
    def test_gold_row_count_recorded_on_primary_sources(self, silver_seeded):
        from src.metadata import audit
        run_gold(batch_id="2024-12-01")
        rows = audit.list_batch_files(batch_id="2024-12-01")
        by_source = {r.source_name: r for r in rows}
        # appearances: primary source for top_scorers_by_season AND
        # top_players_all_time. Last-writer-wins on the audit row;
        # both artifacts produce 12 rows so the result is 12 either way.
        assert by_source["appearances"].gold_row_count == 12
        # games: primary for club_season_summary AND club_performance_metrics
        # — both produce 5 rows.
        assert by_source["games"].gold_row_count == 5
        # player_valuations: primary for the rolling-avg artifact
        assert by_source["player_valuations"].gold_row_count == 18

    def test_non_primary_sources_not_attributed(self, silver_seeded):
        """Sources that are dimensions (joined in but not primary)
        should NOT have a gold_row_count recorded."""
        from src.metadata import audit
        run_gold(batch_id="2024-12-01")
        rows = audit.list_batch_files(batch_id="2024-12-01")
        by_source = {r.source_name: r for r in rows}
        # clubs, competitions, players are joined into Gold artifacts
        # as dimensions but no artifact has them as primary_source
        assert by_source["clubs"].gold_row_count is None
        assert by_source["competitions"].gold_row_count is None
        assert by_source["players"].gold_row_count is None

    def test_player_valuations_gets_gold_audit_despite_no_silver(self, silver_seeded):
        """player_valuations skips the Silver layer entirely but its
        audit row picks up a gold_row_count. That's the brief's
        lineage requirement satisfied for Bronze->Gold direct sources."""
        from src.metadata import audit
        from src.metadata.audit import FileStatus
        run_gold(batch_id="2024-12-01")
        rows = audit.list_batch_files(batch_id="2024-12-01")
        by_source = {r.source_name: r for r in rows}
        pv = by_source["player_valuations"]
        # Silver was never run for this source
        assert pv.silver_row_count is None
        # Status stays at INGESTED (ADR-0005 design)
        assert pv.status == FileStatus.INGESTED
        # But Gold did consume it
        assert pv.gold_row_count == 18

    def test_gold_finished_event_emitted(self, silver_seeded):
        """Each primary source's event timeline must include
        gold_finished after the run."""
        from src.metadata import audit
        from src.metadata.audit import EventType
        run_gold(batch_id="2024-12-01")
        # appearances primary source - check event timeline
        rows = audit.list_batch_files(batch_id="2024-12-01")
        appearances = next(r for r in rows if r.source_name == "appearances")
        timeline = audit.get_event_timeline(
            batch_id="2024-12-01",
            source_file_path=appearances.source_file_path,
        )
        event_types = {e["event_type"] for e in timeline}
        assert EventType.GOLD_FINISHED.value in event_types


# ---------------------------------------------------------------------------
# Summary properties
# ---------------------------------------------------------------------------


class TestSummaryProperties:
    def test_total_rows_sums_correctly(self, silver_seeded):
        summary = run_gold(batch_id="2024-12-01")
        # 12 + 5 + 12 + 18 + 5 = 52
        assert summary.total_rows == 52

    def test_failures_list_empty_on_clean_run(self, silver_seeded):
        summary = run_gold(batch_id="2024-12-01")
        assert summary.failures == []


# ---------------------------------------------------------------------------
# Continue-on-failure / no silver scenario
# ---------------------------------------------------------------------------


class TestNoSilverScenario:
    def test_gold_fails_gracefully_when_silver_absent(self, fresh_db):
        """Running Gold without Bronze/Silver: the session opens (with
        no views registered), but every artifact fails when DuckDB
        can't resolve table names. Layer status: failed, not crashed."""
        summary = run_gold(batch_id="never-built")
        assert summary.layer_status == "failed"
        assert len(summary.results) == 5
        # All artifacts failed; the runner didn't propagate the
        # exception out of run_gold
        assert all(r.status == "failed" for r in summary.results)
