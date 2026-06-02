"""
Integration tests for src.silver.run.

End-to-end tests using the committed data/sample/ CSVs, run through
Bronze first then Silver. These prove the complete pipeline works for
our actual six sources, not just synthetic ones.

Each test runs Bronze + Silver from a clean tmp_path-isolated DATA_ROOT
(provided by conftest._isolate_metadata_db). No tests interfere with
each other.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from src.bronze.run import run_bronze
from src.metadata.db import init_db
from src.silver.run import SilverRunSummary, run_silver
from src.utils.config import get_config


SAMPLES_DIR = Path(__file__).resolve().parents[1] / "data" / "sample"


@pytest.fixture
def fresh_db():
    init_db()


@pytest.fixture
def bronze_seeded(fresh_db):
    """Every Silver test needs Bronze data first. Seeds the canonical batch."""
    run_bronze(batch_id="2024-12-01", raw_root=SAMPLES_DIR)


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


class TestRunSilverHappyPath:
    def test_full_run_builds_all_six_artifacts(self, bronze_seeded):
        summary = run_silver(batch_id="2024-12-01")
        assert isinstance(summary, SilverRunSummary)
        assert summary.layer_status == "success"
        assert summary.skipped_layer is False
        assert len(summary.results) == 6
        artifact_names = {r.artifact_name for r in summary.results}
        assert artifact_names == {
            "dim_clubs", "dim_competitions", "dim_date",
            "dim_players", "fact_games", "fact_appearances",
        }

    def test_all_artifacts_marked_written(self, bronze_seeded):
        summary = run_silver(batch_id="2024-12-01")
        assert all(r.status == "written" for r in summary.results)

    def test_expected_row_counts(self, bronze_seeded):
        summary = run_silver(batch_id="2024-12-01")
        rows_by_artifact = {r.artifact_name: r.rows_written for r in summary.results}
        # Pinned counts that match the committed samples
        assert rows_by_artifact["dim_clubs"] == 5
        assert rows_by_artifact["dim_competitions"] == 3
        assert rows_by_artifact["dim_players"] == 12
        assert rows_by_artifact["fact_games"] == 6
        assert rows_by_artifact["fact_appearances"] == 30
        # dim_date row count depends on the date range (2018-01-01 to 2030-12-31)
        assert rows_by_artifact["dim_date"] > 4000

    def test_partitions_written_on_disk(self, bronze_seeded):
        summary = run_silver(batch_id="2024-12-01")
        assert summary.layer_status == "success"
        silver_root = get_config().paths.silver
        for artifact in [
            "dim_clubs", "dim_competitions", "dim_date",
            "dim_players", "fact_games", "fact_appearances",
        ]:
            partition_dir = silver_root / artifact / "batch_id=2024-12-01"
            assert partition_dir.is_dir(), (
                f"Missing partition directory: {partition_dir}"
            )
            files = list(partition_dir.glob("*.parquet"))
            assert files, f"No parquet files in {partition_dir}"


# ---------------------------------------------------------------------------
# Transformations end-to-end via the runner
# ---------------------------------------------------------------------------


class TestTransformationsEndToEnd:
    def test_position_normalisation_applied_in_dim_players(self, bronze_seeded):
        run_silver(batch_id="2024-12-01")
        silver_root = get_config().paths.silver
        dim = pd.read_parquet(silver_root / "dim_players")
        # Both 'GK' and 'Goalkeeper' rows should have position_canonical='Goalkeeper'
        gk_rows = dim[dim["position"].isin(["GK", "Goalkeeper"])]
        assert len(gk_rows) >= 2     # Sample has both variants
        assert all(gk_rows["position_canonical"] == "Goalkeeper")
        assert all(gk_rows["position_category"] == "goalkeeper")

    def test_country_iso_normalisation_applied(self, bronze_seeded):
        run_silver(batch_id="2024-12-01")
        silver_root = get_config().paths.silver
        dim = pd.read_parquet(silver_root / "dim_players")
        # Player with 'England' citizenship -> GB; 'Brazil' -> BR
        england_rows = dim[dim["country_of_citizenship"] == "England"]
        if len(england_rows):
            assert all(england_rows["country_of_citizenship_iso"] == "GB")
        brazil_rows = dim[dim["country_of_citizenship"] == "Brazil"]
        if len(brazil_rows):
            assert all(brazil_rows["country_of_citizenship_iso"] == "BR")

    def test_match_outcome_derived_in_fact_games(self, bronze_seeded):
        run_silver(batch_id="2024-12-01")
        silver_root = get_config().paths.silver
        fact = pd.read_parquet(silver_root / "fact_games")
        # outcome column must exist and have valid values
        assert "outcome" in fact.columns
        valid_outcomes = {"home_win", "away_win", "draw", "unknown"}
        assert set(fact["outcome"].unique()) <= valid_outcomes

    def test_orphan_player_appearance_has_null_player_sk(self, bronze_seeded):
        """The deliberate orphan player_id=9999 in samples must reach
        Silver with player_sk=NULL (i.e. the as-of resolution finds
        nothing). DQ will flag this in Phase 4."""
        run_silver(batch_id="2024-12-01")
        silver_root = get_config().paths.silver
        fact = pd.read_parquet(silver_root / "fact_appearances")
        orphan = fact[fact["player_id"] == 9999]
        assert len(orphan) == 1
        assert pd.isna(orphan["player_sk"].iloc[0])

    def test_valid_appearances_have_resolved_player_sk(self, bronze_seeded):
        """All 29 non-orphan appearances must have a resolved player_sk."""
        run_silver(batch_id="2024-12-01")
        silver_root = get_config().paths.silver
        fact = pd.read_parquet(silver_root / "fact_appearances")
        valid = fact[fact["player_id"] != 9999]
        assert len(valid) == 29
        assert valid["player_sk"].notna().all()


# ---------------------------------------------------------------------------
# Layer-grain idempotency
# ---------------------------------------------------------------------------


class TestLayerIdempotency:
    def test_repeat_batch_is_skipped(self, bronze_seeded):
        first = run_silver(batch_id="2024-12-01")
        assert first.layer_status == "success"
        second = run_silver(batch_id="2024-12-01")
        assert second.layer_status == "skipped"
        assert second.skipped_layer is True
        assert len(second.results) == 0


# ---------------------------------------------------------------------------
# Continue-on-failure
# ---------------------------------------------------------------------------


class TestContinueOnFailure:
    def test_missing_bronze_partition_does_not_kill_batch(self, fresh_db, tmp_path):
        """
        Bronze for batch X never ran -> Silver for batch X should
        attempt every artifact, fail on the ones whose Bronze isn't
        there, and the layer ends up 'failed' but with continued
        execution recorded.
        """
        # No Bronze seeded, run Silver directly
        summary = run_silver(batch_id="never-ran-bronze-batch")
        assert summary.layer_status == "failed"
        # Every Bronze-dependent artifact failed (dim_date doesn't depend on Bronze)
        statuses = {r.artifact_name: r.status for r in summary.results}
        assert statuses["dim_date"] == "written"            # generated, no Bronze dep
        assert statuses["dim_clubs"] == "failed"
        assert statuses["dim_competitions"] == "failed"
        assert statuses["dim_players"] == "failed"
        assert statuses["fact_games"] == "failed"
        assert statuses["fact_appearances"] == "failed"


# ---------------------------------------------------------------------------
# Audit DAO integration
# ---------------------------------------------------------------------------


class TestAuditIntegration:
    def test_each_bronze_source_audit_row_transitions_to_transformed(
        self, bronze_seeded,
    ):
        """After Silver runs successfully, every file_audit row for a
        source consumed by Silver must be in 'transformed' state.

        Note: player_valuations is ingested to Bronze but NOT consumed
        by any Silver builder — Phase 5's Gold layer queries it directly
        from Bronze. Its audit row honestly stays at 'ingested'."""
        from src.metadata import audit
        from src.metadata.audit import FileStatus

        run_silver(batch_id="2024-12-01")

        rows = audit.list_batch_files(batch_id="2024-12-01")
        assert len(rows) == 6
        by_source = {r.source_name: r for r in rows}

        # The five sources Silver consumes
        silver_consumed = {"clubs", "competitions", "players", "games", "appearances"}
        for src in silver_consumed:
            assert by_source[src].status == FileStatus.TRANSFORMED, (
                f"{src} status={by_source[src].status.value}, expected transformed"
            )
        # player_valuations stays ingested (no Silver consumer)
        assert by_source["player_valuations"].status == FileStatus.INGESTED

    def test_silver_row_counts_recorded_in_audit(self, bronze_seeded):
        from src.metadata import audit
        run_silver(batch_id="2024-12-01")
        rows = audit.list_batch_files(batch_id="2024-12-01")
        by_source = {r.source_name: r for r in rows}
        # silver_row_count for each Silver-consumed source matches its
        # primary artifact's row count (ADR-0005 source-grain attribution)
        assert by_source["clubs"].silver_row_count == 5            # dim_clubs
        assert by_source["competitions"].silver_row_count == 3     # dim_competitions
        assert by_source["players"].silver_row_count == 12         # dim_players
        assert by_source["games"].silver_row_count == 6            # fact_games
        assert by_source["appearances"].silver_row_count == 30     # fact_appearances
        # player_valuations: never went through Silver, so silver_row_count stays None
        assert by_source["player_valuations"].silver_row_count is None


# ---------------------------------------------------------------------------
# Summary helpers
# ---------------------------------------------------------------------------


class TestSummaryProperties:
    def test_summary_total_rows_sums_correctly(self, bronze_seeded):
        summary = run_silver(batch_id="2024-12-01")
        expected = 5 + 3 + 12 + 6 + 30  # dim_date adds thousands
        assert summary.total_rows > expected

    def test_summary_failures_list_empty_on_clean_run(self, bronze_seeded):
        summary = run_silver(batch_id="2024-12-01")
        assert summary.failures == []
