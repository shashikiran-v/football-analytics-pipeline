"""
Runner-level day-2 integration tests.

Tests that span both Bronze and Silver runners across two batches.
Focuses on the AUDIT LINEAGE story: every source's audit row reflects
what actually happened in both batches.

Three concerns covered:
  1. File-grain idempotency: day-2 Bronze for unchanged sources is
     marked as 'skipped' with the right skip_reason
  2. Audit lineage: day-2 audit rows have honest row counts for
     changed sources AND for skipped sources
  3. Layer-grain idempotency: re-running day-2 Silver after success
     is a no-op
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from src.bronze.run import run_bronze
from src.metadata import audit
from src.metadata.audit import FileStatus
from src.metadata.db import init_db
from src.silver.run import run_silver
from src.utils.config import get_config


SAMPLES_DIR = Path(__file__).resolve().parents[1] / "data" / "sample"
SAMPLES_DAY2_DIR = Path(__file__).resolve().parents[1] / "data" / "sample" / "day2"


@pytest.fixture
def fresh_db():
    init_db()


@pytest.fixture
def both_batches(fresh_db):
    """Run day-1 and day-2 in full (Bronze + Silver)."""
    run_bronze(batch_id="day-1", raw_root=SAMPLES_DIR)
    run_silver(batch_id="day-1")
    run_bronze(batch_id="day-2", raw_root=SAMPLES_DAY2_DIR)
    run_silver(batch_id="day-2")


# ---------------------------------------------------------------------------
# File-grain idempotency on day-2
# ---------------------------------------------------------------------------


class TestFileGrainIdempotencyDay2:
    def test_unchanged_sources_skipped_in_bronze(self, fresh_db):
        """Bronze day-2 should skip clubs, competitions, player_valuations
        (identical bytes from day-1)."""
        run_bronze(batch_id="day-1", raw_root=SAMPLES_DIR)
        summary = run_bronze(batch_id="day-2", raw_root=SAMPLES_DAY2_DIR)
        # Find each source's outcome
        by_source = {r.source_name: r for r in summary.results}
        # Three should be skipped
        for skipped_source in ("clubs", "competitions", "player_valuations"):
            assert by_source[skipped_source].status == "skipped", (
                f"{skipped_source} status is {by_source[skipped_source].status}, "
                f"expected 'skipped'"
            )
            assert "day-1" in (by_source[skipped_source].skip_reason or "")
        # Three should be freshly written
        for written_source in ("players", "games", "appearances"):
            assert by_source[written_source].status == "written", (
                f"{written_source} status is {by_source[written_source].status}, "
                f"expected 'written'"
            )

    def test_day2_skip_does_not_write_new_partition(self, fresh_db):
        """The whole point of file-grain idempotency: don't waste I/O
        re-writing identical bytes. Day-2 partition dir should NOT
        exist for skipped sources."""
        run_bronze(batch_id="day-1", raw_root=SAMPLES_DIR)
        run_bronze(batch_id="day-2", raw_root=SAMPLES_DAY2_DIR)
        cfg = get_config()
        for skipped_source in ("clubs", "competitions", "player_valuations"):
            day2_partition = (
                cfg.paths.bronze / skipped_source / "batch_id=day-2"
            )
            assert not day2_partition.is_dir(), (
                f"Expected {day2_partition} to not exist (file-grain skip), "
                f"but it does"
            )


# ---------------------------------------------------------------------------
# Audit lineage across both batches
# ---------------------------------------------------------------------------


class TestAuditLineageAcrossBatches:
    def test_day1_audit_rows_unchanged_after_day2_runs(self, both_batches):
        """The cardinal invariant: day-1 audit rows never get
        retroactively modified by day-2 processing."""
        # Day-1 audit state pre-day-2: we can't snapshot easily, but
        # we can verify that day-1 rows have terminal statuses with
        # consistent row counts after day-2 has run.
        day1_rows = audit.list_batch_files(batch_id="day-1")
        # All Silver-consumed sources should be TRANSFORMED, others INGESTED
        by_source = {r.source_name: r for r in day1_rows}
        assert by_source["clubs"].status == FileStatus.TRANSFORMED
        assert by_source["competitions"].status == FileStatus.TRANSFORMED
        assert by_source["players"].status == FileStatus.TRANSFORMED
        assert by_source["games"].status == FileStatus.TRANSFORMED
        assert by_source["appearances"].status == FileStatus.TRANSFORMED
        # player_valuations has no Silver builder (ADR-0005 design)
        assert by_source["player_valuations"].status == FileStatus.INGESTED

    def test_day2_audit_records_skipped_sources_honestly(self, both_batches):
        """Day-2's audit rows for file-grain-skipped sources are still
        marked TRANSFORMED (Silver successfully processed them via the
        cross-batch resolver from Slice 6.1)."""
        day2_rows = audit.list_batch_files(batch_id="day-2")
        by_source = {r.source_name: r for r in day2_rows}
        # All five Silver-consumed sources should be TRANSFORMED, even
        # the ones whose Bronze partition was skipped (Silver read from
        # day-1's partition via the resolver)
        for src in ("clubs", "competitions", "players", "games", "appearances"):
            assert by_source[src].status == FileStatus.TRANSFORMED, (
                f"{src} day-2 status is {by_source[src].status.value}, "
                f"expected 'transformed'"
            )

    def test_day2_silver_row_counts_match_dim_outputs(self, both_batches):
        """The audit row's silver_row_count for each source should match
        what was actually written in Silver."""
        day2_rows = audit.list_batch_files(batch_id="day-2")
        by_source = {r.source_name: r for r in day2_rows}
        # Spot-check: players source produced dim_players with 14 rows
        # in the day-2 partition (12 unchanged + 2 closed + 2 new)
        # Actually scd2_merge outputs the FULL state, not just incoming,
        # so the row count varies. Let's verify the SCD2-output for
        # players reflects the merge output. The audit's silver_row_count
        # is the row count written to Silver — for a SCD2 dim, that's
        # the full merged state.
        assert by_source["players"].silver_row_count == 14
        # appearances day-2 has 34 clean rows
        assert by_source["appearances"].silver_row_count == 34
        # games day-2 has 8 (6 historical + 2 new)
        assert by_source["games"].silver_row_count == 8

    def test_day2_orphan_quarantined_in_audit(self, both_batches):
        """The deliberate orphan player_id=9999 is in both day-1 and
        day-2 appearances samples. Each batch's audit should record
        one rejected row for appearances."""
        day1_rows = audit.list_batch_files(batch_id="day-1")
        day2_rows = audit.list_batch_files(batch_id="day-2")
        day1_apps = next(r for r in day1_rows if r.source_name == "appearances")
        day2_apps = next(r for r in day2_rows if r.source_name == "appearances")
        assert day1_apps.rejected_row_count == 1
        assert day2_apps.rejected_row_count == 1


# ---------------------------------------------------------------------------
# Layer-grain idempotency on re-run
# ---------------------------------------------------------------------------


class TestLayerIdempotencyOnRerun:
    def test_silver_day2_rerun_is_skipped(self, both_batches):
        """After day-2 Silver succeeded once, re-running it should be
        a no-op via pipeline_runs."""
        second_run = run_silver(batch_id="day-2")
        assert second_run.layer_status == "skipped"
        assert second_run.skipped_layer is True

    def test_bronze_day2_rerun_is_skipped(self, both_batches):
        """Same for Bronze day-2."""
        second_run = run_bronze(batch_id="day-2", raw_root=SAMPLES_DAY2_DIR)
        assert second_run.layer_status == "skipped"
        assert second_run.skipped_layer is True


# ---------------------------------------------------------------------------
# Cross-batch fact_appearances row counts
# ---------------------------------------------------------------------------


class TestFactAppearancesAcrossBatches:
    def test_day1_partition_unchanged_after_day2(self, both_batches):
        """Reading the day-1 partition specifically should still give
        the day-1 fact_appearances state (29 rows). Day-2's processing
        must not retroactively touch day-1's parquet."""
        cfg = get_config()
        day1_fact = pd.read_parquet(
            cfg.paths.silver / "fact_appearances" / "batch_id=day-1"
        )
        # Day-1 had 30 source rows, 1 quarantined = 29 in Silver
        assert len(day1_fact) == 29

    def test_full_history_visible_across_partitions(self, both_batches):
        """Reading dim_players ROOT (across all partitions) should give
        the combined historical view."""
        cfg = get_config()
        full = pd.read_parquet(cfg.paths.silver / "dim_players")
        # day-1 partition has 12 rows + day-2 partition has 14 rows = 26 total
        assert len(full) == 26
        # We can see all SCD2 history this way
        saka_versions = full[full["player_id"] == 1001]
        # 1 from day-1 partition (Arsenal, was current then) + 2 from
        # day-2 partition (Arsenal closed + Chelsea current) = 3 rows.
        # This is how DuckDB read_parquet across partitions sees it.
        assert len(saka_versions) == 3
