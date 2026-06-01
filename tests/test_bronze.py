"""
Tests for the Bronze layer (writer + runner).

Two layers of coverage:

  Unit tests for write_bronze_source: synthetic SourceDefinition +
  tmp CSV. Cover happy path, file-grain idempotency (duplicate MD5),
  missing-file failure, audit lifecycle correctness, partition layout
  on disk.

  Integration tests for run_bronze: spin up against the committed
  data/sample/ CSVs in a tmp DATA_ROOT. Cover happy run (all 6
  sources processed), layer-grain idempotency (re-run is no-op),
  continue-on-failure (one missing source doesn't kill the batch),
  and partition file existence on disk.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from src.bronze.run import BronzeRunSummary, run_bronze
from src.bronze.writer import (
    BATCH_ID_COLUMN,
    BronzeWriteResult,
    write_bronze_source,
)
from src.engines.pandas_engine import PandasEngine
from src.ingestion.registry import SourceDefinition
from src.metadata import audit
from src.metadata.audit import FileStatus
from src.metadata.db import init_db


# ---------------------------------------------------------------------------
# Helpers — small synthetic source + tmp CSV
# ---------------------------------------------------------------------------


def _minimal_source(name: str = "widgets") -> SourceDefinition:
    return SourceDefinition(
        name=name,
        description=f"test source: {name}",
        format="csv",
        path_pattern="{raw_root}/" + name + ".csv",
        primary_key=["id"],
        schema={"id": "int", "label": "string", "price": "float"},
    )


def _write_csv(path: Path, header: str, rows: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(header + "\n" + "\n".join(rows) + "\n", encoding="utf-8")


@pytest.fixture
def engine():
    return PandasEngine()


@pytest.fixture
def fresh_db():
    """Apply schema to the per-test DB (conftest already isolates it)."""
    init_db()


@pytest.fixture
def raw_root_with_widgets(tmp_path):
    """A raw_root containing a single 3-row widgets.csv."""
    root = tmp_path / "raw"
    _write_csv(
        root / "widgets.csv",
        "id,label,price",
        ["1,red,9.99", "2,green,12.50", "3,blue,7.25"],
    )
    return root


# ---------------------------------------------------------------------------
# write_bronze_source — unit tests
# ---------------------------------------------------------------------------


class TestWriteBronzeSource:
    def test_happy_path_returns_written_result(
        self, tmp_path, raw_root_with_widgets, engine, fresh_db,
    ):
        result = write_bronze_source(
            source=_minimal_source(),
            raw_root=raw_root_with_widgets,
            bronze_root=tmp_path / "bronze",
            batch_id="B1",
            engine=engine,
        )
        assert isinstance(result, BronzeWriteResult)
        assert result.status == "written"
        assert result.rows_written == 3
        assert result.error_message is None

    def test_happy_path_creates_hive_partition(
        self, tmp_path, raw_root_with_widgets, engine, fresh_db,
    ):
        bronze_root = tmp_path / "bronze"
        write_bronze_source(
            source=_minimal_source(),
            raw_root=raw_root_with_widgets,
            bronze_root=bronze_root,
            batch_id="B1",
            engine=engine,
        )
        # Hive-style: widgets/_batch_id=B1/*.parquet
        partition_dir = bronze_root / "widgets" / f"{BATCH_ID_COLUMN}=B1"
        assert partition_dir.is_dir()
        parquet_files = list(partition_dir.glob("*.parquet"))
        assert len(parquet_files) >= 1

    def test_written_parquet_contains_batch_id_column(
        self, tmp_path, raw_root_with_widgets, engine, fresh_db,
    ):
        bronze_root = tmp_path / "bronze"
        write_bronze_source(
            source=_minimal_source(),
            raw_root=raw_root_with_widgets,
            bronze_root=bronze_root,
            batch_id="B1",
            engine=engine,
        )
        # Read back the partition — _batch_id column should be present
        # (pyarrow restores it from the directory name automatically).
        reloaded = engine.read_parquet(bronze_root / "widgets")
        cols = engine.columns(reloaded)
        assert BATCH_ID_COLUMN in cols

    def test_audit_row_transitions_to_ingested(
        self, tmp_path, raw_root_with_widgets, engine, fresh_db,
    ):
        write_bronze_source(
            source=_minimal_source(),
            raw_root=raw_root_with_widgets,
            bronze_root=tmp_path / "bronze",
            batch_id="B1",
            engine=engine,
        )
        rows = audit.list_batch_files(batch_id="B1")
        assert len(rows) == 1
        assert rows[0].status == FileStatus.INGESTED
        assert rows[0].source_row_count == 3
        assert rows[0].bronze_row_count == 3

    def test_missing_file_returns_failed_result(
        self, tmp_path, engine, fresh_db,
    ):
        # raw_root exists but widgets.csv is NOT inside it
        empty_root = tmp_path / "empty"
        empty_root.mkdir()
        result = write_bronze_source(
            source=_minimal_source(),
            raw_root=empty_root,
            bronze_root=tmp_path / "bronze",
            batch_id="B1",
            engine=engine,
        )
        assert result.status == "failed"
        assert result.error_message is not None
        assert "not found" in result.error_message

    def test_file_grain_idempotency_skips_in_new_batch(
        self, tmp_path, raw_root_with_widgets, engine, fresh_db,
    ):
        # Ingest under B1, then attempt B2 with the same file content.
        write_bronze_source(
            source=_minimal_source(),
            raw_root=raw_root_with_widgets,
            bronze_root=tmp_path / "bronze",
            batch_id="B1",
            engine=engine,
        )
        result_b2 = write_bronze_source(
            source=_minimal_source(),
            raw_root=raw_root_with_widgets,
            bronze_root=tmp_path / "bronze",
            batch_id="B2",
            engine=engine,
        )
        assert result_b2.status == "skipped"
        assert result_b2.skip_reason is not None
        assert "B1" in result_b2.skip_reason

    def test_file_grain_idempotency_still_records_audit_lifecycle(
        self, tmp_path, raw_root_with_widgets, engine, fresh_db,
    ):
        """When a skip fires, the audit row for THIS batch must still
        reach 'ingested' status — the timeline must be honest about
        what happened, even if no new bytes were written."""
        write_bronze_source(
            source=_minimal_source(),
            raw_root=raw_root_with_widgets,
            bronze_root=tmp_path / "bronze",
            batch_id="B1",
            engine=engine,
        )
        write_bronze_source(
            source=_minimal_source(),
            raw_root=raw_root_with_widgets,
            bronze_root=tmp_path / "bronze",
            batch_id="B2",
            engine=engine,
        )
        rows_b2 = audit.list_batch_files(batch_id="B2")
        assert len(rows_b2) == 1
        assert rows_b2[0].status == FileStatus.INGESTED

    def test_changed_file_in_new_batch_writes_normally(
        self, tmp_path, raw_root_with_widgets, engine, fresh_db,
    ):
        # First batch ingests
        write_bronze_source(
            source=_minimal_source(),
            raw_root=raw_root_with_widgets,
            bronze_root=tmp_path / "bronze",
            batch_id="B1",
            engine=engine,
        )
        # Now MUTATE the source file — different bytes = different MD5
        # = file-grain idempotency does NOT fire.
        _write_csv(
            raw_root_with_widgets / "widgets.csv",
            "id,label,price",
            ["1,red,9.99", "2,green,12.50", "3,blue,7.25", "4,yellow,3.50"],
        )
        result_b2 = write_bronze_source(
            source=_minimal_source(),
            raw_root=raw_root_with_widgets,
            bronze_root=tmp_path / "bronze",
            batch_id="B2",
            engine=engine,
        )
        assert result_b2.status == "written"
        assert result_b2.rows_written == 4

    def test_never_raises_on_failure(
        self, tmp_path, engine, fresh_db,
    ):
        """The contract: write_bronze_source NEVER raises. A bad source
        is captured in BronzeWriteResult.status='failed', not via
        exception propagation."""
        # Try to ingest from a path that doesn't exist at all
        result = write_bronze_source(
            source=_minimal_source(),
            raw_root="/nonexistent/path/that/cannot/exist/x9q3p",
            bronze_root=tmp_path / "bronze",
            batch_id="B1",
            engine=engine,
        )
        assert result.status == "failed"


# ---------------------------------------------------------------------------
# run_bronze — integration tests against committed samples
# ---------------------------------------------------------------------------


SAMPLES_DIR = Path(__file__).resolve().parents[1] / "data" / "sample"


class TestRunBronzeAgainstSamples:
    """End-to-end tests using the real committed sample CSVs. These
    prove the full lifecycle works for our actual six sources, not
    just synthetic ones."""

    def test_full_run_writes_all_six_sources(self, fresh_db):
        # conftest._isolate_metadata_db points DATA_ROOT at tmp_path,
        # so paths.bronze and paths.metadata_db are isolated already.
        summary = run_bronze(batch_id="B1", raw_root=SAMPLES_DIR)
        assert isinstance(summary, BronzeRunSummary)
        assert summary.layer_status == "success"
        assert summary.skipped_layer is False
        assert len(summary.results) == 6
        assert all(r.status == "written" for r in summary.results)

    def test_full_run_writes_expected_row_counts(self, fresh_db):
        summary = run_bronze(batch_id="B1", raw_root=SAMPLES_DIR)
        rows_by_source = {r.source_name: r.rows_written for r in summary.results}
        # Pinned counts that match the committed samples. If the
        # generator drifts, these fail loudly here too.
        assert rows_by_source == {
            "competitions": 3,
            "clubs": 5,
            "players": 12,
            "games": 6,
            "appearances": 30,
            "player_valuations": 18,
        }

    def test_full_run_creates_partitions_on_disk(self, fresh_db):
        from src.utils.config import get_config
        summary = run_bronze(batch_id="B1", raw_root=SAMPLES_DIR)
        assert summary.layer_status == "success"
        bronze_root = get_config().paths.bronze
        for source_name in ["competitions", "clubs", "players",
                            "games", "appearances", "player_valuations"]:
            partition_dir = bronze_root / source_name / f"{BATCH_ID_COLUMN}=B1"
            assert partition_dir.is_dir(), (
                f"Missing partition directory: {partition_dir}"
            )
            files = list(partition_dir.glob("*.parquet"))
            assert files, f"No parquet files in {partition_dir}"

    def test_layer_idempotency_skips_repeat_batch(self, fresh_db):
        """Re-running the same batch_id must be a complete no-op."""
        first = run_bronze(batch_id="B1", raw_root=SAMPLES_DIR)
        assert first.layer_status == "success"
        second = run_bronze(batch_id="B1", raw_root=SAMPLES_DIR)
        assert second.layer_status == "skipped"
        assert second.skipped_layer is True
        assert len(second.results) == 0

    def test_file_idempotency_skips_unchanged_sources_in_new_batch(self, fresh_db):
        """A fresh batch_id with unchanged files must skip ALL sources
        at file-grain (different mechanism than layer-grain)."""
        run_bronze(batch_id="B1", raw_root=SAMPLES_DIR)
        second = run_bronze(batch_id="B2", raw_root=SAMPLES_DIR)
        assert second.layer_status == "success"
        assert second.skipped_layer is False
        # Every source individually skipped — none re-written
        statuses = {r.status for r in second.results}
        assert statuses == {"skipped"}
        assert all("B1" in (r.skip_reason or "") for r in second.results)


class TestRunBronzeFailureModes:
    """Continue-on-failure: a missing source doesn't kill the batch."""

    def test_missing_source_does_not_kill_batch(self, tmp_path, fresh_db):
        # Build a raw_root that has only SOME of the six sources.
        # players.csv exists; the others don't.
        partial_root = tmp_path / "partial"
        partial_root.mkdir()
        (partial_root / "players.csv").write_text(
            (SAMPLES_DIR / "players.csv").read_text()
        )

        summary = run_bronze(batch_id="B1", raw_root=partial_root)

        # players succeeded; the others failed; the batch is overall 'failed'
        # because at least one source failed, but it RAN to completion.
        assert summary.layer_status == "failed"
        assert len(summary.results) == 6
        by_status: dict[str, int] = {}
        for r in summary.results:
            by_status[r.status] = by_status.get(r.status, 0) + 1
        assert by_status.get("written", 0) == 1
        assert by_status.get("failed", 0) == 5

    def test_failed_sources_get_audit_marked_failed(self, tmp_path, fresh_db):
        partial_root = tmp_path / "partial"
        partial_root.mkdir()
        # No files at all
        run_bronze(batch_id="B1", raw_root=partial_root)
        # NB: file_audit rows are only created when register_file
        # is called, which happens AFTER load_source — so a load
        # failure leaves no audit row at all. That's correct: the
        # error is captured in pipeline_runs and in the runner's
        # BronzeRunSummary, not in file_audit.
        from src.metadata import runs
        run_row = runs.get_run("B1", "bronze")
        assert run_row is not None
        assert run_row["status"] == "failed"


# ---------------------------------------------------------------------------
# Helpful properties of the public types
# ---------------------------------------------------------------------------


class TestSummaryProperties:
    def test_summary_total_rows_sums_correctly(self, fresh_db):
        summary = run_bronze(batch_id="B1", raw_root=SAMPLES_DIR)
        assert summary.total_rows == 3 + 5 + 12 + 6 + 30 + 18

    def test_summary_failures_list_empty_on_clean_run(self, fresh_db):
        summary = run_bronze(batch_id="B1", raw_root=SAMPLES_DIR)
        assert summary.failures == []
