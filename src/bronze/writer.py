"""
Bronze writer.

The function `write_bronze_source` ingests ONE source through the full
Bronze lifecycle and returns a typed result. The CLI runner (`run.py`)
iterates the registry and calls this once per source.

Bronze's responsibilities (and what it deliberately does NOT do):

  Bronze DOES:
    - Read the source via the file loader (Phase 2b slice 2)
    - Register the file with the audit DAO (Phase 2a)
    - Apply two layers of idempotency:
        1. file-grain: skip sources whose exact MD5 already succeeded
           in a prior batch (vendor re-send detection)
        2. layer-grain: covered by run.py / pipeline_runs at the batch level
    - Append a single bookkeeping column `batch_id` to the DataFrame
    - Write to partitioned Parquet, Hive-style:
        {paths.bronze}/{source_name}/batch_id={batch_id}/part-0.parquet
    - Record success or failure in BOTH file_audit (per file) and
      pipeline_runs (per layer, handled by run.py)

  Bronze DOES NOT:
    - Clean, normalise, deduplicate, type-coerce beyond what the loader
      already did against the declared schema
    - Apply business rules (Silver's job)
    - Run DQ checks (the dedicated DQ task does that against Bronze)
    - Compute row counts beyond what the loader returned

Why partition by batch_id

  Re-running a batch atomically replaces just that batch's partition.
  Older partitions stay untouched, so the lake retains the full history
  of every successful ingestion. Time-travel queries on Bronze become
  a WHERE clause: `SELECT * FROM bronze.players WHERE batch_id = '...'`.

  We deliberately do NOT prefix the column with an underscore. Pyarrow,
  Spark, and Hive all treat underscore-prefixed paths as hidden (the
  `_SUCCESS` marker convention), which silently breaks partition
  discovery — verified with pyarrow during Phase 2b development.

Why no per-source schema hash check in Bronze

  Schema drift detection lives in the audit DAO (via
  record_schema_drift in Phase 2a). Bronze's job is to land the data
  faithfully; the DQ task in Phase 4 will decide what to do about drift.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from src.engines.base import DataFrameEngine
from src.ingestion.file_loader import FileLoaderError, load_source
from src.ingestion.registry import SourceDefinition
from src.metadata import audit
from src.utils.logging import bind_batch_context, get_logger

log = get_logger(__name__)


# The bookkeeping column appended by Bronze. NOTE: no underscore prefix.
# Hive, Spark, and pyarrow all treat underscore-prefixed paths/files as
# hidden by default (used for _SUCCESS markers etc.). Using `_batch_id`
# would silently break partition discovery — verified with pyarrow during
# Phase 2b development. The plain name is the standard convention used
# in production lakehouses (Delta, Iceberg, Hive).
BATCH_ID_COLUMN = "batch_id"


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BronzeWriteResult:
    """
    Outcome of processing one source in one batch. Returned to the CLI
    runner so it can produce a per-source summary at the end of a run.

    Status values:
      "written"  — fresh data successfully landed in the partition
      "skipped"  — identical checksum already ingested in a prior
                   batch; we registered the file in audit and short-
                   circuited the write
      "failed"   — load or write raised; audit row marked failed,
                   error captured. Runner continues with next source.
    """

    source_name: str
    status: str  # 'written' | 'skipped' | 'failed'
    rows_written: int  # 0 on skipped/failed
    output_path: Path | None  # None on skipped/failed
    skip_reason: str | None = None
    error_message: str | None = None


# ---------------------------------------------------------------------------
# Per-source orchestrator
# ---------------------------------------------------------------------------


def write_bronze_source(
    *,
    source: SourceDefinition,
    raw_root: Path | str,
    bronze_root: Path | str,
    batch_id: str,
    engine: DataFrameEngine,
) -> BronzeWriteResult:
    """
    Ingest one source through the full Bronze lifecycle.

    Never raises. Failures are captured in BronzeWriteResult.status
    and in the audit DAO; the caller (run.py) decides whether to halt
    or continue. This asymmetry — never-raise — is deliberate: a single
    source's failure must not abort the whole batch.

    Args:
        source:        SourceDefinition from the registry
        raw_root:      where the source files live (e.g. data/sample/)
        bronze_root:   the bronze layer root (e.g. data/lake/bronze/)
        batch_id:      string identifier for this run; partition key
        engine:        DataFrameEngine instance from the factory
    """
    bronze_root = Path(bronze_root)
    output_dir = bronze_root / source.name

    # Bind logging context for this source's whole lifecycle. Every log
    # line emitted below carries batch_id, layer=bronze, and source_name.
    with bind_batch_context(
        batch_id=batch_id,
        layer="bronze",
        source_name=source.name,
    ):
        # === Phase 1: load + fingerprint ================================
        try:
            load_result = load_source(
                source=source,
                raw_root=raw_root,
                engine=engine,
            )
        except FileLoaderError as e:
            # File doesn't exist or format unsupported — can't even
            # register an audit row because we have no fingerprint.
            # Log clearly and return failure for the runner to record.
            log.error("bronze_load_failed", error=str(e))
            return BronzeWriteResult(
                source_name=source.name,
                status="failed",
                rows_written=0,
                output_path=None,
                error_message=str(e),
            )

        fingerprint = load_result.fingerprint
        path_str = load_result.source_file_path

        # === Phase 2: register in audit =================================
        # register_file is idempotent on identical fingerprint; safe to
        # call on every run. A conflicting checksum (vendor sent a
        # corrected file mid-batch) raises AuditConflictError — we
        # treat that as a hard failure for this source.
        try:
            audit.register_file(
                batch_id=batch_id,
                source_name=source.name,
                fingerprint=fingerprint,
            )
        except audit.AuditConflictError as e:
            log.error("bronze_audit_conflict", error=str(e))
            return BronzeWriteResult(
                source_name=source.name,
                status="failed",
                rows_written=0,
                output_path=None,
                error_message=str(e),
            )

        # === Phase 3: file-grain idempotency check ======================
        # Has this exact file (by MD5) ever successfully ingested?
        # If so, we register this batch's row and short-circuit the
        # write — the prior partition's data is still on disk and
        # downstream stages don't need a fresh write.
        prior = audit.find_previous_successful_ingestion(
            checksum_md5=fingerprint.checksum_md5,
        )
        if prior is not None and prior.batch_id != batch_id:
            log.info(
                "bronze_skipped_duplicate_checksum",
                prior_batch_id=prior.batch_id,
                checksum_md5=fingerprint.checksum_md5,
            )
            # We still mark this batch's audit row through the
            # lifecycle so the timeline reflects what happened.
            try:
                audit.mark_ingesting(
                    batch_id=batch_id,
                    source_file_path=path_str,
                )
                audit.record_ingestion_complete(
                    batch_id=batch_id,
                    source_file_path=path_str,
                    source_row_count=load_result.source_row_count,
                    bronze_row_count=prior.bronze_row_count or 0,
                )
            except Exception as e:
                # Audit transition failed; promote to failed status.
                audit.mark_failed(
                    batch_id=batch_id,
                    source_file_path=path_str,
                    stage="bronze",
                    error_message=str(e),
                )
                return BronzeWriteResult(
                    source_name=source.name,
                    status="failed",
                    rows_written=0,
                    output_path=None,
                    error_message=str(e),
                )
            return BronzeWriteResult(
                source_name=source.name,
                status="skipped",
                rows_written=0,
                output_path=None,
                skip_reason=(f"identical checksum already ingested in batch {prior.batch_id}"),
            )

        # === Phase 4: ingest -> write -> finalise ======================
        try:
            audit.mark_ingesting(
                batch_id=batch_id,
                source_file_path=path_str,
            )

            # Add the partition key column. The engine handles this
            # for both Pandas and (future) Spark; this is one of the
            # few rare operations that's idempotent — re-adding a
            # constant column yields the same DataFrame.
            df_with_batch = engine.with_constant_column(
                load_result.dataframe,
                name=BATCH_ID_COLUMN,
                value=batch_id,
            )

            # Write Hive-partitioned. mode='overwrite' replaces the
            # partition cleanly on re-runs of the same batch.
            engine.write_parquet(
                df_with_batch,
                output_dir,
                partition_by=[BATCH_ID_COLUMN],
                mode="overwrite",
            )

            # Bronze rows == source rows (no transformations).
            bronze_row_count = load_result.source_row_count

            audit.record_ingestion_complete(
                batch_id=batch_id,
                source_file_path=path_str,
                source_row_count=load_result.source_row_count,
                bronze_row_count=bronze_row_count,
            )

            log.info(
                "bronze_written",
                rows=bronze_row_count,
                output_path=str(output_dir),
            )

            return BronzeWriteResult(
                source_name=source.name,
                status="written",
                rows_written=bronze_row_count,
                output_path=output_dir,
            )

        except Exception as e:
            # Anything beyond mark_ingesting — record the failure
            # against the audit row and the partition we attempted.
            audit.mark_failed(
                batch_id=batch_id,
                source_file_path=path_str,
                stage="bronze",
                error_message=str(e),
            )
            log.error("bronze_write_failed", error=str(e))
            return BronzeWriteResult(
                source_name=source.name,
                status="failed",
                rows_written=0,
                output_path=None,
                error_message=str(e),
            )
