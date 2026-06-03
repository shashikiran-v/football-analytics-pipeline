"""
Silver CLI runner.

Orchestrates the Bronze -> Silver transformation for one batch:

  1. Reads each Bronze source partition for the given batch_id
  2. Builds the four dimensions (clubs, competitions, date, players)
  3. Builds the two facts (games, appearances)
  4. Writes each artifact to Silver partitions
  5. Drives the audit DAO through mark_transforming -> record_silver_complete

Continue-on-failure semantics
-----------------------------
Same shape as the Bronze runner: one bad source/build does NOT kill
the batch. The runner aggregates per-artifact results; failures end
up in audit.mark_failed and pipeline_runs status; successful artifacts
still land on disk.

Layer-grain idempotency
-----------------------
A re-run of a fully-successful (batch_id, 'silver') is a no-op via
pipeline_runs, mirroring Bronze. Re-runs of partial-failure batches
proceed normally so the previously-failed sources can be retried.

Source-grain audit attribution
------------------------------
Facts attribute to their PRIMARY Bronze source (ADR-0005, slice 5):
  fact_games        -> games audit row
  fact_appearances  -> appearances audit row
Dims update their own audit rows for clubs, competitions, players.
dim_date is generated and has no Bronze source — it skips audit.

CLI usage
---------
  python -m src.silver.run --batch-id 2026-06-01T15
  python -m src.silver.run                          # auto batch_id from now

Importable as run_silver() for tests and (later) Airflow.
"""

from __future__ import annotations

import argparse
import sys
import traceback
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path

from src.engines.base import DataFrame, DataFrameEngine
from src.engines.factory import get_engine
from src.ingestion.registry import get_registry
from src.metadata import audit, runs
from src.metadata.db import init_db
from src.bronze.resolver import resolve_bronze_partition
from src.dq.quarantine import quarantine_rejected_rows
from src.dq.report import build_batch_report, build_source_report, write_report
from src.dq.runner import DQResult, build_fk_lookups, run_dq_for_source
from src.silver.dimensions import (
    build_dim_clubs,
    build_dim_competitions,
    build_dim_date,
    build_dim_players,
)
from src.silver.facts import build_fact_appearances, build_fact_games
from src.utils.config import get_config
from src.utils.logging import bind_batch_context, configure_logging, get_logger


log = get_logger(__name__)


# Date-dimension range. We deliberately cover several years either side
# of "now" so any reasonable match date will have a dim_date row.
_DIM_DATE_START = date(2018, 1, 1)
_DIM_DATE_END = date(2030, 12, 31)


# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SilverBuildResult:
    """Per-artifact outcome of a Silver build."""

    artifact_name: str               # 'dim_clubs' | 'fact_appearances' | ...
    status: str                      # 'written' | 'failed'
    rows_written: int
    output_path: Path | None
    # The Bronze source this artifact attributes to (for audit). None for
    # dim_date (which has no Bronze source).
    audit_source_name: str | None = None
    error_message: str | None = None


@dataclass(frozen=True)
class SilverRunSummary:
    """Aggregate outcome of a Silver run across all artifacts."""

    batch_id: str
    layer_status: str                # 'success' | 'failed' | 'skipped'
    results: list[SilverBuildResult]
    skipped_layer: bool = False
    dq_report_path: Path | None = None    # JSON report location, when DQ ran

    @property
    def total_rows(self) -> int:
        return sum(r.rows_written for r in self.results)

    @property
    def failures(self) -> list[SilverBuildResult]:
        return [r for r in self.results if r.status == "failed"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _bronze_partition_path(bronze_root: Path, source_name: str, batch_id: str) -> Path:
    """Return the Hive partition path for a given Bronze source + batch."""
    return bronze_root / source_name / f"batch_id={batch_id}"


def _derive_batch_timestamp(batch_id: str) -> str:
    """
    Derive an ISO-comparable batch_timestamp for SCD2 effective_date.

    SCD2 effective_date and end_date are stored as ISO strings (because
    string comparison preserves chronological order with the right
    format). Fact -> dim as-of joins compare these strings to match
    dates. So whatever we put in here must be a valid ISO date or
    datetime — NOT an arbitrary partition key like "smoke-1".

    Rules:
      - If batch_id parses as ISO date or datetime, use it as-is.
      - Otherwise, fall back to UTC now. This is the development /
        smoke-test path — production batch_ids derive from UTC now
        anyway, so the timestamps will be consistent.
    """
    # Try parsing as ISO datetime ('2026-06-01T15:30:00', '2026-06-01T15', '2026-06-01')
    candidate = batch_id.replace("T", " ").strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d %H", "%Y-%m-%d"):
        try:
            parsed = datetime.strptime(candidate, fmt)
            # Re-serialise to a canonical ISO date string (we use date-
            # granularity for effective_date by convention — matches
            # how scd2_merge's FAR_FUTURE_DATE is also a date).
            return parsed.strftime("%Y-%m-%d")
        except ValueError:
            continue
    # Fallback: UTC now (date-granularity).
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _read_bronze(
    *,
    bronze_root: Path,
    source_name: str,
    batch_id: str,
    engine: DataFrameEngine,
) -> DataFrame:
    """
    Read a Bronze partition for the given source and batch.

    Uses the cross-batch resolver — if the current-batch partition
    doesn't exist on disk (because file-grain idempotency skipped
    re-writing identical bytes; see ADR-0008), falls back to the
    most-recent prior batch's partition where this source was
    successfully ingested.

    Raises FileNotFoundError if NEITHER current nor any prior batch's
    partition exists — meaning the source was never successfully
    ingested at-or-before this batch. The runner catches this and
    records a per-artifact failure.
    """
    path = resolve_bronze_partition(
        bronze_root=bronze_root, source_name=source_name, batch_id=batch_id,
    )
    if path is None:
        raise FileNotFoundError(
            f"No Bronze partition resolvable for source={source_name!r}, "
            f"batch_id={batch_id!r}. Either run `python -m src.bronze.run` "
            f"for batch_id={batch_id}, or ingest this source in an earlier "
            f"batch (file-grain idempotency will then make it available "
            f"to subsequent Silver runs)."
        )
    return engine.read_parquet(path)


def _read_existing_dim_players(
    *,
    silver_root: Path,
    engine: DataFrameEngine,
) -> DataFrame | None:
    """
    Read the current full state of dim_players across ALL batch partitions.

    SCD2 dimensions need the full history loaded so the merge can see
    closed-out versions; reading only the latest partition would drop
    everything before this batch and silently corrupt the dim.

    Returns None on first ever run (dim_players directory doesn't exist).
    """
    dim_root = silver_root / "dim_players"
    if not dim_root.is_dir():
        return None
    # Verify there's at least one partition.
    partitions = list(dim_root.glob("batch_id=*"))
    if not partitions:
        return None
    # Hive-aware reader: pyarrow picks up the partition column when
    # given the root.
    return engine.read_parquet(dim_root)


def _write_silver_artifact(
    *,
    df: DataFrame,
    silver_root: Path,
    artifact_name: str,
    batch_id: str,
    engine: DataFrameEngine,
) -> Path:
    """Write one Silver artifact as a Hive-partitioned parquet by batch_id."""
    df_with_batch = engine.with_constant_column(df, "batch_id", batch_id)
    output_dir = silver_root / artifact_name
    engine.write_parquet(
        df_with_batch,
        output_dir,
        partition_by=["batch_id"],
        mode="overwrite",
    )
    return output_dir


def _format_summary(summary: SilverRunSummary) -> str:
    """Human-readable summary printed by the CLI."""
    lines: list[str] = []
    lines.append(f"Silver run summary — batch_id={summary.batch_id}")
    lines.append(f"  status: {summary.layer_status}")
    if summary.skipped_layer:
        lines.append("  (whole layer skipped — already succeeded for this batch)")
        return "\n".join(lines)
    lines.append(f"  total rows: {summary.total_rows}")
    if summary.dq_report_path is not None:
        lines.append(f"  dq report:  {summary.dq_report_path}")
    lines.append("  per artifact:")
    width = max((len(r.artifact_name) for r in summary.results), default=12)
    for r in summary.results:
        if r.status == "written":
            detail = f"rows={r.rows_written}"
        else:
            detail = f"error={r.error_message}"
        lines.append(
            f"    {r.artifact_name:<{width}}  {r.status:<8}  {detail}"
        )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Per-artifact build wrappers — each catches its own failure
# ---------------------------------------------------------------------------


def _build_artifact_safe(
    *,
    artifact_name: str,
    audit_source_name: str | None,
    builder_fn,
    batch_id: str,
    silver_root: Path,
    engine: DataFrameEngine,
) -> SilverBuildResult:
    """
    Generic wrapper: call builder_fn (which returns a DataFrame), write
    it to Silver, return a per-artifact result. Failures captured, not
    raised.

    Audit lifecycle handled by the wrapper:
      - mark_transforming is called by the runner BEFORE this function
        (once per source, across multiple artifacts if needed)
      - record_silver_complete is called by the runner AFTER, with the
        row counts the artifact contributed
      - mark_failed is called HERE on exception, so the audit reflects
        which specific artifact build crashed
    """
    try:
        df = builder_fn()
        rows = engine.count(df)
        output_path = _write_silver_artifact(
            df=df, silver_root=silver_root,
            artifact_name=artifact_name, batch_id=batch_id, engine=engine,
        )
        log.info("silver_artifact_written",
                 artifact=artifact_name, rows=rows, output_path=str(output_path))
        return SilverBuildResult(
            artifact_name=artifact_name,
            status="written",
            rows_written=rows,
            output_path=output_path,
            audit_source_name=audit_source_name,
        )
    except Exception as e:
        tb = traceback.format_exc()
        log.error("silver_artifact_failed",
                  artifact=artifact_name, error=str(e), traceback=tb)
        return SilverBuildResult(
            artifact_name=artifact_name,
            status="failed",
            rows_written=0,
            output_path=None,
            audit_source_name=audit_source_name,
            error_message=str(e),
        )


# ---------------------------------------------------------------------------
# Programmatic entry point
# ---------------------------------------------------------------------------


def run_silver(*, batch_id: str | None = None) -> SilverRunSummary:
    """
    Run Silver for one batch.

    Args:
        batch_id:  partition key; derived from UTC now if omitted

    Returns:
        SilverRunSummary with one SilverBuildResult per artifact plus
        the layer-level verdict.
    """
    cfg = get_config()
    init_db()

    if batch_id is None:
        now = datetime.now(timezone.utc)
        if cfg.batch.granularity == "hourly":
            batch_id = now.strftime("%Y-%m-%dT%H")
        elif cfg.batch.granularity == "daily":
            batch_id = now.strftime("%Y-%m-%d")
        else:
            batch_id = now.strftime("%Y-%m-%dT%H:%M")

    bronze_root = cfg.paths.bronze
    silver_root = cfg.paths.silver
    engine = get_engine()
    registry = get_registry()

    with bind_batch_context(batch_id=batch_id, layer="silver"):
        # ===== Layer-grain idempotency check =============================
        if (
            cfg.batch.skip_if_already_succeeded
            and runs.has_succeeded(batch_id, "silver")
        ):
            log.info("silver_layer_skipped_already_succeeded")
            return SilverRunSummary(
                batch_id=batch_id,
                layer_status="skipped",
                results=[],
                skipped_layer=True,
            )

        runs.mark_started(batch_id, "silver")

        # ===== DQ pass: quarantine bad rows BEFORE Silver consumes ======
        # Per ADR-0006 (the brief's "DQ as a gate" pattern), DQ runs
        # between Bronze and Silver. Rows that fail critical rules
        # never reach Silver — they're written to _rejected/ with a
        # _dq_failure_reason column, and the audit DAO records
        # rejected_row_count via record_quarantine.
        #
        # We pre-compute clean DataFrames per source ONCE and pass them
        # into the per-artifact builders. This avoids reading Bronze
        # multiple times (once per builder) and ensures every artifact
        # sees the same post-DQ data.
        clean_bronze: dict[str, DataFrame] = {}
        dq_results: list[DQResult] = []
        sources_with_bronze = [
            "clubs", "competitions", "players", "games", "appearances",
        ]

        # Pre-build FK lookup sets from Bronze for all FK rules
        try:
            fk_lookups = build_fk_lookups(
                bronze_root=bronze_root, batch_id=batch_id, engine=engine,
            )
        except Exception as e:
            log.error("dq_fk_lookups_failed", error=str(e))
            fk_lookups = {}

        for src_name in sources_with_bronze:
            try:
                bronze_df = _read_bronze(
                    bronze_root=bronze_root, source_name=src_name,
                    batch_id=batch_id, engine=engine,
                )
            except FileNotFoundError as e:
                # Bronze partition missing — defer the failure to the
                # builder closure, which will produce a per-artifact
                # 'failed' result with a clear error. Don't crash here.
                log.warning(
                    "silver_dq_skipped_missing_bronze",
                    source=src_name, error=str(e),
                )
                continue

            try:
                result = run_dq_for_source(
                    source_name=src_name, df=bronze_df,
                    fk_lookups=fk_lookups, engine=engine,
                )
                dq_results.append(result)
                clean_bronze[src_name] = result.clean_rows

                # Quarantine failing rows + record in audit DAO
                if result.failing_rows is not None and engine.count(result.failing_rows):
                    quarantine_rejected_rows(
                        dq_result=result,
                        rejected_root=cfg.paths.rejected,
                        batch_id=batch_id, engine=engine,
                    )
                    failing_count = engine.count(result.failing_rows)
                    try:
                        audit.record_quarantine(
                            batch_id=batch_id,
                            source_name=src_name,
                            rejected_row_count=failing_count,
                        )
                    except Exception as e:
                        log.error(
                            "audit_record_quarantine_failed",
                            source=src_name, error=str(e),
                        )
            except Exception as e:
                # DQ itself failing is rare but possible. Fall back to
                # uncleaned Bronze so the pipeline continues — builders
                # will then see whatever Bronze contained.
                log.error("silver_dq_run_failed", source=src_name, error=str(e))
                clean_bronze[src_name] = bronze_df

        # Write the batch DQ report regardless of success/failure
        dq_report_path: Path | None = None
        if dq_results:
            try:
                source_reports = [
                    build_source_report(result=r, engine=engine)
                    for r in dq_results
                ]
                report = build_batch_report(
                    batch_id=batch_id, source_reports=source_reports,
                )
                dq_report_path = write_report(
                    report=report, output_dir=cfg.paths.dq_reports,
                )
            except Exception as e:
                log.error("dq_report_write_failed", error=str(e))

        # ===== Mark each Bronze source as transforming ===================
        # Source-grain audit attribution: every source we'll touch gets
        # mark_transforming up-front. Failures during specific builds
        # downgrade individual sources to mark_failed.
        sources_to_transform = [
            "clubs", "competitions", "players",
            "games", "appearances",
        ]
        for src_name in sources_to_transform:
            try:
                audit.mark_transforming(batch_id=batch_id, source_name=src_name)
            except audit.AuditStateError as e:
                # The Bronze partition for this source wasn't ingested,
                # or is in the wrong state. Log and continue — the
                # individual artifact builds will fail with clearer
                # context about WHY the source is missing.
                log.warning(
                    "silver_mark_transforming_failed",
                    source=src_name, error=str(e),
                )

        results: list[SilverBuildResult] = []

        # ===== Build dim_clubs ==========================================
        def _build_dim_clubs():
            if "clubs" not in clean_bronze:
                # Missing means Bronze read failed earlier; surface clean error
                raise FileNotFoundError(
                    f"Bronze partition for 'clubs' missing for batch_id={batch_id}"
                )
            return build_dim_clubs(
                bronze_clubs=clean_bronze["clubs"],
                engine=engine,
            )
        results.append(_build_artifact_safe(
            artifact_name="dim_clubs", audit_source_name="clubs",
            builder_fn=_build_dim_clubs,
            batch_id=batch_id, silver_root=silver_root, engine=engine,
        ))

        # ===== Build dim_competitions ==================================
        def _build_dim_competitions():
            if "competitions" not in clean_bronze:
                raise FileNotFoundError(
                    f"Bronze partition for 'competitions' missing for batch_id={batch_id}"
                )
            return build_dim_competitions(
                bronze_competitions=clean_bronze["competitions"],
                engine=engine,
            )
        results.append(_build_artifact_safe(
            artifact_name="dim_competitions", audit_source_name="competitions",
            builder_fn=_build_dim_competitions,
            batch_id=batch_id, silver_root=silver_root, engine=engine,
        ))

        # ===== Build dim_date (generated, no Bronze source) ============
        def _build_dim_date():
            return build_dim_date(
                start_date=_DIM_DATE_START,
                end_date=_DIM_DATE_END,
                engine=engine,
            )
        results.append(_build_artifact_safe(
            artifact_name="dim_date", audit_source_name=None,
            builder_fn=_build_dim_date,
            batch_id=batch_id, silver_root=silver_root, engine=engine,
        ))

        # ===== Build dim_players (Type-2, needs existing state) ========
        dim_players_df: DataFrame | None = None

        # Derive an ISO batch_timestamp for SCD2 effective_date.
        # If batch_id parses as a date/datetime, use it; otherwise use
        # UTC now. The SCD2 effective_date must be ISO-comparable for
        # the as-of-event resolution in fact_appearances to work.
        batch_timestamp = _derive_batch_timestamp(batch_id)

        def _build_dim_players():
            nonlocal dim_players_df
            if "players" not in clean_bronze:
                raise FileNotFoundError(
                    f"Bronze partition for 'players' missing for batch_id={batch_id}"
                )
            players_source = registry.get("players")
            existing = _read_existing_dim_players(
                silver_root=silver_root, engine=engine,
            )
            merged, _stats = build_dim_players(
                bronze_players=clean_bronze["players"],
                existing_dim=existing,
                players_source=players_source,
                batch_timestamp=batch_timestamp,
                engine=engine,
            )
            dim_players_df = merged
            return merged

        results.append(_build_artifact_safe(
            artifact_name="dim_players", audit_source_name="players",
            builder_fn=_build_dim_players,
            batch_id=batch_id, silver_root=silver_root, engine=engine,
        ))

        # ===== Build fact_games ========================================
        def _build_fact_games():
            if "games" not in clean_bronze:
                raise FileNotFoundError(
                    f"Bronze partition for 'games' missing for batch_id={batch_id}"
                )
            return build_fact_games(
                bronze_games=clean_bronze["games"],
                engine=engine,
            )
        results.append(_build_artifact_safe(
            artifact_name="fact_games", audit_source_name="games",
            builder_fn=_build_fact_games,
            batch_id=batch_id, silver_root=silver_root, engine=engine,
        ))

        # ===== Build fact_appearances ==================================
        # This requires dim_players to have succeeded; if it didn't,
        # we record a clean failure rather than blowing up.
        def _build_fact_appearances():
            if dim_players_df is None:
                raise RuntimeError(
                    "fact_appearances requires dim_players, but the "
                    "dim_players build failed earlier in this batch."
                )
            if "appearances" not in clean_bronze:
                raise FileNotFoundError(
                    f"Bronze partition for 'appearances' missing for batch_id={batch_id}"
                )
            return build_fact_appearances(
                bronze_appearances=clean_bronze["appearances"],
                dim_players=dim_players_df,
                engine=engine,
            )
        results.append(_build_artifact_safe(
            artifact_name="fact_appearances", audit_source_name="appearances",
            builder_fn=_build_fact_appearances,
            batch_id=batch_id, silver_root=silver_root, engine=engine,
        ))

        # ===== Audit reconciliation pass ================================
        # For each source whose artifact succeeded, record_silver_complete
        # with the silver row count of its primary artifact. For each
        # that failed, mark_failed on the source's audit rows.
        for r in results:
            if r.audit_source_name is None:
                continue   # dim_date has no source
            try:
                if r.status == "written":
                    audit.record_silver_complete(
                        batch_id=batch_id,
                        source_name=r.audit_source_name,
                        silver_row_count=r.rows_written,
                    )
                else:
                    # Mark every file_audit row of this source as failed.
                    rows = audit.list_batch_files(batch_id=batch_id)
                    for f in rows:
                        if f.source_name == r.audit_source_name:
                            audit.mark_failed(
                                batch_id=batch_id,
                                source_file_path=f.source_file_path,
                                stage="silver",
                                error_message=r.error_message or "unknown silver failure",
                            )
            except Exception as e:
                # Audit calls are best-effort here; log but don't propagate.
                log.error(
                    "silver_audit_finalisation_failed",
                    source=r.audit_source_name, error=str(e),
                )

        # ===== Aggregate verdict =======================================
        failures = [r for r in results if r.status == "failed"]
        rows_total = sum(r.rows_written for r in results)

        if failures:
            error_summary = "; ".join(
                f"{r.artifact_name}: {r.error_message}" for r in failures
            )
            runs.mark_failed(batch_id, "silver", error=error_summary)
            layer_status = "failed"
        else:
            runs.mark_success(
                batch_id, "silver",
                rows_in=rows_total, rows_out=rows_total,
            )
            layer_status = "success"

        log.info(
            "silver_run_finished",
            status=layer_status,
            artifacts_written=sum(1 for r in results if r.status == "written"),
            artifacts_failed=len(failures),
            total_rows=rows_total,
        )

        return SilverRunSummary(
            batch_id=batch_id,
            layer_status=layer_status,
            results=results,
            dq_report_path=dq_report_path,
        )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run the Silver transformation layer for one batch. Reads "
            "Bronze partitions, builds four dimensions (clubs, competitions, "
            "date, players) and two facts (games, appearances), writes "
            "partitioned Parquet to Silver, records the lifecycle in the "
            "audit DAO."
        ),
    )
    parser.add_argument(
        "--batch-id",
        default=None,
        help=(
            "Partition key for this run. If omitted, derived from UTC now "
            "at the configured granularity. Must match a previously-"
            "ingested Bronze batch."
        ),
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    configure_logging()

    summary = run_silver(batch_id=args.batch_id)
    print(_format_summary(summary))
    return 0 if summary.layer_status in ("success", "skipped") else 1


if __name__ == "__main__":
    sys.exit(main())
