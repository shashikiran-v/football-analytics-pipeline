"""
Bronze CLI runner.

Iterates the source registry, calls write_bronze_source for each, and
produces a summary at the end. This is the entrypoint Airflow's Bronze
task will eventually invoke (Phase 8).

Layered idempotency

  - **Layer-grain (this module).** Before processing anything, check
    pipeline_runs for `(batch_id, 'bronze')`. If it's already succeeded
    and `skip_if_already_succeeded` is True, skip the whole run.
    Re-running a completed batch is a no-op.

  - **File-grain (writer.py).** Inside each source, the writer checks
    if this exact MD5 has already succeeded in any prior batch. If so,
    it short-circuits the write but still records the lifecycle so the
    audit timeline is honest about what happened.

Continue-on-failure semantics

  One source failing (file missing, schema invalid, write error) does
  NOT abort the batch. The runner records the failure in the audit DAO
  and continues with the next source. At the end, if any source failed,
  the Bronze layer status is 'failed' in pipeline_runs; otherwise
  'success'. This mirrors how production batch jobs actually behave —
  partial progress is better than nothing, and the audit makes it
  trivial to see exactly which sources need re-running.

CLI usage

  python -m src.bronze.run --batch-id 2026-06-01T15
  python -m src.bronze.run --batch-id 2026-06-01T15 --raw-root data/sample
  python -m src.bronze.run                              # auto-batch-id from UTC now

The runner is also importable as `run_bronze(...)` for tests and for
Airflow's PythonOperator.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from src.bronze.writer import BronzeWriteResult, write_bronze_source
from src.engines.factory import get_engine
from src.ingestion.registry import get_registry
from src.metadata import runs
from src.metadata.db import init_db
from src.utils.config import get_config
from src.utils.logging import bind_batch_context, configure_logging, get_logger


log = get_logger(__name__)


# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BronzeRunSummary:
    """Aggregate outcome of a Bronze run across all sources."""

    batch_id: str
    layer_status: str                # 'success' | 'failed' | 'skipped'
    results: list[BronzeWriteResult]
    skipped_layer: bool = False      # True if layer-grain idempotency fired

    @property
    def total_rows(self) -> int:
        return sum(r.rows_written for r in self.results)

    @property
    def failures(self) -> list[BronzeWriteResult]:
        return [r for r in self.results if r.status == "failed"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _derive_batch_id(granularity: str) -> str:
    """
    Default batch_id when the caller didn't provide one. Derived from
    UTC now at the configured granularity. Stable across the run so
    every source in the batch shares the same partition key.
    """
    now = datetime.now(timezone.utc)
    if granularity == "hourly":
        return now.strftime("%Y-%m-%dT%H")
    if granularity == "daily":
        return now.strftime("%Y-%m-%d")
    # Defensive default — minute granularity gives us deterministic
    # batch ids in tests and demos where seconds matter.
    return now.strftime("%Y-%m-%dT%H:%M")


def _format_summary(summary: BronzeRunSummary) -> str:
    """Human-readable summary line per source plus an overall verdict."""
    lines: list[str] = []
    lines.append(f"Bronze run summary — batch_id={summary.batch_id}")
    lines.append(f"  status: {summary.layer_status}")
    if summary.skipped_layer:
        lines.append("  (whole layer skipped — already succeeded for this batch)")
        return "\n".join(lines)
    lines.append(f"  total rows: {summary.total_rows}")
    lines.append("  per source:")
    width = max((len(r.source_name) for r in summary.results), default=10)
    for r in summary.results:
        if r.status == "written":
            detail = f"rows={r.rows_written}"
        elif r.status == "skipped":
            detail = f"skip_reason={r.skip_reason}"
        else:
            detail = f"error={r.error_message}"
        lines.append(
            f"    {r.source_name:<{width}}  {r.status:<8}  {detail}"
        )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Programmatic entry point — also used by Airflow and tests
# ---------------------------------------------------------------------------


def run_bronze(
    *,
    batch_id: str | None = None,
    raw_root: Path | str | None = None,
) -> BronzeRunSummary:
    """
    Run Bronze for one batch.

    Args:
        batch_id:  partition key; derived from UTC now if not provided.
        raw_root:  directory containing source files; defaults to
                   config.paths.raw_day1 (production) but tests pass
                   data/sample/ for the committed samples.

    Returns:
        BronzeRunSummary with one BronzeWriteResult per source plus
        the layer-level verdict.

    Side effects:
        - Initialises the metadata DB if it doesn't exist
        - Writes pipeline_runs rows for the layer lifecycle
        - Writes file_audit rows for each source (via the writer)
        - Produces Parquet partitions under {paths.bronze}/{source}/
    """
    cfg = get_config()
    init_db()                                # safe to call repeatedly

    # Default the batch_id if not provided
    if batch_id is None:
        batch_id = _derive_batch_id(cfg.batch.granularity)

    # Default raw_root to the configured day1 location, or
    # fall back to sample if day1 is empty (development convenience)
    if raw_root is None:
        candidate = cfg.paths.raw_day1
        # If day1 has no CSVs (typical pre-seed state), fall back to samples
        if not any(candidate.glob("*.csv")):
            log.info(
                "raw_root_fallback_to_sample",
                reason="no CSVs in raw_day1",
                raw_day1=str(candidate),
                fallback=str(cfg.paths.sample),
            )
            raw_root = cfg.paths.sample
        else:
            raw_root = candidate
    raw_root = Path(raw_root)

    with bind_batch_context(batch_id=batch_id, layer="bronze"):
        # ===== Layer-grain idempotency check =============================
        if (
            cfg.batch.skip_if_already_succeeded
            and runs.has_succeeded(batch_id, "bronze")
        ):
            log.info("bronze_layer_skipped_already_succeeded")
            return BronzeRunSummary(
                batch_id=batch_id,
                layer_status="skipped",
                results=[],
                skipped_layer=True,
            )

        runs.mark_started(batch_id, "bronze")

        # ===== Per-source loop ==========================================
        registry = get_registry()
        engine = get_engine()
        results: list[BronzeWriteResult] = []

        log.info(
            "bronze_run_started",
            sources=registry.names(),
            raw_root=str(raw_root),
            bronze_root=str(cfg.paths.bronze),
            engine=engine.kind,
        )

        for source in registry.all_sources():
            result = write_bronze_source(
                source=source,
                raw_root=raw_root,
                bronze_root=cfg.paths.bronze,
                batch_id=batch_id,
                engine=engine,
            )
            results.append(result)

        # ===== Aggregate verdict ========================================
        failures = [r for r in results if r.status == "failed"]
        rows_total = sum(r.rows_written for r in results)
        rows_written_count = sum(1 for r in results if r.status == "written")

        if failures:
            error_summary = "; ".join(
                f"{r.source_name}: {r.error_message}" for r in failures
            )
            runs.mark_failed(batch_id, "bronze", error=error_summary)
            layer_status = "failed"
        else:
            runs.mark_success(
                batch_id, "bronze",
                rows_in=rows_total,
                rows_out=rows_total,
            )
            layer_status = "success"

        log.info(
            "bronze_run_finished",
            status=layer_status,
            sources_written=rows_written_count,
            sources_skipped=sum(1 for r in results if r.status == "skipped"),
            sources_failed=len(failures),
            total_rows=rows_total,
        )

        return BronzeRunSummary(
            batch_id=batch_id,
            layer_status=layer_status,
            results=results,
        )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run the Bronze ingestion layer for one batch. Iterates the "
            "source registry, applies layer-grain and file-grain idempotency, "
            "writes Hive-partitioned Parquet, and records the lifecycle in "
            "the audit DAO."
        ),
    )
    parser.add_argument(
        "--batch-id",
        default=None,
        help=(
            "Partition key for this run. If omitted, derived from UTC now "
            "at the configured granularity (hourly by default)."
        ),
    )
    parser.add_argument(
        "--raw-root",
        default=None,
        help=(
            "Directory containing source files. Defaults to "
            "config.paths.raw_day1, falling back to data/sample if "
            "day1 is empty."
        ),
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    configure_logging()

    summary = run_bronze(
        batch_id=args.batch_id,
        raw_root=args.raw_root,
    )

    # Always print a human-readable summary on stdout for CLI users.
    print(_format_summary(summary))

    # Exit non-zero if any source failed, so CI / Airflow detect the failure.
    return 0 if summary.layer_status in ("success", "skipped") else 1


if __name__ == "__main__":
    sys.exit(main())
