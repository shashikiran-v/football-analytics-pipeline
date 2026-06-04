"""
Gold CLI runner.

Orchestrates the Bronze + Silver -> Gold pass for one batch:

  1. Checks pipeline_runs for layer-grain idempotency
  2. Opens a single DuckDB session with Silver + Bronze views registered
  3. For each artifact in ALL_ARTIFACTS:
       - Executes the SQL via DuckDB
       - Materialises to data/lake/gold/<artifact>/batch_id=<id>/
       - Calls audit.record_gold_complete on the artifact's primary source
  4. Marks the layer success / failed in pipeline_runs

Layer-grain idempotency
-----------------------
Same pattern as Bronze and Silver: re-running a fully-successful Gold
batch is a no-op via pipeline_runs.

Continue-on-failure
-------------------
Per-artifact try/except. If one artifact's SQL fails (e.g. missing
Silver dependency, syntax error after a refactor), the remaining
artifacts still build and the failed one is recorded in the run summary.

Source-grain audit attribution
------------------------------
Each Gold artifact attributes its row count to its PRIMARY Bronze
source (per ADR-0005/-0007's source-grain attribution pattern):

  top_scorers_by_season          -> appearances
  top_players_all_time           -> appearances (same source!)
  club_season_summary            -> games
  club_performance_metrics       -> games (same source!)
  player_valuation_rolling_avg   -> player_valuations

When two artifacts share a primary source (top_scorers and
top_players_all_time both attribute to appearances), the LAST artifact
written wins on the audit row's gold_row_count. The DQ report and the
per-artifact materialised parquet preserve full granularity.

CLI usage
---------
  python -m src.gold.run --batch-id 2024-12-01
  python -m src.gold.run                         # auto batch_id from now

Importable as run_gold() for tests and (later) Airflow.
"""

from __future__ import annotations

import argparse
import sys
import traceback
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from src.gold.artifacts import ALL_ARTIFACTS, GoldArtifact
from src.gold.builders import GoldBuildResult, build_gold_artifact
from src.gold.duckdb_session import gold_session
from src.metadata import audit, runs
from src.metadata.db import init_db
from src.utils.config import get_config
from src.utils.logging import bind_batch_context, configure_logging, get_logger

log = get_logger(__name__)


# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class GoldArtifactOutcome:
    """Per-artifact outcome of a Gold run."""

    artifact_name: str
    status: str  # 'written' | 'failed'
    rows_written: int
    primary_source: str
    output_path: Path | None
    error_message: str | None = None


@dataclass(frozen=True)
class GoldRunSummary:
    """Aggregate outcome of a Gold run across all artifacts."""

    batch_id: str
    layer_status: str  # 'success' | 'failed' | 'skipped'
    results: list[GoldArtifactOutcome]
    skipped_layer: bool = False

    @property
    def total_rows(self) -> int:
        return sum(r.rows_written for r in self.results)

    @property
    def failures(self) -> list[GoldArtifactOutcome]:
        return [r for r in self.results if r.status == "failed"]


# ---------------------------------------------------------------------------
# Per-artifact build (with continue-on-failure)
# ---------------------------------------------------------------------------


def _build_artifact_safe(
    *,
    artifact: GoldArtifact,
    conn,
    gold_root: Path,
    batch_id: str,
) -> GoldArtifactOutcome:
    """
    Build one Gold artifact with failure isolation. Returns an outcome
    even on exception; the runner never raises out of this function.
    """
    try:
        result: GoldBuildResult = build_gold_artifact(
            artifact=artifact,
            conn=conn,
            gold_root=gold_root,
            batch_id=batch_id,
        )
        return GoldArtifactOutcome(
            artifact_name=artifact.name,
            status="written",
            rows_written=result.row_count,
            primary_source=artifact.primary_source,
            output_path=result.output_path,
        )
    except Exception as e:
        tb = traceback.format_exc()
        log.error(
            "gold_artifact_failed",
            artifact=artifact.name,
            error=str(e),
            traceback=tb,
        )
        return GoldArtifactOutcome(
            artifact_name=artifact.name,
            status="failed",
            rows_written=0,
            primary_source=artifact.primary_source,
            output_path=None,
            error_message=str(e),
        )


# ---------------------------------------------------------------------------
# Summary formatter
# ---------------------------------------------------------------------------


def _format_summary(summary: GoldRunSummary) -> str:
    """Human-readable summary printed by the CLI."""
    lines: list[str] = []
    lines.append(f"Gold run summary — batch_id={summary.batch_id}")
    lines.append(f"  status: {summary.layer_status}")
    if summary.skipped_layer:
        lines.append("  (whole layer skipped — already succeeded for this batch)")
        return "\n".join(lines)
    lines.append(f"  total rows: {summary.total_rows}")
    lines.append("  per artifact:")
    width = max((len(r.artifact_name) for r in summary.results), default=12)
    for r in summary.results:
        if r.status == "written":
            detail = f"rows={r.rows_written} primary_source={r.primary_source}"
        else:
            detail = f"error={r.error_message}"
        lines.append(f"    {r.artifact_name:<{width}}  {r.status:<8}  {detail}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Programmatic entry point
# ---------------------------------------------------------------------------


def run_gold(*, batch_id: str | None = None) -> GoldRunSummary:
    """
    Run Gold for one batch.

    Args:
        batch_id:  partition key; derived from UTC now if omitted

    Returns:
        GoldRunSummary with one outcome per artifact plus the layer verdict.
    """
    cfg = get_config()
    init_db()

    if batch_id is None:
        now = datetime.now(UTC)
        if cfg.batch.granularity == "hourly":
            batch_id = now.strftime("%Y-%m-%dT%H")
        elif cfg.batch.granularity == "daily":
            batch_id = now.strftime("%Y-%m-%d")
        else:
            batch_id = now.strftime("%Y-%m-%dT%H:%M")

    with bind_batch_context(batch_id=batch_id, layer="gold"):
        # ===== Layer-grain idempotency check =============================
        if cfg.batch.skip_if_already_succeeded and runs.has_succeeded(batch_id, "gold"):
            log.info("gold_layer_skipped_already_succeeded")
            return GoldRunSummary(
                batch_id=batch_id,
                layer_status="skipped",
                results=[],
                skipped_layer=True,
            )

        runs.mark_started(batch_id, "gold")

        results: list[GoldArtifactOutcome] = []

        # ===== Open DuckDB session once, build every artifact =============
        try:
            with gold_session(
                silver_root=cfg.paths.silver,
                bronze_root=cfg.paths.bronze,
            ) as conn:
                for artifact in ALL_ARTIFACTS:
                    outcome = _build_artifact_safe(
                        artifact=artifact,
                        conn=conn,
                        gold_root=cfg.paths.gold,
                        batch_id=batch_id,
                    )
                    results.append(outcome)
        except Exception as e:
            # Session itself failed to open — DuckDB error, missing views, etc.
            # Mark every artifact as failed.
            log.error("gold_session_failed", error=str(e))
            results = [
                GoldArtifactOutcome(
                    artifact_name=a.name,
                    status="failed",
                    rows_written=0,
                    primary_source=a.primary_source,
                    output_path=None,
                    error_message=f"session-level failure: {e}",
                )
                for a in ALL_ARTIFACTS
            ]

        # ===== Audit DAO integration ====================================
        # For each successful artifact, record gold_row_count on the
        # primary source's audit row. Best-effort — audit failures
        # log but don't propagate.
        for outcome in results:
            if outcome.status != "written":
                continue
            try:
                audit.record_gold_complete(
                    batch_id=batch_id,
                    source_name=outcome.primary_source,
                    gold_row_count=outcome.rows_written,
                )
            except audit.AuditStateError as e:
                # Source not registered for this batch — usually means
                # the user ran Gold without running Bronze first. Log
                # but don't block other artifacts' audit updates.
                log.warning(
                    "gold_audit_skip_unregistered_source",
                    artifact=outcome.artifact_name,
                    source=outcome.primary_source,
                    error=str(e),
                )
            except Exception as e:
                log.error(
                    "gold_audit_finalisation_failed",
                    artifact=outcome.artifact_name,
                    source=outcome.primary_source,
                    error=str(e),
                )

        # ===== Aggregate verdict ========================================
        failures = [r for r in results if r.status == "failed"]
        rows_total = sum(r.rows_written for r in results)

        if failures:
            error_summary = "; ".join(f"{r.artifact_name}: {r.error_message}" for r in failures)
            runs.mark_failed(batch_id, "gold", error=error_summary)
            layer_status = "failed"
        else:
            runs.mark_success(
                batch_id,
                "gold",
                rows_in=rows_total,
                rows_out=rows_total,
            )
            layer_status = "success"

        log.info(
            "gold_run_finished",
            status=layer_status,
            artifacts_written=sum(1 for r in results if r.status == "written"),
            artifacts_failed=len(failures),
            total_rows=rows_total,
        )

        return GoldRunSummary(
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
            "Run the Gold analytical layer for one batch. Opens a DuckDB "
            "session with Silver + Bronze views registered, executes "
            "every artifact's SQL, materialises partitioned Parquet to "
            "data/lake/gold/, and records gold_row_count on each "
            "artifact's primary Bronze source's audit row."
        ),
    )
    parser.add_argument(
        "--batch-id",
        default=None,
        help=(
            "Partition key for this run. If omitted, derived from UTC "
            "now at the configured granularity. Must match a previously-"
            "run Silver batch."
        ),
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    configure_logging()

    summary = run_gold(batch_id=args.batch_id)
    print(_format_summary(summary))
    return 0 if summary.layer_status in ("success", "skipped") else 1


if __name__ == "__main__":
    sys.exit(main())
