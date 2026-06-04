"""
DQ report generation.

Serialises the outcomes of one Silver run's DQ pass to a per-batch
JSON report at `data/dq_reports/<batch_id>.json`.

The brief calls for this explicitly (§7 Data Quality: "DQ report
output per batch"). The report is a contract for downstream reviewers
and operators — what was checked, what passed, what failed, with what
severity.

Report schema (stable, evolving with caution):

  {
    "batch_id": "demo-1",
    "generated_at": "2026-06-02T08:45:00.000Z",
    "summary": {
      "total_rows_in": 74,
      "total_rows_clean": 73,
      "total_rows_quarantined": 1,
      "critical_failure_count": 1,
      "warning_count": 0,
      "sources_with_critical_failures": ["appearances"]
    },
    "sources": [
      {
        "source_name": "appearances",
        "rows_in": 30,
        "rows_clean": 29,
        "rows_quarantined": 1,
        "rules": [
          {
            "rule_id": "...",
            "severity": "critical",
            "rows_evaluated": 30,
            "rows_failed": 1,
            "pass_rate": 0.967
          }
        ]
      }
    ]
  }
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.dq.rules import SEVERITY_CRITICAL, SEVERITY_WARNING
from src.dq.runner import DQResult
from src.engines.base import DataFrameEngine
from src.utils.logging import get_logger

log = get_logger(__name__)


# ---------------------------------------------------------------------------
# Typed report structure
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DQRuleReport:
    rule_id: str
    severity: str
    rows_evaluated: int
    rows_failed: int
    pass_rate: float


@dataclass(frozen=True)
class DQSourceReport:
    source_name: str
    rows_in: int
    rows_clean: int
    rows_quarantined: int
    rules: list[DQRuleReport]


@dataclass(frozen=True)
class DQBatchSummary:
    total_rows_in: int
    total_rows_clean: int
    total_rows_quarantined: int
    critical_failure_count: int
    warning_count: int
    sources_with_critical_failures: list[str]


@dataclass(frozen=True)
class DQBatchReport:
    batch_id: str
    generated_at: str
    summary: DQBatchSummary
    sources: list[DQSourceReport]

    def to_dict(self) -> dict:
        return asdict(self)


# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------


def build_source_report(
    *,
    result: DQResult,
    engine: DataFrameEngine,
) -> DQSourceReport:
    """Convert a DQResult into the report-shaped representation."""
    rows_quarantined = engine.count(result.failing_rows) if result.failing_rows is not None else 0
    rows_clean = result.rows_in - rows_quarantined
    return DQSourceReport(
        source_name=result.source_name,
        rows_in=result.rows_in,
        rows_clean=rows_clean,
        rows_quarantined=rows_quarantined,
        rules=[
            DQRuleReport(
                rule_id=o.rule_id,
                severity=o.severity,
                rows_evaluated=o.rows_evaluated,
                rows_failed=o.rows_failed,
                pass_rate=round(o.pass_rate, 6),
            )
            for o in result.outcomes
        ],
    )


def build_batch_report(
    *,
    batch_id: str,
    source_reports: list[DQSourceReport],
) -> DQBatchReport:
    """Aggregate per-source reports into the batch-level report."""
    sources_with_critical: list[str] = []
    critical_failure_count = 0
    warning_count = 0
    for sr in source_reports:
        had_critical = False
        for r in sr.rules:
            if r.severity == SEVERITY_CRITICAL and r.rows_failed > 0:
                critical_failure_count += 1
                had_critical = True
            if r.severity == SEVERITY_WARNING and r.rows_failed > 0:
                warning_count += 1
        if had_critical:
            sources_with_critical.append(sr.source_name)

    summary = DQBatchSummary(
        total_rows_in=sum(s.rows_in for s in source_reports),
        total_rows_clean=sum(s.rows_clean for s in source_reports),
        total_rows_quarantined=sum(s.rows_quarantined for s in source_reports),
        critical_failure_count=critical_failure_count,
        warning_count=warning_count,
        sources_with_critical_failures=sources_with_critical,
    )
    return DQBatchReport(
        batch_id=batch_id,
        generated_at=datetime.now(UTC).isoformat(),
        summary=summary,
        sources=source_reports,
    )


# ---------------------------------------------------------------------------
# Writer
# ---------------------------------------------------------------------------


def write_report(
    *,
    report: DQBatchReport,
    output_dir: Path,
) -> Path:
    """
    Write the report to <output_dir>/<batch_id>.json.

    Returns the output path. Directory is created if missing.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{report.batch_id}.json"
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(report.to_dict(), f, indent=2, default=str)
    log.info(
        "dq_report_written",
        batch_id=report.batch_id,
        output_path=str(output_path),
        critical_failures=report.summary.critical_failure_count,
        warnings=report.summary.warning_count,
        rows_quarantined=report.summary.total_rows_quarantined,
    )
    return output_path


def read_dq_report(report_path: Path) -> dict[str, Any]:
    """
    Read a DQ report JSON and return a flattened summary dict.

    Returns a dict with these keys (always present):
      * rows_in_total           : total source rows across all sources
      * rows_clean_total        : rows that passed DQ
      * rows_quarantined_total  : rows that failed critical rules
      * critical_failures_total : count of (rule, source) pairs with >0 critical failures
      * warning_failures_total  : count of (rule, source) pairs with >0 warning failures
      * sources_with_critical   : list of source names with critical failures

    Used by the Airflow DQ gate task and by tests; keeps consumers
    decoupled from the DQBatchReport dataclass shape.
    """
    with report_path.open(encoding="utf-8") as f:
        raw = json.load(f)
    summary = raw.get("summary", {})
    return {
        "batch_id": raw.get("batch_id"),
        "rows_in_total": summary.get("total_rows_in", 0),
        "rows_clean_total": summary.get("total_rows_clean", 0),
        "rows_quarantined_total": summary.get("total_rows_quarantined", 0),
        "critical_failures_total": summary.get("critical_failure_count", 0),
        "warning_failures_total": summary.get("warning_count", 0),
        "sources_with_critical": summary.get("sources_with_critical_failures", []),
    }
