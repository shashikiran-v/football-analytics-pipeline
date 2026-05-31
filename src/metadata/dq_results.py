"""
DAO for dq_results.

Each row records the outcome of one DQ check against one table in one batch.
The full set of rows forms the DQ report for that batch; it can also be
queried across batches to track quality trends over time.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Literal

from src.metadata.db import connect


Severity = Literal["ERROR", "WARN"]


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def record_result(
    batch_id: str,
    table_name: str,
    check_name: str,
    severity: Severity,
    passed: bool,
    rows_checked: int,
    rows_failed: int,
    details: dict[str, Any] | None = None,
) -> None:
    """
    Upsert a DQ check result. ON CONFLICT updates because the same check
    may be re-run on a batch retry; we want the latest outcome to win.
    """
    sql = """
        INSERT INTO dq_results
            (batch_id, table_name, check_name, severity, passed,
             rows_checked, rows_failed, details, recorded_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (batch_id, table_name, check_name) DO UPDATE SET
            severity     = excluded.severity,
            passed       = excluded.passed,
            rows_checked = excluded.rows_checked,
            rows_failed  = excluded.rows_failed,
            details      = excluded.details,
            recorded_at  = excluded.recorded_at
    """
    with connect() as conn:
        conn.execute(
            sql,
            (
                batch_id,
                table_name,
                check_name,
                severity,
                1 if passed else 0,
                rows_checked,
                rows_failed,
                json.dumps(details) if details else None,
                _utcnow(),
            ),
        )


def report_for_batch(batch_id: str) -> list[dict]:
    """Return all DQ results for a batch, ordered by table then check."""
    sql = """
        SELECT * FROM dq_results
        WHERE batch_id=?
        ORDER BY table_name, check_name
    """
    with connect() as conn:
        return [dict(r) for r in conn.execute(sql, (batch_id,)).fetchall()]


def any_errors(batch_id: str) -> bool:
    """True if any ERROR-severity check failed for this batch."""
    sql = """
        SELECT 1 FROM dq_results
        WHERE batch_id=? AND severity='ERROR' AND passed=0
        LIMIT 1
    """
    with connect() as conn:
        return conn.execute(sql, (batch_id,)).fetchone() is not None
