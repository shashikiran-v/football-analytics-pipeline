"""
DAO for pipeline_runs.

Encapsulates the idempotency contract:

  - mark_started(batch_id, layer)        -> upserts a 'running' row
  - mark_success(batch_id, layer, ...)   -> updates to 'success' with row counts
  - mark_failed(batch_id, layer, error)  -> updates to 'failed' with the error msg
  - has_succeeded(batch_id, layer)       -> True if this work is already done

The pattern in every layer task is:

    if cfg.batch.skip_if_already_succeeded and runs.has_succeeded(batch_id, "silver"):
        log.info("skip_already_succeeded"); return
    runs.mark_started(batch_id, "silver")
    try:
        rows_in, rows_out = do_work()
        runs.mark_success(batch_id, "silver", rows_in=rows_in, rows_out=rows_out)
    except Exception as e:
        runs.mark_failed(batch_id, "silver", error=str(e))
        raise
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Literal

from src.metadata.db import connect

Layer = Literal["bronze", "silver", "gold", "dq", "ingestion"]
Status = Literal["running", "success", "failed"]


def _utcnow() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def mark_started(batch_id: str, layer: Layer) -> None:
    """
    Upsert a 'running' row. If the row already exists (e.g. a retry after
    a crash), we reset finished_at/error and bump started_at so the
    history reflects the most recent attempt.
    """
    sql = """
        INSERT INTO pipeline_runs (batch_id, layer, status, started_at)
        VALUES (?, ?, 'running', ?)
        ON CONFLICT (batch_id, layer) DO UPDATE SET
            status      = 'running',
            started_at  = excluded.started_at,
            finished_at = NULL,
            rows_in     = NULL,
            rows_out    = NULL,
            error       = NULL
    """
    with connect() as conn:
        conn.execute(sql, (batch_id, layer, _utcnow()))


def mark_success(
    batch_id: str,
    layer: Layer,
    rows_in: int | None = None,
    rows_out: int | None = None,
) -> None:
    sql = """
        UPDATE pipeline_runs
        SET status='success', finished_at=?, rows_in=?, rows_out=?, error=NULL
        WHERE batch_id=? AND layer=?
    """
    with connect() as conn:
        conn.execute(sql, (_utcnow(), rows_in, rows_out, batch_id, layer))


def mark_failed(batch_id: str, layer: Layer, error: str) -> None:
    sql = """
        UPDATE pipeline_runs
        SET status='failed', finished_at=?, error=?
        WHERE batch_id=? AND layer=?
    """
    with connect() as conn:
        # Truncate very long error messages so the DB doesn't bloat.
        conn.execute(sql, (_utcnow(), error[:4000], batch_id, layer))


def has_succeeded(batch_id: str, layer: Layer) -> bool:
    sql = "SELECT 1 FROM pipeline_runs WHERE batch_id=? AND layer=? AND status='success'"
    with connect() as conn:
        row = conn.execute(sql, (batch_id, layer)).fetchone()
        return row is not None


def get_run(batch_id: str, layer: Layer) -> dict | None:
    with connect() as conn:
        row = conn.execute(
            "SELECT * FROM pipeline_runs WHERE batch_id=? AND layer=?",
            (batch_id, layer),
        ).fetchone()
        return dict(row) if row else None
