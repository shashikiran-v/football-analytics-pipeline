"""
DAO for scd_watermarks.

A watermark records the timestamp up to which a table has been processed.
On the next run, Bronze ingestion (or Silver's SCD2 merge) uses the
watermark to filter source data: only rows newer than the watermark are
considered "new arrivals."

For the Kaggle dataset, the relevant timestamp column varies per table
(games.date, player_valuations.date, etc.). The caller decides which
column to compare against; this DAO is unaware of those semantics.
"""

from __future__ import annotations

from datetime import UTC, datetime

from src.metadata.db import connect


def _utcnow() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def get_watermark(table_name: str) -> str | None:
    """Return ISO timestamp of last processed row, or None if never run."""
    with connect() as conn:
        row = conn.execute(
            "SELECT last_processed_timestamp FROM scd_watermarks WHERE table_name=?",
            (table_name,),
        ).fetchone()
        return row["last_processed_timestamp"] if row else None


def set_watermark(table_name: str, timestamp: str, batch_id: str) -> None:
    """Upsert the watermark for a table."""
    sql = """
        INSERT INTO scd_watermarks
            (table_name, last_processed_timestamp, last_batch_id, updated_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT (table_name) DO UPDATE SET
            last_processed_timestamp = excluded.last_processed_timestamp,
            last_batch_id            = excluded.last_batch_id,
            updated_at               = excluded.updated_at
    """
    with connect() as conn:
        conn.execute(sql, (table_name, timestamp, batch_id, _utcnow()))
