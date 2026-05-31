"""
Metadata database.

A tiny SQLite database that tracks pipeline run state. Lives next to
the data lake on the mounted volume so state survives container restarts.

Three tables, each owned by a single DAO module (runs.py, dq_results.py,
watermarks.py). This module owns schema creation and connection handling.

Why SQLite (not Postgres):
  - zero ops cost, no extra service in docker-compose
  - file-based, easy to inspect (`sqlite3 data/metadata.db`)
  - perfectly adequate for the volume we generate (a few hundred rows/run)
  - if we ever outgrew it, swapping in Postgres is a connection-string change

Concurrency: Airflow's LocalExecutor runs tasks in subprocesses serially
within a DAG run, so contention is minimal. SQLite is opened with
WAL mode + a 5s busy timeout to handle the occasional overlap (e.g. a
DQ task writing while a run task updates status).
"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from src.utils.config import get_config
from src.utils.logging import get_logger


log = get_logger(__name__)


SCHEMA = """
-- One row per (batch_id, layer). Idempotency hinges on this table:
-- before doing work, layers consult pipeline_runs to see if they've
-- already produced output for this batch.
CREATE TABLE IF NOT EXISTS pipeline_runs (
    batch_id     TEXT NOT NULL,
    layer        TEXT NOT NULL,            -- bronze | silver | gold | dq
    status       TEXT NOT NULL,            -- running | success | failed
    started_at   TEXT NOT NULL,            -- ISO8601 UTC
    finished_at  TEXT,
    rows_in      INTEGER,
    rows_out     INTEGER,
    error        TEXT,
    PRIMARY KEY (batch_id, layer)
);

-- One row per DQ check per batch. Used to render the DQ report and to
-- drive alerting / quarantine decisions.
CREATE TABLE IF NOT EXISTS dq_results (
    batch_id     TEXT NOT NULL,
    table_name   TEXT NOT NULL,
    check_name   TEXT NOT NULL,
    severity     TEXT NOT NULL,            -- ERROR | WARN
    passed       INTEGER NOT NULL,         -- 0 | 1 (sqlite has no native bool)
    rows_checked INTEGER NOT NULL,
    rows_failed  INTEGER NOT NULL,
    details      TEXT,                     -- JSON blob: failing PKs, ranges, etc
    recorded_at  TEXT NOT NULL,
    PRIMARY KEY (batch_id, table_name, check_name)
);

-- High-water mark per table for incremental loads. The Bronze layer
-- consults this when running incrementally to decide which rows are new.
CREATE TABLE IF NOT EXISTS scd_watermarks (
    table_name              TEXT PRIMARY KEY,
    last_processed_timestamp TEXT NOT NULL,
    last_batch_id           TEXT NOT NULL,
    updated_at              TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status
    ON pipeline_runs(status);
CREATE INDEX IF NOT EXISTS idx_dq_results_failed
    ON dq_results(passed, severity);
"""


def _ensure_dir(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)


def init_db(db_path: Path | None = None) -> Path:
    """
    Create the metadata DB and apply the schema. Safe to call repeatedly —
    every statement uses IF NOT EXISTS.

    Returns the resolved path so callers can log it.
    """
    path = db_path or get_config().paths.metadata_db
    _ensure_dir(path)
    with connect(path) as conn:
        conn.executescript(SCHEMA)
    log.info("metadata_db_initialised", path=str(path))
    return path


@contextmanager
def connect(db_path: Path | None = None) -> Iterator[sqlite3.Connection]:
    """
    Context-managed SQLite connection. Sets pragmatic defaults:

      - WAL journal mode: readers don't block writers
      - 5s busy timeout: handles brief contention without raising
      - row_factory = Row: lets callers do row["col"] instead of row[0]
    """
    path = db_path or get_config().paths.metadata_db
    _ensure_dir(path)
    conn = sqlite3.connect(path, timeout=5.0, isolation_level=None)  # autocommit
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.row_factory = sqlite3.Row
        yield conn
    finally:
        conn.close()
