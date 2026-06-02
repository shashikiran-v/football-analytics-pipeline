"""
Metadata database.

A tiny SQLite database that tracks pipeline run state. Lives next to
the data lake on the mounted volume so state survives container restarts.

Five tables, each owned by a single DAO module:

  pipeline_runs       (runs.py)         layer-level idempotency
  dq_results          (dq_results.py)   per-check audit trail
  scd_watermarks      (watermarks.py)   incremental high-water marks
  file_audit          (audit.py)        per-file provenance & reconciliation
  file_audit_events   (audit.py)        append-only event timeline per file

This module owns schema creation and connection handling.

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

-- =====================================================================
-- file_audit — per-file provenance and reconciliation.
-- ---------------------------------------------------------------------
-- One mutating row per (batch_id, source_file_path). Always reflects
-- the current state of a file's journey through the pipeline. Pairs
-- with file_audit_events (below) which holds the append-only timeline.
--
-- Row-count semantics:
--   source_row_count  : rows read from the file as ingested
--   bronze_row_count  : rows written to Bronze parquet (after schema enforcement)
--   silver_row_count  : rows that survived DQ + transforms into Silver
--   rejected_row_count: rows quarantined by DQ (so bronze = silver + rejected)
--   gold_row_count    : rows in this source's primary Gold artifact (per
--                       ADR-0007's source-grain attribution: each Gold
--                       artifact attributes to ONE Bronze source)
--
-- Timestamp semantics (see ADR-0001):
--   source_modified_at_vendor    : vendor's authoritative "last changed"
--                                  (from Kaggle API manifest or HTTP Last-Modified)
--   source_modified_at_filesystem: file's mtime on our disk
--   vendor_timestamp_source      : 'manifest' | 'http_header' | 'filesystem_only'
-- =====================================================================
CREATE TABLE IF NOT EXISTS file_audit (
    batch_id                       TEXT NOT NULL,
    source_name                    TEXT NOT NULL,   -- logical source ('players', etc.)
    source_file_path               TEXT NOT NULL,   -- actual path on disk

    -- File fingerprint (immutable for a given content)
    file_size_bytes                INTEGER,
    file_checksum_md5              TEXT,
    schema_version_hash            TEXT,
    source_modified_at_vendor      TEXT,            -- ISO8601, nullable
    source_modified_at_filesystem  TEXT,            -- ISO8601, populated when known
    vendor_timestamp_source        TEXT,            -- enum-as-string, nullable

    -- Row counts at each stage (NULL until that stage runs)
    source_row_count               INTEGER,
    bronze_row_count               INTEGER,
    silver_row_count               INTEGER,
    rejected_row_count             INTEGER,
    gold_row_count                 INTEGER,

    -- Status & timing
    status                         TEXT NOT NULL,   -- see FileStatus enum in audit.py
    registered_at                  TEXT NOT NULL,
    started_at                     TEXT,
    finished_at                    TEXT,
    error_message                  TEXT,
    error_stage                    TEXT,            -- which layer raised

    PRIMARY KEY (batch_id, source_file_path)
);

-- =====================================================================
-- file_audit_events — append-only event log per file.
-- ---------------------------------------------------------------------
-- One row per state transition. Forensic timeline of what happened to
-- each file. NEVER overwritten; mutations to file_audit always write a
-- corresponding event here in the same transaction.
-- =====================================================================
CREATE TABLE IF NOT EXISTS file_audit_events (
    event_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id          TEXT NOT NULL,
    source_file_path  TEXT NOT NULL,
    event_type        TEXT NOT NULL,                -- see EventType enum in audit.py
    event_payload     TEXT,                          -- JSON, optional context
    occurred_at       TEXT NOT NULL,
    FOREIGN KEY (batch_id, source_file_path)
        REFERENCES file_audit(batch_id, source_file_path)
);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status
    ON pipeline_runs(status);
CREATE INDEX IF NOT EXISTS idx_dq_results_failed
    ON dq_results(passed, severity);
CREATE INDEX IF NOT EXISTS idx_file_audit_status
    ON file_audit(status);
CREATE INDEX IF NOT EXISTS idx_file_audit_batch
    ON file_audit(batch_id);
CREATE INDEX IF NOT EXISTS idx_file_audit_checksum
    ON file_audit(file_checksum_md5);              -- for find_previous_successful_ingestion
CREATE INDEX IF NOT EXISTS idx_events_batch_file
    ON file_audit_events(batch_id, source_file_path);
"""


def _ensure_dir(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)


def init_db(db_path: Path | None = None) -> Path:
    """
    Create the metadata DB and apply the schema. Safe to call repeatedly —
    every statement uses IF NOT EXISTS.

    Also handles forward-compatible column additions for users running
    pipelines that started before Phase 5 (gold_row_count is one such
    column). See `_apply_migrations` below for details.

    Returns the resolved path so callers can log it.
    """
    path = db_path or get_config().paths.metadata_db
    _ensure_dir(path)
    with connect(path) as conn:
        conn.executescript(SCHEMA)
        _apply_migrations(conn)
    log.info("metadata_db_initialised", path=str(path))
    return path


def _apply_migrations(conn: sqlite3.Connection) -> None:
    """
    Apply forward-compatible schema migrations for existing DBs.

    `CREATE TABLE IF NOT EXISTS` doesn't add new columns when the table
    already exists from an earlier run. For each column we've added
    since the original schema, we check `PRAGMA table_info` and
    `ALTER TABLE ADD COLUMN` if needed.

    This is the minimal migration system — for a real warehouse we'd
    use Alembic or similar. For a single-table-evolution case like
    Phase 5's gold_row_count, this is proportional.
    """
    existing_cols = {
        row["name"] for row in conn.execute("PRAGMA table_info(file_audit)").fetchall()
    }
    if "gold_row_count" not in existing_cols:
        conn.execute("ALTER TABLE file_audit ADD COLUMN gold_row_count INTEGER")
        log.info("metadata_db_migrated", change="added file_audit.gold_row_count")


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
