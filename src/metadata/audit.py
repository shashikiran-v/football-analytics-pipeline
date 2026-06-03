"""
File audit DAO.

Owns the file_audit (mutating) and file_audit_events (append-only)
tables. Encapsulates the lifecycle, state machine, and reconciliation
logic for files moving through the pipeline.

Public surface (verbs first, then readers, then reconciliation):

  Writers (state-changing):
    register_file                  registered -> table; emits 'registered'
    mark_ingesting                 -> ingesting; emits 'ingest_started'
    record_ingestion_complete      -> ingested + counts; emits 'ingest_finished'
    record_quarantine              counts only; emits 'dq_completed'
    mark_transforming              -> transforming; emits 'silver_started'
    record_silver_complete         -> transformed + count; emits 'silver_finished'
    mark_failed                    -> failed; emits 'failed' (never raises)
    record_schema_drift            informational; emits 'schema_drift_detected'

  Readers:
    get_audit_row
    list_batch_files
    get_event_timeline
    find_previous_successful_ingestion
    latest_schema_hash
    list_failed_since

  Reconciliation:
    reconcile_batch

Design contracts (see ADR-0001):

  * Every state-changing function writes to BOTH file_audit and
    file_audit_events inside a single transaction. They cannot drift.
  * State transitions are explicit and enforced; illegal moves raise
    AuditStateError. Programming bugs surface immediately.
  * mark_failed is the ONE exception: it never raises and accepts any
    prior state. It is the "all bets are off, just record what happened"
    terminal call; if it raised, exception handlers would lose the
    original error.
  * record_quarantine and record_silver_complete attribute counts at
    SOURCE grain (not per-file). For our Kaggle dataset each source has
    one file so this is moot; for multi-file vendors we'd lose per-file
    reject attribution. Documented limitation, not an oversight.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any

from src.metadata.db import connect
from src.utils.logging import get_logger


log = get_logger(__name__)


# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------


class FileStatus(str, Enum):
    """
    Lifecycle states. Legal transitions:

        registered  -> ingesting    -> ingested
                                    -> failed
                    -> failed

        ingested    -> transforming -> transformed
                                    -> failed
                    -> failed

    failed is terminal and reachable from any non-terminal state.
    """

    REGISTERED = "registered"
    INGESTING = "ingesting"
    INGESTED = "ingested"
    TRANSFORMING = "transforming"
    TRANSFORMED = "transformed"
    FAILED = "failed"


class EventType(str, Enum):
    """Event names written to file_audit_events."""

    REGISTERED = "registered"
    INGEST_STARTED = "ingest_started"
    INGEST_FINISHED = "ingest_finished"
    DQ_COMPLETED = "dq_completed"
    SILVER_STARTED = "silver_started"
    SILVER_FINISHED = "silver_finished"
    GOLD_FINISHED = "gold_finished"
    RECONCILED = "reconciled"
    FAILED = "failed"
    SCHEMA_DRIFT_DETECTED = "schema_drift_detected"
    VENDOR_TIMESTAMP_UNAVAILABLE = "vendor_timestamp_unavailable"


# Legal transition map. Keys are current states; values are sets of
# states that can be reached next via a state-changing writer (other
# than mark_failed, which is universally permitted from non-terminal).
_LEGAL_TRANSITIONS: dict[FileStatus, set[FileStatus]] = {
    FileStatus.REGISTERED: {FileStatus.INGESTING},
    FileStatus.INGESTING: {FileStatus.INGESTED},
    FileStatus.INGESTED: {FileStatus.TRANSFORMING},
    FileStatus.TRANSFORMING: {FileStatus.TRANSFORMED},
    FileStatus.TRANSFORMED: set(),    # terminal success
    FileStatus.FAILED: set(),         # terminal failure
}


@dataclass(frozen=True)
class FileFingerprint:
    """
    Everything we capture about a file at registration time.

    Computed once when register_file is called; never mutated. The
    vendor timestamp fields are optional — if no manifest exists, the
    caller passes None for source_modified_at_vendor and the DAO
    records vendor_timestamp_source='filesystem_only' + emits a
    vendor_timestamp_unavailable event.
    """

    path: Path
    size_bytes: int
    checksum_md5: str
    schema_version_hash: str
    source_modified_at_filesystem: str        # always known, ISO8601
    source_modified_at_vendor: str | None = None    # from manifest / HTTP
    vendor_timestamp_source: str | None = None      # 'manifest' | 'http_header' | None


@dataclass(frozen=True)
class AuditRow:
    """Read-only snapshot returned by readers."""

    batch_id: str
    source_name: str
    source_file_path: str
    file_size_bytes: int | None
    file_checksum_md5: str | None
    schema_version_hash: str | None
    source_modified_at_vendor: str | None
    source_modified_at_filesystem: str | None
    vendor_timestamp_source: str | None
    source_row_count: int | None
    bronze_row_count: int | None
    silver_row_count: int | None
    rejected_row_count: int | None
    gold_row_count: int | None
    status: FileStatus
    registered_at: str
    started_at: str | None
    finished_at: str | None
    error_message: str | None
    error_stage: str | None

    @classmethod
    def from_sqlite_row(cls, row: sqlite3.Row) -> AuditRow:
        return cls(
            batch_id=row["batch_id"],
            source_name=row["source_name"],
            source_file_path=row["source_file_path"],
            file_size_bytes=row["file_size_bytes"],
            file_checksum_md5=row["file_checksum_md5"],
            schema_version_hash=row["schema_version_hash"],
            source_modified_at_vendor=row["source_modified_at_vendor"],
            source_modified_at_filesystem=row["source_modified_at_filesystem"],
            vendor_timestamp_source=row["vendor_timestamp_source"],
            source_row_count=row["source_row_count"],
            bronze_row_count=row["bronze_row_count"],
            silver_row_count=row["silver_row_count"],
            rejected_row_count=row["rejected_row_count"],
            gold_row_count=row["gold_row_count"],
            status=FileStatus(row["status"]),
            registered_at=row["registered_at"],
            started_at=row["started_at"],
            finished_at=row["finished_at"],
            error_message=row["error_message"],
            error_stage=row["error_stage"],
        )


@dataclass(frozen=True)
class ReconciliationFinding:
    """One discrepancy from reconcile_batch."""

    batch_id: str
    source_name: str
    source_file_path: str
    severity: str        # "CRITICAL" | "WARN"
    code: str            # short stable identifier
    message: str         # human-readable explanation


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class AuditError(Exception):
    """Base for audit-DAO errors."""


class AuditConflictError(AuditError):
    """register_file called twice with conflicting fingerprints."""


class AuditStateError(AuditError):
    """Illegal state transition attempted."""


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _emit_event(
    conn: sqlite3.Connection,
    *,
    batch_id: str,
    source_file_path: str,
    event_type: EventType,
    payload: dict[str, Any] | None = None,
) -> None:
    """
    Append one row to file_audit_events. Always called inside the
    same connection (and therefore transaction) as the file_audit
    mutation it accompanies.
    """
    conn.execute(
        """
        INSERT INTO file_audit_events
            (batch_id, source_file_path, event_type, event_payload, occurred_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            batch_id,
            source_file_path,
            event_type.value,
            json.dumps(payload) if payload else None,
            _utcnow(),
        ),
    )


def _current_status(
    conn: sqlite3.Connection,
    *,
    batch_id: str,
    source_file_path: str,
) -> FileStatus | None:
    row = conn.execute(
        "SELECT status FROM file_audit WHERE batch_id=? AND source_file_path=?",
        (batch_id, source_file_path),
    ).fetchone()
    return FileStatus(row["status"]) if row else None


def _assert_transition(
    current: FileStatus | None,
    target: FileStatus,
    *,
    batch_id: str,
    source_file_path: str,
) -> None:
    """Raise AuditStateError if (current -> target) is illegal."""
    if current is None:
        raise AuditStateError(
            f"File not registered: batch={batch_id} path={source_file_path}"
        )
    if target not in _LEGAL_TRANSITIONS.get(current, set()):
        raise AuditStateError(
            f"Illegal transition {current.value} -> {target.value} "
            f"for batch={batch_id} path={source_file_path}"
        )


# ---------------------------------------------------------------------------
# Writers
# ---------------------------------------------------------------------------


def register_file(
    *,
    batch_id: str,
    source_name: str,
    fingerprint: FileFingerprint,
) -> None:
    """
    Create the audit row for a file we're about to process.

    Idempotent on same fingerprint: re-registering with identical
    checksum is a no-op. Re-registering with a DIFFERENT checksum
    (vendor sent a corrected file mid-batch) raises AuditConflictError.

    Emits: 'registered' event on first registration.
            'vendor_timestamp_unavailable' event when no vendor timestamp
            is provided (caller passed source_modified_at_vendor=None).
    """
    path_str = str(fingerprint.path)
    vendor_source = fingerprint.vendor_timestamp_source or "filesystem_only"

    with connect() as conn:
        # Use explicit transaction to atomically check + insert + emit.
        # isolation_level=None means we control transactions manually.
        conn.execute("BEGIN")
        try:
            existing = conn.execute(
                """
                SELECT file_checksum_md5 FROM file_audit
                WHERE batch_id=? AND source_file_path=?
                """,
                (batch_id, path_str),
            ).fetchone()

            if existing is not None:
                # Idempotent re-register only if checksum matches.
                if existing["file_checksum_md5"] == fingerprint.checksum_md5:
                    conn.execute("COMMIT")
                    return
                raise AuditConflictError(
                    f"File {path_str} already registered in batch {batch_id} "
                    f"with different checksum (existing="
                    f"{existing['file_checksum_md5']}, "
                    f"new={fingerprint.checksum_md5})"
                )

            now = _utcnow()
            conn.execute(
                """
                INSERT INTO file_audit (
                    batch_id, source_name, source_file_path,
                    file_size_bytes, file_checksum_md5, schema_version_hash,
                    source_modified_at_vendor, source_modified_at_filesystem,
                    vendor_timestamp_source,
                    status, registered_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    batch_id, source_name, path_str,
                    fingerprint.size_bytes,
                    fingerprint.checksum_md5,
                    fingerprint.schema_version_hash,
                    fingerprint.source_modified_at_vendor,
                    fingerprint.source_modified_at_filesystem,
                    vendor_source,
                    FileStatus.REGISTERED.value,
                    now,
                ),
            )
            _emit_event(
                conn,
                batch_id=batch_id,
                source_file_path=path_str,
                event_type=EventType.REGISTERED,
                payload={
                    "source_name": source_name,
                    "size_bytes": fingerprint.size_bytes,
                    "checksum_md5": fingerprint.checksum_md5,
                },
            )
            if fingerprint.source_modified_at_vendor is None:
                _emit_event(
                    conn,
                    batch_id=batch_id,
                    source_file_path=path_str,
                    event_type=EventType.VENDOR_TIMESTAMP_UNAVAILABLE,
                    payload={"filesystem_mtime": fingerprint.source_modified_at_filesystem},
                )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise


def mark_ingesting(
    *,
    batch_id: str,
    source_file_path: str,
) -> None:
    """registered -> ingesting. Sets started_at. Emits 'ingest_started'."""
    path_str = str(source_file_path)
    with connect() as conn:
        conn.execute("BEGIN")
        try:
            current = _current_status(
                conn, batch_id=batch_id, source_file_path=path_str
            )
            _assert_transition(
                current, FileStatus.INGESTING,
                batch_id=batch_id, source_file_path=path_str,
            )
            conn.execute(
                """
                UPDATE file_audit
                SET status=?, started_at=?
                WHERE batch_id=? AND source_file_path=?
                """,
                (FileStatus.INGESTING.value, _utcnow(), batch_id, path_str),
            )
            _emit_event(
                conn,
                batch_id=batch_id,
                source_file_path=path_str,
                event_type=EventType.INGEST_STARTED,
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise


def record_ingestion_complete(
    *,
    batch_id: str,
    source_file_path: str,
    source_row_count: int,
    bronze_row_count: int,
) -> None:
    """
    ingesting -> ingested. Records both row counts in a single UPDATE
    so they cannot be partially recorded. Emits 'ingest_finished'.
    """
    path_str = str(source_file_path)
    with connect() as conn:
        conn.execute("BEGIN")
        try:
            current = _current_status(
                conn, batch_id=batch_id, source_file_path=path_str
            )
            _assert_transition(
                current, FileStatus.INGESTED,
                batch_id=batch_id, source_file_path=path_str,
            )
            conn.execute(
                """
                UPDATE file_audit SET
                    status=?,
                    source_row_count=?,
                    bronze_row_count=?
                WHERE batch_id=? AND source_file_path=?
                """,
                (
                    FileStatus.INGESTED.value,
                    source_row_count, bronze_row_count,
                    batch_id, path_str,
                ),
            )
            _emit_event(
                conn,
                batch_id=batch_id,
                source_file_path=path_str,
                event_type=EventType.INGEST_FINISHED,
                payload={
                    "source_row_count": source_row_count,
                    "bronze_row_count": bronze_row_count,
                },
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise


def record_quarantine(
    *,
    batch_id: str,
    source_name: str,
    rejected_row_count: int,
) -> None:
    """
    DQ has finished and quarantined N rows for this source. Attributes
    rejects at source-grain across ALL files of this source in the batch
    (see module docstring). Does not change status.

    Emits 'dq_completed' on every affected file.
    """
    with connect() as conn:
        conn.execute("BEGIN")
        try:
            file_rows = conn.execute(
                """
                SELECT source_file_path FROM file_audit
                WHERE batch_id=? AND source_name=?
                """,
                (batch_id, source_name),
            ).fetchall()
            if not file_rows:
                raise AuditStateError(
                    f"No files registered for source={source_name} in batch={batch_id}"
                )
            # For multi-file sources we'd split rejected_row_count proportionally
            # by bronze_row_count; with Kaggle's one-file-per-source layout this
            # collapses to a single update. We use total here for transparency.
            conn.execute(
                """
                UPDATE file_audit
                SET rejected_row_count=?
                WHERE batch_id=? AND source_name=?
                """,
                (rejected_row_count, batch_id, source_name),
            )
            for r in file_rows:
                _emit_event(
                    conn,
                    batch_id=batch_id,
                    source_file_path=r["source_file_path"],
                    event_type=EventType.DQ_COMPLETED,
                    payload={"rejected_row_count": rejected_row_count},
                )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise


def mark_transforming(
    *,
    batch_id: str,
    source_name: str,
) -> None:
    """
    ingested -> transforming for every file of this source in the batch.
    Emits 'silver_started' per file.
    """
    with connect() as conn:
        conn.execute("BEGIN")
        try:
            file_rows = conn.execute(
                """
                SELECT source_file_path, status FROM file_audit
                WHERE batch_id=? AND source_name=?
                """,
                (batch_id, source_name),
            ).fetchall()
            if not file_rows:
                raise AuditStateError(
                    f"No files registered for source={source_name} in batch={batch_id}"
                )
            # Validate every file is in 'ingested' state before transitioning ANY.
            # Otherwise we'd leave a half-transitioned source which is the worst
            # of both worlds.
            for r in file_rows:
                _assert_transition(
                    FileStatus(r["status"]),
                    FileStatus.TRANSFORMING,
                    batch_id=batch_id, source_file_path=r["source_file_path"],
                )
            conn.execute(
                """
                UPDATE file_audit SET status=?
                WHERE batch_id=? AND source_name=?
                """,
                (FileStatus.TRANSFORMING.value, batch_id, source_name),
            )
            for r in file_rows:
                _emit_event(
                    conn,
                    batch_id=batch_id,
                    source_file_path=r["source_file_path"],
                    event_type=EventType.SILVER_STARTED,
                )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise


def record_silver_complete(
    *,
    batch_id: str,
    source_name: str,
    silver_row_count: int,
) -> None:
    """
    transforming -> transformed for every file of this source.
    Records silver_row_count at source-grain (same caveat as record_quarantine).
    Emits 'silver_finished'.
    """
    with connect() as conn:
        conn.execute("BEGIN")
        try:
            file_rows = conn.execute(
                """
                SELECT source_file_path, status FROM file_audit
                WHERE batch_id=? AND source_name=?
                """,
                (batch_id, source_name),
            ).fetchall()
            if not file_rows:
                raise AuditStateError(
                    f"No files registered for source={source_name} in batch={batch_id}"
                )
            for r in file_rows:
                _assert_transition(
                    FileStatus(r["status"]),
                    FileStatus.TRANSFORMED,
                    batch_id=batch_id, source_file_path=r["source_file_path"],
                )
            now = _utcnow()
            conn.execute(
                """
                UPDATE file_audit SET
                    status=?,
                    silver_row_count=?,
                    finished_at=?
                WHERE batch_id=? AND source_name=?
                """,
                (
                    FileStatus.TRANSFORMED.value,
                    silver_row_count, now,
                    batch_id, source_name,
                ),
            )
            for r in file_rows:
                _emit_event(
                    conn,
                    batch_id=batch_id,
                    source_file_path=r["source_file_path"],
                    event_type=EventType.SILVER_FINISHED,
                    payload={"silver_row_count": silver_row_count},
                )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise


def record_gold_complete(
    *,
    batch_id: str,
    source_name: str,
    gold_row_count: int,
) -> None:
    """
    Records Gold artifact build success at source-grain.

    Unlike record_silver_complete, this does NOT change file status —
    Gold is an analytical view over already-TRANSFORMED data, not a
    new lifecycle stage. The data's "lifecycle" is complete at
    TRANSFORMED; Gold just derives aggregates from it.

    Two subtleties worth noting:

    1. gold_row_count is set for the PRIMARY Bronze source of the
       Gold artifact (per ADR-0005's source-grain attribution pattern).
       For top_scorers_by_season this is 'appearances'; for
       player_valuation_rolling_avg it's 'player_valuations'.

    2. player_valuations doesn't pass through Silver (it has no
       Silver builder). Its audit row stays at INGESTED status but
       gets a gold_row_count populated, which is the intentional
       lineage record for Bronze→Gold direct sources.

    Emits 'gold_finished' event on every file of this source for the batch.
    """
    with connect() as conn:
        conn.execute("BEGIN")
        try:
            file_rows = conn.execute(
                """
                SELECT source_file_path FROM file_audit
                WHERE batch_id=? AND source_name=?
                """,
                (batch_id, source_name),
            ).fetchall()
            if not file_rows:
                raise AuditStateError(
                    f"No files registered for source={source_name} in batch={batch_id}"
                )
            now = _utcnow()
            conn.execute(
                """
                UPDATE file_audit SET
                    gold_row_count=?,
                    finished_at=?
                WHERE batch_id=? AND source_name=?
                """,
                (gold_row_count, now, batch_id, source_name),
            )
            for r in file_rows:
                _emit_event(
                    conn,
                    batch_id=batch_id,
                    source_file_path=r["source_file_path"],
                    event_type=EventType.GOLD_FINISHED,
                    payload={"gold_row_count": gold_row_count},
                )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise



def mark_failed(
    *,
    batch_id: str,
    source_file_path: str,
    stage: str,
    error_message: str,
) -> None:
    """
    Terminal failure. Records the stage that raised ('bronze' | 'dq' |
    'silver' | 'reconciliation') for triage. Allowed from any
    non-terminal state.

    NEVER raises (except in a truly catastrophic case like DB
    corruption, where re-raising is the lesser evil). Called from
    exception handlers; raising here would lose the original error.
    """
    path_str = str(source_file_path)
    try:
        with connect() as conn:
            conn.execute("BEGIN")
            try:
                # Truncate very long error messages to keep DB tidy.
                truncated = error_message[:4000]
                conn.execute(
                    """
                    UPDATE file_audit SET
                        status=?,
                        error_message=?,
                        error_stage=?,
                        finished_at=?
                    WHERE batch_id=? AND source_file_path=?
                    """,
                    (
                        FileStatus.FAILED.value,
                        truncated, stage, _utcnow(),
                        batch_id, path_str,
                    ),
                )
                _emit_event(
                    conn,
                    batch_id=batch_id,
                    source_file_path=path_str,
                    event_type=EventType.FAILED,
                    payload={"stage": stage, "error": truncated},
                )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
    except Exception as e:
        # Log and swallow — the caller's original exception is what matters.
        log.error(
            "audit_mark_failed_itself_failed",
            batch_id=batch_id,
            source_file_path=path_str,
            error=str(e),
        )


def record_schema_drift(
    *,
    batch_id: str,
    source_file_path: str,
    previous_schema_hash: str,
    current_schema_hash: str,
    columns_added: list[str] | None = None,
    columns_removed: list[str] | None = None,
    dtype_changes: dict[str, tuple[str, str]] | None = None,
) -> None:
    """
    Record schema drift. Informational only — does not change status.
    The DQ task is responsible for deciding whether drift should block
    the load. Emits 'schema_drift_detected'.
    """
    path_str = str(source_file_path)
    payload = {
        "previous_schema_hash": previous_schema_hash,
        "current_schema_hash": current_schema_hash,
        "columns_added": columns_added or [],
        "columns_removed": columns_removed or [],
        "dtype_changes": (
            {k: list(v) for k, v in (dtype_changes or {}).items()}
        ),
    }
    with connect() as conn:
        _emit_event(
            conn,
            batch_id=batch_id,
            source_file_path=path_str,
            event_type=EventType.SCHEMA_DRIFT_DETECTED,
            payload=payload,
        )


# ---------------------------------------------------------------------------
# Readers
# ---------------------------------------------------------------------------


def get_audit_row(
    *,
    batch_id: str,
    source_file_path: str,
) -> AuditRow | None:
    with connect() as conn:
        row = conn.execute(
            """
            SELECT * FROM file_audit
            WHERE batch_id=? AND source_file_path=?
            """,
            (batch_id, str(source_file_path)),
        ).fetchone()
        return AuditRow.from_sqlite_row(row) if row else None


def list_batch_files(
    *,
    batch_id: str,
    status: FileStatus | None = None,
) -> list[AuditRow]:
    sql = "SELECT * FROM file_audit WHERE batch_id=?"
    params: list[Any] = [batch_id]
    if status is not None:
        sql += " AND status=?"
        params.append(status.value)
    sql += " ORDER BY source_name, source_file_path"
    with connect() as conn:
        return [AuditRow.from_sqlite_row(r) for r in conn.execute(sql, params)]


def get_event_timeline(
    *,
    batch_id: str,
    source_file_path: str,
) -> list[dict[str, Any]]:
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT event_type, event_payload, occurred_at
            FROM file_audit_events
            WHERE batch_id=? AND source_file_path=?
            ORDER BY occurred_at, event_id
            """,
            (batch_id, str(source_file_path)),
        ).fetchall()
    return [
        {
            "event_type": r["event_type"],
            "occurred_at": r["occurred_at"],
            "payload": json.loads(r["event_payload"]) if r["event_payload"] else None,
        }
        for r in rows
    ]


def find_previous_successful_ingestion(
    *,
    checksum_md5: str,
) -> AuditRow | None:
    """
    Most-recent audit row for any batch where this checksum reached at
    least 'ingested' status. Used by Bronze as a file-grain idempotency
    pre-flight: if the vendor re-sends an identical file in a later
    batch, we can skip ingestion entirely.
    """
    with connect() as conn:
        row = conn.execute(
            """
            SELECT * FROM file_audit
            WHERE file_checksum_md5 = ?
              AND status IN (?, ?, ?)
            ORDER BY registered_at DESC
            LIMIT 1
            """,
            (
                checksum_md5,
                FileStatus.INGESTED.value,
                FileStatus.TRANSFORMING.value,
                FileStatus.TRANSFORMED.value,
                # not 'failed' — a failed prior ingestion doesn't mean
                # we should skip a fresh one
            ),
        ).fetchone()
        return AuditRow.from_sqlite_row(row) if row else None


def find_most_recent_ingestion_for_source(
    *,
    source_name: str,
    as_of_batch_id: str | None = None,
) -> AuditRow | None:
    """
    Most-recent successful audit row for the given source.

    Used by downstream consumers (Silver runner, DQ FK lookup builder)
    that need to read Bronze data for a source whose current-batch
    partition doesn't exist on disk — typically because file-grain
    idempotency skipped re-writing identical bytes (ADR-0003).

    Ordering semantics
    ------------------
    "Most recent" is defined by `registered_at` timestamp, NOT by
    batch_id. This matters because batch_ids in production can be
    heterogeneous strings — `day-1`, `2026-06-02`, `manual-2026-06-03`,
    etc. — that DON'T sort lexicographically the way you'd expect.
    `"day-2"` is lexicographically GREATER than `"2026-06-02"` (ASCII
    'd' is 0x64; '2' is 0x32), which would cause an `as_of_batch_id`
    filter using `batch_id <=` to wrongly exclude later-named-but-
    earlier-registered batches.

    `registered_at` is always an ISO-8601 UTC timestamp produced by
    `_utcnow()`, so lexicographic ordering matches chronological
    ordering by construction. This was a real bug found during Phase 8
    Airflow integration; the fix and the test that pins it are
    documented in ADR-0008 (cross-batch semantics) and Slice 8.1b.

    If `as_of_batch_id` is provided, we look up the current batch's
    registered_at timestamp, then filter to audit rows with
    registered_at <= that timestamp. For Airflow's scheduled batches
    this is exactly "ingestions that happened at-or-before this
    batch's registration".

    Returns None if no successful ingestion exists for this source.
    Callers decide what to do (raise an error, return empty data,
    fail open). The audit DAO's job is to surface what IS known
    about the data lineage.

    Why source-grain (not file-grain): a source may consist of multiple
    files; we return ONE audit row representing the most recent batch
    where the source was successfully ingested. The caller uses that
    row's batch_id to derive the partition path.
    """
    with connect() as conn:
        if as_of_batch_id is None:
            row = conn.execute(
                """
                SELECT * FROM file_audit
                WHERE source_name = ?
                  AND status IN (?, ?, ?)
                ORDER BY registered_at DESC
                LIMIT 1
                """,
                (
                    source_name,
                    FileStatus.INGESTED.value,
                    FileStatus.TRANSFORMING.value,
                    FileStatus.TRANSFORMED.value,
                ),
            ).fetchone()
        else:
            # Look up the registered_at of the as_of batch first.
            # Using MAX so multi-file sources resolve to the latest
            # registration in that batch.
            as_of_ts_row = conn.execute(
                """
                SELECT MAX(registered_at) AS registered_at
                FROM file_audit
                WHERE batch_id = ?
                """,
                (as_of_batch_id,),
            ).fetchone()
            as_of_ts = (
                as_of_ts_row["registered_at"] if as_of_ts_row else None
            )

            if as_of_ts is None:
                # No audit rows for the as_of batch yet — fall through
                # to the all-batches query. This happens if a Silver
                # task somehow runs without its Bronze counterpart
                # having registered ANY files yet. Conservative
                # behaviour: return the most-recent successful
                # ingestion regardless of batch ordering.
                row = conn.execute(
                    """
                    SELECT * FROM file_audit
                    WHERE source_name = ?
                      AND status IN (?, ?, ?)
                    ORDER BY registered_at DESC
                    LIMIT 1
                    """,
                    (
                        source_name,
                        FileStatus.INGESTED.value,
                        FileStatus.TRANSFORMING.value,
                        FileStatus.TRANSFORMED.value,
                    ),
                ).fetchone()
            else:
                row = conn.execute(
                    """
                    SELECT * FROM file_audit
                    WHERE source_name = ?
                      AND status IN (?, ?, ?)
                      AND registered_at <= ?
                    ORDER BY registered_at DESC
                    LIMIT 1
                    """,
                    (
                        source_name,
                        FileStatus.INGESTED.value,
                        FileStatus.TRANSFORMING.value,
                        FileStatus.TRANSFORMED.value,
                        as_of_ts,
                    ),
                ).fetchone()
        return AuditRow.from_sqlite_row(row) if row else None


def latest_schema_hash(
    *,
    source_name: str,
) -> str | None:
    """
    schema_version_hash of the most recently INGESTED file for this
    source. Returns None on first ever ingestion. Used by drift
    detection to compare incoming files against history.
    """
    with connect() as conn:
        row = conn.execute(
            """
            SELECT schema_version_hash FROM file_audit
            WHERE source_name=?
              AND status IN (?, ?, ?, ?)
            ORDER BY registered_at DESC
            LIMIT 1
            """,
            (
                source_name,
                FileStatus.INGESTED.value,
                FileStatus.TRANSFORMING.value,
                FileStatus.TRANSFORMED.value,
                FileStatus.FAILED.value,
                # Include failed — we still observed that schema
            ),
        ).fetchone()
        return row["schema_version_hash"] if row else None


def list_failed_since(*, since: str) -> list[AuditRow]:
    """
    All files in 'failed' status registered at or after `since`
    (ISO8601 UTC). The single most useful query for triage.
    """
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT * FROM file_audit
            WHERE status=? AND registered_at >= ?
            ORDER BY registered_at DESC
            """,
            (FileStatus.FAILED.value, since),
        ).fetchall()
        return [AuditRow.from_sqlite_row(r) for r in rows]


# ---------------------------------------------------------------------------
# Reconciliation
# ---------------------------------------------------------------------------

# Threshold (fraction of bronze) above which a high reject rate is flagged
# as WARN. 5% is conventional; configurable via config in a later phase.
_HIGH_REJECT_RATE_THRESHOLD = 0.05


def reconcile_batch(
    *,
    batch_id: str,
) -> list[ReconciliationFinding]:
    """
    Audit row-count flow for every file in a batch. See ADR-0001 for
    the rules and rationale.

    Does not raise. Returns a list of findings; the caller decides
    what to do with them (DAG task fails on CRITICAL, dashboard
    surfaces both severities).

    Emits 'reconciled' event per file with the finding count in the
    payload — useful for the timeline view.
    """
    findings: list[ReconciliationFinding] = []
    files = list_batch_files(batch_id=batch_id)
    if not files:
        return findings

    for f in files:
        per_file: list[ReconciliationFinding] = []

        # --- CRITICAL rules ----------------------------------------
        if (
            f.bronze_row_count is not None
            and f.source_row_count is not None
            and f.bronze_row_count > f.source_row_count
        ):
            per_file.append(_make_finding(
                f, "CRITICAL", "bronze_inflated",
                f"Bronze rows ({f.bronze_row_count}) > source rows "
                f"({f.source_row_count}) — invented rows",
            ))

        if (
            f.bronze_row_count is not None
            and f.rejected_row_count is not None
            and f.silver_row_count is not None
        ):
            expected_silver = f.bronze_row_count - f.rejected_row_count
            if f.silver_row_count != expected_silver:
                per_file.append(_make_finding(
                    f, "CRITICAL", "row_count_drift",
                    f"Silver rows ({f.silver_row_count}) != expected "
                    f"({expected_silver}) = bronze ({f.bronze_row_count}) "
                    f"- rejected ({f.rejected_row_count})",
                ))

        if (
            f.bronze_row_count is not None
            and f.bronze_row_count > 0
            and f.silver_row_count == 0
        ):
            per_file.append(_make_finding(
                f, "CRITICAL", "complete_silver_loss",
                f"All {f.bronze_row_count} Bronze rows lost before Silver",
            ))

        # --- WARN rules --------------------------------------------
        if (
            f.bronze_row_count is not None
            and f.bronze_row_count > 0
            and f.rejected_row_count is not None
            and f.rejected_row_count / f.bronze_row_count > _HIGH_REJECT_RATE_THRESHOLD
        ):
            pct = 100 * f.rejected_row_count / f.bronze_row_count
            per_file.append(_make_finding(
                f, "WARN", "high_reject_rate",
                f"DQ rejected {f.rejected_row_count}/{f.bronze_row_count} "
                f"rows ({pct:.1f}%); threshold "
                f"{_HIGH_REJECT_RATE_THRESHOLD * 100:.0f}%",
            ))

        if f.bronze_row_count == 0:
            per_file.append(_make_finding(
                f, "WARN", "empty_source_file",
                f"Source file had 0 rows in Bronze",
            ))

        if f.status not in (FileStatus.TRANSFORMED, FileStatus.FAILED):
            per_file.append(_make_finding(
                f, "WARN", "non_terminal_status",
                f"File did not reach terminal status (still {f.status.value})",
            ))

        findings.extend(per_file)

        # Record one 'reconciled' event per file with the finding count.
        with connect() as conn:
            _emit_event(
                conn,
                batch_id=batch_id,
                source_file_path=f.source_file_path,
                event_type=EventType.RECONCILED,
                payload={
                    "finding_count": len(per_file),
                    "critical_count": sum(1 for x in per_file if x.severity == "CRITICAL"),
                    "warn_count": sum(1 for x in per_file if x.severity == "WARN"),
                },
            )

    return findings


def _make_finding(
    f: AuditRow,
    severity: str,
    code: str,
    message: str,
) -> ReconciliationFinding:
    """Internal helper to construct findings consistently."""
    return ReconciliationFinding(
        batch_id=f.batch_id,
        source_name=f.source_name,
        source_file_path=f.source_file_path,
        severity=severity,
        code=code,
        message=message,
    )
