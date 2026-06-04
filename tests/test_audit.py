"""
Tests for the audit DAO.

Coverage targets:
  - Happy-path lifecycle: register -> ingest -> dq -> silver
  - State machine: illegal transitions raise
  - Idempotent register on identical fingerprint, conflict on differing
  - Vendor-timestamp present vs absent; correct event emitted
  - Event log atomicity: every state change writes an event
  - mark_failed never raises, can be called from any state
  - Reconciliation: each rule fires when expected, doesn't fire otherwise
  - Readers: get_audit_row, list_batch_files, get_event_timeline,
             find_previous_successful_ingestion, latest_schema_hash,
             list_failed_since
"""

from __future__ import annotations

from pathlib import Path

import pytest

from src.metadata import audit
from src.metadata.audit import (
    AuditConflictError,
    AuditStateError,
    EventType,
    FileFingerprint,
    FileStatus,
)
from src.metadata.db import init_db


@pytest.fixture(autouse=True)
def _init_db_for_audit_tests():
    """Every test starts with a fresh DB schema. tmp_path isolation
    from conftest._isolate_metadata_db ensures the DB itself is fresh."""
    init_db()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _fp(
    *,
    path: str = "/data/day1/players.csv",
    size: int = 1024,
    md5: str = "a" * 32,
    schema_hash: str = "b" * 32,
    vendor_ts: str | None = None,
    vendor_source: str | None = None,
    fs_ts: str = "2026-05-31T12:00:00+00:00",
) -> FileFingerprint:
    """Compact builder for a synthetic FileFingerprint."""
    return FileFingerprint(
        path=Path(path),
        size_bytes=size,
        checksum_md5=md5,
        schema_version_hash=schema_hash,
        source_modified_at_filesystem=fs_ts,
        source_modified_at_vendor=vendor_ts,
        vendor_timestamp_source=vendor_source,
    )


def _register(batch: str = "B1", source: str = "players", **fp_kwargs) -> str:
    """Register and return the path used (str)."""
    fp = _fp(**fp_kwargs)
    audit.register_file(batch_id=batch, source_name=source, fingerprint=fp)
    return str(fp.path)


# ---------------------------------------------------------------------------
# Happy-path lifecycle
# ---------------------------------------------------------------------------


class TestHappyPath:
    def test_full_lifecycle_for_single_source(self):
        path = _register()

        audit.mark_ingesting(batch_id="B1", source_file_path=path)
        audit.record_ingestion_complete(
            batch_id="B1",
            source_file_path=path,
            source_row_count=1000,
            bronze_row_count=1000,
        )
        audit.record_quarantine(
            batch_id="B1",
            source_name="players",
            rejected_row_count=5,
        )
        audit.mark_transforming(batch_id="B1", source_name="players")
        audit.record_silver_complete(
            batch_id="B1",
            source_name="players",
            silver_row_count=995,
        )

        row = audit.get_audit_row(batch_id="B1", source_file_path=path)
        assert row is not None
        assert row.status == FileStatus.TRANSFORMED
        assert row.source_row_count == 1000
        assert row.bronze_row_count == 1000
        assert row.rejected_row_count == 5
        assert row.silver_row_count == 995
        assert row.finished_at is not None
        assert row.error_message is None

    def test_lifecycle_emits_expected_events_in_order(self):
        # Pass a vendor timestamp so the optional vendor_timestamp_unavailable
        # event doesn't fire and pollute the standard lifecycle sequence.
        # (That event has its own dedicated tests in TestRegistration.)
        path = _register(
            vendor_ts="2025-05-21T09:34:17+00:00",
            vendor_source="manifest",
        )
        audit.mark_ingesting(batch_id="B1", source_file_path=path)
        audit.record_ingestion_complete(
            batch_id="B1",
            source_file_path=path,
            source_row_count=10,
            bronze_row_count=10,
        )
        audit.record_quarantine(
            batch_id="B1",
            source_name="players",
            rejected_row_count=0,
        )
        audit.mark_transforming(batch_id="B1", source_name="players")
        audit.record_silver_complete(
            batch_id="B1",
            source_name="players",
            silver_row_count=10,
        )

        events = audit.get_event_timeline(batch_id="B1", source_file_path=path)
        types = [e["event_type"] for e in events]
        assert types == [
            EventType.REGISTERED.value,
            EventType.INGEST_STARTED.value,
            EventType.INGEST_FINISHED.value,
            EventType.DQ_COMPLETED.value,
            EventType.SILVER_STARTED.value,
            EventType.SILVER_FINISHED.value,
        ]

    def test_ingest_finished_payload_carries_counts(self):
        path = _register()
        audit.mark_ingesting(batch_id="B1", source_file_path=path)
        audit.record_ingestion_complete(
            batch_id="B1",
            source_file_path=path,
            source_row_count=42,
            bronze_row_count=40,
        )
        events = audit.get_event_timeline(batch_id="B1", source_file_path=path)
        finished = next(e for e in events if e["event_type"] == "ingest_finished")
        assert finished["payload"] == {
            "source_row_count": 42,
            "bronze_row_count": 40,
        }


# ---------------------------------------------------------------------------
# State machine enforcement
# ---------------------------------------------------------------------------


class TestStateMachine:
    def test_mark_ingesting_unregistered_file_raises(self):
        with pytest.raises(AuditStateError, match="not registered"):
            audit.mark_ingesting(batch_id="B1", source_file_path="/nope")

    def test_cannot_skip_ingesting_state(self):
        path = _register()
        with pytest.raises(AuditStateError, match="Illegal transition"):
            audit.record_ingestion_complete(
                batch_id="B1",
                source_file_path=path,
                source_row_count=1,
                bronze_row_count=1,
            )

    def test_cannot_re_ingest_after_completion(self):
        path = _register()
        audit.mark_ingesting(batch_id="B1", source_file_path=path)
        audit.record_ingestion_complete(
            batch_id="B1",
            source_file_path=path,
            source_row_count=1,
            bronze_row_count=1,
        )
        with pytest.raises(AuditStateError):
            audit.mark_ingesting(batch_id="B1", source_file_path=path)

    def test_mark_transforming_fails_if_not_ingested(self):
        _register()
        # File is still 'registered', not 'ingested'
        with pytest.raises(AuditStateError):
            audit.mark_transforming(batch_id="B1", source_name="players")

    def test_mark_transforming_with_no_files_raises(self):
        # source_name has no registered files at all
        with pytest.raises(AuditStateError, match="No files registered"):
            audit.mark_transforming(batch_id="B1", source_name="ghost_source")


# ---------------------------------------------------------------------------
# Registration idempotency / conflict
# ---------------------------------------------------------------------------


class TestRegistration:
    def test_re_register_same_fingerprint_is_noop(self):
        _register(md5="x" * 32)
        # Second call with the same checksum should not raise and should
        # not change the row.
        _register(md5="x" * 32)
        files = audit.list_batch_files(batch_id="B1")
        assert len(files) == 1

    def test_re_register_different_checksum_raises_conflict(self):
        _register(md5="x" * 32)
        with pytest.raises(AuditConflictError, match="different checksum"):
            _register(md5="y" * 32)

    def test_register_with_vendor_timestamp_records_it(self):
        path = _register(
            vendor_ts="2025-05-21T09:34:17+00:00",
            vendor_source="manifest",
        )
        row = audit.get_audit_row(batch_id="B1", source_file_path=path)
        assert row.source_modified_at_vendor == "2025-05-21T09:34:17+00:00"
        assert row.vendor_timestamp_source == "manifest"

    def test_register_without_vendor_timestamp_emits_event(self):
        path = _register()  # no vendor_ts
        events = audit.get_event_timeline(batch_id="B1", source_file_path=path)
        event_types = [e["event_type"] for e in events]
        assert EventType.VENDOR_TIMESTAMP_UNAVAILABLE.value in event_types
        row = audit.get_audit_row(batch_id="B1", source_file_path=path)
        assert row.vendor_timestamp_source == "filesystem_only"

    def test_register_with_vendor_timestamp_does_not_emit_unavailable_event(self):
        path = _register(
            vendor_ts="2025-05-21T09:34:17+00:00",
            vendor_source="manifest",
        )
        events = audit.get_event_timeline(batch_id="B1", source_file_path=path)
        event_types = [e["event_type"] for e in events]
        assert EventType.VENDOR_TIMESTAMP_UNAVAILABLE.value not in event_types


# ---------------------------------------------------------------------------
# mark_failed semantics
# ---------------------------------------------------------------------------


class TestMarkFailed:
    def test_mark_failed_from_registered(self):
        path = _register()
        audit.mark_failed(
            batch_id="B1",
            source_file_path=path,
            stage="bronze",
            error_message="exploded",
        )
        row = audit.get_audit_row(batch_id="B1", source_file_path=path)
        assert row.status == FileStatus.FAILED
        assert row.error_stage == "bronze"
        assert row.error_message == "exploded"

    def test_mark_failed_from_ingesting(self):
        path = _register()
        audit.mark_ingesting(batch_id="B1", source_file_path=path)
        audit.mark_failed(
            batch_id="B1",
            source_file_path=path,
            stage="bronze",
            error_message="mid-ingest crash",
        )
        row = audit.get_audit_row(batch_id="B1", source_file_path=path)
        assert row.status == FileStatus.FAILED

    def test_mark_failed_truncates_long_messages(self):
        path = _register()
        long_error = "x" * 6000
        audit.mark_failed(
            batch_id="B1",
            source_file_path=path,
            stage="bronze",
            error_message=long_error,
        )
        row = audit.get_audit_row(batch_id="B1", source_file_path=path)
        assert row.error_message is not None
        assert len(row.error_message) == 4000

    def test_mark_failed_on_unknown_file_does_not_raise(self):
        # The contract: mark_failed never raises. If the file doesn't
        # exist in the audit, the UPDATE is a no-op and we log internally.
        audit.mark_failed(
            batch_id="B1",
            source_file_path="/never/registered.csv",
            stage="bronze",
            error_message="orphan",
        )
        # No assertion needed — we just verify no exception is raised.

    def test_failed_emits_event_with_stage(self):
        path = _register()
        audit.mark_failed(
            batch_id="B1",
            source_file_path=path,
            stage="silver",
            error_message="boom",
        )
        events = audit.get_event_timeline(batch_id="B1", source_file_path=path)
        failed_event = next(e for e in events if e["event_type"] == "failed")
        assert failed_event["payload"]["stage"] == "silver"


# ---------------------------------------------------------------------------
# Readers
# ---------------------------------------------------------------------------


class TestReaders:
    def test_get_audit_row_returns_none_for_unknown(self):
        assert audit.get_audit_row(batch_id="B1", source_file_path="/nope.csv") is None

    def test_list_batch_files_filters_by_status(self):
        _register(path="/a.csv", md5="1" * 32)
        path_b = _register(path="/b.csv", md5="2" * 32)
        audit.mark_ingesting(batch_id="B1", source_file_path=path_b)

        all_files = audit.list_batch_files(batch_id="B1")
        registered_only = audit.list_batch_files(batch_id="B1", status=FileStatus.REGISTERED)
        ingesting_only = audit.list_batch_files(batch_id="B1", status=FileStatus.INGESTING)
        assert len(all_files) == 2
        assert len(registered_only) == 1
        assert len(ingesting_only) == 1
        assert registered_only[0].source_file_path == "/a.csv"

    def test_find_previous_successful_ingestion(self):
        # Batch 1: file ingests successfully
        path1 = _register(path="/p.csv", md5="abc")
        audit.mark_ingesting(batch_id="B1", source_file_path=path1)
        audit.record_ingestion_complete(
            batch_id="B1",
            source_file_path=path1,
            source_row_count=10,
            bronze_row_count=10,
        )

        # Now the same checksum appears in a future batch's lookup.
        prev = audit.find_previous_successful_ingestion(checksum_md5="abc")
        assert prev is not None
        assert prev.batch_id == "B1"

    def test_find_previous_successful_returns_none_for_unknown_checksum(self):
        assert audit.find_previous_successful_ingestion(checksum_md5="never_seen") is None

    def test_find_previous_excludes_failed_ingestions(self):
        # A FAILED prior ingestion must not count as "already done"
        path = _register(md5="abc")
        audit.mark_failed(
            batch_id="B1",
            source_file_path=path,
            stage="bronze",
            error_message="boom",
        )
        prev = audit.find_previous_successful_ingestion(checksum_md5="abc")
        assert prev is None

    def test_latest_schema_hash_returns_none_for_unknown_source(self):
        assert audit.latest_schema_hash(source_name="unknown") is None

    def test_latest_schema_hash_finds_most_recent(self):
        path = _register(schema_hash="schema_v1")
        audit.mark_ingesting(batch_id="B1", source_file_path=path)
        audit.record_ingestion_complete(
            batch_id="B1",
            source_file_path=path,
            source_row_count=1,
            bronze_row_count=1,
        )
        assert audit.latest_schema_hash(source_name="players") == "schema_v1"

    def test_list_failed_since(self):
        path = _register()
        audit.mark_failed(
            batch_id="B1",
            source_file_path=path,
            stage="bronze",
            error_message="boom",
        )
        # Use a date guaranteed to be earlier than the test's UTC now
        old = "2000-01-01T00:00:00+00:00"
        future = "2999-01-01T00:00:00+00:00"
        assert len(audit.list_failed_since(since=old)) == 1
        assert len(audit.list_failed_since(since=future)) == 0


# ---------------------------------------------------------------------------
# Schema drift
# ---------------------------------------------------------------------------


class TestSchemaDrift:
    def test_record_schema_drift_emits_event_with_diff(self):
        path = _register()
        audit.record_schema_drift(
            batch_id="B1",
            source_file_path=path,
            previous_schema_hash="aaa",
            current_schema_hash="bbb",
            columns_added=["xg"],
            columns_removed=[],
            dtype_changes={"minutes_played": ("int", "float")},
        )
        events = audit.get_event_timeline(batch_id="B1", source_file_path=path)
        drift = next(e for e in events if e["event_type"] == "schema_drift_detected")
        assert drift["payload"]["columns_added"] == ["xg"]
        assert drift["payload"]["dtype_changes"] == {"minutes_played": ["int", "float"]}


# ---------------------------------------------------------------------------
# Reconciliation
# ---------------------------------------------------------------------------


def _full_lifecycle(
    *,
    path: str = "/p.csv",
    source: str = "players",
    source_rows: int = 100,
    bronze_rows: int = 100,
    rejected_rows: int = 0,
    silver_rows: int = 100,
    md5: str = "rc" + "0" * 30,
) -> str:
    """Run a full lifecycle with explicit counts. Helper for reconciliation tests."""
    fp = _fp(path=path, md5=md5)
    audit.register_file(batch_id="B1", source_name=source, fingerprint=fp)
    audit.mark_ingesting(batch_id="B1", source_file_path=str(fp.path))
    audit.record_ingestion_complete(
        batch_id="B1",
        source_file_path=str(fp.path),
        source_row_count=source_rows,
        bronze_row_count=bronze_rows,
    )
    audit.record_quarantine(
        batch_id="B1",
        source_name=source,
        rejected_row_count=rejected_rows,
    )
    audit.mark_transforming(batch_id="B1", source_name=source)
    audit.record_silver_complete(
        batch_id="B1",
        source_name=source,
        silver_row_count=silver_rows,
    )
    return str(fp.path)


class TestReconciliation:
    def test_clean_run_yields_no_findings(self):
        _full_lifecycle(
            source_rows=100,
            bronze_rows=100,
            rejected_rows=0,
            silver_rows=100,
        )
        findings = audit.reconcile_batch(batch_id="B1")
        assert findings == []

    def test_bronze_inflated_is_critical(self):
        _full_lifecycle(
            source_rows=100,
            bronze_rows=110,
            rejected_rows=0,
            silver_rows=110,
        )
        findings = audit.reconcile_batch(batch_id="B1")
        codes = [f.code for f in findings]
        assert "bronze_inflated" in codes
        critical = [f for f in findings if f.code == "bronze_inflated"]
        assert critical[0].severity == "CRITICAL"

    def test_row_count_drift_is_critical(self):
        _full_lifecycle(
            source_rows=100,
            bronze_rows=100,
            rejected_rows=5,
            silver_rows=80,  # expected 95
        )
        findings = audit.reconcile_batch(batch_id="B1")
        codes = [f.code for f in findings]
        assert "row_count_drift" in codes
        drift = next(f for f in findings if f.code == "row_count_drift")
        assert drift.severity == "CRITICAL"

    def test_complete_silver_loss_is_critical(self):
        _full_lifecycle(
            source_rows=100,
            bronze_rows=100,
            rejected_rows=100,
            silver_rows=0,
        )
        findings = audit.reconcile_batch(batch_id="B1")
        codes = [f.code for f in findings]
        # row_count_drift CAN coexist (0 != 100-100=0... no, 0==0, so no drift)
        # so the complete_silver_loss is the primary signal.
        assert "complete_silver_loss" in codes

    def test_high_reject_rate_is_warn(self):
        _full_lifecycle(
            source_rows=100,
            bronze_rows=100,
            rejected_rows=10,
            silver_rows=90,
        )
        findings = audit.reconcile_batch(batch_id="B1")
        rates = [f for f in findings if f.code == "high_reject_rate"]
        assert len(rates) == 1
        assert rates[0].severity == "WARN"

    def test_empty_source_is_warn(self):
        _full_lifecycle(
            source_rows=0,
            bronze_rows=0,
            rejected_rows=0,
            silver_rows=0,
        )
        findings = audit.reconcile_batch(batch_id="B1")
        codes = [f.code for f in findings]
        assert "empty_source_file" in codes
        empty = next(f for f in findings if f.code == "empty_source_file")
        assert empty.severity == "WARN"

    def test_non_terminal_status_is_warn(self):
        # Register a file but never finish it
        path = _register()
        audit.mark_ingesting(batch_id="B1", source_file_path=path)
        findings = audit.reconcile_batch(batch_id="B1")
        codes = [f.code for f in findings]
        assert "non_terminal_status" in codes

    def test_reconcile_emits_reconciled_event(self):
        path = _full_lifecycle()
        audit.reconcile_batch(batch_id="B1")
        events = audit.get_event_timeline(batch_id="B1", source_file_path=path)
        assert any(e["event_type"] == "reconciled" for e in events)

    def test_reconcile_empty_batch_returns_empty(self):
        assert audit.reconcile_batch(batch_id="non_existent_batch") == []
