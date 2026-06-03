"""
Bronze partition resolution with cross-batch fallback.

Why this module exists
----------------------
File-grain idempotency (ADR-0003) means an unchanged file's bytes are
NOT re-written under the new batch's partition. The data continues to
live under the original ingestion's partition.

This creates a contract gap: downstream consumers (Silver runner, DQ
FK lookup builder) that assume "Bronze data for batch X lives under
batch_id=X" will fail to find data for sources that were file-grain
skipped.

The fix is to consult the audit DAO. The audit DAO knows where each
source was last successfully ingested. `resolve_bronze_partition`
returns the right path: the current-batch partition if it exists,
or the most-recent prior batch's partition if not.

This module was added in Phase 6 (Day-2 incremental) where the issue
was surfaced. See ADR-0008 for the full design discussion.
"""

from __future__ import annotations

from pathlib import Path

from src.metadata import audit
from src.metadata.audit import FileStatus
from src.metadata.db import connect
from src.utils.logging import get_logger


log = get_logger(__name__)


def _find_all_batches_with_checksum(
    *,
    checksum_md5: str,
    excluded_batch_id: str,
) -> list[str]:
    """
    Return all batch_ids that have a successful ingestion of the given
    checksum, EXCLUDING the named batch. Ordered most-recent-first.

    Used by the resolver to walk back through a chain of file-grain
    skips: when the current batch's file was skipped (matching some
    prior batch), the data lives wherever the chain's first 'written'
    ingestion put it. We don't know that batch_id in advance, so we
    try each candidate (most recent first) and return the first one
    whose partition exists on disk.
    """
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT DISTINCT batch_id, MAX(registered_at) as registered_at
            FROM file_audit
            WHERE file_checksum_md5 = ?
              AND batch_id != ?
              AND status IN (?, ?, ?)
            GROUP BY batch_id
            ORDER BY registered_at DESC
            """,
            (
                checksum_md5,
                excluded_batch_id,
                FileStatus.INGESTED.value,
                FileStatus.TRANSFORMING.value,
                FileStatus.TRANSFORMED.value,
            ),
        ).fetchall()
        return [r["batch_id"] for r in rows]


def resolve_bronze_partition(
    *,
    bronze_root: Path,
    source_name: str,
    batch_id: str,
) -> Path | None:
    """
    Return the partition path that contains this source's Bronze data
    for the given batch — falling back to a prior batch if the current
    partition was skipped via file-grain idempotency.

    Logic:
      1. If <bronze_root>/<source>/batch_id=<batch_id>/ exists, return it.
      2. Otherwise, the source was file-grain-skipped in this batch.
         Find the current batch's audit row, get the file's checksum,
         then ask the audit DAO for the most-recent batch where that
         checksum was *actually* ingested (i.e. data exists on disk).
         That earlier batch's partition contains the data.
      3. If none of the above works, return None.

    Callers decide what to do with None — Silver runner will record a
    per-artifact failure; DQ FK lookup builder will treat the lookup
    as absent (triggering the FK rule's fail-open path).

    Why this complexity is necessary
    --------------------------------
    File-grain idempotency (ADR-0003) records an audit row for the
    current batch when skipping (status=INGESTED, bronze_row_count
    populated) BUT does not re-write the parquet under the new batch's
    partition. The audit row alone is insufficient to find the data;
    we need to follow the checksum back to the batch where it WAS
    actually written.
    """
    current_partition = bronze_root / source_name / f"batch_id={batch_id}"
    if current_partition.is_dir():
        return current_partition

    # Current-batch partition missing. The source either (a) wasn't
    # ingested in this batch at all, or (b) was file-grain-skipped
    # because of identical bytes from a prior batch. Find the most
    # recent successful ingestion of this source at-or-before this
    # batch to get the file's checksum.
    current_audit = audit.find_most_recent_ingestion_for_source(
        source_name=source_name,
        as_of_batch_id=batch_id,
    )
    if current_audit is None:
        log.warning(
            "bronze_partition_unresolvable",
            source=source_name,
            batch_id=batch_id,
            reason="no prior ingestion found in audit DAO",
        )
        return None

    # We have a checksum. Now find ALL batches that ingested this
    # checksum and try each one's partition until we find one that
    # exists on disk. This handles chains of file-grain skips:
    # day-1 wrote the bytes; day-2 skipped; 2026-06-02 also skipped;
    # the resolver needs to walk back to day-1.
    checksum = current_audit.file_checksum_md5
    if not checksum:
        # Defensive: if the audit row lacks a checksum (data corruption,
        # legacy row), we can't follow the chain.
        log.warning(
            "bronze_partition_audit_no_checksum",
            source=source_name,
            batch_id=batch_id,
            audit_batch=current_audit.batch_id,
        )
        return None

    candidate_batches = _find_all_batches_with_checksum(
        checksum_md5=checksum,
        excluded_batch_id=batch_id,
    )
    for candidate_batch in candidate_batches:
        fallback_partition = bronze_root / source_name / f"batch_id={candidate_batch}"
        if fallback_partition.is_dir():
            log.info(
                "bronze_partition_resolved_via_audit",
                source=source_name,
                requested_batch=batch_id,
                resolved_to_batch=candidate_batch,
                reason="current-batch partition missing (likely file-grain skip)",
            )
            return fallback_partition

    # No candidate batch's partition exists on disk. The audit DAO
    # claims data was ingested but it's not findable anywhere.
    log.warning(
        "bronze_partition_audit_disk_mismatch",
        source=source_name,
        batch_id=batch_id,
        reason=(
            f"checksum {checksum[:8]} known in audit DAO but no batch's "
            f"partition exists on disk; tried {len(candidate_batches)} candidate(s)"
        ),
        candidate_batches=candidate_batches,
    )
    return None
