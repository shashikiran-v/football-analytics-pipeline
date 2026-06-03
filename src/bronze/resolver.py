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
from src.metadata.audit import AuditRow, FileStatus
from src.metadata.db import connect
from src.utils.logging import get_logger


log = get_logger(__name__)


def _find_prior_ingestion_by_checksum_excluding_batch(
    *,
    checksum_md5: str,
    excluded_batch_id: str,
) -> AuditRow | None:
    """
    Find a successful ingestion of the given checksum from any batch
    OTHER than the excluded one. Used by the resolver to follow a
    file-grain-skip back to the batch where the data actually lives
    on disk (the skipped batch records the same checksum but no
    parquet was written).

    Returns the most recently registered prior batch, or None.
    """
    with connect() as conn:
        row = conn.execute(
            """
            SELECT * FROM file_audit
            WHERE file_checksum_md5 = ?
              AND batch_id != ?
              AND status IN (?, ?, ?)
            ORDER BY registered_at DESC
            LIMIT 1
            """,
            (
                checksum_md5,
                excluded_batch_id,
                FileStatus.INGESTED.value,
                FileStatus.TRANSFORMING.value,
                FileStatus.TRANSFORMED.value,
            ),
        ).fetchone()
        return AuditRow.from_sqlite_row(row) if row else None


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
    # because of identical bytes from a prior batch. Find the current
    # batch's audit row (if any) to get the checksum.
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

    # If the audit DAO points us at a batch that's DIFFERENT from the
    # current one, that's the file-grain-skip case — the data lives
    # under the prior batch. If it's the SAME batch, the data should
    # be on disk (we already checked) — so this case is a real gap.
    if current_audit.batch_id == batch_id and current_audit.file_checksum_md5:
        # Follow the checksum to a *different* batch where this data
        # was first ingested (and thus where it lives on disk). We
        # search the audit DAO for ANY successful ingestion of this
        # checksum from a batch OTHER than the current one.
        prior = _find_prior_ingestion_by_checksum_excluding_batch(
            checksum_md5=current_audit.file_checksum_md5,
            excluded_batch_id=batch_id,
        )
        if prior is None:
            log.warning(
                "bronze_partition_audit_disk_mismatch",
                source=source_name,
                batch_id=batch_id,
                reason="audit claims ingested in current batch but disk path missing, "
                       "no prior ingestion of this checksum found in any other batch",
            )
            return None
        candidate_batch = prior.batch_id
    else:
        # Different batch — use as-is.
        candidate_batch = current_audit.batch_id

    fallback_partition = bronze_root / source_name / f"batch_id={candidate_batch}"
    if not fallback_partition.is_dir():
        log.warning(
            "bronze_partition_audit_disk_mismatch",
            source=source_name,
            batch_id=batch_id,
            audit_says_batch=candidate_batch,
            expected_path=str(fallback_partition),
        )
        return None

    log.info(
        "bronze_partition_resolved_via_audit",
        source=source_name,
        requested_batch=batch_id,
        resolved_to_batch=candidate_batch,
        reason="current-batch partition missing (likely file-grain skip)",
    )
    return fallback_partition
