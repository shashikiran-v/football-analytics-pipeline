"""
DQ quarantine writer.

Writes failing rows to data/lake/_rejected/<source>/batch_id=<id>/
using the same Hive-partitioned style as Bronze and Silver. Each
rejected row carries a _dq_failure_reason column populated by the
DQ runner.

This is intentionally a tiny module — quarantine is just "write
the failing_rows DataFrame from DQResult to a _rejected/ path with
the right partition layout."
"""

from __future__ import annotations

from pathlib import Path

from src.dq.runner import DQResult
from src.engines.base import DataFrameEngine
from src.utils.logging import get_logger


log = get_logger(__name__)


# The partition column matches Bronze/Silver convention (no underscore
# prefix; see ADR-0003 for the underscore-prefix trap).
BATCH_ID_COLUMN = "batch_id"


def quarantine_rejected_rows(
    *,
    dq_result: DQResult,
    rejected_root: Path,
    batch_id: str,
    engine: DataFrameEngine,
) -> Path | None:
    """
    Write failing rows from a DQResult to a Hive-partitioned _rejected/
    parquet directory.

    Returns the output path if rows were written, or None if there
    were no failing rows for this source.

    Output layout:
      <rejected_root>/<source_name>/batch_id=<batch_id>/<part>.parquet
    """
    if dq_result.failing_rows is None:
        log.info(
            "quarantine_skipped_no_failures",
            source=dq_result.source_name,
        )
        return None

    failing_count = engine.count(dq_result.failing_rows)
    if failing_count == 0:
        return None

    # Append the partition column. Same pattern as Bronze writer.
    df_with_batch = engine.with_constant_column(
        dq_result.failing_rows, BATCH_ID_COLUMN, batch_id,
    )
    output_dir = rejected_root / dq_result.source_name
    engine.write_parquet(
        df_with_batch,
        output_dir,
        partition_by=[BATCH_ID_COLUMN],
        mode="overwrite",
    )
    log.info(
        "quarantine_written",
        source=dq_result.source_name,
        rows_quarantined=failing_count,
        output_path=str(output_dir),
    )
    return output_dir
