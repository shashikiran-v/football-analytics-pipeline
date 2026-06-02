"""
Gold artifact builders.

`build_gold_artifact` executes one artifact's SQL through DuckDB and
materialises the result to Parquet at:

    <gold_root>/<artifact_name>/batch_id=<batch_id>/<part>.parquet

Same Hive-partitioned layout as Bronze and Silver. Each Gold partition
is one batch's snapshot of that artifact.

The builder is intentionally thin — DuckDB does the heavy lifting via
the artifact's SQL. The builder's responsibility is just:
  1. Execute the query
  2. Add the batch_id partition column
  3. Write to the right path
  4. Return the row count

The Gold runner (Slice 5.3) composes these into a full batch lifecycle
with audit integration and continue-on-failure semantics.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb
import pandas as pd

from src.gold.artifacts import GoldArtifact
from src.utils.logging import get_logger


log = get_logger(__name__)


# Same convention as Bronze/Silver — plain `batch_id`, not underscore-
# prefixed (avoid the pyarrow hidden-file trap from ADR-0003).
BATCH_ID_COLUMN = "batch_id"


@dataclass(frozen=True)
class GoldBuildResult:
    """Outcome of building one Gold artifact."""

    artifact_name: str
    row_count: int
    output_path: Path
    sources: list[str]


def build_gold_artifact(
    *,
    artifact: GoldArtifact,
    conn: duckdb.DuckDBPyConnection,
    gold_root: Path,
    batch_id: str,
) -> GoldBuildResult:
    """
    Execute the artifact's SQL against the DuckDB session and write the
    result to a Hive-partitioned Parquet directory.

    Args:
        artifact:   the GoldArtifact whose SQL we'll execute
        conn:       active DuckDB connection with Silver views registered
        gold_root:  config.paths.gold (root of all Gold artifacts)
        batch_id:   partition key for this run

    Returns:
        GoldBuildResult with the row count and output path.

    Raises:
        Anything DuckDB raises (SQL syntax errors, missing views,
        type mismatches). The Gold runner catches these per-artifact
        for continue-on-failure semantics.
    """
    log.info(
        "gold_artifact_build_starting",
        artifact=artifact.name,
        sources=artifact.sources,
    )
    df = conn.execute(artifact.sql).fetchdf()
    row_count = len(df)

    # Append the partition column. Same pattern as Bronze writer.
    df_with_batch = df.copy()
    df_with_batch[BATCH_ID_COLUMN] = batch_id

    # Write Hive-partitioned. We use pandas + pyarrow for consistency
    # with the rest of the codebase — Bronze and Silver use the engine
    # protocol's write_parquet, but Gold's output is always Pandas
    # (DuckDB.fetchdf returns Pandas) so we go direct.
    output_dir = gold_root / artifact.name
    output_dir.mkdir(parents=True, exist_ok=True)
    df_with_batch.to_parquet(
        output_dir,
        engine="pyarrow",
        partition_cols=[BATCH_ID_COLUMN],
        existing_data_behavior="overwrite_or_ignore",
    )

    log.info(
        "gold_artifact_built",
        artifact=artifact.name,
        rows=row_count,
        output_path=str(output_dir),
    )
    return GoldBuildResult(
        artifact_name=artifact.name,
        row_count=row_count,
        output_path=output_dir,
        sources=list(artifact.sources),
    )
