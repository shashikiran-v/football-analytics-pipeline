"""
DuckDB session management for the Gold layer.

A "session" is a single DuckDB connection used for the duration of one
Gold run. At session creation, every Silver artifact directory is
registered as a DuckDB view that queries the underlying Parquet:

    CREATE OR REPLACE VIEW dim_players AS
        SELECT * FROM read_parquet('data/lake/silver/dim_players/**/*.parquet');

After this, Gold artifact queries reference Silver tables by name —
`dim_players`, `fact_appearances`, etc. — without knowing the file
paths. The session also registers Bronze player_valuations directly
(since we deliberately have no Silver layer for it; the rolling-average
artifact reads it from Bronze).

Why one session per run, not per artifact
------------------------------------------
Registering views is cheap, but doing it 5 times is wasteful — and a
shared session means DuckDB can cache the Parquet schema and
column statistics across queries. The Gold runner creates the session
once at the start of a batch, passes it to every artifact's builder,
and closes it at the end.

Why DuckDB at all
-----------------
DuckDB is the right tool for analytical SQL over Parquet. Its window
functions (ROW_NUMBER, AVG OVER PARTITION BY) handle the brief's
rolling-average requirement natively and fast. Pandas would require
careful sorting + edge-case handling at partition boundaries; SQL
gets it right in one line.

DuckDB also gives reviewers a SQL interface they can demo in 30
seconds — `SELECT * FROM top_scorers_by_season LIMIT 10` is the
analyst-facing payoff of the whole Medallion architecture.
"""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

import duckdb

from src.utils.logging import get_logger


log = get_logger(__name__)


# The Silver tables registered as DuckDB views.
# Each entry: view_name -> Silver subdirectory name.
# The view name is what Gold queries reference; the subdir is where the
# Parquet partitions live under paths.silver.
SILVER_VIEWS: dict[str, str] = {
    "dim_clubs":         "dim_clubs",
    "dim_competitions":  "dim_competitions",
    "dim_date":          "dim_date",
    "dim_players":       "dim_players",
    "fact_games":        "fact_games",
    "fact_appearances":  "fact_appearances",
}

# Bronze sources that Gold queries directly (no Silver layer exists).
# Currently just player_valuations — see ADR-0005 for why it never got
# a Silver builder.
BRONZE_DIRECT_VIEWS: dict[str, str] = {
    "bronze_player_valuations": "player_valuations",
}


def _make_view_sql(view_name: str, parquet_glob: str) -> str:
    """Build the CREATE VIEW SQL for one Silver/Bronze table."""
    return (
        f"CREATE OR REPLACE VIEW {view_name} AS "
        f"SELECT * FROM read_parquet('{parquet_glob}')"
    )


def register_views(
    conn: duckdb.DuckDBPyConnection,
    *,
    silver_root: Path,
    bronze_root: Path,
) -> dict[str, str]:
    """
    Register all Silver (and direct-Bronze) tables as DuckDB views on
    an existing connection.

    Returns a dict of view_name -> parquet_glob_used (for logging /
    debugging). Skips any table whose directory doesn't exist on disk —
    a Gold run on a fresh project will succeed for the views whose
    Silver data exists and fail clearly for the artifacts that need
    missing tables.
    """
    registered: dict[str, str] = {}

    for view_name, subdir in SILVER_VIEWS.items():
        artifact_dir = silver_root / subdir
        if not artifact_dir.is_dir():
            log.warning(
                "gold_view_skipped_no_silver_data",
                view=view_name, expected_dir=str(artifact_dir),
            )
            continue
        # The **/*.parquet glob pulls every partition under the artifact.
        # We let DuckDB scan all partitions and rely on predicate
        # pushdown for filtering by batch_id at query time if needed.
        glob = f"{artifact_dir}/**/*.parquet"
        conn.execute(_make_view_sql(view_name, glob))
        registered[view_name] = glob

    for view_name, subdir in BRONZE_DIRECT_VIEWS.items():
        artifact_dir = bronze_root / subdir
        if not artifact_dir.is_dir():
            log.warning(
                "gold_view_skipped_no_bronze_data",
                view=view_name, expected_dir=str(artifact_dir),
            )
            continue
        glob = f"{artifact_dir}/**/*.parquet"
        conn.execute(_make_view_sql(view_name, glob))
        registered[view_name] = glob

    log.info(
        "gold_views_registered",
        view_count=len(registered),
        views=sorted(registered.keys()),
    )
    return registered


@contextmanager
def gold_session(
    *,
    silver_root: Path,
    bronze_root: Path,
) -> Iterator[duckdb.DuckDBPyConnection]:
    """
    Context manager yielding a DuckDB connection with all Silver and
    direct-Bronze tables registered as views.

    Usage:
        with gold_session(silver_root=..., bronze_root=...) as conn:
            df = conn.execute(artifact.sql).fetchdf()

    The connection is in-memory (no DuckDB file on disk) — Gold
    artifacts are materialised to Parquet, so the DB state itself is
    ephemeral. If we ever wanted DuckDB to BE the analytical store
    (instead of materialising to Parquet), we'd switch to a file path.
    """
    conn = duckdb.connect(database=":memory:")
    try:
        register_views(conn, silver_root=silver_root, bronze_root=bronze_root)
        yield conn
    finally:
        conn.close()
