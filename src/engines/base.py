"""
DataFrameEngine protocol.

The core abstraction that lets the same transformation logic run on either
Pandas or PySpark. Every operation the pipeline needs against a DataFrame
goes through this protocol; engine-specific code lives only in the engine
implementations and a small handful of explicitly engine-aware transforms.

Design principles
-----------------
1. **Minimal surface area.** Only operations our transformations actually
   need. We're not building a query engine; we're building enough of one
   to express our 4 transformations, SCD2 merge, and Gold aggregations.

2. **Engine returns its own DataFrame type.** The `DataFrame` type alias
   below is `Any` — by design. Callers should treat DataFrames as opaque
   handles and only operate on them through the engine. The moment you
   import pandas or pyspark in business logic, the abstraction has leaked.

3. **Row-level functions over column expressions.** Where engines diverge
   most is column expression syntax. We sidestep that by accepting plain
   Python callables for derived columns. They're slower than vectorised
   pandas / native Spark expressions, but for this dataset the difference
   is irrelevant and the readability win is large.

4. **The engines speak Parquet.** Both engines read and write the same
   Parquet files. This is the contract that lets us hand off data between
   engines mid-pipeline (in principle) and lets DuckDB / Superset read
   either engine's output transparently.

Anti-goals
----------
- We're not abstracting SQL execution. DuckDB owns that, downstream of Gold.
- We're not abstracting window expressions beyond a single helper for
  rolling averages (the only window we need for valuation trend).
- We're not handling streaming. Pandas can't, and we don't need to.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable
from pathlib import Path
from typing import Any, Literal

# Opaque DataFrame handle. Engine implementations narrow this internally.
DataFrame = Any

WriteMode = Literal["overwrite", "append", "error"]


class DataFrameEngine(ABC):
    """
    Abstract engine. Implementations: PandasEngine, SparkEngine.

    Instances are cheap to create for Pandas (just a marker object).
    For Spark, the SparkSession is held on the engine; construct once
    per process and reuse via the factory.
    """

    # A short tag used by transforms that genuinely need to know which
    # engine they're on (e.g. for engine-aware vectorisation). Most code
    # should never read this — it's an escape hatch, not the front door.
    kind: str = "abstract"

    # -----------------------------------------------------------------
    # I/O
    # -----------------------------------------------------------------

    @abstractmethod
    def read_csv(
        self,
        path: str | Path,
        *,
        schema: dict[str, str] | None = None,
        na_values: list[str] | None = None,
    ) -> DataFrame:
        """
        Read a CSV. If schema is provided it should map column -> type tag
        ('int', 'long', 'float', 'string', 'bool', 'date', 'timestamp');
        engines coerce as best they can.
        """

    @abstractmethod
    def read_parquet(self, path: str | Path) -> DataFrame:
        """Read a Parquet file or directory of parts."""

    @abstractmethod
    def write_parquet(
        self,
        df: DataFrame,
        path: str | Path,
        *,
        partition_by: list[str] | None = None,
        mode: WriteMode = "overwrite",
    ) -> None:
        """
        Write to Parquet. Partitioning is recommended for Bronze (by
        batch_id) and Silver fact tables (by season). Mode semantics
        match Spark's: overwrite replaces, append adds, error fails if
        the path exists.
        """

    # -----------------------------------------------------------------
    # Row-level operations
    # -----------------------------------------------------------------

    @abstractmethod
    def count(self, df: DataFrame) -> int:
        """Number of rows. Materialises on Spark; cheap on Pandas."""

    @abstractmethod
    def columns(self, df: DataFrame) -> list[str]:
        """Column names in order."""

    @abstractmethod
    def select(self, df: DataFrame, columns: list[str]) -> DataFrame:
        """Project to a subset of columns (and reorder)."""

    @abstractmethod
    def rename(self, df: DataFrame, mapping: dict[str, str]) -> DataFrame:
        """Rename columns. Missing source columns are ignored."""

    @abstractmethod
    def filter_predicate(self, df: DataFrame, predicate: Callable[[dict], bool]) -> DataFrame:
        """
        Keep rows where predicate(row_as_dict) is truthy.

        Slow on Spark (UDF), but only used in tests and tiny reference-data
        joins. Production filters go through `filter_eq` / `filter_isin` /
        `filter_range` which translate to vectorised ops.
        """

    @abstractmethod
    def filter_eq(self, df: DataFrame, column: str, value: Any) -> DataFrame:
        """Keep rows where column == value."""

    @abstractmethod
    def filter_isin(self, df: DataFrame, column: str, values: list[Any]) -> DataFrame:
        """Keep rows where column is in values."""

    @abstractmethod
    def filter_not_null(self, df: DataFrame, columns: list[str]) -> DataFrame:
        """Keep rows where all named columns are non-null."""

    @abstractmethod
    def filter_range(
        self,
        df: DataFrame,
        column: str,
        *,
        ge: float | None = None,
        le: float | None = None,
    ) -> DataFrame:
        """Keep rows where ge <= column <= le (inclusive on both sides)."""

    # -----------------------------------------------------------------
    # Column derivation
    # -----------------------------------------------------------------

    @abstractmethod
    def with_constant_column(self, df: DataFrame, name: str, value: Any) -> DataFrame:
        """Append a column whose value is the same constant for every row."""

    @abstractmethod
    def with_derived_column(
        self,
        df: DataFrame,
        name: str,
        fn: Callable[[dict], Any],
        *,
        input_columns: list[str] | None = None,
    ) -> DataFrame:
        """
        Append `name` = fn(row_dict).

        `input_columns` lets Spark restrict the UDF input (perf hint);
        pandas ignores it (and just passes the full row).
        """

    @abstractmethod
    def with_row_hash(
        self,
        df: DataFrame,
        columns: list[str],
        *,
        hash_column: str = "row_hash",
    ) -> DataFrame:
        """
        Append a row hash over the specified columns, used by SCD2 merge
        to detect changes. The hash must match `src.utils.hashing.hash_row`.
        """

    # -----------------------------------------------------------------
    # Joins, sets, aggregation
    # -----------------------------------------------------------------

    @abstractmethod
    def join(
        self,
        left: DataFrame,
        right: DataFrame,
        on: list[str],
        how: Literal["inner", "left", "right", "outer", "anti", "semi"] = "inner",
    ) -> DataFrame:
        """Join on shared column names. Use rename() first if names differ."""

    @abstractmethod
    def union(self, dfs: list[DataFrame]) -> DataFrame:
        """
        Concatenate two or more DataFrames with the same schema. Columns
        are aligned by name, not position.
        """

    @abstractmethod
    def distinct(self, df: DataFrame, subset: list[str] | None = None) -> DataFrame:
        """Drop duplicates. If subset is given, dedupe on those keys only."""

    @abstractmethod
    def group_by_agg(
        self,
        df: DataFrame,
        by: list[str],
        aggs: dict[str, tuple[str, str]],
    ) -> DataFrame:
        """
        Aggregate.

        Args:
            by: grouping columns.
            aggs: output_column_name -> (source_column, function).
                  Functions: 'sum', 'count', 'count_distinct', 'avg',
                             'min', 'max', 'first', 'last'.

        Example:
            engine.group_by_agg(
                df,
                by=["club_id", "season"],
                aggs={
                    "goals":        ("goals", "sum"),
                    "matches":      ("game_id", "count_distinct"),
                    "minutes":      ("minutes_played", "sum"),
                },
            )
        """

    @abstractmethod
    def rolling_avg(
        self,
        df: DataFrame,
        *,
        partition_by: list[str],
        order_by: str,
        value_column: str,
        window_rows: int,
        output_column: str,
    ) -> DataFrame:
        """
        Add a rolling average of `value_column` over the last `window_rows`
        rows, partitioned by `partition_by` and ordered by `order_by`.

        This is intentionally the only window function in the abstraction —
        it's the one Gold layer needs (player_valuation_trend.rolling_average)
        and abstracting general windows would balloon the surface area.
        """

    # -----------------------------------------------------------------
    # Materialisation for inspection / DQ / tests
    # -----------------------------------------------------------------

    @abstractmethod
    def to_records(self, df: DataFrame) -> list[dict]:
        """
        Collect to a list of dicts. Used by DQ for sampling failing rows
        and by tests for assertions. Never call on full Bronze/Silver data
        in production.
        """
