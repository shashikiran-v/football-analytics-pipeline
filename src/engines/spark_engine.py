"""
Spark engine — STUB.

This is a deliberate stub, not a real implementation. See ADR-0009 for
the engineering rationale.

Why a stub at all?
------------------
The brief asks for an implementation in either Pandas OR PySpark, with
abstraction encouraged. We chose Pandas-only because at the data scale
the brief targets (Kaggle Player Scores, ~9 GB max), Pandas runs the
full pipeline comfortably on a laptop in under a minute. Spark would
add JVM startup, cluster management, and operational complexity for
zero functional benefit at this scale.

But the choice deserves more than just a YAGNI handwave. We built the
engine abstraction (src/engines/base.py) precisely so a Spark engine
could be a contained, contracted addition. The presence of this stub:

  1. Proves the abstraction is real — the factory wiring, config
     selection, and engine protocol all support a Spark engine today
     without any code reshape.
  2. Documents what's missing — each NotImplementedError points at
     ADR-0009 which describes how the method would be implemented.
  3. Sets the boundary — adding Spark is a known-scope task (~2 weeks
     of engineering) that lives entirely within this file, not a
     codebase-wide refactor.

Why no pyspark import?
----------------------
Importing pyspark eagerly costs ~3 seconds of JVM startup. Users who
configured engine=pandas (the default) shouldn't pay that cost just
to import the factory. So this module avoids `import pyspark` at the
top level; a real implementation would do it inside __init__ when
constructing the SparkSession.

This also means installing PySpark is not required to run the Pandas
pipeline — `requirements.txt` keeps PySpark out of the core deps.

What this stub IS
-----------------
A class that implements the DataFrameEngine ABC by declaring every
abstract method but raising NotImplementedError when called. It can
be instantiated (the factory test exercises this); it just can't do
real work.

What this stub IS NOT
---------------------
A scaffolding for future Spark code. We deliberately don't import
pyspark, don't construct a SparkSession, and don't try to fake
behaviour. A real Spark engine would be a clean rewrite of this file
against the protocol — not an extension of the stub.

See also
--------
- ADR-0009 (Spark Engine Scope and Stub Design)
- src/engines/base.py (the protocol all engines satisfy)
- src/engines/pandas_engine.py (the production implementation)
- src/engines/factory.py (the dispatch logic — already wired for both)
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any, Literal

from src.engines.base import DataFrame, DataFrameEngine, WriteMode

_STUB_MESSAGE = (
    "Spark engine is not implemented (deliberate scope decision — "
    "see ADR-0009). Set `engine: pandas` in configs/config.yaml or "
    "the PIPELINE_ENGINE environment variable."
)


class SparkEngine(DataFrameEngine):
    """
    Stub Spark engine. Instantiable; every operation raises
    NotImplementedError. Used to prove the factory/protocol wiring
    works end-to-end without committing to a Spark dependency.

    A real implementation would:
      * Construct a SparkSession in __init__ (or accept one).
      * Translate each method to native Spark DataFrame ops.
      * Use Spark SQL for `rolling_avg` and `group_by_agg` because
        their declarative syntax is cleaner than the DSL.
      * Override `to_records` to call `.collect()` and convert to
        Python dicts (with a guard against accidental full-table
        collects in production).

    See ADR-0009 for the full design sketch.
    """

    kind: str = "spark"

    # -----------------------------------------------------------------
    # I/O
    # -----------------------------------------------------------------

    def read_csv(
        self,
        path: str | Path,
        *,
        schema: dict[str, str] | None = None,
        na_values: list[str] | None = None,
    ) -> DataFrame:
        raise NotImplementedError(_STUB_MESSAGE)

    def read_parquet(self, path: str | Path) -> DataFrame:
        raise NotImplementedError(_STUB_MESSAGE)

    def write_parquet(
        self,
        df: DataFrame,
        path: str | Path,
        *,
        partition_by: list[str] | None = None,
        mode: WriteMode = "overwrite",
    ) -> None:
        raise NotImplementedError(_STUB_MESSAGE)

    # -----------------------------------------------------------------
    # Row-level operations
    # -----------------------------------------------------------------

    def count(self, df: DataFrame) -> int:
        raise NotImplementedError(_STUB_MESSAGE)

    def columns(self, df: DataFrame) -> list[str]:
        raise NotImplementedError(_STUB_MESSAGE)

    def select(self, df: DataFrame, columns: list[str]) -> DataFrame:
        raise NotImplementedError(_STUB_MESSAGE)

    def rename(self, df: DataFrame, mapping: dict[str, str]) -> DataFrame:
        raise NotImplementedError(_STUB_MESSAGE)

    def filter_predicate(self, df: DataFrame, predicate: Callable[[dict], bool]) -> DataFrame:
        raise NotImplementedError(_STUB_MESSAGE)

    def filter_eq(self, df: DataFrame, column: str, value: Any) -> DataFrame:
        raise NotImplementedError(_STUB_MESSAGE)

    def filter_isin(self, df: DataFrame, column: str, values: list[Any]) -> DataFrame:
        raise NotImplementedError(_STUB_MESSAGE)

    def filter_not_null(self, df: DataFrame, columns: list[str]) -> DataFrame:
        raise NotImplementedError(_STUB_MESSAGE)

    def filter_range(
        self,
        df: DataFrame,
        column: str,
        *,
        ge: float | None = None,
        le: float | None = None,
    ) -> DataFrame:
        raise NotImplementedError(_STUB_MESSAGE)

    # -----------------------------------------------------------------
    # Column derivation
    # -----------------------------------------------------------------

    def with_constant_column(self, df: DataFrame, name: str, value: Any) -> DataFrame:
        raise NotImplementedError(_STUB_MESSAGE)

    def with_derived_column(
        self,
        df: DataFrame,
        name: str,
        fn: Callable[[dict], Any],
        *,
        input_columns: list[str] | None = None,
    ) -> DataFrame:
        raise NotImplementedError(_STUB_MESSAGE)

    def with_row_hash(
        self,
        df: DataFrame,
        columns: list[str],
        *,
        hash_column: str = "row_hash",
    ) -> DataFrame:
        raise NotImplementedError(_STUB_MESSAGE)

    # -----------------------------------------------------------------
    # Joins, sets, aggregation
    # -----------------------------------------------------------------

    def join(
        self,
        left: DataFrame,
        right: DataFrame,
        on: list[str],
        how: Literal["inner", "left", "right", "outer", "anti", "semi"] = "inner",
    ) -> DataFrame:
        raise NotImplementedError(_STUB_MESSAGE)

    def union(self, dfs: list[DataFrame]) -> DataFrame:
        raise NotImplementedError(_STUB_MESSAGE)

    def distinct(self, df: DataFrame, subset: list[str] | None = None) -> DataFrame:
        raise NotImplementedError(_STUB_MESSAGE)

    def group_by_agg(
        self,
        df: DataFrame,
        by: list[str],
        aggs: dict[str, tuple[str, str]],
    ) -> DataFrame:
        raise NotImplementedError(_STUB_MESSAGE)

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
        raise NotImplementedError(_STUB_MESSAGE)

    # -----------------------------------------------------------------
    # Materialisation
    # -----------------------------------------------------------------

    def to_records(self, df: DataFrame) -> list[dict]:
        raise NotImplementedError(_STUB_MESSAGE)
