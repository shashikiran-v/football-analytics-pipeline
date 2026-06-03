"""
Pandas implementation of DataFrameEngine.

Everything here is plain pandas + pyarrow for I/O. We never expose pandas
types in method signatures — callers see opaque DataFrames and only
manipulate them via the engine.

A few pandas gotchas worth knowing while reading this:

* Pandas reads strings as object dtype by default; pyarrow preserves
  proper string types in Parquet. We rely on pyarrow throughout for
  Parquet I/O to avoid silent type drift between layers.

* `groupby(..., dropna=False)` is essential — by default pandas drops
  rows where any grouping key is NaN, which would silently lose data.

* For row-level UDFs we use `apply(axis=1)`. It's slow on big frames but
  perfectly fine for our dataset sizes (<2M rows). The Spark engine uses
  proper UDFs.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Literal

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.engines.base import DataFrame, DataFrameEngine, WriteMode
from src.utils.hashing import HASH_SEPARATOR, NULL_SENTINEL, hash_row


# Map our type tags -> pandas / pyarrow dtypes. We deliberately use
# pandas extension dtypes (Int64 with capital I) so nullable integers
# work — the schema-enforced dataset has NaN player IDs to handle in DQ.
_TYPE_MAP = {
    "int": "Int64",
    "long": "Int64",
    "float": "Float64",
    "string": "string",
    "bool": "boolean",
    "date": "datetime64[ns]",
    "timestamp": "datetime64[ns]",
}


class PandasEngine(DataFrameEngine):
    kind = "pandas"

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
        # `low_memory=False` because the football CSVs have mixed types
        # in some columns (e.g. number-or-string score fields) and the
        # chunked parser otherwise warns and infers per-chunk.
        df = pd.read_csv(
            path,
            low_memory=False,
            na_values=na_values,
            keep_default_na=True,
        )
        if schema:
            df = self._coerce_schema(df, schema)
        return df

    def read_parquet(self, path: str | Path) -> DataFrame:
        # pyarrow handles partitioned directories natively
        table = pq.read_table(str(path))
        return table.to_pandas(types_mapper=pd.ArrowDtype)

    def write_parquet(
        self,
        df: DataFrame,
        path: str | Path,
        *,
        partition_by: list[str] | None = None,
        mode: WriteMode = "overwrite",
    ) -> None:
        target = Path(path)
        if mode == "error" and target.exists():
            raise FileExistsError(f"Refusing to overwrite existing path: {target}")

        # Two distinct overwrite semantics:
        #   1. Non-partitioned: overwrite the single file/dir entirely
        #   2. Partitioned: overwrite ONLY the partitions present in the
        #      incoming df; leave other partitions on disk untouched.
        #
        # The previous implementation `shutil.rmtree`d the whole target
        # before writing, which silently wiped prior batches' partitions
        # — discovered during Phase 6 day-2 testing. The fix: for
        # partitioned writes, delete only the specific partition
        # subdirectories we're about to replace, then let pyarrow's
        # `existing_data_behavior="overwrite_or_ignore"` handle the rest.
        if mode == "overwrite" and target.exists() and not partition_by:
            if target.is_dir():
                import shutil
                shutil.rmtree(target)
            else:
                target.unlink()
        target.parent.mkdir(parents=True, exist_ok=True)

        # Convert to a pyarrow Table for consistent Parquet semantics with
        # Spark's writer. preserve_index=False because pandas' RangeIndex
        # would otherwise sneak into the Parquet file as an unnamed column.
        table = pa.Table.from_pandas(df, preserve_index=False)

        if partition_by:
            # Partition-level overwrite: wipe ONLY the partition
            # subdirectories present in the incoming dataframe.
            if mode == "overwrite" and target.exists():
                import shutil
                partition_values_by_col: dict[str, set] = {}
                for col in partition_by:
                    partition_values_by_col[col] = set(df[col].unique())
                # Single-column partition: rmtree each batch_id=... dir
                # whose value is in the incoming df.
                if len(partition_by) == 1:
                    col = partition_by[0]
                    for val in partition_values_by_col[col]:
                        partition_dir = target / f"{col}={val}"
                        if partition_dir.exists():
                            shutil.rmtree(partition_dir)
                else:
                    # Multi-column partitioning isn't used in this
                    # codebase yet; fall back to full wipe to avoid
                    # subtle correctness gaps.
                    shutil.rmtree(target)

            pq.write_to_dataset(
                table,
                root_path=str(target),
                partition_cols=partition_by,
                existing_data_behavior="overwrite_or_ignore",
            )
        else:
            target.mkdir(parents=True, exist_ok=True)
            pq.write_table(table, str(target / "part-00000.parquet"))

    # -----------------------------------------------------------------
    # Row-level
    # -----------------------------------------------------------------

    def count(self, df: DataFrame) -> int:
        return len(df)

    def columns(self, df: DataFrame) -> list[str]:
        return list(df.columns)

    def select(self, df: DataFrame, columns: list[str]) -> DataFrame:
        missing = [c for c in columns if c not in df.columns]
        if missing:
            raise KeyError(f"select() missing columns: {missing}")
        return df[columns].copy()

    def rename(self, df: DataFrame, mapping: dict[str, str]) -> DataFrame:
        # Filter mapping to columns that actually exist; pandas' rename
        # silently ignores missing keys but we want explicit behaviour.
        safe = {k: v for k, v in mapping.items() if k in df.columns}
        return df.rename(columns=safe).copy()

    def filter_predicate(
        self, df: DataFrame, predicate: Callable[[dict], bool]
    ) -> DataFrame:
        mask = df.apply(lambda r: predicate(r.to_dict()), axis=1)
        return df[mask].copy()

    def filter_eq(self, df: DataFrame, column: str, value: Any) -> DataFrame:
        return df[df[column] == value].copy()

    def filter_isin(self, df: DataFrame, column: str, values: list[Any]) -> DataFrame:
        return df[df[column].isin(values)].copy()

    def filter_not_null(self, df: DataFrame, columns: list[str]) -> DataFrame:
        return df.dropna(subset=columns).copy()

    def filter_range(
        self,
        df: DataFrame,
        column: str,
        *,
        ge: float | None = None,
        le: float | None = None,
    ) -> DataFrame:
        mask = pd.Series(True, index=df.index)
        if ge is not None:
            mask &= df[column] >= ge
        if le is not None:
            mask &= df[column] <= le
        return df[mask].copy()

    # -----------------------------------------------------------------
    # Column derivation
    # -----------------------------------------------------------------

    def with_constant_column(self, df: DataFrame, name: str, value: Any) -> DataFrame:
        out = df.copy()
        out[name] = value
        return out

    def with_derived_column(
        self,
        df: DataFrame,
        name: str,
        fn: Callable[[dict], Any],
        *,
        input_columns: list[str] | None = None,
    ) -> DataFrame:
        out = df.copy()
        # input_columns is a perf hint that pandas can also honour: passing
        # a narrower slice to apply() is faster than passing the whole row.
        source = out[input_columns] if input_columns else out
        out[name] = source.apply(lambda r: fn(r.to_dict()), axis=1)
        return out

    def with_row_hash(
        self,
        df: DataFrame,
        columns: list[str],
        *,
        hash_column: str = "row_hash",
    ) -> DataFrame:
        # Vectorised: stringify each tracked column with NaN -> sentinel,
        # join with the separator, hash. Faster than apply().
        #
        # Subtle: for nullable dtypes (Int64 / Float64 / boolean) we must
        # cast to string BEFORE replacing nulls — pandas refuses to write
        # the string sentinel into a numeric extension array. Plain
        # `.astype(str)` on a nullable Int64 column with NA produces "<NA>"
        # not the empty string, so we then replace "<NA>" with our sentinel.
        import hashlib

        def stringify(series: pd.Series) -> pd.Series:
            # Cast to plain object/str first; this turns missing values
            # into the pandas string repr "<NA>" or NaN-as-"nan".
            s = series.astype("string")
            # Now substitute our canonical sentinel for any missing values.
            return s.where(s.notna(), NULL_SENTINEL)

        joined = pd.Series("", index=df.index, dtype="string")
        for i, col in enumerate(columns):
            piece = stringify(df[col])
            joined = piece if i == 0 else joined + HASH_SEPARATOR + piece

        out = df.copy()
        out[hash_column] = joined.map(
            lambda s: hashlib.md5(s.encode("utf-8")).hexdigest()
        )
        return out

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
        # Pandas doesn't have native anti/semi joins; implement via indicator merge.
        if how == "anti":
            merged = left.merge(right[on], on=on, how="left", indicator=True)
            return merged[merged["_merge"] == "left_only"].drop(columns=["_merge"])
        if how == "semi":
            keys = right[on].drop_duplicates()
            return left.merge(keys, on=on, how="inner")
        return left.merge(right, on=on, how=how)

    def union(self, dfs: list[DataFrame]) -> DataFrame:
        if not dfs:
            raise ValueError("union() requires at least one DataFrame")
        # ignore_index=True so the resulting index is a clean RangeIndex;
        # sort=False preserves column order from the first frame.
        return pd.concat(dfs, ignore_index=True, sort=False)

    def distinct(self, df: DataFrame, subset: list[str] | None = None) -> DataFrame:
        return df.drop_duplicates(subset=subset).copy()

    def group_by_agg(
        self,
        df: DataFrame,
        by: list[str],
        aggs: dict[str, tuple[str, str]],
    ) -> DataFrame:
        # pandas' named aggregation API maps cleanly to our (col, fn) spec.
        # The one wrinkle: 'count_distinct' isn't a built-in; use nunique.
        pd_aggs = {}
        for out_col, (src_col, fn) in aggs.items():
            pd_fn = "nunique" if fn == "count_distinct" else fn
            pd_aggs[out_col] = pd.NamedAgg(column=src_col, aggfunc=pd_fn)
        # dropna=False so groups with NaN keys still appear (DQ-friendly).
        return df.groupby(by, dropna=False, as_index=False).agg(**pd_aggs)

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
        out = df.sort_values([*partition_by, order_by]).copy()
        # min_periods=1 so the rolling avg is defined from the first row;
        # otherwise the first window_rows-1 rows would be NaN, which is
        # surprising in a "trend" column.
        out[output_column] = (
            out.groupby(partition_by, dropna=False)[value_column]
            .transform(lambda s: s.rolling(window=window_rows, min_periods=1).mean())
        )
        return out

    # -----------------------------------------------------------------
    # Materialisation
    # -----------------------------------------------------------------

    def to_records(self, df: DataFrame) -> list[dict]:
        # Convert pandas NaN to None for cleaner downstream JSON serialisation.
        return df.where(df.notna(), None).to_dict(orient="records")

    # -----------------------------------------------------------------
    # Internals
    # -----------------------------------------------------------------

    @staticmethod
    def _coerce_schema(df: pd.DataFrame, schema: dict[str, str]) -> pd.DataFrame:
        """Best-effort cast columns to the requested type tags."""
        out = df.copy()
        for col, tag in schema.items():
            if col not in out.columns:
                continue
            pd_dtype = _TYPE_MAP.get(tag)
            if pd_dtype is None:
                raise ValueError(f"Unknown schema type tag: {tag}")
            if tag in {"date", "timestamp"}:
                out[col] = pd.to_datetime(out[col], errors="coerce", utc=False)
            else:
                # Two-stage cast for nullable numeric types: convert
                # via float64 first to handle stray strings like "" gracefully.
                if tag in {"int", "long"}:
                    out[col] = pd.to_numeric(out[col], errors="coerce").astype(pd_dtype)
                elif tag == "float":
                    out[col] = pd.to_numeric(out[col], errors="coerce").astype(pd_dtype)
                else:
                    out[col] = out[col].astype(pd_dtype)
        return out


__all__ = ["PandasEngine"]

# Verify our vectorised hash matches the reference implementation. Catches
# any future drift between the two paths at import time.
def _self_check() -> None:
    df = pd.DataFrame({"a": [1, 2, None], "b": ["x", None, "z"]})
    engine = PandasEngine()
    hashed = engine.with_row_hash(df, ["a", "b"])
    for i, row in hashed.iterrows():
        expected = hash_row([df.iloc[i]["a"], df.iloc[i]["b"]])
        # The vectorised path stringifies "1" not "1.0" for nullable Int64,
        # but here columns are plain object/string so values pass straight
        # through; the reference does the same.
        actual = row["row_hash"]
        assert actual == expected, f"hash mismatch at row {i}: {actual} != {expected}"


_self_check()
