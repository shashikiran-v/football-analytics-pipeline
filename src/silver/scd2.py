"""
SCD Type 2 merge.

The differentiating function in the Silver layer. Takes the current
state of a dimension and a fresh batch of incoming rows, returns the
updated dimension preserving full version history.

The four categories
-------------------
Every incoming row falls into one of four categories, and existing
rows fall into one of two:

  Incoming side:
    NEW       Natural key absent from existing -> insert as first version
              with effective_date = batch_ts, is_current = True
    CHANGED   Natural key present but tracked-column hash differs
              -> close out the existing current version
                 (end_date = batch_ts, is_current = False)
              -> insert a new version with the incoming attributes
    UNCHANGED Natural key present, hash matches -> no-op for output
              (the existing row continues to be 'current')

  Existing side:
    CURRENT      is_current = True and matches incoming key -> may be
                 closed out (CHANGED) or kept (UNCHANGED)
    HISTORICAL   is_current = False -> never mutated, just passed through

Hash-based change detection
---------------------------
Tracking N columns naively means N equality checks per row. We
collapse that to a single hash comparison: incoming row hash matches
existing current row hash -> UNCHANGED. The engine's with_row_hash
implements this in Pandas and (when added) PySpark using the same
canonical algorithm — see ADR-0001 and src/utils/hashing.py.

Surrogate keys
--------------
We allocate auto-incremented integers starting from
(max existing surrogate key) + 1, sorted by natural_key for
determinism. First run starts at 1. The strategy is documented in
ADR-0005 (Phase 3 slice 5) along with the trade-offs.

What this function does NOT do
------------------------------
* It does not write anything (Silver dimension builders do that).
* It does not touch the audit DAO (the builders orchestrate that).
* It does not apply transformations (transforms.py / the builders do).
* It does not enforce the "incoming has the same columns as existing"
  contract — the builders are responsible for shaping incoming
  correctly before calling here. We do verify the natural_key,
  tracked_columns, and surrogate_key_column columns exist.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from src.engines.base import DataFrame, DataFrameEngine
from src.utils.logging import get_logger

log = get_logger(__name__)


# Marker timestamp used for the end_date of currently-active versions.
# Far enough in the future that no real date overtakes it; chosen as a
# date (not datetime) so it formats cleanly across engines.
FAR_FUTURE_DATE = "9999-12-31"

# Names of the SCD2 bookkeeping columns. Convention: short, no underscore
# prefix (same reasoning as ADR-0003: underscore-prefixed names get
# hidden by some readers).
EFFECTIVE_DATE_COLUMN = "effective_date"
END_DATE_COLUMN = "end_date"
IS_CURRENT_COLUMN = "is_current"

# Internal column used during the merge to carry the tracked-columns
# row hash. Dropped before the return.
_ROW_HASH_COLUMN = "_scd2_row_hash"


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SCD2MergeStats:
    """Per-merge metrics — useful for logging and the runner's summary."""

    new_versions: int
    changed_versions: int
    unchanged_versions: int
    historical_preserved: int
    total_output_rows: int


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def scd2_merge(
    *,
    existing_dim: DataFrame | None,
    incoming: DataFrame,
    natural_key: list[str],
    tracked_columns: list[str],
    surrogate_key_column: str,
    batch_timestamp: str,
    engine: DataFrameEngine,
) -> tuple[DataFrame, SCD2MergeStats]:
    """
    Merge a fresh batch into an SCD Type 2 dimension.

    Args:
        existing_dim:          Current state of the dimension. Pass None
                               on first ever run for this dimension.
                               Must have columns:
                                  natural_key, tracked_columns,
                                  surrogate_key_column,
                                  effective_date, end_date, is_current
                               plus any non-tracked attributes you want
                               to carry forward.
        incoming:              Freshly-loaded rows for this batch. Must
                               have at minimum: natural_key + tracked_columns
                               + any non-tracked attributes that should be
                               written into NEW or CHANGED versions.
        natural_key:           Columns forming the dimension's natural key
                               (e.g. ["player_id"]).
        tracked_columns:       Columns whose changes open a new version.
        surrogate_key_column:  Name of the surrogate key column.
        batch_timestamp:       ISO8601 date or datetime string; used as
                               effective_date for new versions and
                               end_date for closed-out versions.
        engine:                DataFrameEngine instance.

    Returns:
        (updated_dimension, stats)

    The updated_dimension contains every row of preserved history,
    every unchanged current row, every closed-out previous version of
    a changed row, every new version of a changed row, and every new
    row from incoming. Column order matches existing_dim where possible,
    with the SCD2 bookkeeping columns appended for first-run cases.
    """
    # --- Validate inputs ---------------------------------------------------
    _validate_inputs(
        incoming=incoming,
        natural_key=natural_key,
        tracked_columns=tracked_columns,
        engine=engine,
    )

    # --- Empty-existing fast path: every incoming row is NEW ---------------
    if existing_dim is None or engine.count(existing_dim) == 0:
        return _initial_load(
            incoming=incoming,
            natural_key=natural_key,
            tracked_columns=tracked_columns,
            surrogate_key_column=surrogate_key_column,
            batch_timestamp=batch_timestamp,
            engine=engine,
        )

    # --- Split existing into 'current' and 'historical' ------------------
    existing_current = engine.filter_eq(existing_dim, IS_CURRENT_COLUMN, True)
    # 'historical' is everything where is_current is anything but True.
    # We can't easily do `!= True` through the protocol, so we compute it
    # by anti-joining existing against existing_current on the surrogate
    # key — that way 'historical' is whatever didn't end up in 'current'.
    historical = engine.join(
        existing_dim,
        engine.select(existing_current, [surrogate_key_column]),
        on=[surrogate_key_column],
        how="anti",
    )

    historical_count = engine.count(historical)

    # --- Hash both sides on tracked_columns ------------------------------
    incoming_hashed = engine.with_row_hash(
        incoming,
        tracked_columns,
        hash_column=_ROW_HASH_COLUMN,
    )
    existing_current_hashed = engine.with_row_hash(
        existing_current,
        tracked_columns,
        hash_column=_ROW_HASH_COLUMN,
    )

    # --- Find NEW rows: in incoming, not in existing_current ------------
    # Anti-join over natural key.
    new_rows = engine.join(
        incoming_hashed,
        engine.select(existing_current_hashed, natural_key),
        on=natural_key,
        how="anti",
    )

    # --- For the natural keys that DO overlap, compare hashes ----------
    # Inner-join incoming with the existing current (on natural_key) so
    # we can compare hashes side by side.
    # We rename the existing hash column so it doesn't collide.
    existing_for_compare = engine.rename(
        engine.select(
            existing_current_hashed,
            [*natural_key, surrogate_key_column, _ROW_HASH_COLUMN],
        ),
        {_ROW_HASH_COLUMN: "_scd2_existing_hash"},
    )
    overlap = engine.join(
        incoming_hashed,
        existing_for_compare,
        on=natural_key,
        how="inner",
    )

    # CHANGED: natural key matches but hash differs.
    # Implementing this through the abstraction: use filter_predicate
    # for the inequality (it's a single column comparison, the engine
    # protocol doesn't have filter_neq because we minimised surface area).
    changed_rows = engine.filter_predicate(
        overlap,
        lambda r: r[_ROW_HASH_COLUMN] != r["_scd2_existing_hash"],
    )
    # UNCHANGED: natural key matches AND hash matches.
    unchanged_rows = engine.filter_predicate(
        overlap,
        lambda r: r[_ROW_HASH_COLUMN] == r["_scd2_existing_hash"],
    )

    new_count = engine.count(new_rows)
    changed_count = engine.count(changed_rows)
    unchanged_count = engine.count(unchanged_rows)

    log.info(
        "scd2_merge_categories_resolved",
        new=new_count,
        changed=changed_count,
        unchanged=unchanged_count,
        historical_preserved=historical_count,
    )

    # --- Surrogate-key allocation -----------------------------------------
    next_sk = _next_surrogate_key(
        existing_dim=existing_dim,
        surrogate_key_column=surrogate_key_column,
        engine=engine,
    )

    # --- Build the four output partitions --------------------------------
    output_parts: list[DataFrame] = []

    # 1. Historical rows pass through untouched.
    if historical_count > 0:
        output_parts.append(historical)

    # 2. UNCHANGED: surface the original existing_current row unchanged.
    # The 'unchanged_rows' DF includes columns from both sides; we
    # extract the existing surrogate keys and join back to existing_current
    # to grab the row as-is.
    if unchanged_count > 0:
        unchanged_sks = engine.select(unchanged_rows, [surrogate_key_column])
        unchanged_passthrough = engine.join(
            existing_current,
            unchanged_sks,
            on=[surrogate_key_column],
            how="semi",
        )
        output_parts.append(unchanged_passthrough)

    # 3. CHANGED -> closed-out versions of the existing rows.
    if changed_count > 0:
        changed_existing_sks = engine.select(changed_rows, [surrogate_key_column])
        closed_out = engine.join(
            existing_current,
            changed_existing_sks,
            on=[surrogate_key_column],
            how="semi",
        )
        # Overwrite end_date and is_current.
        closed_out = engine.with_constant_column(
            closed_out,
            END_DATE_COLUMN,
            batch_timestamp,
        )
        closed_out = engine.with_constant_column(
            closed_out,
            IS_CURRENT_COLUMN,
            False,
        )
        output_parts.append(closed_out)

    # 4. CHANGED -> new versions with incoming attributes
    # Shape: incoming columns + new surrogate key + SCD2 bookkeeping.
    if changed_count > 0:
        changed_new_versions = _build_new_versions(
            rows=changed_rows,
            incoming_columns=engine.columns(incoming),
            natural_key=natural_key,
            tracked_columns=tracked_columns,
            surrogate_key_column=surrogate_key_column,
            batch_timestamp=batch_timestamp,
            start_sk=next_sk,
            engine=engine,
        )
        next_sk += changed_count
        output_parts.append(changed_new_versions)

    # 5. NEW rows with first version surrogate keys.
    if new_count > 0:
        new_versions = _build_new_versions(
            rows=new_rows,
            incoming_columns=engine.columns(incoming),
            natural_key=natural_key,
            tracked_columns=tracked_columns,
            surrogate_key_column=surrogate_key_column,
            batch_timestamp=batch_timestamp,
            start_sk=next_sk,
            engine=engine,
        )
        output_parts.append(new_versions)

    # --- Union and return ---------------------------------------------------
    if not output_parts:
        # Edge case: empty existing AND empty incoming. Return empty existing.
        result = existing_dim
    elif len(output_parts) == 1:
        result = output_parts[0]
    else:
        # All parts share the existing_dim schema. Engine's union aligns
        # by name, not position — see PandasEngine.union for details.
        result = engine.union(output_parts)

    stats = SCD2MergeStats(
        new_versions=new_count,
        changed_versions=changed_count,
        unchanged_versions=unchanged_count,
        historical_preserved=historical_count,
        total_output_rows=engine.count(result),
    )
    return result, stats


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _validate_inputs(
    *,
    incoming: DataFrame,
    natural_key: list[str],
    tracked_columns: list[str],
    engine: DataFrameEngine,
) -> None:
    """Fail loudly on missing columns at merge entry."""
    if not natural_key:
        raise ValueError("natural_key must be a non-empty list")
    if not tracked_columns:
        raise ValueError("tracked_columns must be a non-empty list")

    incoming_cols = set(engine.columns(incoming))
    missing_nk = [c for c in natural_key if c not in incoming_cols]
    if missing_nk:
        raise ValueError(f"incoming missing natural_key column(s): {missing_nk}")
    missing_tc = [c for c in tracked_columns if c not in incoming_cols]
    if missing_tc:
        raise ValueError(f"incoming missing tracked_column(s): {missing_tc}")


def _next_surrogate_key(
    *,
    existing_dim: DataFrame,
    surrogate_key_column: str,
    engine: DataFrameEngine,
) -> int:
    """
    Compute the next available surrogate key. We do this by
    collecting the surrogate column (small — even huge dimensions
    have a few hundred thousand rows tops) and taking max + 1.

    For very large dimensions this is the one place we'd switch to
    an engine-native aggregation. The protocol doesn't expose a
    direct max(); we use group_by_agg over a constant column as
    the engine-agnostic equivalent.
    """
    # Add a constant column to group on, then compute max.
    grouped = engine.with_constant_column(
        engine.select(existing_dim, [surrogate_key_column]),
        "_scd2_group",
        value="all",
    )
    aggregated = engine.group_by_agg(
        grouped,
        by=["_scd2_group"],
        aggs={"max_sk": (surrogate_key_column, "max")},
    )
    records = engine.to_records(aggregated)
    if not records:
        return 1
    current_max = records[0]["max_sk"]
    if current_max is None:
        return 1
    return int(current_max) + 1


def _initial_load(
    *,
    incoming: DataFrame,
    natural_key: list[str],
    tracked_columns: list[str],
    surrogate_key_column: str,
    batch_timestamp: str,
    engine: DataFrameEngine,
) -> tuple[DataFrame, SCD2MergeStats]:
    """First ever load: every incoming row becomes a new version with sk starting at 1."""
    new_count = engine.count(incoming)
    if new_count == 0:
        # Pathological: empty initial load. Return the empty incoming
        # with the SCD2 columns attached so downstream readers don't
        # encounter a schemaless dim.
        seeded = engine.with_constant_column(
            incoming,
            surrogate_key_column,
            0,
        )
        seeded = engine.with_constant_column(
            seeded,
            EFFECTIVE_DATE_COLUMN,
            batch_timestamp,
        )
        seeded = engine.with_constant_column(
            seeded,
            END_DATE_COLUMN,
            FAR_FUTURE_DATE,
        )
        seeded = engine.with_constant_column(
            seeded,
            IS_CURRENT_COLUMN,
            True,
        )
        # Filter to empty (we just want the schema, not the placeholder row)
        empty = engine.filter_eq(seeded, IS_CURRENT_COLUMN, "no-such-value")
        stats = SCD2MergeStats(0, 0, 0, 0, 0)
        return empty, stats

    new_versions = _build_new_versions(
        rows=incoming,
        incoming_columns=engine.columns(incoming),
        natural_key=natural_key,
        tracked_columns=tracked_columns,
        surrogate_key_column=surrogate_key_column,
        batch_timestamp=batch_timestamp,
        start_sk=1,
        engine=engine,
    )
    stats = SCD2MergeStats(
        new_versions=new_count,
        changed_versions=0,
        unchanged_versions=0,
        historical_preserved=0,
        total_output_rows=new_count,
    )
    return new_versions, stats


def _build_new_versions(
    *,
    rows: DataFrame,
    incoming_columns: list[str],
    natural_key: list[str],
    tracked_columns: list[str],
    surrogate_key_column: str,
    batch_timestamp: str,
    start_sk: int,
    engine: DataFrameEngine,
) -> DataFrame:
    """
    Construct DataFrame rows representing brand-new version records.

    Shape of output:
      - All columns from incoming (the source attributes)
      - surrogate_key_column with allocated integer values
      - effective_date = batch_timestamp
      - end_date = FAR_FUTURE_DATE
      - is_current = True

    Surrogate key allocation order: rows sorted by natural_key.
    The same input always yields the same surrogate keys.
    """
    # Restrict to source columns only (drop any merge-internal columns
    # like the row hash that may be present on `rows`).
    rows_clean = engine.select(rows, incoming_columns)

    # Sort by natural_key. We don't have a direct sort in the protocol,
    # but rolling_avg orders internally — that's overkill here. The
    # pragmatic option: collect, sort, rebuild via the engine's union.
    # Since this only runs once per merge and the number of new rows
    # is small relative to the whole dim, the cost is acceptable.
    records = engine.to_records(rows_clean)
    records_sorted = sorted(
        records,
        key=lambda r: tuple(_sort_key(r.get(c)) for c in natural_key),
    )
    # Allocate surrogate keys in sorted order.
    for i, rec in enumerate(records_sorted):
        rec[surrogate_key_column] = start_sk + i
        rec[EFFECTIVE_DATE_COLUMN] = batch_timestamp
        rec[END_DATE_COLUMN] = FAR_FUTURE_DATE
        rec[IS_CURRENT_COLUMN] = True

    # Rebuild a DataFrame from the records. We do this through the engine
    # by writing to a temp parquet and reading back — but that's overkill
    # for what's essentially a list-of-dicts -> df conversion. The engine
    # protocol intentionally doesn't expose 'from_records' to keep its
    # surface small. We use a pragmatic pandas-specific path here and
    # rely on the engine's I/O to make it work on Spark when we get there.
    return _records_to_df(records_sorted, engine=engine)


def _records_to_df(records: list[dict[str, Any]], engine: DataFrameEngine) -> DataFrame:
    """
    Convert a list-of-dicts back to the engine's DataFrame type.

    Pragmatic implementation: for pandas we build directly. When the
    Spark engine lands (Phase 7) we'll add a from_records to the protocol
    or use the SparkSession's createDataFrame here. The function is
    isolated so the upgrade is contained.
    """
    if engine.kind == "pandas":
        import pandas as pd

        return pd.DataFrame(records)
    if engine.kind == "spark":
        # Placeholder — to be implemented when SparkEngine arrives.
        raise NotImplementedError(
            "SparkEngine path through _records_to_df not yet implemented. "
            "Phase 7 will add this when SparkEngine is wired up."
        )
    raise ValueError(f"Unknown engine kind: {engine.kind!r}")


def _sort_key(value: Any) -> tuple[int, Any]:
    """
    Stable sort key that puts None / NaN last and avoids cross-type
    comparison errors (Python 3 refuses to compare int < None directly).
    Returns (sort_bucket, value): 0 for present values, 1 for missing.
    """
    if value is None:
        return (1, 0)
    if isinstance(value, float) and value != value:  # NaN
        return (1, 0)
    return (0, value)
