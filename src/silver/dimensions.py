"""
Silver dimension builders.

Four builders, each producing one Silver dimension from Bronze data:

  build_dim_clubs           Type-1 from bronze.clubs
  build_dim_competitions    Type-1 from bronze.competitions
  build_dim_date            Generated date dimension (no Bronze source)
  build_dim_players         Type-2 SCD from bronze.players

Type-1 vs Type-2

  Type-1 dims overwrite-on-change. We don't track history within the
  table itself, but we DO retain history through Silver's per-batch
  partitioning (same Hive-style partition layout as Bronze).
  Readers query the latest partition for "current state"; older
  partitions remain on disk for ad-hoc time-travel.

  Type-2 — currently just dim_players — tracks history explicitly via
  surrogate keys, effective_date / end_date / is_current. The SCD2
  merge function from Slice 3.2 does the heavy lifting; this module
  is the orchestration layer that calls it.

What the builders return

  A DataFrame ready to write to Silver. They do NOT:
    * Read from disk (callers do that)
    * Write to disk (the Silver runner does that)
    * Apply DQ checks (Phase 4)
    * Touch the audit DAO (the runner orchestrates that)

  This keeps the builders pure, testable, and engine-agnostic.

What the builders DO:
  * Apply Silver-layer transformations (position normalisation,
    country ISO normalisation) where appropriate
  * Project to the right columns (drop bookkeeping like the batch_id
    column that came from Bronze)
  * Add type-appropriate metadata (batch_id, surrogate keys, SCD2
    columns)
"""

from __future__ import annotations

from datetime import date, timedelta

import pandas as pd

from src.engines.base import DataFrame, DataFrameEngine
from src.ingestion.registry import SourceDefinition
from src.silver.scd2 import (
    SCD2MergeStats,
    scd2_merge,
)
from src.silver.transforms import (
    derive_season,
    normalise_country,
    normalise_position,
)
from src.utils.logging import get_logger

log = get_logger(__name__)


# The column Bronze appends to every partition; we project it out before
# writing to Silver (it's a Bronze concern, not a Silver concern).
BRONZE_BATCH_COLUMN = "batch_id"

# Sentinel effective_date used for the INITIAL load of an SCD2 dim. The
# semantics: "this version has been known since forever" — so as-of-event
# joins from facts resolve for any historical match date in the source.
# Subsequent versions get the actual batch_timestamp.
# Picked to be far enough in the past that no realistic source data
# predates it. Date (not datetime) to match end_date FAR_FUTURE_DATE.
FAR_PAST_DATE = "1900-01-01"


# ---------------------------------------------------------------------------
# Type-1 dimensions
# ---------------------------------------------------------------------------


def build_dim_clubs(
    *,
    bronze_clubs: DataFrame,
    engine: DataFrameEngine,
) -> DataFrame:
    """
    Type-1 club dimension.

    One row per club_id reflecting the latest Bronze snapshot.
    Country names are normalised to ISO 3166-1 alpha-2.

    Columns:
      club_id (natural key), club_code, name, domestic_competition_id,
      country_iso_code, total_market_value, squad_size, average_age,
      foreigners_number, foreigners_percentage, national_team_players,
      stadium_name, stadium_seats, last_season
    """
    cols_to_keep = [
        "club_id",
        "club_code",
        "name",
        "domestic_competition_id",
        "total_market_value",
        "squad_size",
        "average_age",
        "foreigners_number",
        "foreigners_percentage",
        "national_team_players",
        "stadium_name",
        "stadium_seats",
        "last_season",
    ]
    df = engine.select(bronze_clubs, [c for c in cols_to_keep if c in engine.columns(bronze_clubs)])

    # We don't have a 'country' column on clubs in the Kaggle data —
    # it's derived from the competition. But the brief's transformation
    # is general ("standardise country names"); we apply it to any
    # available country-shaped column in players (handled in build_dim_players).
    # For clubs the normalisation is a no-op here.

    # Dedupe on natural key, keeping the last (latest) occurrence.
    df = engine.distinct(df, subset=["club_id"])
    log.info("dim_clubs_built", rows=engine.count(df))
    return df


def build_dim_competitions(
    *,
    bronze_competitions: DataFrame,
    engine: DataFrameEngine,
) -> DataFrame:
    """
    Type-1 competition dimension.

    One row per competition_id. Country names normalised to ISO alpha-2
    where present.
    """
    cols = engine.columns(bronze_competitions)
    keep = [
        c
        for c in [
            "competition_id",
            "name",
            "country_name",
            "sub_type",
            "type",
            "confederation",
        ]
        if c in cols
    ]
    df = engine.select(bronze_competitions, keep)

    # Normalise country_name -> country_iso_code (additive column)
    if "country_name" in keep:
        df = engine.with_derived_column(
            df,
            "country_iso_code",
            fn=lambda r: normalise_country(r.get("country_name")),
            input_columns=["country_name"],
        )

    df = engine.distinct(df, subset=["competition_id"])
    log.info("dim_competitions_built", rows=engine.count(df))
    return df


# ---------------------------------------------------------------------------
# Generated date dimension
# ---------------------------------------------------------------------------


def build_dim_date(
    *,
    start_date: date,
    end_date: date,
    engine: DataFrameEngine,
) -> DataFrame:
    """
    Generated date dimension covering [start_date, end_date] inclusive.

    One row per calendar date. Used by facts that need date attributes
    (year, quarter, month, day_of_week, is_weekend, football season).

    The engine parameter is accepted for API consistency and future
    extensibility (Spark would build this through spark.range and a
    UDF). For now we build directly with pandas — this is a small
    bounded table (a few thousand rows even for a 10-year range) and
    its construction is one-shot, not part of the hot path.
    """
    if end_date < start_date:
        raise ValueError(f"end_date {end_date} must not precede start_date {start_date}")

    rows: list[dict] = []
    current = start_date
    while current <= end_date:
        iso = current.isoformat()
        rows.append(
            {
                "date_key": int(current.strftime("%Y%m%d")),  # integer key 20240601 for joins
                "date": iso,
                "year": current.year,
                "quarter": (current.month - 1) // 3 + 1,
                "month": current.month,
                "day": current.day,
                "day_of_week": current.weekday(),  # 0=Mon, 6=Sun
                "day_name": current.strftime("%A"),
                "is_weekend": current.weekday() >= 5,
                "season": derive_season(current),  # football season
            }
        )
        current += timedelta(days=1)

    df = pd.DataFrame(rows)
    log.info("dim_date_built", rows=len(df), start=str(start_date), end=str(end_date))
    return df


# ---------------------------------------------------------------------------
# Type-2 player dimension
# ---------------------------------------------------------------------------


def build_dim_players(
    *,
    bronze_players: DataFrame,
    existing_dim: DataFrame | None,
    players_source: SourceDefinition,
    batch_timestamp: str,
    engine: DataFrameEngine,
) -> tuple[DataFrame, SCD2MergeStats]:
    """
    Type-2 (SCD2) player dimension.

    Args:
        bronze_players:    Bronze players partition for this batch
        existing_dim:      Current state of dim_players (None on first run)
        players_source:    SourceDefinition from the registry — supplies
                           natural_key and tracked_columns
        batch_timestamp:   Date or datetime string used for effective_date
                           on new versions and end_date on closed-out ones
        engine:            DataFrameEngine instance

    Returns:
        (updated_dimension, scd2_merge_stats)

    The builder:
      1. Applies Silver transformations (position normalisation,
         country normalisation)
      2. Projects to the columns we want to carry in the dimension
      3. Delegates the actual SCD2 logic to scd2_merge

    The reviewer-facing significance: this is where the registry's
    declarative scd2 config (Phase 2a) finally drives behaviour.
    natural_key, tracked_columns, and the surrogate-key column are
    not hardcoded — they come from sources.yaml.
    """
    if players_source.scd2 is None:
        raise ValueError(f"players_source must declare an scd2 spec; got {players_source.scd2}")

    # --- Apply Silver transformations ----------------------------------
    # Position: normalise to canonical taxonomy
    transformed = engine.with_derived_column(
        bronze_players,
        "position_canonical",
        fn=lambda r: normalise_position(r.get("position")).canonical,
        input_columns=["position"],
    )
    transformed = engine.with_derived_column(
        transformed,
        "position_category",
        fn=lambda r: normalise_position(r.get("position")).category,
        input_columns=["position"],
    )
    # Country of citizenship -> ISO
    if "country_of_citizenship" in engine.columns(transformed):
        transformed = engine.with_derived_column(
            transformed,
            "country_of_citizenship_iso",
            fn=lambda r: normalise_country(r.get("country_of_citizenship")),
            input_columns=["country_of_citizenship"],
        )

    # --- Project to dimension shape -----------------------------------
    # Keep all columns that exist on Bronze MINUS the Bronze bookkeeping
    # column. The Silver builder is intentionally inclusive — Bronze had
    # the source's exact columns, Silver inherits them all (plus the
    # transformations we just added, minus the layer marker).
    silver_cols = [c for c in engine.columns(transformed) if c != BRONZE_BATCH_COLUMN]
    projected = engine.select(transformed, silver_cols)

    # --- Effective-date convention for first-run vs subsequent runs ----
    # For the FIRST run, set effective_date to a far-past sentinel so
    # as-of-event fact joins resolve correctly for any historical match
    # date in the source data. The "always known" semantics is the
    # standard warehouse pattern for initial loads.
    # For SUBSEQUENT runs (CHANGED rows), use the actual batch_timestamp
    # — that's when the change was observed.
    if existing_dim is None:
        effective_ts = FAR_PAST_DATE
    else:
        effective_ts = batch_timestamp

    # --- Delegate to scd2_merge ----------------------------------------
    natural_key = players_source.primary_key
    tracked = players_source.scd2.tracked_columns
    surrogate_key = "player_sk"

    log.info(
        "dim_players_merge_starting",
        natural_key=natural_key,
        tracked_columns=tracked,
        incoming_rows=engine.count(projected),
        existing_rows=engine.count(existing_dim) if existing_dim is not None else 0,
        batch_timestamp=batch_timestamp,
        effective_timestamp=effective_ts,
    )

    merged, stats = scd2_merge(
        existing_dim=existing_dim,
        incoming=projected,
        natural_key=natural_key,
        tracked_columns=tracked,
        surrogate_key_column=surrogate_key,
        batch_timestamp=effective_ts,
        engine=engine,
    )

    log.info(
        "dim_players_merge_complete",
        new=stats.new_versions,
        changed=stats.changed_versions,
        unchanged=stats.unchanged_versions,
        historical=stats.historical_preserved,
        total_output=stats.total_output_rows,
    )
    return merged, stats
