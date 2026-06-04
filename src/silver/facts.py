"""
Silver fact builders.

Two builders, each producing one Silver fact table:

  build_fact_games          One row per match
  build_fact_appearances    One row per (player, match) appearance

Both consume Bronze data plus the dimensions built in Slice 3.3.

The as-of-event FK pattern
--------------------------
The differentiating choice in this module is how facts reference the
SCD2 player dimension. The naive option (link by player_id to the
current dim_players row) loses the SCD2 advantage — if a player's
attributes changed since the match, the fact would attribute the
appearance to the new attributes.

The correct option is as-of-event resolution: for each appearance,
find the dim_players VERSION whose [effective_date, end_date) window
contains the match date. The fact's player_sk then points at the
player's attributes at the time of the match. This is what SCD2
exists to enable.

Implementation note: with the committed samples (no history yet),
this collapses to a simple join on player_id where is_current=True.
But the as-of logic is implemented and tested correctly; Phase 6's
day-2 demo will exercise it against real version history.

What the builders do NOT do
---------------------------
* Read from or write to disk (the Silver runner does that)
* Touch the audit DAO (the runner orchestrates that)
* Run DQ checks (Phase 4)
"""

from __future__ import annotations

from typing import Any

from src.engines.base import DataFrame, DataFrameEngine
from src.silver.transforms import derive_match_outcome, derive_season
from src.utils.logging import get_logger

log = get_logger(__name__)


# The column Bronze appends to every partition; we project it out before
# writing to Silver, same convention as the dim builders (Slice 3.3).
BRONZE_BATCH_COLUMN = "batch_id"


# ---------------------------------------------------------------------------
# fact_games
# ---------------------------------------------------------------------------


def build_fact_games(
    *,
    bronze_games: DataFrame,
    engine: DataFrameEngine,
) -> DataFrame:
    """
    Build the games fact table.

    One row per match. Adds two derived columns required by the brief:
      outcome    'home_win' | 'away_win' | 'draw' | 'unknown'
      season     football season label like '2024-25'

    Plus a date_key (int YYYYMMDD) for joining to dim_date.

    The home_club_id and away_club_id columns are preserved as natural
    foreign keys into dim_clubs (which is Type-1, so no version
    resolution needed).

    Returns:
        DataFrame ready for Silver write. Bronze batch_id column dropped.
    """
    # --- Add derived columns ----------------------------------------------
    df = engine.with_derived_column(
        bronze_games,
        "outcome",
        fn=lambda r: derive_match_outcome(
            r.get("home_club_goals"),
            r.get("away_club_goals"),
        ),
        input_columns=["home_club_goals", "away_club_goals"],
    )
    df = engine.with_derived_column(
        df,
        "season",
        fn=lambda r: derive_season(r.get("date")),
        input_columns=["date"],
    )
    # date_key for join to dim_date — YYYYMMDD as int.
    df = engine.with_derived_column(
        df,
        "date_key",
        fn=lambda r: _date_to_key(r.get("date")),
        input_columns=["date"],
    )

    # --- Drop Bronze bookkeeping column ----------------------------------
    cols_to_keep = [c for c in engine.columns(df) if c != BRONZE_BATCH_COLUMN]
    df = engine.select(df, cols_to_keep)

    log.info("fact_games_built", rows=engine.count(df))
    return df


# ---------------------------------------------------------------------------
# fact_appearances — with as-of-event FK resolution to dim_players
# ---------------------------------------------------------------------------


def build_fact_appearances(
    *,
    bronze_appearances: DataFrame,
    dim_players: DataFrame,
    engine: DataFrameEngine,
) -> DataFrame:
    """
    Build the appearances fact table.

    One row per (player, match) appearance. References dim_players via
    AS-OF-EVENT player_sk resolution — the surrogate key points at the
    version of the player that was current at the time of the match.

    The brief's "derived columns" requirement is implicit here: the
    season is propagated from fact_games via date, and the player_sk
    FK is the SCD2-aware join key.

    Args:
        bronze_appearances:  Bronze appearances partition for this batch
        dim_players:         The CURRENT Silver dim_players state. Must
                             have effective_date, end_date, is_current,
                             and a player_sk column.
        engine:              DataFrameEngine

    Returns:
        DataFrame with one row per Bronze appearance, plus:
          - player_sk:  the as-of-event surrogate key (None if no version found)
          - season:     derived football season for joining/grouping
          - date_key:   YYYYMMDD int for joining to dim_date

        The original player_id is preserved for non-SCD-aware queries.
    """
    # --- Derive season and date_key on the appearance --------------------
    df = engine.with_derived_column(
        bronze_appearances,
        "season",
        fn=lambda r: derive_season(r.get("date")),
        input_columns=["date"],
    )
    df = engine.with_derived_column(
        df,
        "date_key",
        fn=lambda r: _date_to_key(r.get("date")),
        input_columns=["date"],
    )

    # --- AS-OF-EVENT JOIN to dim_players ---------------------------------
    # For each appearance, find the dim_players row where:
    #   dim_players.player_id == appearance.player_id
    #   dim_players.effective_date <= appearance.date < dim_players.end_date
    #
    # The engine protocol's join operations are equi-joins only (by
    # design — Spark's join hints work cleanly for equi-joins and badly
    # for theta-joins). So we implement the temporal predicate via a
    # records-based lookup: build an in-memory index of dim_players
    # versions per player_id, then for each appearance row, look up
    # the version whose window contains the match date.
    #
    # For our scale (a few thousand players, dozens of versions each)
    # this is trivially fast. For multi-million-row dims we'd switch to
    # an engine-native range join or a broadcast lookup.
    player_versions = _build_player_version_index(dim_players, engine)

    df = engine.with_derived_column(
        df,
        "player_sk",
        fn=lambda r: _lookup_player_sk_at(
            player_id=r.get("player_id"),
            match_date=r.get("date"),
            player_versions=player_versions,
        ),
        input_columns=["player_id", "date"],
    )

    # --- Drop Bronze bookkeeping column ----------------------------------
    cols_to_keep = [c for c in engine.columns(df) if c != BRONZE_BATCH_COLUMN]
    df = engine.select(df, cols_to_keep)

    # --- Log FK resolution rate (helps detect upstream issues) -----------
    # Note: we use to_records here for accurate None counting; Pandas
    # equality with None returns NaN, not True, so engine.filter_eq
    # with value=None doesn't give the right answer for nullable columns.
    total_rows = engine.count(df)
    if total_rows:
        records = engine.to_records(engine.select(df, ["player_sk"]))
        unresolved = sum(1 for rec in records if rec["player_sk"] is None)
    else:
        unresolved = 0
    log.info(
        "fact_appearances_built",
        rows=total_rows,
        player_sk_unresolved=unresolved,
        player_sk_resolved_pct=(
            round(100 * (total_rows - unresolved) / total_rows, 1) if total_rows else None
        ),
    )
    return df


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _date_to_key(value: Any) -> int | None:
    """
    Convert an ISO date string (or date-like value) to a YYYYMMDD int.
    Returns None for unparseable input. Matches dim_date's date_key.
    """
    if value is None:
        return None
    if isinstance(value, int):
        # Already a date_key
        return value
    s = str(value).strip()
    if not s:
        return None
    # Strip any time portion: '2024-10-15T15:30:00' or '2024-10-15 15:30'
    date_part = s.split("T", 1)[0].split(" ", 1)[0]
    parts = date_part.split("-")
    if len(parts) != 3:
        return None
    try:
        return int(parts[0]) * 10000 + int(parts[1]) * 100 + int(parts[2])
    except ValueError:
        return None


def _build_player_version_index(
    dim_players: DataFrame,
    engine: DataFrameEngine,
) -> dict[Any, list[dict]]:
    """
    Build an in-memory lookup of dim_players versions, keyed by player_id.

    Each value is a list of dicts: {effective_date, end_date, player_sk}.
    Sorted descending by effective_date so the most-recent version that
    matches is found first.

    Memory cost: O(versions). For our dim it's a few KB; in pathological
    multi-million-row dims this would warrant a different approach.
    """
    needed_cols = ["player_id", "effective_date", "end_date", "player_sk"]
    have = engine.columns(dim_players)
    missing = [c for c in needed_cols if c not in have]
    if missing:
        raise ValueError(f"dim_players missing columns required for as-of join: {missing}")

    records = engine.to_records(engine.select(dim_players, needed_cols))
    index: dict[Any, list[dict]] = {}
    for rec in records:
        pid = rec["player_id"]
        index.setdefault(pid, []).append(
            {
                "effective_date": rec["effective_date"],
                "end_date": rec["end_date"],
                "player_sk": rec["player_sk"],
            }
        )
    # Sort each player's versions newest-effective_date first so the
    # window-containment check returns the right version quickly.
    for pid in index:
        index[pid].sort(key=lambda v: v["effective_date"], reverse=True)
    return index


def _lookup_player_sk_at(
    *,
    player_id: Any,
    match_date: Any,
    player_versions: dict[Any, list[dict]],
) -> int | None:
    """
    Find the player_sk whose [effective_date, end_date) window contains
    match_date.

    Returns None when:
      - player_id not in dim_players at all (orphan FK in Bronze — the
        DQ layer will flag this; our committed samples have one such
        row deliberately)
      - match_date can't be parsed
      - no version's window contains match_date (shouldn't happen if
        the dim covers the right time range; would indicate a bug or
        missing history)
    """
    if player_id is None:
        return None
    if match_date is None:
        return None
    versions = player_versions.get(player_id)
    if not versions:
        return None
    match_str = str(match_date).strip()
    if not match_str:
        return None
    # Normalise the match date to ISO date form ('YYYY-MM-DD') for
    # string-comparison correctness. effective_date and end_date are
    # already stored as ISO strings by scd2_merge.
    date_part = match_str.split("T", 1)[0].split(" ", 1)[0]

    for v in versions:
        eff = v["effective_date"]
        end = v["end_date"]
        # eff <= match_date <= end (both inclusive — end_date is
        # FAR_FUTURE_DATE for currently-active versions, so this is fine)
        if eff <= date_part <= end:
            return v["player_sk"]
    return None
