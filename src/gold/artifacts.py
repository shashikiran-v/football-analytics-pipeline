"""
Gold artifact definitions.

Each artifact is a typed Pydantic model declaring:
  - name:        the artifact identifier (matches the materialised
                 Parquet directory name AND the DuckDB view name)
  - sql:         the DuckDB query that produces the artifact's rows
  - sources:     which Silver / Bronze views the query reads (used by
                 the audit DAO to record source-grain provenance)
  - description: human-readable explanation of the artifact

Why these are Python constants, not YAML
-----------------------------------------
SQL is code, not configuration. A typo in a column name should fail
loud at import time (Python parses the constant) rather than at
runtime when an analyst opens a Superset chart. We also want
syntax highlighting and IDE support for the multi-line SQL, which
YAML doesn't give.

This is consistent with our framework-as-config pattern from ADR-0002
(source registry) — the *behaviour* is data-driven (artifacts have
declarative metadata; the runner loops over them), but the SQL
itself is treated as first-class code.

Adding a new artifact = adding a new constant. No new code paths,
no new registrations. The Gold runner iterates over `ALL_ARTIFACTS`
at the bottom of this file.

What's in this slice (5.1)
--------------------------
- top_scorers_by_season:  GROUP BY season, player; SUM(goals)
- club_season_summary:    GROUP BY season, club; SUM(goals_for/against),
                          wins/draws/losses, points

Slice 5.2 adds:
- top_players_all_time
- player_valuation_rolling_avg
- club_performance_metrics
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class GoldArtifact(BaseModel):
    """One Gold analytical artifact, defined as a DuckDB query."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    name: str
    sql: str
    sources: list[str]
    primary_source: str
    description: str


# ---------------------------------------------------------------------------
# §6.1 — Top scorers per season
# ---------------------------------------------------------------------------

top_scorers_by_season = GoldArtifact(
    name="top_scorers_by_season",
    sources=["fact_appearances", "dim_players", "dim_clubs"],
    primary_source="appearances",
    description=(
        "Top scorers per season. One row per (season, player), with "
        "total_goals, total_assists, total_minutes, appearance_count. "
        "Joins to dim_players via player_sk (SCD2-aware) so the player's "
        "club AT THE TIME of the appearance is preserved — not their "
        "current club. The brief's §6.1 question implemented correctly."
    ),
    sql="""
        SELECT
            fa.season,
            fa.player_sk,
            dp.player_id,
            dp.name AS player_name,
            dp.position_canonical,
            dp.current_club_id AS club_id_at_event,
            dc.name AS club_name_at_event,
            SUM(COALESCE(fa.goals, 0))   AS total_goals,
            SUM(COALESCE(fa.assists, 0)) AS total_assists,
            SUM(COALESCE(fa.minutes_played, 0)) AS total_minutes,
            COUNT(*) AS appearance_count
        FROM fact_appearances fa
        INNER JOIN dim_players dp
            ON dp.player_sk = fa.player_sk
        LEFT JOIN dim_clubs dc
            ON dc.club_id = dp.current_club_id
        WHERE fa.player_sk IS NOT NULL
        GROUP BY
            fa.season,
            fa.player_sk,
            dp.player_id,
            dp.name,
            dp.position_canonical,
            dp.current_club_id,
            dc.name
        ORDER BY fa.season DESC, total_goals DESC
    """,
)


# ---------------------------------------------------------------------------
# §6.2 — Season summaries (clubs)
# ---------------------------------------------------------------------------

club_season_summary = GoldArtifact(
    name="club_season_summary",
    sources=["fact_games", "dim_clubs"],
    primary_source="games",
    description=(
        "Per-club, per-season summary. One row per (season, club_id), with "
        "matches_played, wins/draws/losses, goals_for, goals_against, "
        "goal_difference, points. Built by unioning home + away "
        "perspectives so each match contributes one row per club. "
        "The brief's §6.2 question."
    ),
    sql="""
        WITH home_perspective AS (
            SELECT
                fg.season,
                fg.home_club_id AS club_id,
                fg.home_club_goals AS goals_for,
                fg.away_club_goals AS goals_against,
                CASE fg.outcome
                    WHEN 'home_win' THEN 'win'
                    WHEN 'away_win' THEN 'loss'
                    WHEN 'draw'     THEN 'draw'
                    ELSE 'unknown'
                END AS result
            FROM fact_games fg
        ),
        away_perspective AS (
            SELECT
                fg.season,
                fg.away_club_id AS club_id,
                fg.away_club_goals AS goals_for,
                fg.home_club_goals AS goals_against,
                CASE fg.outcome
                    WHEN 'away_win' THEN 'win'
                    WHEN 'home_win' THEN 'loss'
                    WHEN 'draw'     THEN 'draw'
                    ELSE 'unknown'
                END AS result
            FROM fact_games fg
        ),
        all_perspectives AS (
            SELECT * FROM home_perspective
            UNION ALL
            SELECT * FROM away_perspective
        )
        SELECT
            ap.season,
            ap.club_id,
            dc.name AS club_name,
            dc.domestic_competition_id,
            COUNT(*) AS matches_played,
            CAST(SUM(CASE WHEN ap.result = 'win'  THEN 1 ELSE 0 END) AS INTEGER) AS wins,
            CAST(SUM(CASE WHEN ap.result = 'draw' THEN 1 ELSE 0 END) AS INTEGER) AS draws,
            CAST(SUM(CASE WHEN ap.result = 'loss' THEN 1 ELSE 0 END) AS INTEGER) AS losses,
            SUM(COALESCE(ap.goals_for, 0))     AS goals_for,
            SUM(COALESCE(ap.goals_against, 0)) AS goals_against,
            SUM(COALESCE(ap.goals_for, 0)) - SUM(COALESCE(ap.goals_against, 0))
                AS goal_difference,
            CAST(
                3 * SUM(CASE WHEN ap.result = 'win'  THEN 1 ELSE 0 END)
              + 1 * SUM(CASE WHEN ap.result = 'draw' THEN 1 ELSE 0 END)
                AS INTEGER
            ) AS points
        FROM all_perspectives ap
        LEFT JOIN dim_clubs dc
            ON dc.club_id = ap.club_id
        GROUP BY ap.season, ap.club_id, dc.name, dc.domestic_competition_id
        ORDER BY ap.season DESC, points DESC
    """,
)


# ---------------------------------------------------------------------------
# §6.3 — Top players by total goals (all-time)
# ---------------------------------------------------------------------------

top_players_all_time = GoldArtifact(
    name="top_players_all_time",
    sources=["fact_appearances", "dim_players", "dim_clubs"],
    primary_source="appearances",
    description=(
        "Lifetime totals per player across all seasons. One row per "
        "player, with total_goals, total_assists, total_minutes, "
        "appearance_count, seasons_played. Player attributes come from "
        "the player's CURRENT dim_players row (is_current=True) since "
        "lifetime aggregates are inherently across all SCD2 versions. "
        "The brief's §6.3 question."
    ),
    sql="""
        WITH per_player_totals AS (
            SELECT
                dp.player_id,
                SUM(COALESCE(fa.goals, 0))   AS total_goals,
                SUM(COALESCE(fa.assists, 0)) AS total_assists,
                SUM(COALESCE(fa.minutes_played, 0)) AS total_minutes,
                COUNT(*) AS appearance_count,
                COUNT(DISTINCT fa.season) AS seasons_played
            FROM fact_appearances fa
            INNER JOIN dim_players dp
                ON dp.player_sk = fa.player_sk
            WHERE fa.player_sk IS NOT NULL
            GROUP BY dp.player_id
        )
        SELECT
            t.player_id,
            dp.name AS player_name,
            dp.position_canonical,
            dp.country_of_citizenship_iso,
            dp.current_club_id,
            dc.name AS current_club_name,
            t.total_goals,
            t.total_assists,
            t.total_minutes,
            t.appearance_count,
            t.seasons_played,
            CAST(
                CASE WHEN t.appearance_count > 0
                     THEN CAST(t.total_goals AS DOUBLE) / t.appearance_count
                     ELSE 0
                END
                AS DOUBLE
            ) AS goals_per_appearance
        FROM per_player_totals t
        INNER JOIN dim_players dp
            ON dp.player_id = t.player_id
            AND dp.is_current = TRUE
        LEFT JOIN dim_clubs dc
            ON dc.club_id = dp.current_club_id
        ORDER BY t.total_goals DESC, t.appearance_count DESC
    """,
)


# ---------------------------------------------------------------------------
# §6.4 — Player valuation trends (90-day rolling average)
# ---------------------------------------------------------------------------

player_valuation_rolling_avg = GoldArtifact(
    name="player_valuation_rolling_avg",
    sources=["bronze_player_valuations", "dim_players", "dim_clubs"],
    primary_source="player_valuations",
    description=(
        "90-day rolling average of player market value. Reads "
        "bronze_player_valuations directly (per ADR-0005, no Silver "
        "layer exists for this source — its shape is already "
        "aggregation-ready). Joins to dim_players AS-OF the valuation "
        "date for SCD2-aware player context. Implemented via DuckDB "
        "window function: AVG(market_value_in_eur) OVER "
        "(PARTITION BY player_id ORDER BY date ROWS BETWEEN 89 PRECEDING "
        "AND CURRENT ROW). The brief's §6.4 question."
    ),
    sql="""
        WITH valuations_with_rolling AS (
            SELECT
                pv.player_id,
                pv.date,
                pv.market_value_in_eur,
                AVG(pv.market_value_in_eur) OVER (
                    PARTITION BY pv.player_id
                    ORDER BY pv.date
                    ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
                ) AS rolling_avg_90d,
                COUNT(*) OVER (
                    PARTITION BY pv.player_id
                    ORDER BY pv.date
                    ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
                ) AS rolling_sample_count,
                ROW_NUMBER() OVER (
                    PARTITION BY pv.player_id
                    ORDER BY pv.date
                ) AS rn_per_player
            FROM bronze_player_valuations pv
            WHERE pv.market_value_in_eur IS NOT NULL
              AND pv.date IS NOT NULL
        ),
        valuations_as_of AS (
            SELECT
                v.player_id,
                v.date,
                v.market_value_in_eur,
                v.rolling_avg_90d,
                v.rolling_sample_count,
                v.rn_per_player,
                dp.player_sk,
                dp.name AS player_name,
                dp.position_canonical,
                dp.current_club_id AS club_id_at_event,
                dc.name AS club_name_at_event
            FROM valuations_with_rolling v
            LEFT JOIN dim_players dp
                ON dp.player_id = v.player_id
                AND CAST(v.date AS VARCHAR) >= dp.effective_date
                AND CAST(v.date AS VARCHAR) <= dp.end_date
            LEFT JOIN dim_clubs dc
                ON dc.club_id = dp.current_club_id
        )
        SELECT
            player_id,
            player_sk,
            player_name,
            position_canonical,
            club_id_at_event,
            club_name_at_event,
            date,
            market_value_in_eur,
            CAST(rolling_avg_90d AS DOUBLE) AS rolling_avg_90d,
            CAST(rolling_sample_count AS INTEGER) AS rolling_sample_count
        FROM valuations_as_of
        ORDER BY player_id, date
    """,
)


# ---------------------------------------------------------------------------
# §6.5 — Club performance metrics (all-time)
# ---------------------------------------------------------------------------

club_performance_metrics = GoldArtifact(
    name="club_performance_metrics",
    sources=["fact_games", "dim_clubs"],
    primary_source="games",
    description=(
        "Per-club lifetime performance metrics across all seasons. One "
        "row per club_id with matches_played, win_rate, goals_per_game, "
        "goals_conceded_per_game, clean_sheets, clean_sheet_rate. Like "
        "club_season_summary but without the season partition — useful "
        "for ranking clubs over the dataset's full history. The brief's "
        "§6.5 question."
    ),
    sql="""
        WITH home_perspective AS (
            SELECT
                fg.home_club_id AS club_id,
                fg.home_club_goals AS goals_for,
                fg.away_club_goals AS goals_against,
                CASE fg.outcome
                    WHEN 'home_win' THEN 'win'
                    WHEN 'away_win' THEN 'loss'
                    WHEN 'draw'     THEN 'draw'
                    ELSE 'unknown'
                END AS result
            FROM fact_games fg
        ),
        away_perspective AS (
            SELECT
                fg.away_club_id AS club_id,
                fg.away_club_goals AS goals_for,
                fg.home_club_goals AS goals_against,
                CASE fg.outcome
                    WHEN 'away_win' THEN 'win'
                    WHEN 'home_win' THEN 'loss'
                    WHEN 'draw'     THEN 'draw'
                    ELSE 'unknown'
                END AS result
            FROM fact_games fg
        ),
        all_perspectives AS (
            SELECT * FROM home_perspective
            UNION ALL
            SELECT * FROM away_perspective
        ),
        per_club_aggregated AS (
            SELECT
                ap.club_id,
                COUNT(*) AS matches_played,
                CAST(SUM(CASE WHEN ap.result = 'win'  THEN 1 ELSE 0 END) AS INTEGER) AS wins,
                CAST(SUM(CASE WHEN ap.result = 'draw' THEN 1 ELSE 0 END) AS INTEGER) AS draws,
                CAST(SUM(CASE WHEN ap.result = 'loss' THEN 1 ELSE 0 END) AS INTEGER) AS losses,
                SUM(COALESCE(ap.goals_for, 0))     AS total_goals_for,
                SUM(COALESCE(ap.goals_against, 0)) AS total_goals_against,
                CAST(
                    SUM(CASE WHEN COALESCE(ap.goals_against, 0) = 0 THEN 1 ELSE 0 END)
                    AS INTEGER
                ) AS clean_sheets
            FROM all_perspectives ap
            GROUP BY ap.club_id
        )
        SELECT
            p.club_id,
            dc.name AS club_name,
            dc.domestic_competition_id,
            p.matches_played,
            p.wins,
            p.draws,
            p.losses,
            p.total_goals_for,
            p.total_goals_against,
            p.clean_sheets,
            CASE WHEN p.matches_played > 0
                 THEN CAST(p.wins AS DOUBLE) / p.matches_played
                 ELSE 0 END AS win_rate,
            CASE WHEN p.matches_played > 0
                 THEN CAST(p.total_goals_for AS DOUBLE) / p.matches_played
                 ELSE 0 END AS goals_per_game,
            CASE WHEN p.matches_played > 0
                 THEN CAST(p.total_goals_against AS DOUBLE) / p.matches_played
                 ELSE 0 END AS goals_conceded_per_game,
            CASE WHEN p.matches_played > 0
                 THEN CAST(p.clean_sheets AS DOUBLE) / p.matches_played
                 ELSE 0 END AS clean_sheet_rate
        FROM per_club_aggregated p
        LEFT JOIN dim_clubs dc
            ON dc.club_id = p.club_id
        ORDER BY p.wins DESC, p.total_goals_for DESC
    """,
)


# ---------------------------------------------------------------------------
# Registry — the runner iterates over this
# ---------------------------------------------------------------------------

ALL_ARTIFACTS: list[GoldArtifact] = [
    top_scorers_by_season,
    club_season_summary,
    top_players_all_time,
    player_valuation_rolling_avg,
    club_performance_metrics,
]


def get_artifact(name: str) -> GoldArtifact:
    """Lookup helper. Raises if no matching artifact is registered."""
    for artifact in ALL_ARTIFACTS:
        if artifact.name == name:
            return artifact
    available = ", ".join(a.name for a in ALL_ARTIFACTS)
    raise KeyError(
        f"Gold artifact {name!r} not registered. Available: {available}"
    )
