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
    description: str


# ---------------------------------------------------------------------------
# §6.1 — Top scorers per season
# ---------------------------------------------------------------------------

top_scorers_by_season = GoldArtifact(
    name="top_scorers_by_season",
    sources=["fact_appearances", "dim_players", "dim_clubs"],
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
# Registry — the runner iterates over this
# ---------------------------------------------------------------------------

ALL_ARTIFACTS: list[GoldArtifact] = [
    top_scorers_by_season,
    club_season_summary,
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
