"""
Tests for src.gold.artifacts and src.gold.builders.

Two concerns:
  1. Each artifact's SQL produces the expected output against committed
     samples (pinned row counts, correct aggregations, sensible values).
  2. The builder materialises to Parquet at the right path with the
     right partition layout.

These are integration tests — they run Bronze + Silver first, then
execute Gold queries through the real DuckDB session. The samples are
small enough that pinning specific aggregate values is reasonable.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from src.bronze.run import run_bronze
from src.gold.artifacts import (
    ALL_ARTIFACTS,
    club_performance_metrics,
    club_season_summary,
    get_artifact,
    player_valuation_rolling_avg,
    top_players_all_time,
    top_scorers_by_season,
)
from src.gold.builders import build_gold_artifact
from src.gold.duckdb_session import gold_session
from src.metadata.db import init_db
from src.silver.run import run_silver
from src.utils.config import get_config


SAMPLES_DIR = Path(__file__).resolve().parents[1] / "data" / "sample"


@pytest.fixture
def fresh_db():
    init_db()


@pytest.fixture
def silver_seeded(fresh_db):
    """Bronze + Silver populated; Gold can now build its artifacts."""
    run_bronze(batch_id="2024-12-01", raw_root=SAMPLES_DIR)
    run_silver(batch_id="2024-12-01")


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


class TestArtifactRegistry:
    def test_all_artifacts_have_required_fields(self):
        for artifact in ALL_ARTIFACTS:
            assert artifact.name
            assert artifact.sql.strip()
            assert artifact.sources, f"{artifact.name} declares no sources"
            assert artifact.description

    def test_get_artifact_returns_named(self):
        a = get_artifact("top_scorers_by_season")
        assert a is top_scorers_by_season

    def test_get_artifact_raises_on_unknown(self):
        with pytest.raises(KeyError, match="not registered"):
            get_artifact("no_such_artifact")


# ---------------------------------------------------------------------------
# top_scorers_by_season
# ---------------------------------------------------------------------------


class TestTopScorersBySeason:
    def test_one_row_per_player_per_season(self, silver_seeded):
        cfg = get_config()
        with gold_session(
            silver_root=cfg.paths.silver, bronze_root=cfg.paths.bronze,
        ) as conn:
            df = conn.execute(top_scorers_by_season.sql).fetchdf()
        # Samples have 12 players, all in 2024-25, all with appearances
        assert len(df) == 12
        # Composite key (season, player_sk) is unique
        assert df.groupby(["season", "player_sk"]).size().max() == 1

    def test_top_scorer_calculation(self, silver_seeded):
        """Pinned aggregate — Bellingham and Lewandowski tied with 4 goals."""
        cfg = get_config()
        with gold_session(
            silver_root=cfg.paths.silver, bronze_root=cfg.paths.bronze,
        ) as conn:
            df = conn.execute(top_scorers_by_season.sql).fetchdf()
        top_goals = int(df["total_goals"].max())
        assert top_goals == 4
        top_scorers = set(df[df["total_goals"] == 4]["player_name"])
        assert "Jude Bellingham" in top_scorers
        assert "Robert Lewandowski" in top_scorers

    def test_position_normalised_through_to_gold(self, silver_seeded):
        """The Silver position_canonical column flows through Gold —
        verifies the dimensional join is wired correctly."""
        cfg = get_config()
        with gold_session(
            silver_root=cfg.paths.silver, bronze_root=cfg.paths.bronze,
        ) as conn:
            df = conn.execute(top_scorers_by_season.sql).fetchdf()
        # Goalkeeper should appear (sample has Raya, Sanchez, Neuer)
        assert "Goalkeeper" in set(df["position_canonical"])

    def test_orphan_excluded_post_dq(self, silver_seeded):
        """The orphan player_id=9999 was quarantined in Phase 4 DQ.
        It must not appear in Gold."""
        cfg = get_config()
        with gold_session(
            silver_root=cfg.paths.silver, bronze_root=cfg.paths.bronze,
        ) as conn:
            df = conn.execute(top_scorers_by_season.sql).fetchdf()
        assert 9999 not in set(df["player_id"])


# ---------------------------------------------------------------------------
# club_season_summary
# ---------------------------------------------------------------------------


class TestClubSeasonSummary:
    def test_one_row_per_club_per_season(self, silver_seeded):
        cfg = get_config()
        with gold_session(
            silver_root=cfg.paths.silver, bronze_root=cfg.paths.bronze,
        ) as conn:
            df = conn.execute(club_season_summary.sql).fetchdf()
        # 5 clubs in samples, all in 2024-25
        assert len(df) == 5
        assert df.groupby(["season", "club_id"]).size().max() == 1

    def test_match_counts_consistent(self, silver_seeded):
        """Each match contributes to TWO clubs' matches_played (home + away).
        With 6 sample matches across 5 clubs, sum should be 12."""
        cfg = get_config()
        with gold_session(
            silver_root=cfg.paths.silver, bronze_root=cfg.paths.bronze,
        ) as conn:
            df = conn.execute(club_season_summary.sql).fetchdf()
        assert int(df["matches_played"].sum()) == 12

    def test_points_calculation_correct(self, silver_seeded):
        """Real Madrid: 1 win + 2 draws = 3 + 2 = 5 points."""
        cfg = get_config()
        with gold_session(
            silver_root=cfg.paths.silver, bronze_root=cfg.paths.bronze,
        ) as conn:
            df = conn.execute(club_season_summary.sql).fetchdf()
        rma = df[df["club_name"] == "Real Madrid"].iloc[0]
        assert int(rma["wins"]) == 1
        assert int(rma["draws"]) == 2
        assert int(rma["losses"]) == 0
        assert int(rma["points"]) == 5

    def test_wins_draws_losses_are_integer_typed(self, silver_seeded):
        """Pandas can implicitly upcast SUM-results to float when nulls
        are possible; we CAST AS INTEGER in the SQL to keep them clean."""
        cfg = get_config()
        with gold_session(
            silver_root=cfg.paths.silver, bronze_root=cfg.paths.bronze,
        ) as conn:
            df = conn.execute(club_season_summary.sql).fetchdf()
        # In pandas, integer dtypes have 'int' in their kind
        for col in ["wins", "draws", "losses", "points"]:
            assert df[col].dtype.kind == "i", (
                f"{col} dtype is {df[col].dtype} — expected integer"
            )

    def test_goal_difference_math(self, silver_seeded):
        """goals_for - goals_against = goal_difference, always."""
        cfg = get_config()
        with gold_session(
            silver_root=cfg.paths.silver, bronze_root=cfg.paths.bronze,
        ) as conn:
            df = conn.execute(club_season_summary.sql).fetchdf()
        for _, row in df.iterrows():
            assert (
                row["goal_difference"]
                == row["goals_for"] - row["goals_against"]
            ), f"Bad math for {row['club_name']}"


# ---------------------------------------------------------------------------
# Builder — Parquet materialisation
# ---------------------------------------------------------------------------


class TestGoldBuilder:
    def test_build_writes_partitioned_parquet(self, silver_seeded):
        cfg = get_config()
        with gold_session(
            silver_root=cfg.paths.silver, bronze_root=cfg.paths.bronze,
        ) as conn:
            result = build_gold_artifact(
                artifact=top_scorers_by_season,
                conn=conn,
                gold_root=cfg.paths.gold,
                batch_id="2024-12-01",
            )
        assert result.row_count == 12
        partition_dir = (
            cfg.paths.gold / "top_scorers_by_season" / "batch_id=2024-12-01"
        )
        assert partition_dir.is_dir()
        files = list(partition_dir.glob("*.parquet"))
        assert files

    def test_materialised_parquet_readable_via_pandas(self, silver_seeded):
        cfg = get_config()
        with gold_session(
            silver_root=cfg.paths.silver, bronze_root=cfg.paths.bronze,
        ) as conn:
            build_gold_artifact(
                artifact=club_season_summary, conn=conn,
                gold_root=cfg.paths.gold, batch_id="2024-12-01",
            )
        df = pd.read_parquet(cfg.paths.gold / "club_season_summary")
        assert len(df) == 5
        assert "club_name" in df.columns

    def test_result_reports_correct_sources(self, silver_seeded):
        cfg = get_config()
        with gold_session(
            silver_root=cfg.paths.silver, bronze_root=cfg.paths.bronze,
        ) as conn:
            result = build_gold_artifact(
                artifact=top_scorers_by_season, conn=conn,
                gold_root=cfg.paths.gold, batch_id="2024-12-01",
            )
        # The result should expose the source views the artifact reads
        assert "fact_appearances" in result.sources
        assert "dim_players" in result.sources


# ---------------------------------------------------------------------------
# top_players_all_time
# ---------------------------------------------------------------------------


class TestTopPlayersAllTime:
    def test_one_row_per_player(self, silver_seeded):
        cfg = get_config()
        with gold_session(
            silver_root=cfg.paths.silver, bronze_root=cfg.paths.bronze,
        ) as conn:
            df = conn.execute(top_players_all_time.sql).fetchdf()
        # 12 players in samples, all appear in fact_appearances
        assert len(df) == 12
        assert df["player_id"].is_unique

    def test_lifetime_aggregates_match(self, silver_seeded):
        """Lifetime totals must match per-season totals summed across seasons.
        Pinned values: Bellingham 4 goals in 3 apps; Lewandowski 4 in 2."""
        cfg = get_config()
        with gold_session(
            silver_root=cfg.paths.silver, bronze_root=cfg.paths.bronze,
        ) as conn:
            df = conn.execute(top_players_all_time.sql).fetchdf()
        bellingham = df[df["player_name"] == "Jude Bellingham"].iloc[0]
        assert int(bellingham["total_goals"]) == 4
        assert int(bellingham["appearance_count"]) == 3
        lewandowski = df[df["player_name"] == "Robert Lewandowski"].iloc[0]
        assert int(lewandowski["total_goals"]) == 4
        assert int(lewandowski["appearance_count"]) == 2

    def test_goals_per_appearance_math(self, silver_seeded):
        """goals_per_appearance = total_goals / appearance_count, always."""
        cfg = get_config()
        with gold_session(
            silver_root=cfg.paths.silver, bronze_root=cfg.paths.bronze,
        ) as conn:
            df = conn.execute(top_players_all_time.sql).fetchdf()
        for _, row in df.iterrows():
            expected = row["total_goals"] / row["appearance_count"]
            assert abs(row["goals_per_appearance"] - expected) < 1e-9

    def test_joins_to_current_dim_player_only(self, silver_seeded):
        """Lifetime aggregates use the CURRENT dim row (is_current=True).
        Samples have all-current rows so this is trivially satisfied; but
        the JOIN clause means a future multi-version dim won't double-count."""
        cfg = get_config()
        with gold_session(
            silver_root=cfg.paths.silver, bronze_root=cfg.paths.bronze,
        ) as conn:
            df = conn.execute(top_players_all_time.sql).fetchdf()
        # Still 12 rows (one per player_id); JOIN didn't duplicate
        assert len(df) == 12


# ---------------------------------------------------------------------------
# player_valuation_rolling_avg
# ---------------------------------------------------------------------------


class TestPlayerValuationRollingAvg:
    def test_row_per_valuation_observation(self, silver_seeded):
        """One row per (player_id, date) — same cardinality as Bronze
        player_valuations after filtering nulls."""
        cfg = get_config()
        with gold_session(
            silver_root=cfg.paths.silver, bronze_root=cfg.paths.bronze,
        ) as conn:
            df = conn.execute(player_valuation_rolling_avg.sql).fetchdf()
        # Samples have 18 valuations across multiple players
        assert len(df) == 18

    def test_first_observation_per_player_equals_market_value(self, silver_seeded):
        """The rolling avg of a single observation IS the observation."""
        cfg = get_config()
        with gold_session(
            silver_root=cfg.paths.silver, bronze_root=cfg.paths.bronze,
        ) as conn:
            df = conn.execute(player_valuation_rolling_avg.sql).fetchdf()
        # Find each player's first observation (rolling_sample_count == 1)
        first_obs = df[df["rolling_sample_count"] == 1]
        for _, row in first_obs.iterrows():
            assert row["rolling_avg_90d"] == row["market_value_in_eur"]

    def test_rolling_avg_increases_with_rising_market_value(self, silver_seeded):
        """Saka's market value rises 110M → 115M → 120M; rolling avg should
        rise monotonically too."""
        cfg = get_config()
        with gold_session(
            silver_root=cfg.paths.silver, bronze_root=cfg.paths.bronze,
        ) as conn:
            df = conn.execute(player_valuation_rolling_avg.sql).fetchdf()
        saka = df[df["player_name"] == "Bukayo Saka"].sort_values("date")
        rolling_vals = saka["rolling_avg_90d"].tolist()
        assert rolling_vals == sorted(rolling_vals), (
            f"Rolling avg not monotonically increasing: {rolling_vals}"
        )

    def test_scd2_as_of_join_resolved(self, silver_seeded):
        """Every valuation should resolve to a dim_players version
        (player_sk not null). In samples, dim_players has effective_date
        FAR_PAST_DATE so every historical valuation window is covered."""
        cfg = get_config()
        with gold_session(
            silver_root=cfg.paths.silver, bronze_root=cfg.paths.bronze,
        ) as conn:
            df = conn.execute(player_valuation_rolling_avg.sql).fetchdf()
        # Every row should have a resolved player_sk
        assert df["player_sk"].notna().all(), (
            f"{df['player_sk'].isna().sum()} valuations unresolved to dim_players"
        )


# ---------------------------------------------------------------------------
# club_performance_metrics
# ---------------------------------------------------------------------------


class TestClubPerformanceMetrics:
    def test_one_row_per_club(self, silver_seeded):
        cfg = get_config()
        with gold_session(
            silver_root=cfg.paths.silver, bronze_root=cfg.paths.bronze,
        ) as conn:
            df = conn.execute(club_performance_metrics.sql).fetchdf()
        # 5 clubs in samples
        assert len(df) == 5
        assert df["club_id"].is_unique

    def test_clean_sheet_count(self, silver_seeded):
        """Arsenal has the only clean sheet in the samples."""
        cfg = get_config()
        with gold_session(
            silver_root=cfg.paths.silver, bronze_root=cfg.paths.bronze,
        ) as conn:
            df = conn.execute(club_performance_metrics.sql).fetchdf()
        arsenal = df[df["club_name"] == "Arsenal FC"].iloc[0]
        assert int(arsenal["clean_sheets"]) == 1
        # Other clubs have 0 clean sheets in samples
        others = df[df["club_name"] != "Arsenal FC"]
        assert (others["clean_sheets"] == 0).all()

    def test_win_rate_math(self, silver_seeded):
        """win_rate = wins / matches_played, always."""
        cfg = get_config()
        with gold_session(
            silver_root=cfg.paths.silver, bronze_root=cfg.paths.bronze,
        ) as conn:
            df = conn.execute(club_performance_metrics.sql).fetchdf()
        for _, row in df.iterrows():
            expected = row["wins"] / row["matches_played"]
            assert abs(row["win_rate"] - expected) < 1e-9, (
                f"Bad win_rate math for {row['club_name']}: "
                f"{row['win_rate']} != {row['wins']}/{row['matches_played']}"
            )

    def test_clean_sheet_rate_math(self, silver_seeded):
        """clean_sheet_rate = clean_sheets / matches_played, always."""
        cfg = get_config()
        with gold_session(
            silver_root=cfg.paths.silver, bronze_root=cfg.paths.bronze,
        ) as conn:
            df = conn.execute(club_performance_metrics.sql).fetchdf()
        for _, row in df.iterrows():
            expected = row["clean_sheets"] / row["matches_played"]
            assert abs(row["clean_sheet_rate"] - expected) < 1e-9


# ---------------------------------------------------------------------------
# Registry coverage post-5.2
# ---------------------------------------------------------------------------


class TestPhase5RegistryComplete:
    def test_all_five_brief_artifacts_registered(self):
        """The brief's §6 names five analytical questions; we must have
        all five Gold artifacts registered."""
        names = {a.name for a in ALL_ARTIFACTS}
        assert names == {
            "top_scorers_by_season",       # §6.1
            "club_season_summary",         # §6.2
            "top_players_all_time",        # §6.3
            "player_valuation_rolling_avg", # §6.4
            "club_performance_metrics",    # §6.5
        }
