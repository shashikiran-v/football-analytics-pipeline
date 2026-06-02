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
    club_season_summary,
    get_artifact,
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
