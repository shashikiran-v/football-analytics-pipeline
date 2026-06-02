"""
Tests for src.silver.facts.

Two builders, but the differentiating test is the as-of-event FK
resolution in fact_appearances. That's the SCD2 advantage actually
being delivered.
"""

from __future__ import annotations

import pandas as pd
import pytest

from src.engines.pandas_engine import PandasEngine
from src.silver.facts import build_fact_appearances, build_fact_games


@pytest.fixture
def engine():
    return PandasEngine()


# ---------------------------------------------------------------------------
# fact_games
# ---------------------------------------------------------------------------


class TestFactGames:
    def test_one_row_per_match(self, engine):
        bronze = pd.DataFrame([
            {"game_id": 1, "home_club_id": 100, "away_club_id": 200,
             "home_club_goals": 2, "away_club_goals": 0,
             "date": "2024-10-15", "batch_id": "B1"},
            {"game_id": 2, "home_club_id": 200, "away_club_id": 100,
             "home_club_goals": 1, "away_club_goals": 1,
             "date": "2024-11-23", "batch_id": "B1"},
        ])
        fact = build_fact_games(bronze_games=bronze, engine=engine)
        assert engine.count(fact) == 2

    def test_outcome_derived_for_all_three_cases(self, engine):
        bronze = pd.DataFrame([
            {"game_id": 1, "home_club_id": 100, "away_club_id": 200,
             "home_club_goals": 2, "away_club_goals": 0, "date": "2024-10-15", "batch_id": "B1"},
            {"game_id": 2, "home_club_id": 200, "away_club_id": 100,
             "home_club_goals": 0, "away_club_goals": 1, "date": "2024-10-22", "batch_id": "B1"},
            {"game_id": 3, "home_club_id": 100, "away_club_id": 300,
             "home_club_goals": 1, "away_club_goals": 1, "date": "2024-11-05", "batch_id": "B1"},
        ])
        fact = build_fact_games(bronze_games=bronze, engine=engine)
        records = {r["game_id"]: r for r in engine.to_records(fact)}
        assert records[1]["outcome"] == "home_win"
        assert records[2]["outcome"] == "away_win"
        assert records[3]["outcome"] == "draw"

    def test_season_derived(self, engine):
        bronze = pd.DataFrame([
            {"game_id": 1, "home_club_id": 100, "away_club_id": 200,
             "home_club_goals": 2, "away_club_goals": 0,
             "date": "2024-10-15", "batch_id": "B1"},
            {"game_id": 2, "home_club_id": 100, "away_club_id": 200,
             "home_club_goals": 2, "away_club_goals": 0,
             "date": "2025-08-15", "batch_id": "B1"},
        ])
        fact = build_fact_games(bronze_games=bronze, engine=engine)
        records = {r["game_id"]: r for r in engine.to_records(fact)}
        assert records[1]["season"] == "2024-25"
        assert records[2]["season"] == "2025-26"

    def test_date_key_is_integer_yyyymmdd(self, engine):
        bronze = pd.DataFrame([
            {"game_id": 1, "home_club_id": 100, "away_club_id": 200,
             "home_club_goals": 2, "away_club_goals": 0,
             "date": "2024-10-15", "batch_id": "B1"},
        ])
        fact = build_fact_games(bronze_games=bronze, engine=engine)
        rec = engine.to_records(fact)[0]
        assert rec["date_key"] == 20241015

    def test_drops_bronze_batch_id_column(self, engine):
        bronze = pd.DataFrame([
            {"game_id": 1, "home_club_id": 100, "away_club_id": 200,
             "home_club_goals": 2, "away_club_goals": 0,
             "date": "2024-10-15", "batch_id": "B1"},
        ])
        fact = build_fact_games(bronze_games=bronze, engine=engine)
        assert "batch_id" not in engine.columns(fact)


# ---------------------------------------------------------------------------
# fact_appearances — the as-of-event FK is the centrepiece test
# ---------------------------------------------------------------------------


def _make_dim_players(rows: list[dict]) -> pd.DataFrame:
    """Build a minimal dim_players-shaped DataFrame for testing."""
    return pd.DataFrame(rows)


class TestFactAppearances:
    def test_basic_resolution_single_version(self, engine):
        """One player, one version with a wide window. Match falls inside."""
        dim = _make_dim_players([
            {"player_id": 100, "player_sk": 1,
             "effective_date": "1900-01-01", "end_date": "9999-12-31",
             "is_current": True},
        ])
        bronze = pd.DataFrame([
            {"appearance_id": "A1", "player_id": 100, "game_id": 5001,
             "date": "2024-10-15", "minutes_played": 90, "goals": 1, "batch_id": "B1"},
        ])
        fact = build_fact_appearances(
            bronze_appearances=bronze, dim_players=dim, engine=engine,
        )
        rec = engine.to_records(fact)[0]
        assert rec["player_sk"] == 1

    def test_orphan_player_id_resolves_to_none(self, engine):
        """The deliberate edge case from data/sample/: player_id=9999
        is not in dim_players, so its player_sk MUST be None.
        DQ catches this; the fact builder doesn't drop the row."""
        dim = _make_dim_players([
            {"player_id": 100, "player_sk": 1,
             "effective_date": "1900-01-01", "end_date": "9999-12-31",
             "is_current": True},
        ])
        bronze = pd.DataFrame([
            {"appearance_id": "A1", "player_id": 9999, "game_id": 5001,
             "date": "2024-10-15", "minutes_played": 90, "goals": 1, "batch_id": "B1"},
        ])
        fact = build_fact_appearances(
            bronze_appearances=bronze, dim_players=dim, engine=engine,
        )
        recs = engine.to_records(fact)
        # Row is preserved but player_sk is None
        assert len(recs) == 1
        assert recs[0]["player_id"] == 9999
        assert recs[0]["player_sk"] is None

    def test_as_of_event_picks_correct_version(self, engine):
        """
        The differentiating test. Player 100 has TWO versions:
          sk=1: effective 2024-01-01, end 2024-06-30 (Arsenal era)
          sk=2: effective 2024-07-01, end 9999-12-31 (Real Madrid era)

        An appearance on 2024-04-15 should resolve to sk=1 (Arsenal).
        An appearance on 2024-09-15 should resolve to sk=2 (Real Madrid).
        """
        dim = _make_dim_players([
            {"player_id": 100, "player_sk": 1, "current_club_id": "ARS",
             "effective_date": "2024-01-01", "end_date": "2024-06-30",
             "is_current": False},
            {"player_id": 100, "player_sk": 2, "current_club_id": "RMA",
             "effective_date": "2024-07-01", "end_date": "9999-12-31",
             "is_current": True},
        ])
        bronze = pd.DataFrame([
            {"appearance_id": "A_april", "player_id": 100, "game_id": 1,
             "date": "2024-04-15", "minutes_played": 90, "batch_id": "B1"},
            {"appearance_id": "A_september", "player_id": 100, "game_id": 2,
             "date": "2024-09-15", "minutes_played": 90, "batch_id": "B1"},
        ])
        fact = build_fact_appearances(
            bronze_appearances=bronze, dim_players=dim, engine=engine,
        )
        records = {r["appearance_id"]: r for r in engine.to_records(fact)}
        assert records["A_april"]["player_sk"] == 1
        assert records["A_september"]["player_sk"] == 2

    def test_match_outside_any_window_resolves_to_none(self, engine):
        """
        If the match predates the earliest known version of a player,
        the as-of join finds nothing. (In production this shouldn't
        happen with our FAR_PAST_DATE sentinel for initial loads, but
        the function must handle it gracefully.)
        """
        dim = _make_dim_players([
            {"player_id": 100, "player_sk": 1,
             "effective_date": "2024-07-01", "end_date": "9999-12-31",
             "is_current": True},
        ])
        bronze = pd.DataFrame([
            {"appearance_id": "A1", "player_id": 100, "game_id": 1,
             "date": "2024-01-15", "minutes_played": 90, "batch_id": "B1"},
        ])
        fact = build_fact_appearances(
            bronze_appearances=bronze, dim_players=dim, engine=engine,
        )
        rec = engine.to_records(fact)[0]
        assert rec["player_sk"] is None

    def test_season_and_date_key_derived(self, engine):
        dim = _make_dim_players([
            {"player_id": 100, "player_sk": 1,
             "effective_date": "1900-01-01", "end_date": "9999-12-31",
             "is_current": True},
        ])
        bronze = pd.DataFrame([
            {"appearance_id": "A1", "player_id": 100, "game_id": 1,
             "date": "2024-10-15", "minutes_played": 90, "batch_id": "B1"},
        ])
        fact = build_fact_appearances(
            bronze_appearances=bronze, dim_players=dim, engine=engine,
        )
        rec = engine.to_records(fact)[0]
        assert rec["season"] == "2024-25"
        assert rec["date_key"] == 20241015

    def test_drops_bronze_batch_id_column(self, engine):
        dim = _make_dim_players([
            {"player_id": 100, "player_sk": 1,
             "effective_date": "1900-01-01", "end_date": "9999-12-31",
             "is_current": True},
        ])
        bronze = pd.DataFrame([
            {"appearance_id": "A1", "player_id": 100, "game_id": 1,
             "date": "2024-10-15", "minutes_played": 90, "batch_id": "B1"},
        ])
        fact = build_fact_appearances(
            bronze_appearances=bronze, dim_players=dim, engine=engine,
        )
        assert "batch_id" not in engine.columns(fact)

    def test_dim_missing_required_columns_raises(self, engine):
        # dim missing 'end_date'
        dim = pd.DataFrame([
            {"player_id": 100, "player_sk": 1,
             "effective_date": "1900-01-01", "is_current": True},
        ])
        bronze = pd.DataFrame([
            {"appearance_id": "A1", "player_id": 100, "game_id": 1,
             "date": "2024-10-15", "minutes_played": 90, "batch_id": "B1"},
        ])
        with pytest.raises(ValueError, match="as-of join"):
            build_fact_appearances(
                bronze_appearances=bronze, dim_players=dim, engine=engine,
            )
