"""
Tests for the DataFrameEngine protocol.

These tests are parametrised by engine, so the same suite proves both
PandasEngine and (later) SparkEngine satisfy the contract.

Test names follow `test_<operation>_<scenario>` so a failure tells you
immediately which operation broke on which engine.
"""

from __future__ import annotations

import pandas as pd
import pytest

from src.engines.base import DataFrameEngine
from src.utils.hashing import hash_row

# ---------------------------------------------------------------------------
# Helpers: build small dataframes via the engine's own I/O so each test is
# engine-native without leaking pandas-specific construction.
# ---------------------------------------------------------------------------


def _make_appearances(engine: DataFrameEngine, tmp_path):
    """Tiny dataset: 4 appearances across 2 players, 2 games."""
    csv = tmp_path / "appearances.csv"
    pd.DataFrame(
        {
            "appearance_id": ["a1", "a2", "a3", "a4"],
            "player_id": [10, 20, 10, 20],
            "game_id": [100, 100, 101, 101],
            "minutes_played": [90, 75, 90, 0],
            "goals": [1, 0, 2, 0],
        }
    ).to_csv(csv, index=False)
    return engine.read_csv(
        csv,
        schema={
            "appearance_id": "string",
            "player_id": "int",
            "game_id": "int",
            "minutes_played": "int",
            "goals": "int",
        },
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestIO:
    def test_csv_roundtrip_preserves_rowcount(self, engine, tmp_path):
        df = _make_appearances(engine, tmp_path)
        assert engine.count(df) == 4

    def test_parquet_roundtrip(self, engine, tmp_path, tmp_lake):
        df = _make_appearances(engine, tmp_path)
        out = tmp_lake / "appearances"
        engine.write_parquet(df, out)
        reloaded = engine.read_parquet(out)
        assert engine.count(reloaded) == 4
        assert set(engine.columns(reloaded)) == set(engine.columns(df))

    def test_parquet_partitioned_write(self, engine, tmp_path, tmp_lake):
        df = _make_appearances(engine, tmp_path)
        df = engine.with_constant_column(df, "batch_id", "2026-05-29T15")
        out = tmp_lake / "appearances_partitioned"
        engine.write_parquet(df, out, partition_by=["batch_id"])
        # Should create a Hive-style partition directory
        assert (out / "batch_id=2026-05-29T15").exists()


class TestRowLevel:
    def test_select_and_columns(self, engine, tmp_path):
        df = _make_appearances(engine, tmp_path)
        projected = engine.select(df, ["player_id", "goals"])
        assert engine.columns(projected) == ["player_id", "goals"]

    def test_select_missing_column_raises(self, engine, tmp_path):
        df = _make_appearances(engine, tmp_path)
        with pytest.raises(KeyError):
            engine.select(df, ["does_not_exist"])

    def test_rename(self, engine, tmp_path):
        df = _make_appearances(engine, tmp_path)
        out = engine.rename(df, {"goals": "goals_scored"})
        assert "goals_scored" in engine.columns(out)
        assert "goals" not in engine.columns(out)

    def test_filter_eq(self, engine, tmp_path):
        df = _make_appearances(engine, tmp_path)
        out = engine.filter_eq(df, "player_id", 10)
        assert engine.count(out) == 2

    def test_filter_isin(self, engine, tmp_path):
        df = _make_appearances(engine, tmp_path)
        out = engine.filter_isin(df, "game_id", [100])
        assert engine.count(out) == 2

    def test_filter_range_both_bounds(self, engine, tmp_path):
        df = _make_appearances(engine, tmp_path)
        out = engine.filter_range(df, "minutes_played", ge=1, le=89)
        # 75 qualifies, 0 and 90 don't
        assert engine.count(out) == 1

    def test_filter_not_null(self, engine, tmp_path):
        csv = tmp_path / "with_nulls.csv"
        pd.DataFrame({"a": [1, None, 3], "b": [10, 20, None]}).to_csv(csv, index=False)
        df = engine.read_csv(csv)
        out = engine.filter_not_null(df, ["a", "b"])
        assert engine.count(out) == 1


class TestColumnDerivation:
    def test_with_constant_column(self, engine, tmp_path):
        df = _make_appearances(engine, tmp_path)
        out = engine.with_constant_column(df, "season", "2024-25")
        recs = engine.to_records(out)
        assert all(r["season"] == "2024-25" for r in recs)

    def test_with_derived_column(self, engine, tmp_path):
        df = _make_appearances(engine, tmp_path)
        out = engine.with_derived_column(
            df,
            "goal_rate",
            fn=lambda r: r["goals"] / max(r["minutes_played"], 1),
            input_columns=["goals", "minutes_played"],
        )
        recs = engine.to_records(out)
        # player 10 game 100: 1 goal / 90 minutes
        rec = next(r for r in recs if r["appearance_id"] == "a1")
        assert rec["goal_rate"] == pytest.approx(1 / 90)

    def test_with_row_hash_matches_reference(self, engine, tmp_path):
        df = _make_appearances(engine, tmp_path)
        out = engine.with_row_hash(df, ["player_id", "goals"])
        recs = engine.to_records(out)
        for r in recs:
            expected = hash_row([r["player_id"], r["goals"]])
            assert r["row_hash"] == expected, f"hash mismatch on {engine.kind} for row {r}"

    def test_with_row_hash_handles_nulls(self, engine, tmp_path):
        csv = tmp_path / "nullable.csv"
        pd.DataFrame({"a": [1, None], "b": ["x", "y"]}).to_csv(csv, index=False)
        df = engine.read_csv(csv, schema={"a": "int", "b": "string"})
        out = engine.with_row_hash(df, ["a", "b"])
        recs = engine.to_records(out)
        # Null in 'a' should produce a deterministic hash, not crash
        assert all(len(r["row_hash"]) == 32 for r in recs)
        # And the two hashes should differ (different inputs)
        assert recs[0]["row_hash"] != recs[1]["row_hash"]


class TestJoinsAndSets:
    def test_inner_join(self, engine, tmp_path):
        df = _make_appearances(engine, tmp_path)
        players_csv = tmp_path / "p.csv"
        pd.DataFrame({"player_id": [10, 20, 30], "name": ["Alice", "Bob", "Carol"]}).to_csv(
            players_csv, index=False
        )
        players = engine.read_csv(players_csv, schema={"player_id": "int", "name": "string"})
        joined = engine.join(df, players, on=["player_id"], how="inner")
        assert engine.count(joined) == 4
        assert "name" in engine.columns(joined)

    def test_left_join_keeps_unmatched(self, engine, tmp_path):
        df = _make_appearances(engine, tmp_path)
        players_csv = tmp_path / "p.csv"
        pd.DataFrame({"player_id": [10], "name": ["Alice"]}).to_csv(players_csv, index=False)
        players = engine.read_csv(players_csv, schema={"player_id": "int", "name": "string"})
        joined = engine.join(df, players, on=["player_id"], how="left")
        assert engine.count(joined) == 4

    def test_anti_join(self, engine, tmp_path):
        df = _make_appearances(engine, tmp_path)
        players_csv = tmp_path / "p.csv"
        pd.DataFrame({"player_id": [10]}).to_csv(players_csv, index=False)
        players = engine.read_csv(players_csv, schema={"player_id": "int"})
        # appearances whose player_id is NOT in [10] -> player 20's 2 rows
        out = engine.join(df, players, on=["player_id"], how="anti")
        assert engine.count(out) == 2

    def test_distinct(self, engine, tmp_path):
        csv = tmp_path / "dups.csv"
        pd.DataFrame({"k": [1, 1, 2, 2, 3]}).to_csv(csv, index=False)
        df = engine.read_csv(csv, schema={"k": "int"})
        out = engine.distinct(df, subset=["k"])
        assert engine.count(out) == 3

    def test_union(self, engine, tmp_path):
        df = _make_appearances(engine, tmp_path)
        out = engine.union([df, df])
        assert engine.count(out) == 8


class TestAggregation:
    def test_group_by_sum(self, engine, tmp_path):
        df = _make_appearances(engine, tmp_path)
        out = engine.group_by_agg(
            df,
            by=["player_id"],
            aggs={
                "total_goals": ("goals", "sum"),
                "total_minutes": ("minutes_played", "sum"),
                "games": ("game_id", "count_distinct"),
            },
        )
        recs = {r["player_id"]: r for r in engine.to_records(out)}
        # player 10: 1+2=3 goals, 90+90=180 minutes, 2 games
        assert recs[10]["total_goals"] == 3
        assert recs[10]["total_minutes"] == 180
        assert recs[10]["games"] == 2

    def test_rolling_avg(self, engine, tmp_path):
        # 3 valuations for player 10 over 3 dates
        csv = tmp_path / "vals.csv"
        pd.DataFrame(
            {
                "player_id": [10, 10, 10],
                "date": ["2024-01-01", "2024-02-01", "2024-03-01"],
                "market_value": [1_000_000, 2_000_000, 3_000_000],
            }
        ).to_csv(csv, index=False)
        df = engine.read_csv(
            csv,
            schema={
                "player_id": "int",
                "date": "date",
                "market_value": "float",
            },
        )
        out = engine.rolling_avg(
            df,
            partition_by=["player_id"],
            order_by="date",
            value_column="market_value",
            window_rows=2,
            output_column="rolling_avg",
        )
        recs = sorted(engine.to_records(out), key=lambda r: r["date"])
        # row 1: avg of [1m]              = 1.0m
        # row 2: avg of [1m, 2m]          = 1.5m
        # row 3: avg of [2m, 3m]          = 2.5m   (window=2)
        assert recs[0]["rolling_avg"] == pytest.approx(1_000_000)
        assert recs[1]["rolling_avg"] == pytest.approx(1_500_000)
        assert recs[2]["rolling_avg"] == pytest.approx(2_500_000)
