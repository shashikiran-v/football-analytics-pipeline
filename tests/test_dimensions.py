"""
Tests for src.silver.dimensions.

Coverage:
  Type-1 builders (clubs, competitions): one row per natural key,
  transformations applied (country ISO normalisation), no SCD2 metadata.

  Generated dim_date: row counts, column shape, season derivation
  applied correctly, integer date keys.

  Type-2 dim_players: orchestration on top of scd2_merge. We rely on
  scd2_merge's own exhaustive coverage; here we verify the BUILDER's
  responsibilities — transformations applied, registry config drives
  the merge, all source columns survived.
"""

from __future__ import annotations

import datetime as dt

import pandas as pd
import pytest

from src.engines.pandas_engine import PandasEngine
from src.ingestion.registry import SourceDefinition
from src.silver.dimensions import (
    BRONZE_BATCH_COLUMN,
    build_dim_clubs,
    build_dim_competitions,
    build_dim_date,
    build_dim_players,
)
from src.silver.scd2 import (
    EFFECTIVE_DATE_COLUMN,
    END_DATE_COLUMN,
    IS_CURRENT_COLUMN,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def engine():
    return PandasEngine()


@pytest.fixture
def players_source():
    """A SourceDefinition matching what the real registry declares."""
    return SourceDefinition(
        name="players",
        description="t",
        format="csv",
        path_pattern="{raw_root}/players.csv",
        primary_key=["player_id"],
        schema={
            "player_id": "int",
            "name": "string",
            "current_club_id": "int",
            "position": "string",
            "market_value_in_eur": "float",
            "country_of_citizenship": "string",
        },
        scd2={
            "tracked_columns": ["current_club_id", "position", "market_value_in_eur"],
        },
    )


def _bronze_players_partition(rows: list[dict], batch_id: str = "B1") -> pd.DataFrame:
    """Synthesise a Bronze players partition (rows + batch_id column)."""
    df = pd.DataFrame(rows)
    df[BRONZE_BATCH_COLUMN] = batch_id
    return df


# ---------------------------------------------------------------------------
# build_dim_clubs (Type-1)
# ---------------------------------------------------------------------------


class TestDimClubs:
    def test_one_row_per_club(self, engine):
        bronze = pd.DataFrame([
            {"club_id": 1, "name": "Arsenal FC", "squad_size": 25,
             "domestic_competition_id": "GB1"},
            {"club_id": 2, "name": "Chelsea FC", "squad_size": 28,
             "domestic_competition_id": "GB1"},
        ])
        dim = build_dim_clubs(bronze_clubs=bronze, engine=engine)
        assert engine.count(dim) == 2

    def test_handles_duplicate_natural_keys(self, engine):
        """If Bronze accidentally has duplicates (e.g. multi-file source),
        the dim dedupes on natural key."""
        bronze = pd.DataFrame([
            {"club_id": 1, "name": "Arsenal FC", "squad_size": 25,
             "domestic_competition_id": "GB1"},
            {"club_id": 1, "name": "Arsenal FC (updated)", "squad_size": 26,
             "domestic_competition_id": "GB1"},
        ])
        dim = build_dim_clubs(bronze_clubs=bronze, engine=engine)
        assert engine.count(dim) == 1


# ---------------------------------------------------------------------------
# build_dim_competitions (Type-1, with country normalisation)
# ---------------------------------------------------------------------------


class TestDimCompetitions:
    def test_one_row_per_competition(self, engine):
        bronze = pd.DataFrame([
            {"competition_id": "GB1", "name": "Premier League", "country_name": "England"},
            {"competition_id": "ES1", "name": "La Liga", "country_name": "Spain"},
        ])
        dim = build_dim_competitions(bronze_competitions=bronze, engine=engine)
        assert engine.count(dim) == 2

    def test_adds_country_iso_code(self, engine):
        """The brief asks for ISO standardisation; verify it's applied
        in the dim with the messy variants from data/sample/."""
        bronze = pd.DataFrame([
            {"competition_id": "GB1", "name": "Premier League", "country_name": "England"},
            {"competition_id": "ES1", "name": "La Liga", "country_name": "Spain"},
            {"competition_id": "XX1", "name": "Mystery Cup", "country_name": "Atlantis"},
        ])
        dim = build_dim_competitions(bronze_competitions=bronze, engine=engine)
        records = {r["competition_id"]: r for r in engine.to_records(dim)}
        assert records["GB1"]["country_iso_code"] == "GB"
        assert records["ES1"]["country_iso_code"] == "ES"
        assert records["XX1"]["country_iso_code"] == "XX"   # unknown sentinel


# ---------------------------------------------------------------------------
# build_dim_date (generated)
# ---------------------------------------------------------------------------


class TestDimDate:
    def test_row_count_matches_inclusive_range(self, engine):
        dim = build_dim_date(
            start_date=dt.date(2024, 1, 1),
            end_date=dt.date(2024, 1, 31),
            engine=engine,
        )
        assert engine.count(dim) == 31

    def test_has_expected_columns(self, engine):
        dim = build_dim_date(
            start_date=dt.date(2024, 1, 1),
            end_date=dt.date(2024, 1, 1),
            engine=engine,
        )
        cols = set(engine.columns(dim))
        assert {
            "date_key", "date", "year", "quarter", "month", "day",
            "day_of_week", "day_name", "is_weekend", "season",
        } <= cols

    def test_date_key_is_yyyymmdd_int(self, engine):
        dim = build_dim_date(
            start_date=dt.date(2024, 6, 15),
            end_date=dt.date(2024, 6, 15),
            engine=engine,
        )
        rec = engine.to_records(dim)[0]
        assert rec["date_key"] == 20240615

    def test_season_derived_correctly(self, engine):
        # Single date in October should fall into the new football season.
        dim = build_dim_date(
            start_date=dt.date(2024, 10, 15),
            end_date=dt.date(2024, 10, 15),
            engine=engine,
        )
        rec = engine.to_records(dim)[0]
        assert rec["season"] == "2024-25"

    def test_weekend_flag(self, engine):
        # 2024-06-08 is a Saturday
        dim = build_dim_date(
            start_date=dt.date(2024, 6, 8),
            end_date=dt.date(2024, 6, 9),
            engine=engine,
        )
        recs = engine.to_records(dim)
        by_day = {r["day_name"]: r for r in recs}
        assert by_day["Saturday"]["is_weekend"] is True
        assert by_day["Sunday"]["is_weekend"] is True

    def test_rejects_inverted_range(self, engine):
        with pytest.raises(ValueError, match="must not precede"):
            build_dim_date(
                start_date=dt.date(2024, 12, 31),
                end_date=dt.date(2024, 1, 1),
                engine=engine,
            )


# ---------------------------------------------------------------------------
# build_dim_players (Type-2)
# ---------------------------------------------------------------------------


class TestDimPlayers:
    def test_first_run_creates_versions(self, engine, players_source):
        bronze = _bronze_players_partition([
            {"player_id": 1001, "name": "Saka", "current_club_id": 1,
             "position": "RW", "market_value_in_eur": 120_000_000,
             "country_of_citizenship": "England"},
            {"player_id": 1002, "name": "Ødegaard", "current_club_id": 1,
             "position": "CAM", "market_value_in_eur": 100_000_000,
             "country_of_citizenship": "Norway"},
        ])
        dim, stats = build_dim_players(
            bronze_players=bronze, existing_dim=None,
            players_source=players_source,
            batch_timestamp="2024-01-01", engine=engine,
        )
        assert stats.new_versions == 2
        assert stats.unchanged_versions == 0
        assert engine.count(dim) == 2

    def test_drops_bronze_batch_id_column(self, engine, players_source):
        bronze = _bronze_players_partition([
            {"player_id": 1001, "name": "Saka", "current_club_id": 1,
             "position": "RW", "market_value_in_eur": 120_000_000,
             "country_of_citizenship": "England"},
        ])
        dim, _ = build_dim_players(
            bronze_players=bronze, existing_dim=None,
            players_source=players_source,
            batch_timestamp="2024-01-01", engine=engine,
        )
        # The Bronze 'batch_id' bookkeeping column should NOT survive
        # into the Silver dim; SCD2 effective_date/end_date take its place.
        assert BRONZE_BATCH_COLUMN not in engine.columns(dim)

    def test_applies_position_normalisation(self, engine, players_source):
        """The deliberate edge case from data/sample/: 'GK' should
        normalise to 'Goalkeeper' inside the dim."""
        bronze = _bronze_players_partition([
            {"player_id": 1004, "name": "Raya", "current_club_id": 1,
             "position": "GK", "market_value_in_eur": 35_000_000,
             "country_of_citizenship": "Spain"},
        ])
        dim, _ = build_dim_players(
            bronze_players=bronze, existing_dim=None,
            players_source=players_source,
            batch_timestamp="2024-01-01", engine=engine,
        )
        rec = engine.to_records(dim)[0]
        assert rec["position_canonical"] == "Goalkeeper"
        assert rec["position_category"] == "goalkeeper"
        # Original position column is also preserved
        assert rec["position"] == "GK"

    def test_applies_country_iso_normalisation(self, engine, players_source):
        bronze = _bronze_players_partition([
            {"player_id": 1001, "name": "Saka", "current_club_id": 1,
             "position": "RW", "market_value_in_eur": 120_000_000,
             "country_of_citizenship": "England, United Kingdom"},
        ])
        dim, _ = build_dim_players(
            bronze_players=bronze, existing_dim=None,
            players_source=players_source,
            batch_timestamp="2024-01-01", engine=engine,
        )
        rec = engine.to_records(dim)[0]
        assert rec["country_of_citizenship_iso"] == "GB"

    def test_uses_registry_tracked_columns(self, engine, players_source):
        """Verify the SCD2 merge respects the tracked_columns from the
        registry — a change in an UNTRACKED column produces no new version."""
        bronze_v1 = _bronze_players_partition([
            {"player_id": 1001, "name": "Saka", "current_club_id": 1,
             "position": "RW", "market_value_in_eur": 120_000_000,
             "country_of_citizenship": "England"},
        ])
        v1, _ = build_dim_players(
            bronze_players=bronze_v1, existing_dim=None,
            players_source=players_source,
            batch_timestamp="2024-01-01", engine=engine,
        )
        # Same tracked columns, different non-tracked column
        # (country_of_citizenship is NOT in tracked_columns)
        bronze_v2 = _bronze_players_partition([
            {"player_id": 1001, "name": "Saka", "current_club_id": 1,
             "position": "RW", "market_value_in_eur": 120_000_000,
             "country_of_citizenship": "United Kingdom"},
        ])
        v2, stats = build_dim_players(
            bronze_players=bronze_v2, existing_dim=v1,
            players_source=players_source,
            batch_timestamp="2024-06-01", engine=engine,
        )
        assert stats.changed_versions == 0
        assert stats.unchanged_versions == 1

    def test_tracked_column_change_creates_new_version(self, engine, players_source):
        bronze_v1 = _bronze_players_partition([
            {"player_id": 1001, "name": "Saka", "current_club_id": 1,
             "position": "RW", "market_value_in_eur": 120_000_000,
             "country_of_citizenship": "England"},
        ])
        v1, _ = build_dim_players(
            bronze_players=bronze_v1, existing_dim=None,
            players_source=players_source,
            batch_timestamp="2024-01-01", engine=engine,
        )
        # market_value_in_eur changes — tracked column
        bronze_v2 = _bronze_players_partition([
            {"player_id": 1001, "name": "Saka", "current_club_id": 1,
             "position": "RW", "market_value_in_eur": 150_000_000,
             "country_of_citizenship": "England"},
        ])
        v2, stats = build_dim_players(
            bronze_players=bronze_v2, existing_dim=v1,
            players_source=players_source,
            batch_timestamp="2024-06-01", engine=engine,
        )
        assert stats.changed_versions == 1
        # Two rows for player 1001: closed-out v1 + new v2
        recs = engine.to_records(v2)
        rows_for_saka = [r for r in recs if r["player_id"] == 1001]
        assert len(rows_for_saka) == 2

    def test_rejects_source_without_scd2_spec(self, engine):
        """Defensive: building dim_players requires the registry to
        declare scd2 config. If someone removes it, fail loudly."""
        non_scd2_source = SourceDefinition(
            name="players", description="t", format="csv",
            path_pattern="{raw_root}/players.csv",
            primary_key=["player_id"],
            schema={"player_id": "int"},
            # NO scd2 spec
        )
        bronze = _bronze_players_partition([
            {"player_id": 1001, "name": "Saka", "current_club_id": 1,
             "position": "RW", "market_value_in_eur": 120_000_000,
             "country_of_citizenship": "England"},
        ])
        with pytest.raises(ValueError, match="scd2 spec"):
            build_dim_players(
                bronze_players=bronze, existing_dim=None,
                players_source=non_scd2_source,
                batch_timestamp="2024-01-01", engine=engine,
            )
