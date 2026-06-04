"""
Tests for src.dq.runner and src.dq.quarantine.

These tests exercise the runner as a unit (in-memory FK lookups, no
Bronze I/O) AND end-to-end against the committed samples (where the
orphan player_id=9999 must end up in quarantine).
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from src.bronze.run import run_bronze
from src.dq.quarantine import quarantine_rejected_rows
from src.dq.rules import load_dq_rules
from src.dq.runner import (
    DQ_FAILURE_REASON_COLUMN,
    build_fk_lookups,
    run_dq_for_source,
)
from src.engines.pandas_engine import PandasEngine
from src.metadata.db import init_db
from src.utils.config import get_config

SAMPLES_DIR = Path(__file__).resolve().parents[1] / "data" / "sample"


@pytest.fixture
def engine():
    return PandasEngine()


@pytest.fixture(autouse=True)
def _clear_caches():
    """Reset config + rules caches per test so the test runs against a fresh
    config (each pytest test gets its own tmp_path-scoped DATA_ROOT)."""
    get_config.cache_clear()
    load_dq_rules.cache_clear()


@pytest.fixture
def fresh_db():
    init_db()


@pytest.fixture
def bronze_seeded(fresh_db):
    """Bronze populated before each DQ test."""
    run_bronze(batch_id="2024-12-01", raw_root=SAMPLES_DIR)


# ---------------------------------------------------------------------------
# Unit: runner against synthetic input
# ---------------------------------------------------------------------------


class TestRunnerUnit:
    def test_clean_data_passes_through_unchanged(self, engine):
        """No critical failures, no quarantine."""
        df = pd.DataFrame(
            [
                {
                    "competition_id": "GB1",
                    "name": "Premier League",
                    "country_name": "England",
                    "sub_type": "first_tier",
                    "type": "domestic_league",
                    "confederation": "uefa",
                    "url": "u",
                },
                {
                    "competition_id": "ES1",
                    "name": "La Liga",
                    "country_name": "Spain",
                    "sub_type": "first_tier",
                    "type": "domestic_league",
                    "confederation": "uefa",
                    "url": "u",
                },
            ]
        )
        result = run_dq_for_source(
            source_name="competitions",
            df=df,
            fk_lookups={},
            engine=engine,
        )
        assert engine.count(result.clean_rows) == 2
        assert result.failing_rows is None
        assert result.critical_failures == []

    def test_failing_row_quarantined_with_reason(self, engine):
        """Synthetic FK violation — players.current_club_id references
        a non-existent club. The rule catches it; the failing row has
        _dq_failure_reason populated."""
        df = pd.DataFrame(
            [
                {"player_id": 1001, "name": "Saka", "current_club_id": 1, "position": "RW"},
                {"player_id": 1002, "name": "Orphan", "current_club_id": 999, "position": "ST"},
            ]
        )
        fk_lookups = {("clubs", "club_id"): {1, 2, 3}}
        result = run_dq_for_source(
            source_name="players",
            df=df,
            fk_lookups=fk_lookups,
            engine=engine,
        )
        assert engine.count(result.clean_rows) == 1
        assert result.failing_rows is not None
        failing_records = engine.to_records(result.failing_rows)
        assert len(failing_records) == 1
        assert failing_records[0]["player_id"] == 1002
        assert "foreign_key:players.current_club_id" in failing_records[0][DQ_FAILURE_REASON_COLUMN]

    def test_warning_rule_does_not_quarantine(self, engine):
        """A row failing only a WARNING rule (not critical) passes through
        to clean_rows. The warning is recorded in outcomes."""
        # market_value out of warning range (>500M) but FK + not_null fine
        df = pd.DataFrame(
            [
                {
                    "player_id": 1001,
                    "name": "Saka",
                    "current_club_id": 1,
                    "position": "RW",
                    "market_value_in_eur": 600_000_000,
                },  # warning threshold = 500M
            ]
        )
        fk_lookups = {("clubs", "club_id"): {1, 2, 3}}
        result = run_dq_for_source(
            source_name="players",
            df=df,
            fk_lookups=fk_lookups,
            engine=engine,
        )
        # The row passes through (warnings don't quarantine)
        assert engine.count(result.clean_rows) == 1
        assert result.failing_rows is None
        # But the warning is reported
        assert len(result.warnings) >= 1
        market_value_warning = next(
            (o for o in result.warnings if "market_value" in o.rule_id),
            None,
        )
        assert market_value_warning is not None
        assert market_value_warning.rows_failed == 1

    def test_empty_input_handled(self, engine):
        df = pd.DataFrame(
            columns=[
                "competition_id",
                "name",
                "country_name",
                "sub_type",
                "type",
                "confederation",
                "url",
            ]
        )
        result = run_dq_for_source(
            source_name="competitions",
            df=df,
            fk_lookups={},
            engine=engine,
        )
        assert result.rows_in == 0
        assert engine.count(result.clean_rows) == 0
        assert result.failing_rows is None

    def test_unknown_source_with_no_rules_passes_all(self, engine):
        df = pd.DataFrame([{"x": 1}, {"x": 2}])
        result = run_dq_for_source(
            source_name="no_such_source",
            df=df,
            fk_lookups={},
            engine=engine,
        )
        assert result.rows_in == 2
        assert engine.count(result.clean_rows) == 2
        assert result.outcomes == []


# ---------------------------------------------------------------------------
# Integration: full DQ flow against committed samples
# ---------------------------------------------------------------------------


class TestDQAgainstSamples:
    def test_build_fk_lookups_from_real_bronze(self, bronze_seeded, engine):
        """Verifies build_fk_lookups can read every FK target Bronze
        partition and produce correct sets."""
        cfg = get_config()
        lookups = build_fk_lookups(
            bronze_root=cfg.paths.bronze,
            batch_id="2024-12-01",
            engine=engine,
        )
        # Spot-check known sizes from samples
        assert len(lookups[("competitions", "competition_id")]) == 3
        assert len(lookups[("clubs", "club_id")]) == 5
        assert len(lookups[("players", "player_id")]) == 12
        assert len(lookups[("games", "game_id")]) == 6

    def test_orphan_player_id_caught_by_fk_rule(self, bronze_seeded, engine):
        """The end-to-end story. The seeded orphan player_id=9999 in
        Bronze appearances must be caught by the FK rule and end up in
        result.failing_rows with the right reason."""
        cfg = get_config()
        lookups = build_fk_lookups(
            bronze_root=cfg.paths.bronze,
            batch_id="2024-12-01",
            engine=engine,
        )
        appearances = engine.read_parquet(cfg.paths.bronze / "appearances")
        result = run_dq_for_source(
            source_name="appearances",
            df=appearances,
            fk_lookups=lookups,
            engine=engine,
        )
        assert result.rows_in == 30
        assert engine.count(result.clean_rows) == 29
        assert result.failing_rows is not None
        assert engine.count(result.failing_rows) == 1
        failing = engine.to_records(result.failing_rows)[0]
        assert failing["player_id"] == 9999
        assert (
            "foreign_key:appearances.player_id->players.player_id"
            in failing[DQ_FAILURE_REASON_COLUMN]
        )

    def test_clean_sources_have_no_critical_failures(self, bronze_seeded, engine):
        """The 5 sources WITHOUT seeded violations should have no
        critical failures. Demonstrates the rules don't generate false
        positives on healthy data."""
        cfg = get_config()
        lookups = build_fk_lookups(
            bronze_root=cfg.paths.bronze,
            batch_id="2024-12-01",
            engine=engine,
        )
        for source_name in ["competitions", "clubs", "players", "games", "player_valuations"]:
            df = engine.read_parquet(cfg.paths.bronze / source_name)
            result = run_dq_for_source(
                source_name=source_name,
                df=df,
                fk_lookups=lookups,
                engine=engine,
            )
            assert result.failing_rows is None or engine.count(result.failing_rows) == 0, (
                f"{source_name} unexpectedly had critical failures: "
                f"{[o.rule_id for o in result.critical_failures]}"
            )


# ---------------------------------------------------------------------------
# Quarantine writer
# ---------------------------------------------------------------------------


class TestQuarantine:
    def test_quarantine_creates_partitioned_parquet(self, bronze_seeded, engine, tmp_path):
        cfg = get_config()
        lookups = build_fk_lookups(
            bronze_root=cfg.paths.bronze,
            batch_id="2024-12-01",
            engine=engine,
        )
        appearances = engine.read_parquet(cfg.paths.bronze / "appearances")
        result = run_dq_for_source(
            source_name="appearances",
            df=appearances,
            fk_lookups=lookups,
            engine=engine,
        )
        output_path = quarantine_rejected_rows(
            dq_result=result,
            rejected_root=cfg.paths.rejected,
            batch_id="2024-12-01",
            engine=engine,
        )
        assert output_path is not None
        # Hive partition layout
        partition_dir = cfg.paths.rejected / "appearances" / "batch_id=2024-12-01"
        assert partition_dir.is_dir()
        parquet_files = list(partition_dir.glob("*.parquet"))
        assert len(parquet_files) >= 1

    def test_quarantine_writes_failure_reason_column(self, bronze_seeded, engine):
        cfg = get_config()
        lookups = build_fk_lookups(
            bronze_root=cfg.paths.bronze,
            batch_id="2024-12-01",
            engine=engine,
        )
        appearances = engine.read_parquet(cfg.paths.bronze / "appearances")
        result = run_dq_for_source(
            source_name="appearances",
            df=appearances,
            fk_lookups=lookups,
            engine=engine,
        )
        quarantine_rejected_rows(
            dq_result=result,
            rejected_root=cfg.paths.rejected,
            batch_id="2024-12-01",
            engine=engine,
        )
        # Read back and check
        quarantined = pd.read_parquet(cfg.paths.rejected / "appearances")
        assert DQ_FAILURE_REASON_COLUMN in quarantined.columns
        assert len(quarantined) == 1
        assert "foreign_key" in quarantined[DQ_FAILURE_REASON_COLUMN].iloc[0]

    def test_quarantine_skips_when_no_failures(self, bronze_seeded, engine):
        """A clean source's DQResult has failing_rows=None; quarantine
        is a no-op."""
        cfg = get_config()
        lookups = build_fk_lookups(
            bronze_root=cfg.paths.bronze,
            batch_id="2024-12-01",
            engine=engine,
        )
        clubs = engine.read_parquet(cfg.paths.bronze / "clubs")
        result = run_dq_for_source(
            source_name="clubs",
            df=clubs,
            fk_lookups=lookups,
            engine=engine,
        )
        out = quarantine_rejected_rows(
            dq_result=result,
            rejected_root=cfg.paths.rejected,
            batch_id="2024-12-01",
            engine=engine,
        )
        assert out is None
