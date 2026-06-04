"""
Tests for src.gold.duckdb_session.

Two concerns: (1) views are registered correctly when Silver data
exists, and (2) the context manager closes the connection cleanly.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from src.bronze.run import run_bronze
from src.gold.duckdb_session import (
    SILVER_VIEWS,
    gold_session,
)
from src.metadata.db import init_db
from src.silver.run import run_silver
from src.utils.config import get_config

SAMPLES_DIR = Path(__file__).resolve().parents[1] / "data" / "sample"


@pytest.fixture
def fresh_db():
    init_db()


@pytest.fixture
def silver_seeded(fresh_db):
    """Bronze + Silver populated for the test, so Silver views can register."""
    run_bronze(batch_id="2024-12-01", raw_root=SAMPLES_DIR)
    run_silver(batch_id="2024-12-01")


class TestRegisterViews:
    def test_all_silver_views_registered(self, silver_seeded):
        cfg = get_config()
        with gold_session(
            silver_root=cfg.paths.silver,
            bronze_root=cfg.paths.bronze,
        ) as conn:
            # Query DuckDB's information_schema for registered views
            views = conn.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_type='VIEW' ORDER BY table_name"
            ).fetchall()
        view_names = {row[0] for row in views}
        # Every expected Silver view must be there
        for expected in SILVER_VIEWS:
            assert expected in view_names, f"Missing view: {expected}"

    def test_bronze_direct_view_registered(self, silver_seeded):
        cfg = get_config()
        with gold_session(
            silver_root=cfg.paths.silver,
            bronze_root=cfg.paths.bronze,
        ) as conn:
            count = conn.execute("SELECT COUNT(*) FROM bronze_player_valuations").fetchone()[0]
        # Sample has 18 valuations
        assert count == 18

    def test_dim_players_queryable(self, silver_seeded):
        cfg = get_config()
        with gold_session(
            silver_root=cfg.paths.silver,
            bronze_root=cfg.paths.bronze,
        ) as conn:
            df = conn.execute("SELECT * FROM dim_players").fetchdf()
        # Samples have 12 players
        assert len(df) == 12

    def test_fact_appearances_clean_post_dq(self, silver_seeded):
        """After Phase 4's DQ gate, fact_appearances has 29 rows
        (orphan player_id=9999 quarantined). Verify Gold sees this."""
        cfg = get_config()
        with gold_session(
            silver_root=cfg.paths.silver,
            bronze_root=cfg.paths.bronze,
        ) as conn:
            count = conn.execute("SELECT COUNT(*) FROM fact_appearances").fetchone()[0]
        assert count == 29

    def test_missing_silver_data_skipped_gracefully(self, fresh_db, tmp_path):
        """If a Silver artifact's directory doesn't exist, the view is
        skipped (with a log warning), not raised. Gold runs that depend
        on the missing view will fail at query time with a clear
        DuckDB error — but the session itself opens fine."""
        with gold_session(
            silver_root=tmp_path,  # empty directory
            bronze_root=tmp_path,  # empty directory
        ) as conn:
            # No views registered, but session opens
            views = conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_type='VIEW'"
            ).fetchall()
        assert len(views) == 0


class TestSessionLifecycle:
    def test_context_manager_closes_connection(self, silver_seeded):
        cfg = get_config()
        with gold_session(
            silver_root=cfg.paths.silver,
            bronze_root=cfg.paths.bronze,
        ) as conn:
            captured = conn
        # After exit, attempting to use the connection should fail
        with pytest.raises(Exception):
            captured.execute("SELECT 1")
