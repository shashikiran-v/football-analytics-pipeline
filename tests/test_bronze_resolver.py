"""
Tests for src.bronze.resolver.

The resolver is the bridge between file-grain idempotency (which skips
re-writing identical bytes under a new batch's partition) and downstream
consumers (Silver runner, DQ FK lookup) that need to read data for the
current batch.

Three scenarios to cover:
  1. Current-batch partition exists on disk → return it directly
  2. Current-batch partition missing, audit DAO knows of a prior batch
     with this source ingested → return the prior batch's path
  3. Current-batch partition missing AND no prior batch has data for
     this source → return None (caller decides what to do)

Plus the subtle case introduced by file-grain idempotency: the current-
batch audit row exists (status=INGESTED) and has the same checksum as
a prior batch, but no parquet was written under the current batch.
The resolver must follow the checksum to find where the data actually
lives on disk.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from src.bronze.resolver import resolve_bronze_partition
from src.bronze.run import run_bronze
from src.metadata.db import init_db


SAMPLES_DIR = Path(__file__).resolve().parents[1] / "data" / "sample"
SAMPLES_DAY2_DIR = Path(__file__).resolve().parents[1] / "data" / "sample" / "day2"


@pytest.fixture
def fresh_db():
    init_db()


@pytest.fixture
def day1_seeded(fresh_db):
    """Bronze day-1 populated."""
    run_bronze(batch_id="day-1", raw_root=SAMPLES_DIR)


@pytest.fixture
def day1_and_day2_seeded(day1_seeded):
    """Bronze day-1 then day-2 — three sources are skipped via file-grain
    idempotency (clubs, competitions, player_valuations) so their day-2
    partitions DON'T exist on disk."""
    run_bronze(batch_id="day-2", raw_root=SAMPLES_DAY2_DIR)


# ---------------------------------------------------------------------------
# Scenario 1: current-batch partition exists
# ---------------------------------------------------------------------------


class TestPartitionExistsOnDisk:
    def test_returns_current_partition_directly(self, day1_seeded):
        from src.utils.config import get_config
        cfg = get_config()
        # Day-1 wrote a clubs partition under batch_id=day-1
        path = resolve_bronze_partition(
            bronze_root=cfg.paths.bronze,
            source_name="clubs",
            batch_id="day-1",
        )
        assert path is not None
        assert path == cfg.paths.bronze / "clubs" / "batch_id=day-1"
        assert path.is_dir()


# ---------------------------------------------------------------------------
# Scenario 2: current-batch partition missing, day-1 has data
# ---------------------------------------------------------------------------


class TestCrossBatchResolution:
    def test_skipped_source_resolves_to_day1_partition(self, day1_and_day2_seeded):
        """clubs.csv is identical between day-1 and day-2 → file-grain
        idempotency skips re-writing under day-2/. The resolver must
        find day-1's partition by following the checksum."""
        from src.utils.config import get_config
        cfg = get_config()
        path = resolve_bronze_partition(
            bronze_root=cfg.paths.bronze,
            source_name="clubs",
            batch_id="day-2",
        )
        assert path is not None
        # Should NOT be day-2/ (which doesn't exist)
        assert "batch_id=day-2" not in str(path)
        # Should be day-1/ (where the data lives)
        assert path == cfg.paths.bronze / "clubs" / "batch_id=day-1"
        assert path.is_dir()

    def test_changed_source_resolves_to_day2_partition(self, day1_and_day2_seeded):
        """players.csv has the deliberate Saka+Neuer changes → day-2/
        partition was written. Resolver returns day-2 directly."""
        from src.utils.config import get_config
        cfg = get_config()
        path = resolve_bronze_partition(
            bronze_root=cfg.paths.bronze,
            source_name="players",
            batch_id="day-2",
        )
        assert path is not None
        assert path == cfg.paths.bronze / "players" / "batch_id=day-2"
        assert path.is_dir()

    def test_resolved_data_is_correct(self, day1_and_day2_seeded):
        """Read the resolved partition and verify it contains the day-1
        clubs data (5 clubs)."""
        from src.utils.config import get_config
        cfg = get_config()
        path = resolve_bronze_partition(
            bronze_root=cfg.paths.bronze,
            source_name="clubs",
            batch_id="day-2",
        )
        df = pd.read_parquet(path)
        assert len(df) == 5
        # Spot-check: Arsenal is in there
        assert "Arsenal FC" in set(df["name"].astype(str))


# ---------------------------------------------------------------------------
# Scenario 3: no successful ingestion exists
# ---------------------------------------------------------------------------


class TestNoIngestionFound:
    def test_unknown_source_returns_none(self, day1_seeded):
        from src.utils.config import get_config
        cfg = get_config()
        path = resolve_bronze_partition(
            bronze_root=cfg.paths.bronze,
            source_name="never_existed",
            batch_id="day-1",
        )
        assert path is None

    def test_unknown_batch_with_known_source_falls_back(self, day1_seeded):
        """If we ask for a batch that doesn't have its own partition but
        an earlier batch ingested the source, we get that earlier batch.
        Note: as_of_batch_id filters to "this batch or earlier", so
        asking for a future batch will still find day-1 (because day-1
        ≤ asked_batch)."""
        from src.utils.config import get_config
        cfg = get_config()
        # day-99 doesn't exist but day-1 is ≤ day-99
        # (string comparison; works for our naming convention)
        path = resolve_bronze_partition(
            bronze_root=cfg.paths.bronze,
            source_name="clubs",
            batch_id="day-99",
        )
        # Falls back to day-1
        assert path is not None
        assert path == cfg.paths.bronze / "clubs" / "batch_id=day-1"
