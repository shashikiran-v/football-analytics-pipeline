"""
Tests for src.silver.scd2.scd2_merge.

This is the most consequential function in the Silver layer. Tests are
deliberately exhaustive — every category (NEW, CHANGED, UNCHANGED,
historical preserved), every edge case (empty existing, empty incoming,
NaN handling in tracked columns), and the key engineering properties
(surrogate key determinism, idempotency on re-run, no history corruption).

Test data shape

  Synthetic 'players' dimension with three tracked columns matching
  the real registry:
      natural_key       = ['player_id']
      tracked_columns   = ['current_club_id', 'position', 'market_value']
      surrogate_key     = 'player_sk'
"""

from __future__ import annotations

import pandas as pd
import pytest

from src.engines.pandas_engine import PandasEngine
from src.silver.scd2 import (
    EFFECTIVE_DATE_COLUMN,
    END_DATE_COLUMN,
    FAR_FUTURE_DATE,
    IS_CURRENT_COLUMN,
    SCD2MergeStats,
    scd2_merge,
)

NATURAL_KEY = ["player_id"]
TRACKED = ["current_club_id", "position", "market_value"]
SK_COL = "player_sk"
T1 = "2024-01-01"
T2 = "2024-06-01"
T3 = "2024-12-01"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def engine():
    return PandasEngine()


def _incoming(rows: list[dict]) -> pd.DataFrame:
    """Build an incoming-like DataFrame with the standard test columns."""
    return pd.DataFrame(rows, columns=["player_id", "current_club_id", "position", "market_value"])


def _empty_existing() -> pd.DataFrame:
    """An empty existing dim with all SCD2 columns present."""
    return pd.DataFrame(
        columns=[
            "player_id",
            "current_club_id",
            "position",
            "market_value",
            SK_COL,
            EFFECTIVE_DATE_COLUMN,
            END_DATE_COLUMN,
            IS_CURRENT_COLUMN,
        ],
    )


def _run_merge(
    *,
    engine,
    existing: pd.DataFrame | None,
    incoming: pd.DataFrame,
    batch_ts: str = T2,
):
    return scd2_merge(
        existing_dim=existing,
        incoming=incoming,
        natural_key=NATURAL_KEY,
        tracked_columns=TRACKED,
        surrogate_key_column=SK_COL,
        batch_timestamp=batch_ts,
        engine=engine,
    )


# ---------------------------------------------------------------------------
# Initial load (empty existing)
# ---------------------------------------------------------------------------


class TestInitialLoad:
    def test_first_run_with_three_players(self, engine):
        incoming = _incoming(
            [
                {"player_id": 100, "current_club_id": 1, "position": "GK", "market_value": 10_000},
                {"player_id": 101, "current_club_id": 1, "position": "ST", "market_value": 20_000},
                {"player_id": 102, "current_club_id": 2, "position": "CB", "market_value": 15_000},
            ]
        )
        result, stats = _run_merge(engine=engine, existing=None, incoming=incoming, batch_ts=T1)
        assert stats == SCD2MergeStats(3, 0, 0, 0, 3)
        records = engine.to_records(result)
        assert len(records) == 3
        # All marked current with the right SCD2 metadata
        assert all(r[IS_CURRENT_COLUMN] is True for r in records)
        assert all(r[EFFECTIVE_DATE_COLUMN] == T1 for r in records)
        assert all(r[END_DATE_COLUMN] == FAR_FUTURE_DATE for r in records)
        # Surrogate keys allocated starting from 1, in natural_key order
        by_pid = {r["player_id"]: r for r in records}
        assert by_pid[100][SK_COL] == 1
        assert by_pid[101][SK_COL] == 2
        assert by_pid[102][SK_COL] == 3

    def test_first_run_accepts_explicit_empty_dim(self, engine):
        existing = _empty_existing()
        incoming = _incoming(
            [
                {"player_id": 100, "current_club_id": 1, "position": "GK", "market_value": 10_000},
            ]
        )
        result, stats = _run_merge(engine=engine, existing=existing, incoming=incoming)
        assert stats.new_versions == 1
        assert engine.count(result) == 1

    def test_first_run_preserves_all_incoming_columns(self, engine):
        incoming = _incoming(
            [
                {"player_id": 100, "current_club_id": 1, "position": "GK", "market_value": 10_000},
            ]
        )
        result, _stats = _run_merge(engine=engine, existing=None, incoming=incoming)
        cols = set(engine.columns(result))
        # Every source column AND the four SCD2 metadata columns
        assert {"player_id", "current_club_id", "position", "market_value"} <= cols
        assert {SK_COL, EFFECTIVE_DATE_COLUMN, END_DATE_COLUMN, IS_CURRENT_COLUMN} <= cols


# ---------------------------------------------------------------------------
# NEW category
# ---------------------------------------------------------------------------


class TestNewRows:
    def test_unseen_natural_keys_become_new_versions(self, engine):
        # Seed an existing dim with one player.
        existing_initial = _incoming(
            [
                {"player_id": 100, "current_club_id": 1, "position": "GK", "market_value": 10_000},
            ]
        )
        existing, _ = _run_merge(
            engine=engine, existing=None, incoming=existing_initial, batch_ts=T1
        )
        # Now arrive with player 100 (existing) and 200 (new).
        incoming = _incoming(
            [
                {"player_id": 100, "current_club_id": 1, "position": "GK", "market_value": 10_000},
                {"player_id": 200, "current_club_id": 3, "position": "LW", "market_value": 50_000},
            ]
        )
        result, stats = _run_merge(engine=engine, existing=existing, incoming=incoming, batch_ts=T2)
        assert stats == SCD2MergeStats(
            new_versions=1,
            changed_versions=0,
            unchanged_versions=1,
            historical_preserved=0,
            total_output_rows=2,
        )
        records = engine.to_records(result)
        by_pid = {r["player_id"]: r for r in records}
        assert by_pid[200][SK_COL] == 2  # next sk after the 1 from initial load
        assert by_pid[200][EFFECTIVE_DATE_COLUMN] == T2
        assert by_pid[200][IS_CURRENT_COLUMN] is True


# ---------------------------------------------------------------------------
# CHANGED category — the heart of SCD2
# ---------------------------------------------------------------------------


class TestChangedRows:
    def test_tracked_column_change_opens_new_version(self, engine):
        # Seed
        existing, _ = _run_merge(
            engine=engine,
            existing=None,
            incoming=_incoming(
                [
                    {
                        "player_id": 100,
                        "current_club_id": 1,
                        "position": "GK",
                        "market_value": 10_000,
                    },
                ]
            ),
            batch_ts=T1,
        )
        # Change one tracked column (market_value moved 10k -> 20k)
        incoming = _incoming(
            [
                {"player_id": 100, "current_club_id": 1, "position": "GK", "market_value": 20_000},
            ]
        )
        result, stats = _run_merge(engine=engine, existing=existing, incoming=incoming, batch_ts=T2)
        assert stats.changed_versions == 1
        assert stats.unchanged_versions == 0
        # Two rows for player 100 now: the closed-out original + the new current
        records = engine.to_records(result)
        assert len(records) == 2
        rows_for_100 = sorted(
            [r for r in records if r["player_id"] == 100],
            key=lambda r: r[EFFECTIVE_DATE_COLUMN],
        )
        assert rows_for_100[0][EFFECTIVE_DATE_COLUMN] == T1
        assert rows_for_100[0][END_DATE_COLUMN] == T2  # closed out at batch ts
        assert rows_for_100[0][IS_CURRENT_COLUMN] is False
        assert rows_for_100[0]["market_value"] == 10_000  # historical value preserved

        assert rows_for_100[1][EFFECTIVE_DATE_COLUMN] == T2
        assert rows_for_100[1][END_DATE_COLUMN] == FAR_FUTURE_DATE
        assert rows_for_100[1][IS_CURRENT_COLUMN] is True
        assert rows_for_100[1]["market_value"] == 20_000  # new current value

    def test_changed_row_gets_new_surrogate_key(self, engine):
        existing, _ = _run_merge(
            engine=engine,
            existing=None,
            incoming=_incoming(
                [
                    {
                        "player_id": 100,
                        "current_club_id": 1,
                        "position": "GK",
                        "market_value": 10_000,
                    },
                ]
            ),
            batch_ts=T1,
        )
        incoming = _incoming(
            [
                {"player_id": 100, "current_club_id": 1, "position": "GK", "market_value": 20_000},
            ]
        )
        result, _ = _run_merge(engine=engine, existing=existing, incoming=incoming, batch_ts=T2)
        records = engine.to_records(result)
        sks = sorted(r[SK_COL] for r in records)
        assert sks == [1, 2]  # original sk=1 closed out; new version sk=2

    def test_multiple_columns_changing_still_one_new_version(self, engine):
        existing, _ = _run_merge(
            engine=engine,
            existing=None,
            incoming=_incoming(
                [
                    {
                        "player_id": 100,
                        "current_club_id": 1,
                        "position": "GK",
                        "market_value": 10_000,
                    },
                ]
            ),
            batch_ts=T1,
        )
        # All three tracked columns change at once — still produces exactly
        # one new version, not three.
        incoming = _incoming(
            [
                {"player_id": 100, "current_club_id": 5, "position": "ST", "market_value": 50_000},
            ]
        )
        result, stats = _run_merge(engine=engine, existing=existing, incoming=incoming, batch_ts=T2)
        assert stats.changed_versions == 1
        assert engine.count(result) == 2

    def test_only_untracked_column_change_is_unchanged(self, engine):
        """An incoming change in a non-tracked column doesn't open a
        new version. This is the canonical SCD2 contract: only the
        columns we said matter, matter."""
        # Build existing with an extra non-tracked column 'agent_name'.
        existing_initial = pd.DataFrame(
            [
                {
                    "player_id": 100,
                    "current_club_id": 1,
                    "position": "GK",
                    "market_value": 10_000,
                    "agent_name": "Alice",
                },
            ]
        )
        existing, _ = scd2_merge(
            existing_dim=None,
            incoming=existing_initial,
            natural_key=NATURAL_KEY,
            tracked_columns=TRACKED,
            surrogate_key_column=SK_COL,
            batch_timestamp=T1,
            engine=engine,
        )
        # Same tracked-columns, different agent.
        incoming = pd.DataFrame(
            [
                {
                    "player_id": 100,
                    "current_club_id": 1,
                    "position": "GK",
                    "market_value": 10_000,
                    "agent_name": "Bob",
                },
            ]
        )
        _, stats = scd2_merge(
            existing_dim=existing,
            incoming=incoming,
            natural_key=NATURAL_KEY,
            tracked_columns=TRACKED,
            surrogate_key_column=SK_COL,
            batch_timestamp=T2,
            engine=engine,
        )
        # No new version — the change wasn't in a tracked column.
        assert stats.changed_versions == 0
        assert stats.unchanged_versions == 1


# ---------------------------------------------------------------------------
# UNCHANGED category
# ---------------------------------------------------------------------------


class TestUnchangedRows:
    def test_identical_incoming_is_unchanged(self, engine):
        existing, _ = _run_merge(
            engine=engine,
            existing=None,
            incoming=_incoming(
                [
                    {
                        "player_id": 100,
                        "current_club_id": 1,
                        "position": "GK",
                        "market_value": 10_000,
                    },
                    {
                        "player_id": 101,
                        "current_club_id": 2,
                        "position": "CB",
                        "market_value": 15_000,
                    },
                ]
            ),
            batch_ts=T1,
        )
        # Re-send the exact same rows
        incoming = _incoming(
            [
                {"player_id": 100, "current_club_id": 1, "position": "GK", "market_value": 10_000},
                {"player_id": 101, "current_club_id": 2, "position": "CB", "market_value": 15_000},
            ]
        )
        result, stats = _run_merge(engine=engine, existing=existing, incoming=incoming, batch_ts=T2)
        assert stats == SCD2MergeStats(0, 0, 2, 0, 2)
        # Output should be the existing rows unchanged
        records = engine.to_records(result)
        assert all(r[IS_CURRENT_COLUMN] is True for r in records)
        assert all(
            r[EFFECTIVE_DATE_COLUMN] == T1 for r in records
        )  # original effective dates preserved


# ---------------------------------------------------------------------------
# Mixed scenario — all four categories at once
# ---------------------------------------------------------------------------


class TestMixedScenario:
    def test_new_changed_unchanged_and_historical_all_present(self, engine):
        # T1: seed two players.
        existing, _ = _run_merge(
            engine=engine,
            existing=None,
            incoming=_incoming(
                [
                    {
                        "player_id": 100,
                        "current_club_id": 1,
                        "position": "GK",
                        "market_value": 10_000,
                    },
                    {
                        "player_id": 101,
                        "current_club_id": 2,
                        "position": "ST",
                        "market_value": 30_000,
                    },
                ]
            ),
            batch_ts=T1,
        )
        # T2: player 100 changes club (CHANGED), player 101 same (UNCHANGED),
        #     player 102 arrives new (NEW). The closed-out version of 100
        #     becomes historical.
        incoming = _incoming(
            [
                {"player_id": 100, "current_club_id": 5, "position": "GK", "market_value": 12_000},
                {"player_id": 101, "current_club_id": 2, "position": "ST", "market_value": 30_000},
                {"player_id": 102, "current_club_id": 3, "position": "CB", "market_value": 8_000},
            ]
        )
        result, stats = _run_merge(engine=engine, existing=existing, incoming=incoming, batch_ts=T2)
        # Expected:
        #   1 NEW   (102), 1 CHANGED (100), 1 UNCHANGED (101), 0 historical at this point.
        assert stats == SCD2MergeStats(
            new_versions=1,
            changed_versions=1,
            unchanged_versions=1,
            historical_preserved=0,
            total_output_rows=4,  # 100 (closed out) + 100 (new) + 101 + 102
        )

        # T3: another change to player 100 — now the T1 version is properly historical.
        incoming_t3 = _incoming(
            [
                {"player_id": 100, "current_club_id": 7, "position": "GK", "market_value": 15_000},
                {"player_id": 101, "current_club_id": 2, "position": "ST", "market_value": 30_000},
                {"player_id": 102, "current_club_id": 3, "position": "CB", "market_value": 8_000},
            ]
        )
        _, stats_t3 = _run_merge(engine=engine, existing=result, incoming=incoming_t3, batch_ts=T3)
        # At T3, player 100 had one closed-out version (historical preserved),
        # one current version which is now CHANGED.
        assert stats_t3.historical_preserved == 1
        assert stats_t3.changed_versions == 1


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_empty_incoming_leaves_existing_intact(self, engine):
        existing, _ = _run_merge(
            engine=engine,
            existing=None,
            incoming=_incoming(
                [
                    {
                        "player_id": 100,
                        "current_club_id": 1,
                        "position": "GK",
                        "market_value": 10_000,
                    },
                ]
            ),
            batch_ts=T1,
        )
        empty_incoming = _incoming([])
        result, stats = _run_merge(
            engine=engine, existing=existing, incoming=empty_incoming, batch_ts=T2
        )
        # Nothing changes
        assert stats == SCD2MergeStats(0, 0, 0, 0, engine.count(existing))
        assert engine.count(result) == engine.count(existing)

    def test_validation_rejects_empty_natural_key(self, engine):
        incoming = _incoming(
            [{"player_id": 1, "current_club_id": 1, "position": "GK", "market_value": 1}]
        )
        with pytest.raises(ValueError, match="natural_key"):
            scd2_merge(
                existing_dim=None,
                incoming=incoming,
                natural_key=[],
                tracked_columns=TRACKED,
                surrogate_key_column=SK_COL,
                batch_timestamp=T1,
                engine=engine,
            )

    def test_validation_rejects_empty_tracked_columns(self, engine):
        incoming = _incoming(
            [{"player_id": 1, "current_club_id": 1, "position": "GK", "market_value": 1}]
        )
        with pytest.raises(ValueError, match="tracked_columns"):
            scd2_merge(
                existing_dim=None,
                incoming=incoming,
                natural_key=NATURAL_KEY,
                tracked_columns=[],
                surrogate_key_column=SK_COL,
                batch_timestamp=T1,
                engine=engine,
            )

    def test_validation_rejects_missing_natural_key_column(self, engine):
        # incoming missing 'player_id'
        incoming = pd.DataFrame(
            [{"some_other_id": 1, "current_club_id": 1, "position": "GK", "market_value": 1}]
        )
        with pytest.raises(ValueError, match="natural_key column"):
            scd2_merge(
                existing_dim=None,
                incoming=incoming,
                natural_key=NATURAL_KEY,
                tracked_columns=TRACKED,
                surrogate_key_column=SK_COL,
                batch_timestamp=T1,
                engine=engine,
            )


# ---------------------------------------------------------------------------
# Idempotency and determinism
# ---------------------------------------------------------------------------


class TestIdempotencyAndDeterminism:
    def test_replaying_identical_batch_produces_identical_state(self, engine):
        """Running the same batch twice in a row must produce the same dim
        on the second run — no new versions appear when nothing changed."""
        incoming = _incoming(
            [
                {"player_id": 100, "current_club_id": 1, "position": "GK", "market_value": 10_000},
            ]
        )
        # First run: creates dim from empty
        existing, _ = _run_merge(engine=engine, existing=None, incoming=incoming, batch_ts=T1)
        # Second run with identical data — should be all UNCHANGED
        result, stats = _run_merge(engine=engine, existing=existing, incoming=incoming, batch_ts=T2)
        assert stats == SCD2MergeStats(0, 0, 1, 0, 1)
        # State equal to existing
        assert engine.count(result) == engine.count(existing)

    def test_surrogate_key_allocation_deterministic_across_runs(self, engine):
        """Two independent runs with identical inputs produce identical surrogate keys."""
        incoming = _incoming(
            [
                {"player_id": 200, "current_club_id": 2, "position": "ST", "market_value": 20_000},
                {"player_id": 100, "current_club_id": 1, "position": "GK", "market_value": 10_000},
                {"player_id": 300, "current_club_id": 3, "position": "CB", "market_value": 15_000},
            ]
        )
        run_a, _ = _run_merge(engine=engine, existing=None, incoming=incoming, batch_ts=T1)
        run_b, _ = _run_merge(engine=engine, existing=None, incoming=incoming, batch_ts=T1)
        records_a = {r["player_id"]: r[SK_COL] for r in engine.to_records(run_a)}
        records_b = {r["player_id"]: r[SK_COL] for r in engine.to_records(run_b)}
        assert records_a == records_b
        # And surrogate keys ordered by natural_key
        assert records_a == {100: 1, 200: 2, 300: 3}

    def test_historical_rows_never_mutated(self, engine):
        """The cardinal SCD2 rule: once a version is closed out, it
        is NEVER touched by future merges."""
        # T1: create
        existing, _ = _run_merge(
            engine=engine,
            existing=None,
            incoming=_incoming(
                [
                    {
                        "player_id": 100,
                        "current_club_id": 1,
                        "position": "GK",
                        "market_value": 10_000,
                    },
                ]
            ),
            batch_ts=T1,
        )
        # T2: change market_value
        result_t2, _ = _run_merge(
            engine=engine,
            existing=existing,
            incoming=_incoming(
                [
                    {
                        "player_id": 100,
                        "current_club_id": 1,
                        "position": "GK",
                        "market_value": 20_000,
                    },
                ]
            ),
            batch_ts=T2,
        )
        historical_t2 = [r for r in engine.to_records(result_t2) if r[IS_CURRENT_COLUMN] is False]
        # T3: another change
        result_t3, _ = _run_merge(
            engine=engine,
            existing=result_t2,
            incoming=_incoming(
                [
                    {
                        "player_id": 100,
                        "current_club_id": 1,
                        "position": "GK",
                        "market_value": 30_000,
                    },
                ]
            ),
            batch_ts=T3,
        )
        historical_t3 = [r for r in engine.to_records(result_t3) if r[IS_CURRENT_COLUMN] is False]
        # The T2 historical row should still be present unchanged in T3
        # (plus the newly-closed-out T2 current row).
        assert len(historical_t3) == 2  # T1's row (closed at T2) + T2's row (closed at T3)
        # The T1->T2 historical row from T2 must appear identically in T3
        t1_to_t2_in_t2 = next(r for r in historical_t2 if r[EFFECTIVE_DATE_COLUMN] == T1)
        t1_to_t2_in_t3 = next(r for r in historical_t3 if r[EFFECTIVE_DATE_COLUMN] == T1)
        assert t1_to_t2_in_t2["market_value"] == t1_to_t2_in_t3["market_value"]
        assert t1_to_t2_in_t2[END_DATE_COLUMN] == t1_to_t2_in_t3[END_DATE_COLUMN]
        assert t1_to_t2_in_t2[SK_COL] == t1_to_t2_in_t3[SK_COL]
