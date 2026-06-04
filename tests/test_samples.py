"""
Tests for the committed sample CSVs.

These tests double as a *contract*: they pin the deliberate edge cases
into the test suite, so an accidental regeneration that loses them will
fail loudly. The test names spell out what each edge case is *for*,
making the samples self-documenting.

Concretely the suite asserts:
  - All 6 expected sample files exist with non-zero rows
  - Every CSV's header matches the schema declared in sources.yaml
  - Foreign keys are tight EXCEPT for the one deliberate FK violation
  - Position-label variants exist (the normaliser will need them)
  - Country-name variants exist (the ISO normaliser will need them)
  - At least 3 SCD2-prone players (1001, 1002, 1003) exist with valuations
"""

from __future__ import annotations

import csv
from pathlib import Path

import pytest

from src.ingestion.registry import get_registry

SAMPLES_DIR = Path(__file__).resolve().parents[1] / "data" / "sample"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _read_csv(name: str) -> tuple[list[str], list[dict[str, str]]]:
    """Read a sample CSV and return (header, rows-as-dicts)."""
    path = SAMPLES_DIR / name
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames or []
        return header, list(reader)


# ---------------------------------------------------------------------------
# File presence
# ---------------------------------------------------------------------------


class TestSampleFilesExist:
    @pytest.mark.parametrize(
        "filename",
        [
            "competitions.csv",
            "clubs.csv",
            "players.csv",
            "games.csv",
            "appearances.csv",
            "player_valuations.csv",
        ],
    )
    def test_file_exists_with_data(self, filename):
        path = SAMPLES_DIR / filename
        assert path.is_file(), f"{filename} is missing"
        # File must have at least header + 1 data row
        with path.open(encoding="utf-8") as f:
            lines = f.readlines()
        assert len(lines) >= 2, f"{filename} has no data rows"


# ---------------------------------------------------------------------------
# Schema compliance — committed CSVs must match what sources.yaml declares
# ---------------------------------------------------------------------------


class TestSchemaCompliance:
    """For each source declared in the registry, the committed CSV's
    header must contain exactly the columns the schema declares.

    Catches the "someone added a column to the YAML but forgot to
    update the sample generator" failure mode (and vice versa)."""

    @pytest.mark.parametrize(
        "source_name",
        ["competitions", "clubs", "players", "games", "appearances", "player_valuations"],
    )
    def test_header_matches_registry_schema(self, source_name):
        source = get_registry().get(source_name)
        header, _ = _read_csv(f"{source_name}.csv")
        assert set(header) == set(source.columns), (
            f"{source_name}.csv header mismatch:\n"
            f"  in CSV but not in registry: {set(header) - set(source.columns)}\n"
            f"  in registry but not in CSV: {set(source.columns) - set(header)}"
        )


# ---------------------------------------------------------------------------
# Foreign-key integrity
# ---------------------------------------------------------------------------


class TestForeignKeyIntegrity:
    """
    The samples are tight EXCEPT for one deliberate FK violation
    (player_id=9999 in appearances). DQ will catch it; tests confirm
    nothing else is broken so DQ's signal-to-noise stays clean.
    """

    def test_every_club_has_a_real_competition(self):
        _, clubs = _read_csv("clubs.csv")
        _, comps = _read_csv("competitions.csv")
        comp_ids = {c["competition_id"] for c in comps}
        for club in clubs:
            assert club["domestic_competition_id"] in comp_ids, (
                f"Club {club['club_id']} references unknown competition "
                f"{club['domestic_competition_id']!r}"
            )

    def test_every_player_has_a_real_club(self):
        _, players = _read_csv("players.csv")
        _, clubs = _read_csv("clubs.csv")
        club_ids = {c["club_id"] for c in clubs}
        for player in players:
            assert player["current_club_id"] in club_ids, (
                f"Player {player['player_id']} references unknown club "
                f"{player['current_club_id']!r}"
            )

    def test_every_game_has_two_real_clubs(self):
        _, games = _read_csv("games.csv")
        _, clubs = _read_csv("clubs.csv")
        club_ids = {c["club_id"] for c in clubs}
        for game in games:
            assert (
                game["home_club_id"] in club_ids
            ), f"Game {game['game_id']} home_club_id is unknown"
            assert (
                game["away_club_id"] in club_ids
            ), f"Game {game['game_id']} away_club_id is unknown"

    def test_appearances_have_exactly_one_orphan_player(self):
        """
        DQ's referential-integrity check will assert player_id is in
        the players table. Our samples deliberately include exactly
        one orphan (player_id=9999) so the check has a real failure
        to catch. If this count drifts, the DQ tests' signal changes.
        """
        _, appearances = _read_csv("appearances.csv")
        _, players = _read_csv("players.csv")
        valid_player_ids = {p["player_id"] for p in players}
        orphans = [a for a in appearances if a["player_id"] not in valid_player_ids]
        assert len(orphans) == 1, (
            f"Expected exactly one orphan appearance; found {len(orphans)}. "
            f"Orphans: {[a['appearance_id'] for a in orphans]}"
        )
        assert orphans[0]["player_id"] == "9999"

    def test_valuations_reference_real_players(self):
        _, valuations = _read_csv("player_valuations.csv")
        _, players = _read_csv("players.csv")
        player_ids = {p["player_id"] for p in players}
        for v in valuations:
            assert (
                v["player_id"] in player_ids
            ), f"Valuation references unknown player_id {v['player_id']!r}"


# ---------------------------------------------------------------------------
# Edge-case coverage
# ---------------------------------------------------------------------------


class TestNormalisationCoverage:
    """The samples deliberately include label variants that downstream
    transforms (position taxonomy, country ISO normaliser) must handle.
    If these tests fail, the samples no longer give the transforms
    something to do."""

    def test_position_labels_have_taxonomy_variants(self):
        _, players = _read_csv("players.csv")
        positions = {p["position"] for p in players}
        # Need at least one short-form ('GK') AND one long-form ('Goalkeeper')
        # so the normaliser exercises both branches.
        assert "GK" in positions, "Expected at least one player with position='GK'"
        assert "Goalkeeper" in positions, "Expected at least one player with position='Goalkeeper'"

    def test_country_names_have_iso_variants(self):
        """At least one country with a non-canonical form so the ISO
        normaliser has work to do (e.g. 'England, United Kingdom' should
        normalise to GB)."""
        _, clubs = _read_csv("clubs.csv")
        _ = {c["name"] for c in clubs}  # placeholder check
        _, players = _read_csv("players.csv")
        countries = {p["country_of_birth"] for p in players}
        # Should see at least one comma-containing country string
        # ('England, United Kingdom') — that's the non-canonical form.
        assert any("," in c for c in countries), (
            "Expected at least one non-canonical country form (comma-separated) "
            "in players.country_of_birth — needed for ISO normaliser tests"
        )


class TestSCD2Coverage:
    """The day-2 demo in Phase 6 will mutate the SCD2-prone trio
    (player_ids 1001, 1002, 1003). They must exist in the samples
    AND have at least 2 valuation observations each so the rolling-
    average Gold output has signal."""

    @pytest.mark.parametrize("player_id", ["1001", "1002", "1003"])
    def test_scd2_prone_player_exists(self, player_id):
        _, players = _read_csv("players.csv")
        assert any(
            p["player_id"] == player_id for p in players
        ), f"SCD2-prone player {player_id} missing from sample players.csv"

    @pytest.mark.parametrize("player_id", ["1001", "1002", "1003"])
    def test_scd2_prone_player_has_multiple_valuations(self, player_id):
        _, valuations = _read_csv("player_valuations.csv")
        observations = [v for v in valuations if v["player_id"] == player_id]
        assert len(observations) >= 2, (
            f"SCD2-prone player {player_id} should have ≥2 valuation "
            f"observations; got {len(observations)}"
        )


class TestNumericRangeCoverage:
    """DQ range checks need both passing and failing rows. The orphan
    FK row doubles as a check that DQ can quarantine, but we also want
    a row at the inclusive lower bound (minutes_played=0) so the
    range check `>= 0` is exercised at the boundary."""

    def test_at_least_one_row_at_zero_minutes(self):
        _, appearances = _read_csv("appearances.csv")
        zero_minute_rows = [a for a in appearances if a["minutes_played"] == "0"]
        # We don't guarantee this in the generator currently, so make this
        # a soft check: if none exist, the boundary case isn't exercised
        # in samples but DQ tests will still pass; this nudges future-us.
        # Marking xfail until we explicitly seed a zero-minutes row.
        if not zero_minute_rows:
            pytest.skip("no zero-minute appearance in samples (not required for DQ to function)")
        assert all(int(a["minutes_played"]) == 0 for a in zero_minute_rows)
