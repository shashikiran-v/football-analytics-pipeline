"""
Tests for src.silver.transforms.

Each transformation gets its own test class covering:
  - Happy paths (the obvious cases)
  - Edge cases (null, empty, unknown values)
  - Reference data variants (the deliberate inconsistencies we
    seeded in data/sample/ in Phase 2b)
"""

from __future__ import annotations

import datetime as dt

import pytest

from src.silver.transforms import (
    UNKNOWN_COUNTRY_ISO,
    UNKNOWN_POSITION_CANONICAL,
    UNKNOWN_POSITION_CATEGORY,
    PositionMapping,
    derive_match_outcome,
    derive_season,
    normalise_country,
    normalise_position,
)

# ---------------------------------------------------------------------------
# Position normalisation
# ---------------------------------------------------------------------------


class TestNormalisePosition:
    def test_short_form_goalkeeper(self):
        result = normalise_position("GK")
        assert result.canonical == "Goalkeeper"
        assert result.category == "goalkeeper"

    def test_long_form_goalkeeper_maps_same(self):
        """The two variants we seeded in data/sample/players.csv —
        both must reach the same canonical label."""
        assert normalise_position("GK") == normalise_position("Goalkeeper")

    def test_centre_back_taxonomy(self):
        result = normalise_position("CB")
        assert result.canonical == "Centre-Back"
        assert result.category == "defender"

    @pytest.mark.parametrize(
        ("raw", "expected_canonical", "expected_category"),
        [
            ("ST", "Striker", "forward"),
            ("Striker", "Striker", "forward"),
            ("CAM", "Attacking Midfield", "midfielder"),
            ("RB", "Right-Back", "defender"),
            ("LW", "Left Winger", "forward"),
            ("CDM", "Defensive Midfield", "midfielder"),
        ],
    )
    def test_each_taxonomy_branch(self, raw, expected_canonical, expected_category):
        result = normalise_position(raw)
        assert result.canonical == expected_canonical
        assert result.category == expected_category

    def test_case_insensitive_fallback(self):
        # "goalkeeper" (lower) isn't in the YAML as a key; the fallback
        # branch finds "Goalkeeper" via case-insensitive match.
        result = normalise_position("goalkeeper")
        assert result.canonical == "Goalkeeper"

    def test_none_input_returns_unknown(self):
        result = normalise_position(None)
        assert result.canonical == UNKNOWN_POSITION_CANONICAL
        assert result.category == UNKNOWN_POSITION_CATEGORY

    def test_empty_string_returns_unknown(self):
        result = normalise_position("")
        assert result.canonical == UNKNOWN_POSITION_CANONICAL

    def test_whitespace_only_returns_unknown(self):
        result = normalise_position("   ")
        assert result.canonical == UNKNOWN_POSITION_CANONICAL

    def test_unknown_label_returns_unknown(self):
        result = normalise_position("Sweeper")  # not in taxonomy
        assert result.canonical == UNKNOWN_POSITION_CANONICAL
        assert result.category == UNKNOWN_POSITION_CATEGORY


# ---------------------------------------------------------------------------
# Country normalisation
# ---------------------------------------------------------------------------


class TestNormaliseCountry:
    def test_well_formed_country_via_pycountry(self):
        assert normalise_country("Brazil") == "BR"

    def test_alpha_2_input_returns_alpha_2(self):
        # pycountry accepts an alpha-2 code as input
        assert normalise_country("BR") == "BR"

    def test_alpha_3_input_normalises_to_alpha_2(self):
        assert normalise_country("BRA") == "BR"

    def test_override_for_uk_constituent_country(self):
        """The overrides YAML maps 'England' to 'GB' since England
        is not a standalone ISO 3166-1 entry."""
        assert normalise_country("England") == "GB"

    def test_override_for_messy_real_world_string(self):
        """The deliberate edge case from data/sample/: 'England,
        United Kingdom' should normalise to GB via the overrides."""
        assert normalise_country("England, United Kingdom") == "GB"

    def test_override_case_insensitive_fallback(self):
        # "ENGLAND" isn't an exact key in the overrides; case-insensitive
        # fallback should still catch it.
        assert normalise_country("ENGLAND") == "GB"

    def test_usa_shorthand(self):
        assert normalise_country("USA") == "US"

    def test_none_returns_unknown(self):
        assert normalise_country(None) == UNKNOWN_COUNTRY_ISO

    def test_empty_string_returns_unknown(self):
        assert normalise_country("") == UNKNOWN_COUNTRY_ISO

    def test_atlantis_returns_unknown(self):
        # Not in pycountry, not in overrides
        assert normalise_country("Atlantis") == UNKNOWN_COUNTRY_ISO

    def test_unknown_iso_code_is_uppercase(self):
        # The XX sentinel should be uppercase like every other ISO code
        assert normalise_country(None) == normalise_country(None).upper()


# ---------------------------------------------------------------------------
# Match outcome
# ---------------------------------------------------------------------------


class TestDeriveMatchOutcome:
    def test_home_win(self):
        assert derive_match_outcome(2, 0) == "home_win"

    def test_away_win(self):
        assert derive_match_outcome(0, 1) == "away_win"

    def test_draw(self):
        assert derive_match_outcome(1, 1) == "draw"

    def test_zero_zero_draw(self):
        assert derive_match_outcome(0, 0) == "draw"

    def test_large_score_home_win(self):
        assert derive_match_outcome(7, 0) == "home_win"

    def test_home_goals_none_returns_unknown(self):
        assert derive_match_outcome(None, 0) == "unknown"

    def test_away_goals_none_returns_unknown(self):
        assert derive_match_outcome(2, None) == "unknown"

    def test_both_none_returns_unknown(self):
        assert derive_match_outcome(None, None) == "unknown"

    def test_pandas_nan_treated_as_unknown(self):
        """pandas/numpy NaN is a float that != itself. The transform
        must recognise it as 'unknown', not coerce to 0."""
        nan = float("nan")
        assert derive_match_outcome(nan, 1) == "unknown"
        assert derive_match_outcome(1, nan) == "unknown"

    def test_float_inputs_handled(self):
        # pandas often gives us nullable Int64 that surface as float —
        # the transform must still produce the right outcome.
        assert derive_match_outcome(2.0, 0.0) == "home_win"


# ---------------------------------------------------------------------------
# Season derivation
# ---------------------------------------------------------------------------


class TestDeriveSeason:
    def test_october_belongs_to_current_season(self):
        # October 2024 -> 2024-25 (season started August 2024)
        assert derive_season("2024-10-15") == "2024-25"

    def test_march_belongs_to_previous_year_start_season(self):
        # March 2025 still 2024-25 (season ends May 2025)
        assert derive_season("2025-03-20") == "2024-25"

    def test_august_start_belongs_to_new_season(self):
        assert derive_season("2025-08-12") == "2025-26"

    def test_july_belongs_to_previous_season(self):
        # Off-season — pre-season tournaments still attributed to old season
        assert derive_season("2025-07-01") == "2024-25"

    def test_december_belongs_to_current_season(self):
        assert derive_season("2024-12-31") == "2024-25"

    def test_january_belongs_to_previous_year_start_season(self):
        assert derive_season("2025-01-15") == "2024-25"

    def test_may_belongs_to_previous_year_start_season(self):
        # Season ends May; a May match still attributed to it.
        assert derive_season("2025-05-28") == "2024-25"

    def test_none_returns_none(self):
        assert derive_season(None) is None

    def test_empty_string_returns_none(self):
        assert derive_season("") is None

    def test_invalid_date_string_returns_none(self):
        assert derive_season("not-a-date") is None

    def test_iso_datetime_with_time_portion(self):
        # Some sources emit "2024-10-15T15:30:00" or "2024-10-15 15:30:00"
        assert derive_season("2024-10-15T15:30:00") == "2024-25"
        assert derive_season("2024-10-15 15:30:00") == "2024-25"

    def test_accepts_datetime_date_object(self):
        assert derive_season(dt.date(2024, 10, 15)) == "2024-25"

    def test_accepts_datetime_datetime_object(self):
        assert derive_season(dt.datetime(2024, 10, 15, 15, 30, 0)) == "2024-25"

    def test_returns_proper_two_digit_year_suffix(self):
        # Year-2099 -> "2099-00" (with zero-padded suffix)
        assert derive_season("2099-10-01") == "2099-00"

    def test_returns_proper_double_digit_suffix(self):
        # Year 2008 -> "2008-09"
        assert derive_season("2008-10-01") == "2008-09"


# ---------------------------------------------------------------------------
# Reference data smoke tests
# ---------------------------------------------------------------------------


class TestReferenceDataLoaders:
    """Sanity checks on the bundled YAML files. If someone edits
    them and breaks the structure, these fail loudly."""

    def test_position_taxonomy_loads(self):
        # Touching this via any transform forces the YAML load
        result = normalise_position("GK")
        assert isinstance(result, PositionMapping)

    def test_country_overrides_loads(self):
        result = normalise_country("England, United Kingdom")
        assert result == "GB"
