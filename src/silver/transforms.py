"""
Silver layer transformations.

Pure-function transformations applied during the Bronze -> Silver
transition. Each transform is a small Python callable that takes a
value (or a small dict of values) and returns a value. They're
intended for use with `engine.with_derived_column(df, name, fn)` —
which means they work uniformly on Pandas and PySpark.

The four required transformations (from the assessment brief):

  normalise_position   "GK" / "Goalkeeper" -> "Goalkeeper"
  normalise_country    "England, United Kingdom" -> "GB" (ISO 3166-1 alpha-2)
  derive_match_outcome (home_goals, away_goals) -> "home_win" | "away_win" | "draw"
  derive_season        ISO date string -> "2024-25" (football Aug-May season)

Design notes
------------
* All transforms tolerate None / NaN input by returning a sentinel
  rather than raising. The pipeline NEVER crashes on bad input here;
  the DQ layer (Phase 4) decides what to do about it.
* Reference data (position taxonomy, country overrides) is loaded
  lazily on first use via lru_cache. Loading is a few-ms YAML parse,
  but doing it once per process is faster and more obvious in logs.
* The transformations are designed so the Silver builders compose
  them via engine.with_derived_column rather than calling them
  directly on a DataFrame — this is what keeps the abstraction
  engine-agnostic.

What this module does NOT do
----------------------------
* It does not iterate DataFrames. The engine does that.
* It does not validate ranges (DQ's job).
* It does not handle PII (Phase 10's anonymiser).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from functools import lru_cache
from pathlib import Path

import pycountry
import yaml

from src.utils.config import get_config
from src.utils.logging import get_logger

log = get_logger(__name__)


# Sentinels used when input is null/unrecognised. Recorded as-is in
# Silver; DQ can flag based on these values.
UNKNOWN_POSITION_CANONICAL = "Unknown"
UNKNOWN_POSITION_CATEGORY = "Unknown"
UNKNOWN_COUNTRY_ISO = "XX"  # ISO 3166-1 reserves 'XX' for user-defined codes


# ---------------------------------------------------------------------------
# Reference-data loaders (lazy, cached)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PositionMapping:
    """Result of normalising one raw position label."""

    canonical: str
    category: str


@lru_cache(maxsize=1)
def _load_position_taxonomy() -> dict[str, PositionMapping]:
    """Load and cache the position taxonomy YAML."""
    cfg = get_config()
    path = Path(cfg.reference.position_taxonomy)
    if not path.is_absolute():
        path = Path(__file__).resolve().parents[2] / path
    with path.open(encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    mapping: dict[str, PositionMapping] = {}
    for label, spec in raw.items():
        if not isinstance(spec, dict):
            log.warning(
                "position_taxonomy_skipped_malformed_entry",
                label=label,
                spec_type=type(spec).__name__,
            )
            continue
        mapping[str(label)] = PositionMapping(
            canonical=spec["canonical"],
            category=spec["category"],
        )
    log.info(
        "position_taxonomy_loaded",
        path=str(path),
        entries=len(mapping),
    )
    return mapping


@lru_cache(maxsize=1)
def _load_country_overrides() -> dict[str, str]:
    """Load and cache the country override YAML (vendor-name -> ISO alpha-2)."""
    cfg = get_config()
    path = Path(cfg.reference.country_iso)
    if not path.is_absolute():
        path = Path(__file__).resolve().parents[2] / path
    with path.open(encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    # Strip and uppercase values for safety; keys stay as-given for
    # exact-match lookups.
    mapping: dict[str, str] = {str(k): str(v).strip().upper() for k, v in raw.items()}
    log.info(
        "country_overrides_loaded",
        path=str(path),
        entries=len(mapping),
    )
    return mapping


# ---------------------------------------------------------------------------
# Position normalisation
# ---------------------------------------------------------------------------


def normalise_position(raw: str | None) -> PositionMapping:
    """
    Map a raw position label to its canonical form and category.

    Tolerates None / empty / unrecognised input by returning the
    Unknown sentinels. Does NOT raise.

    Examples:
      normalise_position("GK")           -> PositionMapping("Goalkeeper", "goalkeeper")
      normalise_position("Goalkeeper")   -> PositionMapping("Goalkeeper", "goalkeeper")
      normalise_position("CB")           -> PositionMapping("Centre-Back", "defender")
      normalise_position(None)           -> PositionMapping("Unknown", "Unknown")
      normalise_position("Sweeper")      -> PositionMapping("Unknown", "Unknown")
    """
    if raw is None:
        return PositionMapping(UNKNOWN_POSITION_CANONICAL, UNKNOWN_POSITION_CATEGORY)
    stripped = str(raw).strip()
    if not stripped:
        return PositionMapping(UNKNOWN_POSITION_CANONICAL, UNKNOWN_POSITION_CATEGORY)
    taxonomy = _load_position_taxonomy()
    mapping = taxonomy.get(stripped)
    if mapping is not None:
        return mapping
    # Try case-insensitive fallback for vendor inconsistencies
    # (e.g. "goalkeeper" instead of "Goalkeeper"). Reasonably cheap;
    # only fires when an exact-match lookup missed.
    for key, value in taxonomy.items():
        if key.lower() == stripped.lower():
            return value
    return PositionMapping(UNKNOWN_POSITION_CANONICAL, UNKNOWN_POSITION_CATEGORY)


# ---------------------------------------------------------------------------
# Country normalisation
# ---------------------------------------------------------------------------


def normalise_country(raw: str | None) -> str:
    """
    Map a raw country string to its ISO 3166-1 alpha-2 code.

    Lookup order:
      1. The overrides YAML (vendor-specific variants like
         "England, United Kingdom" -> "GB")
      2. pycountry's built-in lookup (handles official names, common
         names, alpha-2, alpha-3, etc.)
      3. Fallback: UNKNOWN_COUNTRY_ISO ('XX')

    Tolerates None / empty / unrecognised input. Never raises.

    Examples:
      normalise_country("England, United Kingdom") -> "GB"
      normalise_country("Brazil")                  -> "BR"
      normalise_country("Côte d'Ivoire")           -> "CI"
      normalise_country(None)                      -> "XX"
      normalise_country("Atlantis")                -> "XX"
    """
    if raw is None:
        return UNKNOWN_COUNTRY_ISO
    stripped = str(raw).strip()
    if not stripped:
        return UNKNOWN_COUNTRY_ISO

    # 1. Overrides — exact match first, then case-insensitive.
    overrides = _load_country_overrides()
    if stripped in overrides:
        return overrides[stripped]
    for key, value in overrides.items():
        if key.lower() == stripped.lower():
            return value

    # 2. pycountry — try several lookup strategies. pycountry raises
    # LookupError on miss; we swallow it and fall through.
    try:
        result = pycountry.countries.lookup(stripped)
        return result.alpha_2.upper()
    except LookupError:
        pass

    # 3. Unknown.
    return UNKNOWN_COUNTRY_ISO


# ---------------------------------------------------------------------------
# Match outcome
# ---------------------------------------------------------------------------


def derive_match_outcome(
    home_goals: int | float | None,
    away_goals: int | float | None,
) -> str:
    """
    Derive match outcome from goal counts.

    Returns one of: 'home_win', 'away_win', 'draw', 'unknown'.

    The 'unknown' return happens when either side's goals is None /
    NaN — match wasn't played or wasn't fully reported. This is a
    legitimate state for future scheduled games; DQ may or may not
    care depending on context.

    Examples:
      derive_match_outcome(2, 0)     -> 'home_win'
      derive_match_outcome(0, 1)     -> 'away_win'
      derive_match_outcome(1, 1)     -> 'draw'
      derive_match_outcome(None, 0)  -> 'unknown'
      derive_match_outcome(3, None)  -> 'unknown'
    """
    if home_goals is None or away_goals is None:
        return "unknown"
    # Handle NaN (pandas) — NaN != NaN
    if isinstance(home_goals, float) and home_goals != home_goals:
        return "unknown"
    if isinstance(away_goals, float) and away_goals != away_goals:
        return "unknown"
    h, a = int(home_goals), int(away_goals)
    if h > a:
        return "home_win"
    if a > h:
        return "away_win"
    return "draw"


# ---------------------------------------------------------------------------
# Season derivation
# ---------------------------------------------------------------------------


def derive_season(match_date: str | date | datetime | None) -> str | None:
    """
    Map a match date to its football season label.

    Football seasons in European leagues run August through May. A
    match in October 2024 belongs to season "2024-25"; a match in
    March 2025 also belongs to "2024-25". A match in July 2024 (off-
    season / pre-season tournament) gets attributed to "2023-24" since
    the new season hasn't started yet by convention.

    Convention used here:
      Months Aug-Dec  -> "YYYY-YY"   (current year + next year suffix)
      Months Jan-Jul  -> "YYYY-YY"   (previous year + current year suffix)

    Examples:
      derive_season("2024-10-15") -> "2024-25"
      derive_season("2025-03-20") -> "2024-25"
      derive_season("2025-08-12") -> "2025-26"
      derive_season("2025-07-01") -> "2024-25"
      derive_season(None)         -> None
    """
    if match_date is None:
        return None

    # Accept str, datetime.date, or datetime.datetime
    if isinstance(match_date, str):
        stripped = match_date.strip()
        if not stripped:
            return None
        try:
            # ISO format, possibly with a time portion. Strip after T if present.
            date_part = stripped.split("T", 1)[0].split(" ", 1)[0]
            parsed = datetime.strptime(date_part, "%Y-%m-%d").date()
        except ValueError:
            return None
    elif isinstance(match_date, datetime):
        parsed = match_date.date()
    elif isinstance(match_date, date):
        parsed = match_date
    else:
        return None

    if parsed.month >= 8:
        start_year = parsed.year
    else:
        start_year = parsed.year - 1
    end_year_suffix = (start_year + 1) % 100
    return f"{start_year}-{end_year_suffix:02d}"
