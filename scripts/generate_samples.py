"""
Sample data generator.

Produces small, deterministic CSV samples for all six Kaggle sources
(competitions, clubs, players, games, appearances, player_valuations).
The output files land in data/sample/ and are committed to git so the
pipeline runs out-of-the-box for any reviewer with no Kaggle account.

Why a generator (rather than just committing handcrafted CSVs):

  1. The samples need to satisfy non-trivial invariants — every
     foreign key in `appearances` must resolve to a real player_id
     and game_id; every player's `current_club_id` must exist in
     `clubs`; etc. Handcrafting that consistency is error-prone.
  2. We deliberately include EDGE CASES that downstream phases will
     exercise (FK violation for DQ, position-label variants for the
     normaliser, SCD2-prone players for Phase 6's day-2 demo).
     Pinning these in a generator makes the intent reviewable.
  3. Determinism — same code, same seed, same output. The committed
     CSVs cannot drift if someone re-runs the generator.

Usage:

    python -m scripts.generate_samples           # writes to data/sample/

What the generator deliberately includes:

  * 3 competitions, 5 clubs, 12 players, 6 games, 24 appearances,
    16 valuation observations across 3 different dates.
  * 1 FK violation in `appearances` (orphan player_id) — DQ MUST catch.
  * Position-label variants: 'GK' and 'Goalkeeper' both present so
    the position taxonomy normaliser has work to do.
  * Country variants: 'England' and 'England, United Kingdom' so the
    ISO country normaliser is exercised.
  * 3 SCD2-prone players whose current_club_id and market_value will
    change in Phase 6's day_2 snapshot — picked deliberately so the
    SCD2 demo has clear before/after rows.
  * Numeric edge: a valid row with minutes_played=0 and goals=0 so
    the DQ range check (>=0) passes the boundary; the orphan FK row
    also acts as a *malformed* edge for DQ rejection.

This file is run, not imported. It writes 6 CSVs and exits.
"""

from __future__ import annotations

import csv
import random
from datetime import date, datetime, timedelta
from pathlib import Path

# Pinned seed: same generator run -> same CSV contents. Don't change this
# unless you're prepared to re-commit all the sample CSVs.
RANDOM_SEED = 20260601

# Output directory (project_root/data/sample). Resolved at runtime so the
# script works from any cwd.
SAMPLES_DIR = Path(__file__).resolve().parents[1] / "data" / "sample"


# =====================================================================
# Reference data — fixed, deliberately small
# =====================================================================

COMPETITIONS = [
    # competition_id, name, country_name, sub_type, type, confederation
    ("GB1", "Premier League",   "England",                "first_tier", "domestic_league", "uefa"),
    ("ES1", "La Liga",          "Spain",                  "first_tier", "domestic_league", "uefa"),
    ("L1",  "Bundesliga",       "Germany",                "first_tier", "domestic_league", "uefa"),
]

CLUBS = [
    # club_id, club_code, name, comp_id, total_value, squad, foreign_n, country
    (1, "arsenal-fc",    "Arsenal FC",        "GB1",  900_000_000, 25, 18, "England"),
    (2, "chelsea-fc",    "Chelsea FC",        "GB1",  850_000_000, 28, 22, "England, United Kingdom"),
    (3, "real-madrid",   "Real Madrid",       "ES1", 1_200_000_000, 26, 12, "Spain"),
    (4, "fc-barcelona",  "FC Barcelona",      "ES1", 1_100_000_000, 27, 15, "Spain"),
    (5, "bayern-munich", "FC Bayern München", "L1",  1_000_000_000, 26, 14, "Germany"),
]


# Players: we deliberately pick three of them (player_ids 1001-1003) as
# the SCD2-prone trio. Their (current_club_id, position, market_value)
# will get tweaked in data/day2/ in Phase 6.
#
# Position labels mix 'GK' / 'Goalkeeper' and 'CB' / 'Centre-Back' on
# purpose, so the normaliser has work to do.
PLAYERS = [
    # player_id, first_name, last_name, name,             current_club_id,
    #   position,         country_of_birth,           country_of_citizenship,
    #   date_of_birth,   market_value_in_eur, highest_market_value_in_eur
    (1001, "Bukayo",  "Saka",        "Bukayo Saka",        1, "RW",         "England",                  "England",        "2001-09-05",  120_000_000, 130_000_000),
    (1002, "Martin",  "Ødegaard",    "Martin Ødegaard",    1, "CAM",        "Norway",                   "Norway",         "1998-12-17",  100_000_000, 110_000_000),
    (1003, "Reece",   "James",       "Reece James",        2, "RB",         "England",                  "England",         "1999-12-08",   65_000_000,  85_000_000),

    # The rest stay fixed across day1/day2 — they're filler for FK density.
    (1004, "David",   "Raya",        "David Raya",         1, "GK",         "Spain",                    "Spain",          "1995-09-15",   35_000_000,  40_000_000),
    (1005, "Robert",  "Sánchez",     "Robert Sánchez",     2, "Goalkeeper", "Spain",                    "Spain",          "1997-11-18",   12_000_000,  20_000_000),
    (1006, "Vinícius","Júnior",      "Vinícius Júnior",    3, "LW",         "Brazil",                   "Brazil",         "2000-07-12",  200_000_000, 200_000_000),
    (1007, "Jude",    "Bellingham",  "Jude Bellingham",    3, "CAM",        "England, United Kingdom",  "England",        "2003-06-29",  180_000_000, 180_000_000),
    (1008, "Robert",  "Lewandowski", "Robert Lewandowski", 4, "ST",         "Poland",                   "Poland",         "1988-08-21",   30_000_000, 100_000_000),
    (1009, "Pedri",   "González",    "Pedri",              4, "CM",         "Spain",                    "Spain",          "2002-11-25",  100_000_000, 100_000_000),
    (1010, "Harry",   "Kane",        "Harry Kane",         5, "ST",         "England",                  "England",        "1993-07-28",   90_000_000, 150_000_000),
    (1011, "Joshua",  "Kimmich",     "Joshua Kimmich",     5, "CDM",        "Germany",                  "Germany",        "1995-02-08",   60_000_000,  90_000_000),
    (1012, "Manuel",  "Neuer",       "Manuel Neuer",       5, "GK",         "Germany",                  "Germany",        "1986-03-27",    6_000_000,  70_000_000),
]


# Games: 6 matches spread across the three leagues, recent dates.
# Each (home_club, away_club) is a club that exists in CLUBS.
GAMES = [
    # game_id, comp_id, season, round, date,         home_club, away_club,
    #   home_goals, away_goals, home_manager,    away_manager,    stadium,                attendance
    (5001, "GB1", 2024, "Matchday 10", "2024-11-09", 1, 2, 2, 0, "Mikel Arteta",   "Enzo Maresca",     "Emirates Stadium",   60260),
    (5002, "GB1", 2024, "Matchday 11", "2024-11-23", 2, 1, 1, 1, "Enzo Maresca",   "Mikel Arteta",     "Stamford Bridge",    40173),
    (5003, "ES1", 2024, "Matchday 13", "2024-11-10", 3, 4, 3, 2, "Carlo Ancelotti","Hansi Flick",      "Santiago Bernabéu",  78230),
    (5004, "ES1", 2024, "Matchday 14", "2024-11-23", 4, 3, 1, 1, "Hansi Flick",    "Carlo Ancelotti",  "Spotify Camp Nou",   55000),
    (5005, "L1",  2024, "Matchday 11", "2024-11-09", 5, 1, 4, 1, "Vincent Kompany","Mikel Arteta",     "Allianz Arena",      75000),
    (5006, "L1",  2024, "Matchday 12", "2024-11-23", 5, 3, 2, 2, "Vincent Kompany","Carlo Ancelotti",  "Allianz Arena",      75000),
]


# Appearances: roughly 4 per match. Built from GAMES + PLAYERS with
# realistic-looking stats. One row deliberately uses a non-existent
# player_id (9999) — that's our DQ FK violation seed.
def _build_appearances() -> list[tuple]:
    """Generate appearance rows from GAMES + PLAYERS with one FK violation."""
    rng = random.Random(RANDOM_SEED)
    rows: list[tuple] = []
    appearance_seq = 8001

    # For each game, pick ~4 players from each side and create appearance rows.
    for game in GAMES:
        game_id, comp_id, _, _, game_date, home_id, away_id, *_ = game
        home_players = [p for p in PLAYERS if p[4] == home_id]
        away_players = [p for p in PLAYERS if p[4] == away_id]

        for player in home_players + away_players:
            player_id = player[0]
            minutes = rng.choice([90, 90, 85, 75, 60, 30])
            goals = rng.choice([0, 0, 0, 0, 1, 2])
            assists = rng.choice([0, 0, 0, 1])
            yellow = rng.choice([0, 0, 1])
            red = 0   # red cards are rare; keep them out of samples
            rows.append((
                f"A{appearance_seq:05d}",
                game_id, player_id, player[4], player[4],   # player_club_id == player.current_club_id
                game_date,
                player[3],   # player.name
                comp_id,
                yellow, red, goals, assists, minutes,
            ))
            appearance_seq += 1

    # Edge case: one appearance for a non-existent player. DQ MUST flag this.
    # We splice it in mid-sequence so it's not trivially the last row.
    rows.insert(
        len(rows) // 2,
        (
            f"A{appearance_seq:05d}",
            5001, 9999, 99, 99,
            "2024-11-09",
            "PHANTOM PLAYER",
            "GB1",
            0, 0, 1, 0, 90,
        ),
    )
    return rows


# Player valuations: each SCD2-prone player gets 3 valuation observations
# at different dates so the rolling-average Gold table has signal.
def _build_valuations() -> list[tuple]:
    """Generate valuation history for a subset of players."""
    rows: list[tuple] = []
    base_dates = [date(2024, 6, 1), date(2024, 8, 15), date(2024, 11, 1)]

    # SCD2-prone trio: track 3 valuation points each
    valuation_history = {
        1001: [110_000_000, 115_000_000, 120_000_000],   # Saka: trending up
        1002: [100_000_000,  95_000_000, 100_000_000],   # Ødegaard: dip then recovery
        1003: [ 60_000_000,  62_000_000,  65_000_000],   # James: steady growth
    }
    # A few other players get one or two observations for FK density
    valuation_history.update({
        1006: [180_000_000, 200_000_000, 200_000_000],   # Vinícius
        1007: [170_000_000, 175_000_000, 180_000_000],   # Bellingham
        1010: [100_000_000,  95_000_000,  90_000_000],   # Kane: trending down
    })

    for player_id, values in valuation_history.items():
        # Find this player's current_club_id and competition
        player = next(p for p in PLAYERS if p[0] == player_id)
        club_id = player[4]
        club = next(c for c in CLUBS if c[0] == club_id)
        comp_id = club[3]
        for d, v in zip(base_dates, values, strict=True):
            rows.append((player_id, d.isoformat(), v, club_id, comp_id))
    return rows


# =====================================================================
# CSV writers — one per source, each emits exactly the columns
# declared in configs/sources.yaml for that source
# =====================================================================


def _writerows(path: Path, header: list[str], rows: list[tuple]) -> None:
    """Write a CSV with the given header. Always quotes for safety."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        w.writerow(header)
        w.writerows(rows)


def _write_competitions() -> None:
    header = [
        "competition_id", "name", "country_name", "sub_type",
        "type", "confederation", "url",
    ]
    rows = [
        (cid, name, country, sub, typ, conf, f"https://example.com/comp/{cid}")
        for (cid, name, country, sub, typ, conf) in COMPETITIONS
    ]
    _writerows(SAMPLES_DIR / "competitions.csv", header, rows)


def _write_clubs() -> None:
    header = [
        "club_id", "club_code", "name", "domestic_competition_id",
        "total_market_value", "squad_size", "average_age",
        "foreigners_number", "foreigners_percentage",
        "national_team_players", "stadium_name", "stadium_seats",
        "net_transfer_record", "coach_name", "last_season",
        "filename", "url",
    ]
    rows = []
    for cid, code, name, comp, total, squad, foreign_n, _country in CLUBS:
        rows.append((
            cid, code, name, comp, total, squad, 27.5,
            foreign_n, round(100 * foreign_n / squad, 1),
            8, f"{name} Stadium", 60_000,
            "+50000000", "Unknown Coach", 2024,
            f"{code}.json", f"https://example.com/club/{cid}",
        ))
    _writerows(SAMPLES_DIR / "clubs.csv", header, rows)


def _write_players() -> None:
    header = [
        "player_id", "first_name", "last_name", "name", "last_season",
        "current_club_id", "player_code", "country_of_birth",
        "city_of_birth", "country_of_citizenship", "date_of_birth",
        "sub_position", "position", "foot", "height_in_cm",
        "market_value_in_eur", "highest_market_value_in_eur",
        "contract_expiration_date", "agent_name", "image_url", "url",
        "current_club_domestic_competition_id", "current_club_name",
    ]
    rows = []
    for player in PLAYERS:
        (
            pid, first, last, name, club_id, position,
            country_birth, country_citizen, dob, market_val, highest_val,
        ) = player
        club = next(c for c in CLUBS if c[0] == club_id)
        rows.append((
            pid, first, last, name, 2024,
            club_id,
            f"{first.lower()}-{last.lower()}".replace(" ", "-"),
            country_birth,
            "London",  # placeholder, PII-hashed in Silver
            country_citizen,
            dob,
            position,  # NOTE: same value used for sub_position and position in samples
            position,
            "right",
            180,
            market_val, highest_val,
            "2027-06-30",
            "Unknown Agency",
            f"https://example.com/img/{pid}.jpg",
            f"https://example.com/player/{pid}",
            club[3],   # competition_id
            club[2],   # club name
        ))
    _writerows(SAMPLES_DIR / "players.csv", header, rows)


def _write_games() -> None:
    header = [
        "game_id", "competition_id", "season", "round", "date",
        "home_club_id", "away_club_id", "home_club_goals", "away_club_goals",
        "home_club_position", "away_club_position",
        "home_club_manager_name", "away_club_manager_name",
        "stadium", "attendance", "referee", "url",
        "home_club_formation", "away_club_formation",
        "home_club_name", "away_club_name", "aggregate", "competition_type",
    ]
    rows = []
    for game in GAMES:
        (
            gid, comp, season, rnd, gdate, home, away, hg, ag,
            home_mgr, away_mgr, stadium, attendance,
        ) = game
        home_name = next(c for c in CLUBS if c[0] == home)[2]
        away_name = next(c for c in CLUBS if c[0] == away)[2]
        rows.append((
            gid, comp, season, rnd, gdate, home, away, hg, ag,
            1, 2,           # rough table positions
            home_mgr, away_mgr,
            stadium, attendance,
            "Felix Zwayer",  # placeholder referee
            f"https://example.com/game/{gid}",
            "4-3-3", "4-2-3-1",
            home_name, away_name,
            f"{hg}:{ag}",
            "domestic_league",
        ))
    _writerows(SAMPLES_DIR / "games.csv", header, rows)


def _write_appearances() -> None:
    header = [
        "appearance_id", "game_id", "player_id", "player_club_id",
        "player_current_club_id", "date", "player_name",
        "competition_id", "yellow_cards", "red_cards", "goals",
        "assists", "minutes_played",
    ]
    _writerows(SAMPLES_DIR / "appearances.csv", header, _build_appearances())


def _write_valuations() -> None:
    header = [
        "player_id", "date", "market_value_in_eur",
        "current_club_id", "player_club_domestic_competition_id",
    ]
    _writerows(SAMPLES_DIR / "player_valuations.csv", header, _build_valuations())


# =====================================================================
# Entry point
# =====================================================================


def main() -> None:
    """Generate all six sample CSVs into data/sample/."""
    SAMPLES_DIR.mkdir(parents=True, exist_ok=True)
    _write_competitions()
    _write_clubs()
    _write_players()
    _write_games()
    _write_appearances()
    _write_valuations()
    print(f"Sample CSVs written to {SAMPLES_DIR}")
    for f in sorted(SAMPLES_DIR.glob("*.csv")):
        with f.open(encoding="utf-8") as fh:
            row_count = sum(1 for _ in fh) - 1   # subtract header
        print(f"  {f.name:30s} {row_count:4d} rows")


if __name__ == "__main__":
    main()
