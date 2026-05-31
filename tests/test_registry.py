"""
Tests for the source registry.

We cover three things:
  1. The bundled configs/sources.yaml parses and has the expected shape
     (so an accidental edit doesn't silently break ingestion).
  2. The SourceRegistry API (lookup, filtered views, __contains__) works.
  3. Error paths fail loudly (unknown source, malformed YAML, duplicate names).
"""

from __future__ import annotations

import pytest
import yaml
from pydantic import ValidationError

from src.ingestion.registry import (
    SourceDefinition,
    SourceRegistry,
    load_registry,
)


# ---------------------------------------------------------------------------
# Sanity checks on the bundled sources.yaml
# These tests double as a contract: if any of them break, someone changed
# sources.yaml in a way that may invalidate downstream assumptions.
# ---------------------------------------------------------------------------


class TestBundledSourcesYaml:
    def test_loads_without_error(self):
        registry = load_registry()
        assert isinstance(registry, SourceRegistry)
        assert len(registry) > 0

    def test_all_six_kaggle_sources_present(self):
        registry = load_registry()
        expected = {
            "competitions",
            "clubs",
            "players",
            "games",
            "appearances",
            "player_valuations",
        }
        assert set(registry.names()) == expected

    def test_players_is_only_scd2_source(self):
        registry = load_registry()
        scd2_names = {s.name for s in registry.scd2_sources()}
        assert scd2_names == {"players"}

    def test_players_tracks_expected_scd2_columns(self):
        players = load_registry().get("players")
        assert players.is_scd2
        assert set(players.scd2.tracked_columns) == {
            "current_club_id",
            "position",
            "market_value_in_eur",
        }

    def test_players_is_only_pii_source(self):
        registry = load_registry()
        pii_names = {s.name for s in registry.pii_sources()}
        assert pii_names == {"players"}

    def test_incremental_sources_have_timestamp_columns(self):
        registry = load_registry()
        for source in registry.incremental_sources():
            assert source.timestamp_column is not None, (
                f"{source.name} is in incremental_sources() but has no "
                f"timestamp_column"
            )

    def test_games_is_incremental_by_date(self):
        games = load_registry().get("games")
        assert games.is_incremental
        assert games.timestamp_column == "date"

    def test_player_valuations_has_composite_primary_key(self):
        valuations = load_registry().get("player_valuations")
        assert valuations.primary_key == ["player_id", "date"]


# ---------------------------------------------------------------------------
# SourceDefinition API
# ---------------------------------------------------------------------------


class TestSourceDefinitionAccessors:
    def test_columns_returns_schema_keys(self):
        players = load_registry().get("players")
        cols = players.columns
        assert "player_id" in cols
        assert "first_name" in cols
        # column order is meaningful (matches schema declaration order)
        assert cols[0] == "player_id"

    def test_resolve_path_substitutes_raw_root(self, tmp_path):
        players = load_registry().get("players")
        resolved = players.resolve_path(tmp_path)
        assert str(resolved).endswith("players.csv")
        assert str(tmp_path) in str(resolved)

    def test_is_scd2_false_for_non_scd2_source(self):
        clubs = load_registry().get("clubs")
        assert not clubs.is_scd2

    def test_has_pii_false_for_non_pii_source(self):
        clubs = load_registry().get("clubs")
        assert not clubs.has_pii


# ---------------------------------------------------------------------------
# SourceRegistry API
# ---------------------------------------------------------------------------


class TestSourceRegistryLookup:
    def test_get_known_source_returns_definition(self):
        source = load_registry().get("players")
        assert source.name == "players"

    def test_get_unknown_source_raises_keyerror(self):
        with pytest.raises(KeyError, match="Unknown source"):
            load_registry().get("nonexistent_table")

    def test_contains_checks_membership(self):
        registry = load_registry()
        assert "players" in registry
        assert "nonexistent" not in registry
        # non-string membership check returns False, doesn't raise
        assert 42 not in registry

    def test_iteration_yields_all_sources(self):
        registry = load_registry()
        names_via_iter = [s.name for s in registry]
        names_via_method = registry.names()
        assert names_via_iter == names_via_method

    def test_len_matches_source_count(self):
        registry = load_registry()
        assert len(registry) == 6


# ---------------------------------------------------------------------------
# Error paths — these are what protect against silent breakage
# ---------------------------------------------------------------------------


def _write_yaml(path, content: str) -> None:
    """Helper: write arbitrary YAML to disk for a load test."""
    path.write_text(content)


class TestRegistryErrorPaths:
    def test_missing_file_raises_filenotfounderror(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            load_registry(tmp_path / "does_not_exist.yaml")

    def test_unknown_field_in_source_raises_validation_error(self, tmp_path):
        bad = tmp_path / "sources.yaml"
        _write_yaml(bad, """
version: 1
sources:
  - name: t
    description: x
    format: csv
    path_pattern: "{raw_root}/t.csv"
    primary_key: [id]
    schema:
      id: int
    not_a_real_field: oops
""")
        with pytest.raises(ValidationError):
            load_registry(bad)

    def test_missing_required_field_raises_validation_error(self, tmp_path):
        bad = tmp_path / "sources.yaml"
        # 'description' is required but missing
        _write_yaml(bad, """
version: 1
sources:
  - name: t
    format: csv
    path_pattern: "{raw_root}/t.csv"
    primary_key: [id]
    schema:
      id: int
""")
        with pytest.raises(ValidationError):
            load_registry(bad)

    def test_duplicate_source_names_raise_valueerror(self, tmp_path):
        bad = tmp_path / "sources.yaml"
        _write_yaml(bad, """
version: 1
sources:
  - name: t
    description: first
    format: csv
    path_pattern: "{raw_root}/t.csv"
    primary_key: [id]
    schema: { id: int }
  - name: t
    description: duplicate
    format: csv
    path_pattern: "{raw_root}/t.csv"
    primary_key: [id]
    schema: { id: int }
""")
        with pytest.raises(ValueError, match="Duplicate source names"):
            load_registry(bad)

    def test_empty_sources_list_raises_validation_error(self, tmp_path):
        bad = tmp_path / "sources.yaml"
        _write_yaml(bad, """
version: 1
sources: []
""")
        with pytest.raises(ValidationError):
            load_registry(bad)

    def test_invalid_format_raises_validation_error(self, tmp_path):
        bad = tmp_path / "sources.yaml"
        # 'avro' isn't in the allowed pattern
        _write_yaml(bad, """
version: 1
sources:
  - name: t
    description: x
    format: avro
    path_pattern: "{raw_root}/t.avro"
    primary_key: [id]
    schema: { id: int }
""")
        with pytest.raises(ValidationError):
            load_registry(bad)

    def test_scd2_with_empty_tracked_columns_rejected(self, tmp_path):
        bad = tmp_path / "sources.yaml"
        _write_yaml(bad, """
version: 1
sources:
  - name: t
    description: x
    format: csv
    path_pattern: "{raw_root}/t.csv"
    primary_key: [id]
    schema: { id: int }
    scd2:
      tracked_columns: []
""")
        with pytest.raises(ValidationError):
            load_registry(bad)


# ---------------------------------------------------------------------------
# Construct registry directly (bypassing YAML) — useful for unit tests
# elsewhere that want to mint synthetic sources
# ---------------------------------------------------------------------------


class TestDirectConstruction:
    def test_can_build_registry_from_definitions(self):
        sd = SourceDefinition(
            name="synthetic",
            description="test-only source",
            format="csv",
            path_pattern="{raw_root}/synthetic.csv",
            primary_key=["id"],
            schema={"id": "int", "value": "string"},
        )
        registry = SourceRegistry([sd])
        assert "synthetic" in registry
        assert registry.get("synthetic").columns == ["id", "value"]

    def test_construction_rejects_duplicate_names(self):
        sd_a = SourceDefinition(
            name="dup", description="a", format="csv",
            path_pattern="{raw_root}/a.csv", primary_key=["id"],
            schema={"id": "int"},
        )
        sd_b = SourceDefinition(
            name="dup", description="b", format="csv",
            path_pattern="{raw_root}/b.csv", primary_key=["id"],
            schema={"id": "int"},
        )
        with pytest.raises(ValueError, match="Duplicate source names"):
            SourceRegistry([sd_a, sd_b])
