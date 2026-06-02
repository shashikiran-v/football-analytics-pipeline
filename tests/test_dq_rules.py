"""
Tests for src.dq.rules.

Each rule type gets its own test class covering happy paths, edge cases
(null, NaN, type-mismatch), and the configurations specific to that
rule type.
"""

from __future__ import annotations

import pandas as pd
import pytest

from src.dq.rules import (
    SEVERITY_CRITICAL,
    DQEvalContext,
    ForeignKeyRule,
    NotNullRule,
    RangeRule,
    SchemaRule,
    UniqueRule,
    fk_dependencies,
    load_dq_rules,
    rules_for_source,
)
from src.engines.pandas_engine import PandasEngine


@pytest.fixture
def engine():
    return PandasEngine()


@pytest.fixture
def empty_context():
    return DQEvalContext(fk_lookups={})


# ---------------------------------------------------------------------------
# NotNullRule
# ---------------------------------------------------------------------------


class TestNotNullRule:
    def test_passes_when_all_columns_populated(self, engine, empty_context):
        rule = NotNullRule(
            source="x", rule_type="not_null",
            columns=["a", "b"], severity="critical",
        )
        df = pd.DataFrame([{"a": 1, "b": "x"}, {"a": 2, "b": "y"}])
        assert rule.evaluate(df, engine, empty_context) == [True, True]

    def test_fails_on_null_in_any_column(self, engine, empty_context):
        rule = NotNullRule(
            source="x", rule_type="not_null",
            columns=["a", "b"], severity="critical",
        )
        df = pd.DataFrame([
            {"a": 1, "b": "x"},
            {"a": None, "b": "y"},
            {"a": 3, "b": None},
        ])
        assert rule.evaluate(df, engine, empty_context) == [True, False, False]

    def test_fails_on_nan(self, engine, empty_context):
        rule = NotNullRule(
            source="x", rule_type="not_null",
            columns=["a"], severity="critical",
        )
        df = pd.DataFrame([{"a": 1.0}, {"a": float("nan")}])
        assert rule.evaluate(df, engine, empty_context) == [True, False]


# ---------------------------------------------------------------------------
# RangeRule
# ---------------------------------------------------------------------------


class TestRangeRule:
    def test_passes_in_range(self, engine, empty_context):
        rule = RangeRule(
            source="x", rule_type="range",
            column="v", min=0, max=100, severity="warning",
        )
        df = pd.DataFrame([{"v": 0}, {"v": 50}, {"v": 100}])
        assert rule.evaluate(df, engine, empty_context) == [True, True, True]

    def test_fails_below_min(self, engine, empty_context):
        rule = RangeRule(
            source="x", rule_type="range",
            column="v", min=0, max=100, severity="warning",
        )
        df = pd.DataFrame([{"v": -1}, {"v": 0}])
        assert rule.evaluate(df, engine, empty_context) == [False, True]

    def test_fails_above_max(self, engine, empty_context):
        rule = RangeRule(
            source="x", rule_type="range",
            column="v", min=0, max=100, severity="warning",
        )
        df = pd.DataFrame([{"v": 100}, {"v": 101}])
        assert rule.evaluate(df, engine, empty_context) == [True, False]

    def test_null_passes(self, engine, empty_context):
        """Range treats null as pass — NotNullRule handles nullability."""
        rule = RangeRule(
            source="x", rule_type="range",
            column="v", min=0, max=100, severity="warning",
        )
        df = pd.DataFrame([{"v": None}, {"v": float("nan")}])
        assert rule.evaluate(df, engine, empty_context) == [True, True]

    def test_only_min_specified(self, engine, empty_context):
        rule = RangeRule(
            source="x", rule_type="range",
            column="v", min=0, severity="warning",
        )
        df = pd.DataFrame([{"v": -1}, {"v": 0}, {"v": 1_000_000}])
        assert rule.evaluate(df, engine, empty_context) == [False, True, True]

    def test_non_numeric_fails(self, engine, empty_context):
        rule = RangeRule(
            source="x", rule_type="range",
            column="v", min=0, max=100, severity="warning",
        )
        df = pd.DataFrame([{"v": "not-a-number"}, {"v": 5}])
        assert rule.evaluate(df, engine, empty_context) == [False, True]


# ---------------------------------------------------------------------------
# UniqueRule
# ---------------------------------------------------------------------------


class TestUniqueRule:
    def test_all_unique_passes_all(self, engine, empty_context):
        rule = UniqueRule(
            source="x", rule_type="unique",
            columns=["id"], severity="critical",
        )
        df = pd.DataFrame([{"id": 1}, {"id": 2}, {"id": 3}])
        assert rule.evaluate(df, engine, empty_context) == [True, True, True]

    def test_duplicates_fail_all_occurrences(self, engine, empty_context):
        """If id=2 appears twice, BOTH rows fail — not just one."""
        rule = UniqueRule(
            source="x", rule_type="unique",
            columns=["id"], severity="critical",
        )
        df = pd.DataFrame([{"id": 1}, {"id": 2}, {"id": 2}, {"id": 3}])
        assert rule.evaluate(df, engine, empty_context) == [True, False, False, True]

    def test_composite_key(self, engine, empty_context):
        rule = UniqueRule(
            source="x", rule_type="unique",
            columns=["a", "b"], severity="critical",
        )
        df = pd.DataFrame([
            {"a": 1, "b": "x"},
            {"a": 1, "b": "y"},   # same a, different b — fine
            {"a": 1, "b": "x"},   # dup of row 0
        ])
        # Row 0 and row 2 are dups; row 1 is unique
        assert rule.evaluate(df, engine, empty_context) == [False, True, False]


# ---------------------------------------------------------------------------
# ForeignKeyRule — the one that catches our orphan
# ---------------------------------------------------------------------------


class TestForeignKeyRule:
    def test_value_in_lookup_passes(self, engine):
        rule = ForeignKeyRule(
            source="appearances", rule_type="foreign_key",
            column="player_id",
            references_source="players", references_column="player_id",
            severity="critical",
        )
        context = DQEvalContext(fk_lookups={
            ("players", "player_id"): {1001, 1002, 1003},
        })
        df = pd.DataFrame([{"player_id": 1001}, {"player_id": 1002}])
        assert rule.evaluate(df, engine, context) == [True, True]

    def test_value_not_in_lookup_fails(self, engine):
        """The canonical orphan case from data/sample/."""
        rule = ForeignKeyRule(
            source="appearances", rule_type="foreign_key",
            column="player_id",
            references_source="players", references_column="player_id",
            severity="critical",
        )
        context = DQEvalContext(fk_lookups={
            ("players", "player_id"): {1001, 1002, 1003},
        })
        df = pd.DataFrame([{"player_id": 1001}, {"player_id": 9999}])
        assert rule.evaluate(df, engine, context) == [True, False]

    def test_null_passes(self, engine):
        """FK treats null as pass; NotNullRule handles nullability."""
        rule = ForeignKeyRule(
            source="x", rule_type="foreign_key",
            column="ref_id",
            references_source="other", references_column="id",
            severity="critical",
        )
        context = DQEvalContext(fk_lookups={
            ("other", "id"): {1, 2, 3},
        })
        df = pd.DataFrame([{"ref_id": None}, {"ref_id": float("nan")}, {"ref_id": 1}])
        assert rule.evaluate(df, engine, context) == [True, True, True]

    def test_missing_lookup_fails_open(self, engine, empty_context):
        """If FK lookup wasn't provided, the rule passes all rows
        (failing closed would quarantine real rows for a config gap).
        The runner / operator sees the error log."""
        rule = ForeignKeyRule(
            source="x", rule_type="foreign_key",
            column="ref_id",
            references_source="missing", references_column="id",
            severity="critical",
        )
        df = pd.DataFrame([{"ref_id": 1}, {"ref_id": 2}])
        # empty_context has no lookups, so rule should fall back to pass-all
        assert rule.evaluate(df, engine, empty_context) == [True, True]


# ---------------------------------------------------------------------------
# SchemaRule
# ---------------------------------------------------------------------------


class TestSchemaRule:
    def test_all_columns_present_passes(self, engine, empty_context):
        rule = SchemaRule(
            source="x", rule_type="schema",
            expected_columns={"a": "int", "b": "string"},
            severity="critical",
        )
        df = pd.DataFrame([{"a": 1, "b": "x"}, {"a": 2, "b": "y"}])
        assert rule.evaluate(df, engine, empty_context) == [True, True]

    def test_missing_column_fails_all_rows(self, engine, empty_context):
        rule = SchemaRule(
            source="x", rule_type="schema",
            expected_columns={"a": "int", "b": "string", "c": "float"},
            severity="critical",
        )
        df = pd.DataFrame([{"a": 1, "b": "x"}, {"a": 2, "b": "y"}])
        assert rule.evaluate(df, engine, empty_context) == [False, False]


# ---------------------------------------------------------------------------
# YAML loader + introspection
# ---------------------------------------------------------------------------


class TestRuleLoader:
    def test_load_dq_rules_returns_typed_rules(self):
        from src.utils.config import get_config
        # Ensure config cache is reset so the new dq_rules path is picked up
        get_config.cache_clear()
        load_dq_rules.cache_clear()
        rules = load_dq_rules()
        assert len(rules) > 0
        # Every rule has a severity in the expected set
        for rule in rules:
            assert rule.severity in ("critical", "warning")

    def test_rules_for_source_filters_correctly(self):
        from src.utils.config import get_config
        get_config.cache_clear()
        load_dq_rules.cache_clear()
        # appearances has multiple rules in the bundled YAML
        app_rules = rules_for_source("appearances")
        assert len(app_rules) > 0
        assert all(r.source == "appearances" for r in app_rules)
        # Specifically the FK to players is there — the rule that
        # catches our orphan
        fk_rules = [r for r in app_rules if isinstance(r, ForeignKeyRule)]
        assert any(
            r.references_source == "players" and r.references_column == "player_id"
            for r in fk_rules
        )

    def test_fk_dependencies_captures_all_fk_targets(self):
        from src.utils.config import get_config
        get_config.cache_clear()
        load_dq_rules.cache_clear()
        deps = fk_dependencies()
        # Every FK target in the bundled rules should appear
        assert ("players", "player_id") in deps
        assert ("clubs", "club_id") in deps
        assert ("competitions", "competition_id") in deps
        assert ("games", "game_id") in deps
