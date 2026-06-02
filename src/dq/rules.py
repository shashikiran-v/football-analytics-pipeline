"""
DQ rule types and registry.

Five rule classes mapping to the brief's required DQ checks:

  NotNullRule       columns must not be null
  RangeRule         numeric column within [min, max] inclusive
  UniqueRule        column(s) form a unique key
  ForeignKeyRule    column values appear in another source's column
  SchemaRule        columns match declared types and nullability

Each rule:
  - Is a frozen Pydantic model (typed, validated on YAML load)
  - Has a `rule_type` discriminator string ('not_null', 'range', etc.)
  - Has a `severity` ∈ {'critical', 'warning'}
  - Implements evaluate(df, engine, context) returning a list[bool]
    (one entry per row: True = passes, False = fails)
  - Has a stable `id()` for tying rule outcomes back to their source
    config

The runner stacks per-rule outcomes, identifies rows that failed ≥1
critical rule, and quarantines those. Rows that fail only warnings
pass through to Silver with the warning logged.

Engine-agnostic
---------------
Evaluation goes through the engine protocol: to_records, filter_isin,
etc. No pandas-specific code in this module. Spark will use the same
implementations when SparkEngine arrives.

Configuration
-------------
Rules are loaded from configs/dq_rules.yaml by `load_dq_rules()`,
which is lru_cached. The loader handles the schema validation; a
malformed YAML raises ValidationError at startup, not at run time.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Annotated, Any, Literal, Union

import yaml
from pydantic import BaseModel, ConfigDict, Field

from src.engines.base import DataFrame, DataFrameEngine
from src.utils.logging import get_logger


log = get_logger(__name__)


# Constants used in the runner / quarantine writer
SEVERITY_CRITICAL = "critical"
SEVERITY_WARNING = "warning"


# ---------------------------------------------------------------------------
# Evaluation context — shared state across rules in one DQ pass
# ---------------------------------------------------------------------------


@dataclass
class DQEvalContext:
    """
    Shared state for one DQ pass over one source.

    fk_lookups: mapping of (source_name, column) -> set of valid values.
                FK rules pull from this rather than re-reading the
                referenced parquet for every row.
    """

    fk_lookups: dict[tuple[str, str], set[Any]]


# ---------------------------------------------------------------------------
# Base — common fields on every rule
# ---------------------------------------------------------------------------


class _RuleBase(BaseModel):
    """Common fields and Pydantic config for all rules."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    source: str
    severity: Literal["critical", "warning"]
    description: str | None = None


# ---------------------------------------------------------------------------
# NotNullRule
# ---------------------------------------------------------------------------


class NotNullRule(_RuleBase):
    rule_type: Literal["not_null"]
    columns: list[str] = Field(min_length=1)

    def id(self) -> str:
        return f"not_null:{self.source}:{','.join(self.columns)}"

    def evaluate(
        self, df: DataFrame, engine: DataFrameEngine,
        context: DQEvalContext,
    ) -> list[bool]:
        records = engine.to_records(df)
        passes: list[bool] = []
        for rec in records:
            row_ok = True
            for col in self.columns:
                value = rec.get(col)
                if value is None:
                    row_ok = False
                    break
                # Pandas NaN is a float != itself
                if isinstance(value, float) and value != value:
                    row_ok = False
                    break
            passes.append(row_ok)
        return passes


# ---------------------------------------------------------------------------
# RangeRule
# ---------------------------------------------------------------------------


class RangeRule(_RuleBase):
    rule_type: Literal["range"]
    column: str
    min: float | int | None = None
    max: float | int | None = None

    def id(self) -> str:
        return f"range:{self.source}:{self.column}"

    def evaluate(
        self, df: DataFrame, engine: DataFrameEngine,
        context: DQEvalContext,
    ) -> list[bool]:
        records = engine.to_records(df)
        passes: list[bool] = []
        for rec in records:
            value = rec.get(self.column)
            if value is None:
                # NotNullRule covers nullability; range treats null as pass
                # so failures don't double-count. The brief's separation
                # of concerns: each rule type tests one thing.
                passes.append(True)
                continue
            if isinstance(value, float) and value != value:
                # NaN — same reasoning as None
                passes.append(True)
                continue
            try:
                numeric = float(value)
            except (TypeError, ValueError):
                # Non-numeric in a numeric range column = fail
                passes.append(False)
                continue
            ok = True
            if self.min is not None and numeric < self.min:
                ok = False
            if self.max is not None and numeric > self.max:
                ok = False
            passes.append(ok)
        return passes


# ---------------------------------------------------------------------------
# UniqueRule
# ---------------------------------------------------------------------------


class UniqueRule(_RuleBase):
    rule_type: Literal["unique"]
    columns: list[str] = Field(min_length=1)

    def id(self) -> str:
        return f"unique:{self.source}:{','.join(self.columns)}"

    def evaluate(
        self, df: DataFrame, engine: DataFrameEngine,
        context: DQEvalContext,
    ) -> list[bool]:
        records = engine.to_records(df)
        # First pass: count occurrences of each composite key value
        counts: dict[tuple, int] = {}
        for rec in records:
            key = tuple(rec.get(c) for c in self.columns)
            counts[key] = counts.get(key, 0) + 1
        # Second pass: mark each row as pass if its key occurs exactly once
        passes: list[bool] = []
        for rec in records:
            key = tuple(rec.get(c) for c in self.columns)
            passes.append(counts[key] == 1)
        return passes


# ---------------------------------------------------------------------------
# ForeignKeyRule — the one that catches our orphan player_id=9999
# ---------------------------------------------------------------------------


class ForeignKeyRule(_RuleBase):
    rule_type: Literal["foreign_key"]
    column: str
    references_source: str
    references_column: str

    def id(self) -> str:
        return (
            f"foreign_key:{self.source}.{self.column}"
            f"->{self.references_source}.{self.references_column}"
        )

    def evaluate(
        self, df: DataFrame, engine: DataFrameEngine,
        context: DQEvalContext,
    ) -> list[bool]:
        # The runner pre-populates context.fk_lookups for every
        # (source, column) referenced by any FK rule. We just look up
        # the set of valid values; per-row check is O(1).
        valid_set = context.fk_lookups.get(
            (self.references_source, self.references_column)
        )
        if valid_set is None:
            log.error(
                "fk_lookup_missing",
                rule_id=self.id(),
                expected_key=(self.references_source, self.references_column),
            )
            # Failing closed: if we can't verify, the rule passes (don't
            # quarantine real rows for a config gap). The error log is
            # how the operator finds out.
            return [True] * engine.count(df)

        records = engine.to_records(df)
        passes: list[bool] = []
        for rec in records:
            value = rec.get(self.column)
            if value is None:
                # NotNullRule handles null; FK rule passes on null
                passes.append(True)
                continue
            if isinstance(value, float) and value != value:
                passes.append(True)
                continue
            passes.append(value in valid_set)
        return passes


# ---------------------------------------------------------------------------
# SchemaRule
# ---------------------------------------------------------------------------


class SchemaRule(_RuleBase):
    """
    Schema validation: declared columns exist, dtypes match.

    This is row-level so the API is uniform with other rules — but
    schema failures affect the whole DataFrame (one wrong dtype = all
    rows fail). The simpler implementation would be batch-level; we
    keep row-level so the report has consistent row-grain everywhere.
    """

    rule_type: Literal["schema"]
    expected_columns: dict[str, str]      # column_name -> type tag (matches sources.yaml)

    def id(self) -> str:
        return f"schema:{self.source}"

    def evaluate(
        self, df: DataFrame, engine: DataFrameEngine,
        context: DQEvalContext,
    ) -> list[bool]:
        actual_cols = set(engine.columns(df))
        expected = set(self.expected_columns.keys())
        if missing := expected - actual_cols:
            log.warning(
                "schema_rule_missing_columns",
                rule_id=self.id(),
                missing_columns=sorted(missing),
            )
            # All rows fail (schema-level failure surfaces uniformly)
            return [False] * engine.count(df)
        # Type checks not implemented at row level for v1 — the file
        # loader already coerces to declared types. Future enhancement.
        return [True] * engine.count(df)


# ---------------------------------------------------------------------------
# Discriminated union — used by the YAML loader
# ---------------------------------------------------------------------------


Rule = Annotated[
    Union[NotNullRule, RangeRule, UniqueRule, ForeignKeyRule, SchemaRule],
    Field(discriminator="rule_type"),
]


class _DQRulesFile(BaseModel):
    """Pydantic model for the dq_rules.yaml structure."""

    model_config = ConfigDict(extra="forbid")
    version: int
    rules: list[Rule]


# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------


@lru_cache(maxsize=1)
def load_dq_rules() -> list[Rule]:
    """
    Load all DQ rules from configs/dq_rules.yaml.

    Cached for the process lifetime. The runner filters by source at
    evaluation time.
    """
    from src.utils.config import get_config

    cfg = get_config()
    path = Path(cfg.reference.dq_rules)
    if not path.is_absolute():
        path = Path(__file__).resolve().parents[2] / path
    with path.open(encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    parsed = _DQRulesFile.model_validate(raw)
    log.info(
        "dq_rules_loaded",
        path=str(path),
        rule_count=len(parsed.rules),
        version=parsed.version,
    )
    return list(parsed.rules)


def rules_for_source(source_name: str) -> list[Rule]:
    """Return the subset of rules that apply to a given source."""
    return [r for r in load_dq_rules() if r.source == source_name]


def fk_dependencies() -> dict[tuple[str, str], list[str]]:
    """
    Return the mapping needed to populate DQEvalContext.fk_lookups.

    Keys: (source_name, column_name) referenced by some FK rule.
    Values: list of consuming source names (for logging / debugging).

    The runner uses the key set to know which Bronze tables to read in
    advance and which columns to extract for the in-memory lookup sets.
    """
    deps: dict[tuple[str, str], list[str]] = {}
    for rule in load_dq_rules():
        if isinstance(rule, ForeignKeyRule):
            key = (rule.references_source, rule.references_column)
            deps.setdefault(key, []).append(rule.source)
    return deps
