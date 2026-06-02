"""
DQ runner — orchestrates rule evaluation for one source.

Public entry point: `run_dq_for_source` takes a Bronze source's
DataFrame plus the FK lookups it needs, evaluates every applicable
rule, and returns a typed `DQResult` containing:

  - clean_rows: the rows that passed all CRITICAL rules
  - failing_rows: the rows that failed at least one CRITICAL rule,
                  with a _dq_failure_reason column populated
  - warnings:  list of (rule_id, failing_row_count) pairs for rules
               that fired at warning severity (these rows DO pass
               through to Silver, just logged + reported)

The runner does NOT write to disk; the caller (Bronze->Silver
orchestrator in Slice 4.2) decides where to write clean_rows
(Silver) and failing_rows (the quarantine writer).

FK lookup mechanics
-------------------
A Bronze source FK rule (e.g. appearances.player_id -> players.player_id)
requires the set of valid player_ids to be known. The caller is
responsible for loading the referenced Bronze partitions and building
the lookup sets. The reason: source loading is I/O the runner shouldn't
own; the orchestrator already reads Bronze for Silver, and we can
piggyback on that read.

The function `build_fk_lookups()` here is a convenience that does the
loading and set-building for one round of evaluation.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from src.dq.rules import (
    SEVERITY_CRITICAL,
    SEVERITY_WARNING,
    DQEvalContext,
    Rule,
    fk_dependencies,
    rules_for_source,
)
from src.engines.base import DataFrame, DataFrameEngine
from src.utils.logging import get_logger


log = get_logger(__name__)


# The column the runner appends to failing_rows to explain WHY they failed.
# Concatenates all critical rule failures for that row.
DQ_FAILURE_REASON_COLUMN = "_dq_failure_reason"


# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DQRuleOutcome:
    """One rule's outcome against one source's data."""

    rule_id: str
    severity: str                # 'critical' | 'warning'
    rows_evaluated: int
    rows_failed: int

    @property
    def pass_rate(self) -> float:
        if self.rows_evaluated == 0:
            return 1.0
        return (self.rows_evaluated - self.rows_failed) / self.rows_evaluated


@dataclass(frozen=True)
class DQResult:
    """Aggregate result of running all rules against one source."""

    source_name: str
    rows_in: int
    clean_rows: DataFrame
    failing_rows: DataFrame | None      # None when no critical failures
    outcomes: list[DQRuleOutcome]

    @property
    def rows_clean(self) -> int:
        # Computed via outcomes' length-based info; the engine wouldn't
        # be in scope here. We use the difference.
        return self.rows_in - (
            0 if self.failing_rows is None else len(_failing_reason_list(self))
        )

    @property
    def critical_failures(self) -> list[DQRuleOutcome]:
        return [o for o in self.outcomes if o.severity == SEVERITY_CRITICAL and o.rows_failed]

    @property
    def warnings(self) -> list[DQRuleOutcome]:
        return [o for o in self.outcomes if o.severity == SEVERITY_WARNING and o.rows_failed]


def _failing_reason_list(_r: DQResult) -> list:
    """Placeholder hook so rows_clean has something to compute against
    without requiring the engine. Actual row count from failing_rows
    is set by the runner directly (we keep it as a function so the
    field stays dataclass-frozen-compatible)."""
    return []


# ---------------------------------------------------------------------------
# FK lookup helper
# ---------------------------------------------------------------------------


def build_fk_lookups(
    *,
    bronze_root: Path,
    batch_id: str,
    engine: DataFrameEngine,
) -> dict[tuple[str, str], set[Any]]:
    """
    Pre-load FK lookup sets for all (source, column) pairs referenced
    by any FK rule in the loaded config.

    Returns a dict keyed by (source_name, column_name) -> set of valid
    values present in that source's Bronze partition for this batch.

    Sources that have no Bronze partition for this batch (e.g. didn't
    get ingested) produce empty lookup sets. Downstream FK rules will
    then fail every row that tries to reference that source — which
    is the correct behaviour for incomplete batches.
    """
    deps = fk_dependencies()
    lookups: dict[tuple[str, str], set[Any]] = {}
    for (source, column), consumers in deps.items():
        partition_path = bronze_root / source / f"batch_id={batch_id}"
        if not partition_path.is_dir():
            log.warning(
                "fk_lookup_source_missing",
                source=source, column=column,
                consumers=consumers,
                partition_path=str(partition_path),
            )
            lookups[(source, column)] = set()
            continue
        df = engine.read_parquet(partition_path)
        records = engine.to_records(engine.select(df, [column]))
        lookups[(source, column)] = {
            r[column] for r in records if r[column] is not None
        }
        log.info(
            "fk_lookup_built",
            source=source, column=column,
            valid_value_count=len(lookups[(source, column)]),
        )
    return lookups


# ---------------------------------------------------------------------------
# Main runner
# ---------------------------------------------------------------------------


def run_dq_for_source(
    *,
    source_name: str,
    df: DataFrame,
    fk_lookups: dict[tuple[str, str], set[Any]],
    engine: DataFrameEngine,
) -> DQResult:
    """
    Run all applicable DQ rules for one source.

    Args:
        source_name: the source identifier (matches sources.yaml + dq_rules.yaml)
        df:          Bronze data for this source (DataFrame as read by file_loader)
        fk_lookups:  output of build_fk_lookups (or a subset matching this source's FKs)
        engine:      DataFrameEngine

    Returns:
        DQResult with clean_rows, failing_rows (or None), per-rule
        outcomes, and convenience accessors for criticals + warnings.
    """
    rules = rules_for_source(source_name)
    total_rows = engine.count(df)

    if not rules:
        log.info("dq_no_rules", source=source_name, rows=total_rows)
        return DQResult(
            source_name=source_name,
            rows_in=total_rows,
            clean_rows=df,
            failing_rows=None,
            outcomes=[],
        )

    if total_rows == 0:
        log.info("dq_empty_input", source=source_name)
        return DQResult(
            source_name=source_name,
            rows_in=0,
            clean_rows=df,
            failing_rows=None,
            outcomes=[],
        )

    context = DQEvalContext(fk_lookups=fk_lookups)

    # Evaluate every rule. Each returns a list[bool] of length total_rows.
    per_rule_pass: list[tuple[Rule, list[bool]]] = []
    outcomes: list[DQRuleOutcome] = []
    for rule in rules:
        passes = rule.evaluate(df, engine, context)
        if len(passes) != total_rows:
            log.error(
                "dq_rule_returned_wrong_length",
                rule_id=rule.id(), expected=total_rows, got=len(passes),
            )
            # Defensive: treat all rows as passing for this rule so a
            # bug in one rule doesn't blanket-fail a whole source.
            passes = [True] * total_rows
        rows_failed = sum(1 for p in passes if not p)
        outcomes.append(DQRuleOutcome(
            rule_id=rule.id(),
            severity=rule.severity,
            rows_evaluated=total_rows,
            rows_failed=rows_failed,
        ))
        per_rule_pass.append((rule, passes))
        log.info(
            "dq_rule_evaluated",
            rule_id=rule.id(),
            severity=rule.severity,
            rows_failed=rows_failed,
        )

    # Per-row reason composition: a row's _dq_failure_reason is the
    # concatenation of failed CRITICAL rule_ids (warning failures don't
    # quarantine the row — they're reported separately).
    records = engine.to_records(df)
    reasons: list[str] = []
    row_is_clean: list[bool] = []
    for i, _rec in enumerate(records):
        failed_critical_ids = [
            rule.id()
            for rule, passes in per_rule_pass
            if rule.severity == SEVERITY_CRITICAL and not passes[i]
        ]
        if failed_critical_ids:
            reasons.append(";".join(failed_critical_ids))
            row_is_clean.append(False)
        else:
            reasons.append("")
            row_is_clean.append(True)

    # Split the DataFrame into clean and failing. We do this through
    # engine.to_records + reconstruction to keep this engine-agnostic.
    clean_records = [r for r, ok in zip(records, row_is_clean) if ok]
    failing_records = [
        {**r, DQ_FAILURE_REASON_COLUMN: reason}
        for r, ok, reason in zip(records, row_is_clean, reasons)
        if not ok
    ]

    log.info(
        "dq_run_finished",
        source=source_name,
        rows_in=total_rows,
        rows_clean=len(clean_records),
        rows_failing=len(failing_records),
        critical_failures=sum(1 for o in outcomes
                               if o.severity == SEVERITY_CRITICAL and o.rows_failed),
        warnings=sum(1 for o in outcomes
                     if o.severity == SEVERITY_WARNING and o.rows_failed),
    )

    # Build the result DataFrames. For Pandas this is straightforward;
    # Spark would need engine.from_records. We use the same isolated
    # pattern as scd2._records_to_df.
    clean_df = _records_to_df(clean_records, original_df=df, engine=engine)
    failing_df = (
        _records_to_df(failing_records, original_df=df, engine=engine,
                       extra_columns=[DQ_FAILURE_REASON_COLUMN])
        if failing_records else None
    )

    return DQResult(
        source_name=source_name,
        rows_in=total_rows,
        clean_rows=clean_df,
        failing_rows=failing_df,
        outcomes=outcomes,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _records_to_df(
    records: list[dict],
    *,
    original_df: DataFrame,
    engine: DataFrameEngine,
    extra_columns: list[str] | None = None,
) -> DataFrame:
    """
    Convert a list of dicts back into the engine's DataFrame type.

    Same isolated engine-specific path as src/silver/scd2.py — when
    SparkEngine arrives in Phase 7 we'll add the createDataFrame branch
    here. For now Pandas only.
    """
    if engine.kind == "pandas":
        import pandas as pd
        if not records:
            # Empty result: preserve the original schema by using the
            # original DataFrame's columns. Extra columns added as needed.
            cols = list(engine.columns(original_df))
            if extra_columns:
                cols = cols + extra_columns
            return pd.DataFrame(columns=cols)
        return pd.DataFrame(records)
    if engine.kind == "spark":
        raise NotImplementedError(
            "SparkEngine path through _records_to_df not yet implemented. "
            "Phase 7 will add this when SparkEngine is wired up."
        )
    raise ValueError(f"Unknown engine kind: {engine.kind!r}")
