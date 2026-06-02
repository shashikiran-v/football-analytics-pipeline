# ADR-0006: Data Quality Framework Design

## Status

Accepted — 2026-06-02

## Context

The brief calls for a configurable Data Quality framework with explicit
required rule types (§7 Data Quality):

> - Schema validation (column names, types, nullability)
> - Range checks (e.g. minutes_played ≥ 0)
> - Referential integrity (FK relationships across sources)
> - Duplicate detection
> - Configurable rule definitions
> - DQ report output (per batch)

Several non-trivial sub-decisions arise within "build DQ":

1. **Where does DQ sit in the pipeline?** Before Silver as a gate?
   After Silver as a validator? Parallel to Silver as a flagger?
2. **How are rules expressed?** Inline Python, declarative YAML,
   or a DSL?
3. **What happens to failing rows?** Drop, flag, or quarantine?
4. **How are severities handled?** Single severity, multiple tiers,
   per-source configuration?
5. **How do FK rules behave when the reference source is missing?**
   Fail open or fail closed?
6. **What's the audit attribution for quarantine?**

Each has a tempting wrong answer that compounds over time. Phase 4
documents the reasoning chain.

## Decision

### DQ as a gate between Bronze and Silver

`run_dq_for_source` runs against each Bronze source's data BEFORE
any Silver builder consumes it. Failing rows never reach Silver —
they're quarantined to `_rejected/` and the clean subset flows
through to dims and facts.

The Silver runner's flow:

```
read_bronze(source)
  → run_dq_for_source(source, bronze_df, fk_lookups)
    → if failures: quarantine_rejected_rows + audit.record_quarantine
  → clean_bronze[source] = result.clean_rows
... after all sources ...
  → write_dq_report
  → mark_transforming(source)
  → builders consume clean_bronze[source]
```

The key property is **Silver is clean by construction**. Downstream
queries don't need defensive filtering because the orphan and
schema-violating rows never reached the dim/fact tables. `fact_appearances`
has 29 rows, not 30; every row has a resolved `player_sk`.

The brief explicitly mentions `_rejected/` as the quarantine pattern
in §7, which signalled the gate position was the intended interpretation.

### Declarative YAML rules with bundled rule implementations

Rules live in `configs/dq_rules.yaml` as data, not code. Each rule has:

- `source`: which Bronze source it applies to
- `rule_type`: one of `not_null`, `range`, `unique`, `foreign_key`, `schema`
- type-specific parameters (e.g. `columns` for not_null, `min`/`max` for range)
- `severity`: `critical` or `warning`
- optional `description` for human context

The Python framework provides typed Pydantic implementations of each
rule type with a `Field(discriminator="rule_type")` discriminated union
in the loader. New rules of existing types are YAML edits; new rule
*types* are Python additions to `src/dq/rules.py`.

This is the same framework-style argument from ADR-0002 (Source Registry
as a Framework): declarative configuration over imperative code, with
type safety enforced at load time so YAML typos fail loud at startup,
not silently at runtime.

### Quarantine to `_rejected/` with `_dq_failure_reason` column

Failing rows are written to:

```
data/lake/_rejected/<source_name>/batch_id=<batch_id>/<part>.parquet
```

Same Hive-partitioned layout as Bronze and Silver. The `batch_id`
column is plain (not `_batch_id`) — same convention from ADR-0003
that avoids pyarrow's hidden-file treatment of underscore-prefixed
partition columns.

Each row carries a `_dq_failure_reason` column composed by concatenating
the IDs of the critical rules it failed:

```
foreign_key:appearances.player_id->players.player_id;foreign_key:appearances.game_id->games.game_id
```

Reviewers and operators can `cat data/lake/_rejected/appearances/`
and see exactly which rows failed which rules. The audit story is
visible on disk, not buried in logs.

### Severity tiers: critical quarantines, warning flags

Each rule declares `severity: critical | warning`:

- **Critical** failures quarantine the row. The row never reaches
  Silver. The audit DAO's `rejected_row_count` is updated.
- **Warning** failures pass the row through to Silver. The warning
  is logged with the rule ID and the failing row count, and recorded
  in the per-batch DQ report.

This matches ADR-0001's reconciliation tiers (WARN vs CRITICAL):
two levels of severity, both fully captured in the audit trail,
with different downstream consequences. The dual-severity model
gives reviewers and operators the right information without
forcing them to choose between "everything is critical" and
"everything is informational."

The senior judgement here: not every check is gate-worthy. Range
violations on `minutes_played` (out-of-bounds value) should land
in a report for a data engineer to investigate, but probably
shouldn't block the whole match's stats from reaching Silver.
FK violations on `player_id` (orphan reference) should block —
they break the dimensional model. The severity tier encodes this
judgement declaratively.

### Source-grain audit attribution for quarantine

`audit.record_quarantine(batch_id, source_name, rejected_row_count)`
attributes rejected rows to the source they came from, matching the
source-grain attribution from ADR-0001 (Audit Table Design) and
ADR-0005 (SCD2 fact attribution).

This means the `file_audit` row for `appearances` ends up with:

```
bronze_row_count = 30
rejected_row_count = 1
silver_row_count = 29
```

The math reconciles: `bronze - rejected = silver`. The reconciliation
rules from Phase 2a (`row_count_drift`, etc.) pass against this row
because the math is honest.

### FK fail-open semantics on missing lookups

If `build_fk_lookups` can't find the reference source's Bronze
partition for the current batch (e.g. the referenced source was never
ingested), the FK rule logs an error and **passes all rows**. We do
NOT fail closed (quarantine every row that uses that FK column).

The reasoning: failing closed would create thousands of false-positive
quarantines for what's actually a config gap. An operator investigating
"why did appearances quarantine 30 rows today?" would have to grep
through logs to find the actual cause. Failing open + a clear error
log keeps the operator in control.

Implementation: `ForeignKeyRule.evaluate` returns `[True] * len(df)`
when `context.fk_lookups[(ref_source, ref_column)]` is absent, and
emits a `fk_lookup_missing` error event with the rule ID and the
missing key.

## Consequences

**Gained:**

- **Brief's §7 requirements fully covered:** schema validation,
  range checks, FK rules, duplicate detection, configurable rules,
  and per-batch DQ report output.
- **Silver is clean by construction.** The dim/fact tables can be
  trusted without downstream filtering. `fact_appearances.player_sk`
  is never null (it was, before this phase; it isn't now).
- **The orphan story has a permanent home.** `player_id=9999` is in
  `_rejected/appearances/` with a precise failure reason, not lost
  in logs or buried under a NULL FK.
- **Reviewable rule configuration.** Adding a "minutes_played ≥ 0"
  rule for a new source is a YAML edit. Adding a new rule type
  (e.g. regex-match) is a Python addition. Both are scoped contributions
  a reviewer can approve.
- **Audit-honest reconciliation.** `bronze_row_count - rejected_row_count
  = silver_row_count` holds at source grain. The reconciliation rules
  pass.
- **Per-batch JSON report at `data/dq_reports/<batch_id>.json`.** Downstream
  tooling (Airflow tasks, Superset dashboards, on-call investigations)
  can parse the report rather than scraping logs.

**Given up:**

- **No row-level type validation in SchemaRule v1.** Column existence
  is checked, but per-row type coercion is trusted to the file_loader
  layer (Phase 2b). Adding type-checking to the rule would require
  introspecting the engine's type system per row, which is more
  complex than its current value justifies. Documented as a future
  enhancement when type drift becomes a real risk.
- **FK lookup sets loaded fully into memory once per batch.** Fine
  for our scale (Kaggle: ~30K players, ~150K games). For hyperscale
  warehouses we'd switch to engine-native range joins (Spark
  broadcast joins, or a temp table with an indexed FK). Same caveat
  as ADR-0005's in-memory version index for SCD2 as-of joins.
- **No DQ-driven pipeline halt.** Even if 100% of rows fail critical
  rules, the Silver runner continues (with zero rows in clean_bronze).
  Pipelines that need a hard fail on too-high-rejection-rate should
  add a downstream check on the DQ report's `total_rows_quarantined`.
  Phase 8's Airflow DAG is the natural home for such a check.
- **Discriminated union union-types lock-in.** New rule types require
  editing both `src/dq/rules.py` (the union definition) and adding
  a new model. A more flexible approach would have been a plugin
  registry pattern, but the union gives static type safety and
  IDE autocomplete which is worth more for this codebase size.

## Alternatives considered

### DQ in parallel to Silver

Run DQ alongside Silver instead of as a gate. Silver processes
everything; DQ produces a report independently.

**Rejected because:** the brief's `_rejected/` pattern in §7
implies a gate. More importantly, parallel processing means Silver
contains bad data — the orphan reaches `fact_appearances` with a
NULL `player_sk`, downstream queries need defensive filtering, and
the "trusted by construction" property is lost. The gate approach
makes Silver's correctness verifiable.

### DQ after Silver

Run DQ against Silver dims and facts to verify correctness.

**Rejected because:** it shifts DQ from prevention to detection. By
the time DQ runs, the bad data is already in Silver and any
downstream consumer that read Silver early might have used it.
Prevention is the right model for a data warehouse.

### Inline Python rules

Each rule is a Python function. New rules = code change.

**Rejected because:** the brief specifies "configurable rule
definitions" (§7). YAML over code means rule additions don't need
Python review. Operations teams without Python expertise can update
the rules.

### Generic DSL expressions

YAML expressions like `minutes_played >= 0`. Maximum flexibility.

**Rejected because:** of complexity. A DSL parser would need to
handle operators, function calls, type coercion, error messages
for malformed expressions, and engine-portability across Pandas
and Spark. The bundled rule types cover the brief's required cases
with type safety and clear error messages. A DSL would be a
substantial additional codebase for marginal value.

### Silent drop of failing rows

Failing rows are removed; no `_rejected/` directory.

**Rejected because:** it destroys the audit trail. A row that's
silently dropped is a row that can't be investigated. The brief's
emphasis on lineage and auditability means rejected rows must be
inspectable, not invisible.

### Flag column on Silver rows

All rows reach Silver with a `_dq_status` column; downstream queries
filter.

**Rejected because:** it mixes clean and dirty data in the same
table, which downstream queries must remember to filter. The
"trusted by construction" property is lost. Some shops use this
pattern legitimately when their semantic contract is "every Bronze
row maps 1:1 to a Silver row" — we don't have that constraint, so
the gate pattern is cleaner.

### Tiered hybrid (some quarantine, some flag)

Hard failures quarantine; soft warnings flag in Silver via
`_dq_status`.

**Considered seriously and partially adopted.** We have severity
tiers (critical vs warning), but warnings DON'T add a `_dq_status`
column to Silver — they're reported in the per-batch DQ report
and logged. This keeps Silver's schema stable and the DQ findings
in their proper home (the report). If a warning needs to be
addressable downstream, it's promoted to critical in the YAML.

### Single severity

Every rule fails-blocks. No "warning" tier.

**Rejected because:** of the senior-judgement argument above. Not
every check is gate-worthy. `minutes_played > 120` is probably a
data-entry typo but doesn't justify quarantining the whole match's
appearance. The severity tier encodes this judgement declaratively.

### FK fail closed

If the lookup source is missing, fail every row that uses that FK.

**Rejected because:** it creates false-positive quarantines for
config gaps. An operator investigating "why did appearances
quarantine 30 rows today?" would have to grep through logs to find
the actual cause was a missing players ingestion. Failing open with
a clear error log keeps the operator in control.

### Joint-grain audit attribution

Quarantine of `fact_appearances` rows updates audit rows for
`appearances`, `players`, and `games`.

**Rejected for the same reasons as ADR-0005:** it pollutes the
status of audit rows for sources whose Bronze data was perfectly
fine. Source-grain attribution is honest about which Bronze source
had the bad row.

## See also

- Implementation:
  - `configs/dq_rules.yaml` (27 declarative rules across 6 sources)
  - `src/dq/rules.py` (5 typed Pydantic rule classes + discriminated
    union loader)
  - `src/dq/runner.py` (`run_dq_for_source`, `build_fk_lookups`,
    `DQResult` type)
  - `src/dq/quarantine.py` (Hive-partitioned writer for failing rows)
  - `src/dq/report.py` (typed batch report, JSON serialiser)
  - `src/silver/run.py` (DQ pass integration)
- Tests:
  - `tests/test_dq_rules.py` (21 rule-type tests covering happy paths,
    null/NaN handling, type mismatches, composite keys, fail-open
    semantics)
  - `tests/test_dq_runner.py` (11 runner + quarantine tests, including
    the orphan-FK end-to-end story)
  - `tests/test_dq_report.py` (9 report builder + serialiser tests)
  - `tests/test_silver_run.py` (15 runner tests including
    `TestDQIntegration` — 6 tests for the integrated flow)
- Related:
  - ADR-0001 (Audit Table Design) — source-grain attribution and
    reconciliation rules
  - ADR-0002 (Source Registry as a Framework) — the framework-style
    argument for declarative YAML over imperative code
  - ADR-0005 (SCD2 Implementation) — fact→dim resolution that benefits
    from DQ's clean Bronze guarantee
