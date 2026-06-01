# ADR-0002: Source Registry as a Framework

## Status

Accepted — 2026-05-31

## Context

The Kaggle Football dataset has six source tables: competitions, clubs,
players, games, appearances, player_valuations. A naïve pipeline hard-codes
references to all six in ingestion code, DQ checks, SCD2 config, and
PII rules — typically scattered across five or six Python files.

Two costs follow from that approach:

1. **Engineering cost per new dataset.** Onboarding a seventh table means
   editing every place the existing six are mentioned. In a real shop
   this is a multi-day exercise across multiple PRs.
2. **Reviewer comprehension cost.** Understanding "what does this pipeline
   handle?" requires reading code rather than configuration. The list
   of sources isn't a fact you can look up; it's a fact you derive by
   grepping.

The brief explicitly asks for *"adding a new dataset should require
minimal code changes"* (§12 Code Quality & Modularity). That's a
direct request for a framework, not a hardcoded list.

## Decision

We define every source declaratively in `configs/sources.yaml` and load
it into typed `SourceDefinition` objects via `src/ingestion/registry.py`.
Every downstream module (Bronze, DQ, Silver, Gold, DAG) consults the
registry rather than maintaining its own list.

### Self-contained per source

Each source's full configuration — schema, primary key, SCD2 settings,
PII settings, incremental settings, audit thresholds — lives in **one
YAML block**. A new joiner reading the `players` source needs to look
at exactly one place to understand everything about it.

This contrasts with the alternative of spreading concerns across files
(`sources.yaml` for shape, `scd2.yaml` for change tracking, `pii.yaml`
for anonymisation). That separation reads as more "principled" but adds
navigation cost for the reader. For a single-developer codebase, the
self-contained form wins on comprehension.

### Typed loading with pydantic

The loader parses the YAML through pydantic models with `extra='forbid'`.
A typo in the YAML (`tracket_columns` instead of `tracked_columns`,
or a misspelled type tag) raises `ValidationError` at startup — not a
silent bug at runtime. The earliest failure is the cheapest failure.

### Filtered views

`SourceRegistry` exposes `.scd2_sources()`, `.pii_sources()`,
`.incremental_sources()`. Consumers ask the registry for the slice they
need rather than filtering manually at every call site. This keeps
business code free of conditional plumbing:

```python
# Without filtered views
for source in registry.all_sources():
    if source.scd2 is not None:
        process_scd2(source)

# With filtered views
for source in registry.scd2_sources():
    process_scd2(source)
```

### Versioned schema

The YAML carries `version: 1` at the top. When the registry shape
evolves in a backwards-incompatible way, we bump the version. The
loader warns on mismatch rather than failing — gives operators time to
migrate. Not enforced today, but the hook is in place.

### `path_pattern` instead of fixed paths

Each source declares `path_pattern: "{raw_root}/players.csv"`, not a
resolved path. The registry doesn't know what `raw_root` is at load
time. The Bronze layer calls `source.resolve_path(raw_root)` when it's
about to read the file, plugging in whichever raw-root is appropriate
for the current run (day1, day2, sample, or future paths). This keeps
the registry environment-agnostic.

## Consequences

**Gained:**

- **Adding a new dataset is a YAML edit.** No Python changes required
  for ingestion, DQ, or SCD/PII setup. The cost story for the
  interview: *engineering cost per new pipeline goes to ~minutes,
  not days.*
- **The pipeline self-documents.** `cat configs/sources.yaml` is a
  complete inventory of what the pipeline handles, with all relevant
  settings visible in one read.
- **Misconfiguration fails loudly at startup.** Unknown fields,
  missing required fields, invalid format strings all raise pydantic
  ValidationError before any data is touched.
- **Filtered views remove conditional plumbing.** Downstream code
  expresses intent ("process the SCD2 sources") rather than mechanics
  ("loop through all, skip those without scd2 set").
- **Pluggable per-source audit thresholds.** `expected_min_rows` and
  similar per-source tunables can be set in YAML without code changes.

**Given up:**

- **Single point of failure.** A malformed `sources.yaml` blocks the
  entire pipeline. Mitigated by the pydantic validation surfacing
  errors with clear messages, and by `tests/test_registry.py` 's 26
  tests covering happy paths and 7 distinct error scenarios.
- **One more file for new joiners to read.** Mitigated by it being
  self-documenting; reading `sources.yaml` literally tells you what
  the pipeline does.

## Alternatives considered

### Hardcoded lists in each module

`bronze.py` has a `TABLES = ["competitions", "clubs", ...]` constant;
`dq.py` has its own table list with the checks; `silver/dimensions.py`
has the SCD2-specific list.

**Rejected because** of the engineering-cost story above. The brief is
explicit about wanting framework-grade extensibility.

### Single Python module declaring sources as objects

Replace `sources.yaml` with `src/ingestion/sources.py` containing
`SOURCES = [SourceDefinition(name="players", ...), ...]`.

**Rejected because** YAML is more reviewable. Configuration changes
shouldn't require Python expertise; a junior analyst could (in
principle) add a new source to the registry without writing code.
YAML diffs in PRs are also more readable than Python diffs.

### Separate files per concern (sources.yaml + scd2.yaml + pii.yaml)

Decompose by concern: ingestion shape in one file, SCD2 rules in
another, PII rules in a third.

**Rejected because** of comprehension cost: a reader investigating
"what does the pipeline do with `players`?" would have to consult
three files and join the data mentally. For a single-developer
codebase the self-contained form is more legible.

This decision is **reversible without breaking changes** — if the
registry ever grows past ~30 sources or the cross-cutting concerns
get gnarly enough to justify the separation, we can split the YAML
and update the loader without changing the consumer API.

### Declarative ingestion via DBT-like manifest

Use DBT (or similar) to drive the pipeline.

**Rejected because** DBT is a SQL-first tool optimised for
in-warehouse transforms, not file-to-lakehouse ETL. We'd be using a
fraction of its capability and accepting a heavy dependency. The
custom registry is ~200 lines of code and solves exactly our problem.

### Pydantic without YAML (config as code)

Define sources directly in Python via pydantic models, skip YAML
parsing entirely.

**Rejected because** of the runtime override story. We want
`PIPELINE_ENGINE=spark python -m bronze.run` to swap behaviour
without code changes. YAML + env-var interpolation gives us that;
pure Python config does not.

## See also

- Implementation: `src/ingestion/registry.py`, `configs/sources.yaml`
- Tests: `tests/test_registry.py` (26 tests covering bundled-YAML
  contracts, lookup API, filtered views, and seven distinct error paths)
- Related: ADR-0001 (Audit Table Design) — the registry's
  `SourceDefinition` carries the schema that produces the
  `schema_version_hash` consumed by the audit DAO.
