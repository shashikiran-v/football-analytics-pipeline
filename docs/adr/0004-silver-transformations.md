# ADR-0004: Silver Transformation Strategy

## Status

Accepted — 2026-06-01

## Context

The Silver layer is where Bronze's "faithful to source" data becomes
business-ready data. The brief calls for four specific transformations
(position normalisation, country ISO, match outcome, season), but the
real decision is about the *shape* of how transformations are applied
across the codebase. Three independent questions arose:

1. **Where does the engine abstraction stop?** Transformations operate
   on individual values. Pandas and PySpark diverge significantly on
   row-level operations — Pandas does `.apply()`, PySpark uses UDFs
   that are slow on big data unless vectorised. Do we go engine-agnostic
   or per-engine optimised?

2. **Where does reference data live?** Position taxonomy and country
   mappings are reference data. Hardcoded in Python? YAML? CSV? Some
   combination?

3. **How does the transformation layer handle bad input?** Vendors
   send unrecognised position labels, malformed countries, missing
   dates. Does the transform raise (forcing every caller to handle it)
   or absorb the badness with a sentinel value?

Each of these has a sensible-looking wrong answer that compounds over
time. Worth being deliberate.

## Decision

### Engine-agnostic via `with_derived_column`

Every transform is a pure Python function taking a value (or a small
dict of values) and returning a value. The engine's
`with_derived_column(df, name, fn, input_columns)` applies it
uniformly: Pandas via `.apply()`, PySpark (when added) via a UDF.

This means the transformations themselves contain zero
engine-specific code. The dim/fact builders call
`engine.with_derived_column` — the transformation appears identically
on both engines.

We accept the performance cost. For our scale (Kaggle dataset is ~2GB
unzipped; our committed samples are 50 rows) UDF overhead is irrelevant.
If Spark performance ever became a constraint, the transformations are
already in a single module — refactoring them to vectorised Spark
expressions would be a contained change. Premature optimisation now
would couple business logic to engine internals.

### Reference data in YAML, lazily loaded

Position taxonomy and country mappings live in:

```
configs/position_taxonomy.yaml
configs/country_iso.yaml
```

Loaded once per process via `@lru_cache(maxsize=1)`. New entries are
YAML edits, not code changes. Reviewable in PRs as YAML diffs.

The position taxonomy is a two-level structure:

```yaml
GK:
  canonical: Goalkeeper
  category: goalkeeper
```

`canonical` is what gets stored in `dim_players.position_canonical`;
`category` is a coarse grouping for analytics (used in Gold).

Country normalisation uses a hybrid approach:
1. **Overrides YAML first**: handles vendor-specific variants like
   `"England, United Kingdom" → "GB"` that pycountry doesn't recognise
2. **pycountry fallback**: handles the ~250 standard forms (alpha-2,
   alpha-3, official names, common names)
3. **Sentinel last**: returns `"XX"` for anything unknown

This split keeps the YAML small (we only override what pycountry can't
handle) while ensuring all real-world variants resolve correctly.

### Additive normalisation columns

When we normalise, we *add* a new column rather than overwriting the
original. `position` (raw from vendor) stays alongside `position_canonical`
(normalised). Country of citizenship has both the raw string and
`country_of_citizenship_iso`.

This matters for two reasons:

1. **Analytical flexibility.** Downstream consumers can choose to
   group by canonical labels (analytics) or join on raw values (vendor
   lineage / debugging).
2. **Auditability.** When a reviewer sees `position_canonical = "Unknown"`,
   the original `position` column tells them what the vendor sent —
   essential for triaging mapping gaps.

The cost is a few extra bytes per row. Trivial.

### Sentinel values for unknown input; transforms never raise

Every transform tolerates None / NaN / empty / unrecognised input and
returns a sentinel:

- `normalise_position` returns `PositionMapping("Unknown", "Unknown")`
- `normalise_country` returns `"XX"` (ISO 3166-1 reserves XX for
  user-defined codes)
- `derive_match_outcome` returns `"unknown"`
- `derive_season` returns `None`

This means the Silver builder never has to wrap calls in try/except,
and the audit DAO has visibility into which rows had bad inputs
(because they're in Silver with sentinels, not silently dropped or
crashed-on). DQ in Phase 4 decides what to do about the sentinels.

If transforms raised, the failure mode would be: one bad row crashes
the entire batch, the audit DAO records "transformation failed" with
no further detail, and the operator has to grep logs to find the
offending row. That's the wrong abstraction.

## Consequences

**Gained:**

- Adding a position variant or country mapping is a YAML edit — no code
  change required. New joiners can update reference data without
  Python expertise.
- Transformations work identically on Pandas and (when added) Spark
  because they're pure Python functions, not engine-specific operations.
- Bad input never breaks the pipeline. A vendor sending unrecognised
  position `"Sweeper"` results in a Silver row with `position = "Sweeper"`
  and `position_canonical = "Unknown"`. The DQ task can flag this; the
  pipeline continues.
- The original vendor values stay queryable in Silver alongside the
  normalised ones — useful for lineage debugging.

**Given up:**

- UDF overhead on Spark. For our scale this is irrelevant; for
  multi-billion-row workloads we'd refactor specific transforms to
  vectorised Spark expressions. The module boundary makes this
  contained.
- The `"XX"` sentinel for country means downstream queries grouping by
  country will have an extra bucket. Documented; not a problem because
  the bucket size reveals our mapping gap.
- One extra column per normalised attribute. Storage cost: negligible.

## Alternatives considered

### Per-engine optimised paths

Pandas vectorises position normalisation via `.map(taxonomy_dict)`;
Spark uses `when().otherwise()` chains. Faster, but duplicates business
logic across engines.

**Rejected because** the abstraction has paid off in Bronze and Silver
already; breaking it for a perf gain we don't need would be premature
optimisation. If Phase 7's Spark engine arrives and benchmarks show
this is the bottleneck, refactoring is contained to one module.

### Hardcoded Python dicts

Define the position taxonomy as a Python constant in
`transforms.py`. Simplest. No YAML loader needed.

**Rejected because** the brief specifically asks for "configurable
position taxonomy" (§4 Schema Standardization). YAML over code means
PRs editing the taxonomy don't need Python review; the diff reads as
data, not code.

### CSV instead of YAML for reference data

Slightly more "data-like" feel.

**Rejected because** YAML handles the two-level structure
(canonical + category) more naturally; CSV would need either
joining two files or a wide format with type encoded in a column.
For ~30 entries, the YAML weight is right.

### Raise on unknown values

Force every transformation caller to handle bad input explicitly.

**Rejected because** of the failure-mode argument above. One bad
vendor row should not cascade into a batch-wide failure. The sentinel
approach is honest (the value is preserved, the canonical is
"Unknown"), the audit is informative, and DQ owns the policy decision.

### Pycountry only, no overrides

Skip the YAML overrides; rely entirely on pycountry's built-in lookup.

**Rejected because** pycountry doesn't recognise the messy real-world
strings the Kaggle vendor sends (`"England, United Kingdom"`,
`"Korea, South"`, etc.). The overrides YAML solves the gap with
~15 entries.

## See also

- Implementation: `src/silver/transforms.py`
- Reference data: `configs/position_taxonomy.yaml`,
  `configs/country_iso.yaml`
- Tests: `tests/test_silver_transforms.py` (52 tests across the four
  transforms, including edge cases for null/empty/NaN and the
  deliberate sample variants — `"GK"`/`"Goalkeeper"`,
  `"England, United Kingdom"`)
- Related: ADR-0002 (Source Registry as a Framework) — the registry's
  per-source config drives which transforms apply where
