# ADR-0009: Spark Engine Scope and Stub Design

## Status

Accepted — 2026-06-03

## Context

The brief (§1) says:

> Implement using either Python (Pandas/PyArrow) or PySpark, or both
> with abstraction.

We need a single, defensible answer to this question. "Both, but only
one is real" is the answer this ADR articulates.

Three sub-questions arise:

1. **Which engine should be the production implementation?**
2. **Should the other engine be built at all?**
3. **If we build one as a stub, what does that stub look like — and
   how do we keep "stub" from sliding into "shell that nobody could
   ever build on top of"?**

Phase 1 established the engine abstraction (`src/engines/base.py`)
as a protocol that both engines satisfy. Phase 7 forces the real
decision about what lives behind the protocol.

## Decision

### Pandas is the production engine

Three reasons:

**1. The data scale matches Pandas's sweet spot.** The Kaggle Player
Scores dataset is ~9 GB across six CSVs. The full Bronze→Silver→Gold
pipeline against the full dataset completes in ~60 seconds on a
mid-spec laptop. Pandas with PyArrow as the parquet engine is the
right tool for this — no JVM startup, no cluster management, no
serialisation overhead between worker processes.

**2. Pandas iterates faster during development.** Every code change is
visible in the next `pytest tests/` run (~30 seconds). A Spark
equivalent would mean every iteration restarts the JVM, deserialises
schemas, and starts a SparkSession. For a development pipeline,
fast feedback dominates.

**3. The brief's testing requirements favour Pandas.** Unit-testing a
Spark pipeline requires `spark-testing-base` or hand-rolled
fixtures with SparkSession lifecycle management. Pandas tests are
just `pytest` against DataFrames. We have 414 passing tests today;
porting them to Spark would be a project in itself.

### A Spark engine stub IS built, deliberately

This is the more interesting half of the decision. We don't drop the
abstraction or pretend the Spark engine doesn't exist. We build it as
a stub that:

- Implements the full `DataFrameEngine` protocol
- Raises `NotImplementedError` on every operation
- Avoids `import pyspark` (zero install dependency)
- Routes through the factory just like the Pandas engine
- Has tests proving the factory wiring works for both

The stub demonstrates the abstraction is genuinely contracted — not
aspirational scaffolding. A user can switch to the Spark engine TODAY
via `engine: spark` in `configs/config.yaml` (or `PIPELINE_ENGINE=spark`)
and the factory will produce a `SparkEngine` instance. The instance
just refuses to do work, with a clear pointer to this ADR.

### What a real Spark implementation would look like

The stub explicitly does NOT scaffold a real implementation — a
production Spark engine would be a clean rewrite of
`src/engines/spark_engine.py`, not an extension of the stub. Sketching
the design here makes the cost estimate honest:

**I/O methods (`read_csv`, `read_parquet`, `write_parquet`):**
Thin wrappers over Spark's DataFrameReader / DataFrameWriter. Spark
already handles partition pruning, predicate pushdown, and parallel
reads natively. ~30 lines, half a day's work.

**Row-level operations (`filter_eq`, `filter_isin`, `filter_not_null`,
`filter_range`):**
Each translates to a `df.filter(F.col(c) == v)` style expression.
Straightforward. ~50 lines.

**Column derivations (`with_constant_column`, `with_derived_column`,
`with_row_hash`):**
`with_constant_column` is `df.withColumn(name, F.lit(value))`.
`with_derived_column` is the awkward one — Spark UDFs are slow and
require schema declaration. A production implementation would pass
the callable through `pandas_udf` for vectorised performance. The
row-hash function needs careful attention: it must produce
byte-identical hashes to the Pandas implementation (using the same
`\u241F` separator and `<NULL>` sentinel) so SCD2 dimensions don't
spuriously thrash when an operator switches engines mid-pipeline.
~150 lines, two days of careful testing.

**Joins, sets, aggregation (`join`, `union`, `distinct`, `group_by_agg`):**
Spark's native idioms. `group_by_agg` is cleanest as Spark SQL
because the dynamic agg dict translates to SQL more naturally
than to the DataFrame DSL. ~80 lines, one day.

**Rolling average (`rolling_avg`):**
A Spark SQL window function with `ROWS BETWEEN N PRECEDING AND
CURRENT ROW`. Identical to what we already wrote in DuckDB for the
`player_valuation_rolling_avg` Gold artifact (see ADR-0007). One day.

**`to_records` and partition-aware overwrites:**
`.collect()` for to_records (with a row-count guard against accidental
full-table collects). Partition-aware writes go through
`mode("overwrite").option("partitionOverwriteMode", "dynamic")` — a
direct equivalent of the fix from ADR-0008 Slice 6.2. Half a day.

**Total: ~2 weeks of focused engineering.** That's a real number, not
a YAGNI handwave. It buys nothing at our current scale. It would buy
real value at three thresholds:

1. **Data scale crosses ~50 GB.** Pandas starts hitting memory walls;
   Spark's lazy evaluation and disk spill earn their keep.
2. **Deployment moves to managed clusters (EMR, Databricks).** Production
   ops want one engine across all workloads.
3. **Multi-tenant shared infrastructure.** Spark's resource scheduling
   handles many concurrent jobs better than process-isolated Pandas
   workers.

## Consequences

**Gained:**

- **One engine implemented well** rather than two implemented partially.
  Quality density: the Pandas engine is genuinely production-grade,
  not a sketch.
- **The abstraction proven real.** A reviewer can verify the protocol
  works for both engines today — only the Spark body is missing. This
  is architectural foresight made concrete.
- **A defined upgrade path.** Adding Spark fully is a known-scope task
  (one file, ~2 weeks) that doesn't require codebase reshape. The
  cost is named, not hidden.
- **Fast iteration during build.** 414 tests run in ~75 seconds via
  Pandas. A Spark-first codebase would be 5-10x slower per iteration.
- **No PySpark in `requirements.txt`.** Lower install surface, fewer
  version-conflict pain points for users running the pipeline.

**Given up:**

- **No demonstrated Spark capability at production.** A reviewer
  who wants to see "PySpark code that does real work" won't find it
  here. We chose to argue the case in this ADR rather than build the
  evidence half-way.
- **The stub is dead code in the runtime sense.** Every method is
  unreachable. We accept this because the *protocol surface* the stub
  declares IS the live contract — adding a new method to the base
  protocol forces a declaration on both engines, and `pytest` catches
  the gap before runtime.
- **The abstraction has ongoing maintenance cost.** Every new
  `DataFrameEngine` method costs ~10 lines (one in Pandas, one stub
  in Spark). The cost is real but small; the alternative (no
  abstraction) would leave us with no migration path at all.
- **Some operations are Pandas-shaped in ways that won't translate
  cleanly.** `filter_predicate` accepts an arbitrary Python callable,
  which is a Spark UDF — slow on Spark. Documented in the protocol's
  docstring; a real Spark implementation would route the callable
  through `pandas_udf` to recover performance.

## Alternatives considered

### Build both engines fully

Implement Pandas AND Spark to production quality, with a CI matrix
running tests against both.

**Rejected because** the cost is ~2 weeks for the Spark engine plus
ongoing dual maintenance. The brief's data scale doesn't justify it.
A "we built Spark too" line on the README would be a vanity signal
masking unnecessary work.

### Build Spark only

Skip Pandas entirely. Treat the brief's "or both" as a single
production-grade choice and commit to Spark.

**Rejected because** development iteration speed dominates for a
solo-built portfolio project. A Spark-first codebase would mean
30-second test runs become 5-minute test runs. The 414-test suite
we have would be impractical to maintain at Spark speeds.

### Drop the abstraction entirely

Build Pandas directly with no protocol layer. Save the ~50 lines of
`base.py` and the ~30 lines of the factory. The codebase becomes
slightly simpler.

**Rejected because** the abstraction is cheap and the optionality is
genuine. The ~80 lines bought us:
- A clean place to put the Spark stub (proving architectural
  thinking without committing to implementation cost)
- A consistent calling surface for `with_row_hash`, `rolling_avg`,
  and the other operations that benefit from a deliberate API
- A natural seam for future engines (DuckDB-as-engine, Polars, etc.)
  if the project's scope expands

### Build the Spark stub more thoroughly (skeleton with imports)

Construct a `SparkSession` in `__init__`, validate connectivity,
implement a single method (e.g. `read_parquet`) as a proof of concept.
Each unimplemented method raises NotImplementedError with a TODO.

**Rejected because** it would create a misleading impression of "this
is almost done, just one or two methods to go." The honest signal is
that none of it is done — the design is clear, the implementation
cost is named, and someone wanting to build it should treat the stub
as a starting point for a fresh implementation rather than a
half-completed effort.

### Add `pyspark` to `requirements.txt` for the stub

Make PySpark a declared dependency even though the stub doesn't use
it, so the development environment is "ready" for someone to start
implementation.

**Rejected because** it imposes the JVM startup cost and ~400 MB of
install footprint on every user, including those who never touch
the Spark engine. A future contributor implementing Spark properly
would add the dependency at that point.

## See also

- Implementation:
  - `src/engines/spark_engine.py` (the stub itself, with extensive
    docstring explaining the engineering position)
  - `src/engines/base.py` (the `DataFrameEngine` protocol both
    engines satisfy)
  - `src/engines/factory.py` (dispatch logic — already wired for
    both engines, lazy-imports Spark)
  - `src/engines/pandas_engine.py` (the production implementation)
- Tests:
  - `tests/test_spark_engine_stub.py` (7 tests: factory wiring +
    stub behaviour + zero-dependency enforcement)
- Related:
  - ADR-0007 (Gold Layer Storage and Analytics) — the DuckDB
    rolling-average and as-of-event window patterns that a future
    Spark engine would re-implement in Spark SQL
  - ADR-0008 (Cross-Batch Semantics) — the partition-aware
    overwrite fix that's currently Pandas-only and would need a
    Spark equivalent (`partitionOverwriteMode=dynamic`)
