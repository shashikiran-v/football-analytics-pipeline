# ADR-0010: Airflow Orchestration Design

## Status

Accepted — 2026-06-03

## Context

The brief (§8) calls for orchestration. The pipeline's three CLI
commands (`python -m src.bronze.run`, `python -m src.silver.run`,
`python -m src.gold.run`) need to be wired into a scheduled DAG with
correct failure semantics, idempotency wiring, and a DQ hard-fail gate
deferred from ADR-0006.

Five design sub-questions arise:

1. **What's the deployment posture?** Functional LocalExecutor demo,
   or production-style CeleryExecutor with separate scheduler/worker?
2. **How are the CLI runners wrapped as Airflow tasks?**
3. **How does Airflow's run context map to our `batch_id` semantics?**
4. **Where does the DQ hard-fail gate live, and what threshold triggers it?**
5. **What dependencies and install posture does Airflow add to the project?**

Phase 8 also surfaced **two latent bugs** in the cross-batch resolver
(ADR-0008) that no prior phase had exercised. The bugs and their fixes
are documented here as part of the orchestration story, because the
mechanism that triggered them — heterogeneous batch_id formats from
`data_interval_start` colliding with previously-named test batches — is
fundamentally an orchestration concern.

## Decision

### LocalExecutor for the deliverable; CeleryExecutor as the production path

The DAG runs against Airflow's `LocalExecutor` — single-process,
sequential task execution. `airflow standalone` brings up scheduler +
webserver + worker as one process. This is sufficient to:

- Parse the DAG
- Run all four tasks (Bronze → Silver → DQ gate → Gold) end-to-end
- Demonstrate retry semantics, layer idempotency, XCom-based summaries

For production, `CeleryExecutor` (or its KubernetesExecutor cousin) is
the natural upgrade. The DAG file itself doesn't change; only the
deployment topology does. The argument is the same scope-discipline
move as ADR-0009: build the orchestration model fully, articulate the
production upgrade explicitly, don't pay the operational overhead of
distributed execution at a scale that doesn't need it.

### PythonOperator + thin wrapper module

The runners in `src/{bronze,silver,gold}/run.py` have NO Airflow
imports. They accept a `batch_id` string and return dataclass summaries.
This isolation is preserved: a runner can be invoked from a CLI, a
notebook, a test, or Airflow without coupling.

`src/orchestration/airflow_wrappers.py` is the only module that knows
about Airflow. Four functions — `run_bronze_task`, `run_silver_task`,
`dq_gate_task`, `run_gold_task` — each:

1. Derive `batch_id` from Airflow's context dict
2. Call the underlying runner
3. Translate the result to a summary dict (returned for XCom)
4. Raise `AirflowException` if the runner reports failure, so Airflow
   marks the task FAILED rather than SUCCESS

Even Airflow itself is imported lazily inside each function — the
module-level import line is `from src.bronze.run import run_bronze`,
not `from airflow.exceptions import AirflowException`. This keeps
tests for the wrapper functions executable without Airflow installed
(they skip cleanly via `pytest.importorskip`).

### `data_interval_start` → `batch_id` (ISO date format)

The wrapper derives `batch_id` from Airflow's `data_interval_start`,
formatted as `YYYY-MM-DD`. For the default `@daily` schedule this
gives one batch per scheduled day, with `batch_id` values like
`2026-06-02`, `2026-06-03`, etc.

This choice has two consequences worth naming:

1. **Idempotency comes for free.** Same `data_interval_start` →
   same `batch_id` → the runners' layer-grain idempotency takes over.
   Re-running a DAG run against a `batch_id` that already succeeded
   is a no-op via `pipeline_runs`. Tested and demonstrated in Phase 8.

2. **Batch_id format consistency matters.** Mixing
   `2026-06-02` (Airflow-derived) and `day-1` (manual-test-derived)
   batch_ids in the same metadata DB is what surfaced both resolver
   bugs documented below. The audit DAO must order/filter audit rows
   by `registered_at` timestamps (which are always ISO-8601), never
   by `batch_id` string (which has no enforced format).

   For production, we recommend standardising on the ISO date or
   timestamp format from day one. The pipeline supports any
   batch_id string, but operators benefit from a single naming
   convention.

### DQ gate as a separate task, proportional fail

ADR-0006 deferred the "Airflow task should fail on
`total_rows_quarantined > N`" decision. It now lands as a separate
`dq_gate` task between Silver and Gold.

Two reasons it's a separate task, not baked into Silver:

- **Clear failure modes.** Silver always succeeds when its artifacts
  wrote successfully; the gate independently decides whether to let
  Gold run based on DQ outcomes. A reviewer reading "task silver
  succeeded; task dq_gate failed" understands the situation in one
  glance.
- **Configurability per environment.** The threshold can be tuned at
  the DAG level (`op_kwargs={"quarantine_threshold_pct": 5.0}`) without
  changing Silver's behaviour. Dev environments might use 50%; prod
  might use 1%.

The gate's failure condition is **proportional**:

```
fail when:  critical_failures > 0 AND quarantine_pct > threshold
```

Both conditions must hold. The reasoning:

- A handful of orphan rows in a 10-million-row table shouldn't kill
  the daily pipeline. Realistic data has noise; the pipeline should
  tolerate it within bounds.
- Widespread DQ failure indicates a real upstream problem (schema
  change, source corruption, ingestion bug) and should stop downstream
  consumers from seeing partial data.
- The 5% default threshold reflects "this is more than acceptable
  noise" without being so strict as to false-alarm on natural variation.

### Airflow is an optional dependency

`apache-airflow` is NOT in `requirements.txt`. It's listed separately
in `requirements-airflow.txt` with the Apache-published constraints
file URL. Installing Airflow without those constraints almost always
causes transitive dependency conflicts (Flask, Werkzeug, SQLAlchemy
versions matter).

Same reasoning as Spark in ADR-0009: most users running the pipeline
don't need Airflow. Tests for the orchestration layer skip cleanly
when Airflow isn't importable.

## Two bugs surfaced and fixed

Phase 8's Airflow integration triggered the cross-batch resolver
(ADR-0008) against a heterogeneous batch_id format —
`data_interval_start` produces `2026-06-02` against an audit DAO
populated with `day-1` and `day-2` rows from earlier CLI-based
testing. Two latent bugs surfaced:

### Bug 1: lexicographic batch_id comparison in audit DAO

`find_most_recent_ingestion_for_source` used `WHERE batch_id <= ?`
in SQL — a string comparison. ASCII `d` (0x64) sorts higher than `2`
(0x32), so `"day-1" <= "2026-06-02"` evaluates `False`. The query
filtered OUT valid prior audit rows whenever batch_ids crossed format
boundaries.

**Fix:** Order and filter by `registered_at` (an ISO-8601 UTC
timestamp produced by `_utcnow()`). Lexicographic ordering on ISO
timestamps matches chronological ordering by construction. Two-step
query: look up the as_of batch's `registered_at`, then filter audit
rows by `registered_at <= as_of_ts`.

### Bug 2: resolver only walked back one step

The resolver looked up "most-recent prior batch via audit DAO,"
checked that batch's partition on disk, and returned `None` if
absent. That handles one file-grain skip but not chains: day-1 wrote
the bytes; day-2 skipped (matching day-1 checksum); 2026-06-02
skipped again (matching day-2 checksum, which itself matched day-1).
The resolver found day-2 as "most recent prior," saw its partition
was missing, gave up.

**Fix:** Chain-following via `_find_all_batches_with_checksum` which
returns a list of all batches with the matching checksum, ordered
most-recent-first. The resolver iterates through candidates and
returns the first one whose partition exists on disk. Handles
chains of arbitrary depth.

### Pattern: every multi-batch phase finds latent bugs

Phase 6's day-2 testing found two bugs (the resolver's existence
gap and the destructive parquet writer). Phase 8's Airflow
integration found two more. **This is the pattern of integration
testing surfacing edge cases that unit testing missed** — and a
strong argument for end-to-end testing at every architectural
boundary the codebase adds.

The pipeline now has 9 dedicated tests in `tests/test_bronze_resolver.py`
covering single-batch, two-batch, and chained-batch scenarios.
Future engineers extending the resolver have a regression net.

## Consequences

**Gained:**

- **The brief's §8 orchestration requirement is satisfied** with a
  runnable DAG a reviewer can launch in under 5 minutes via
  `airflow standalone`.
- **Idempotent re-triggers proven end-to-end** through Airflow.
  Re-triggering a successful DAG run is a no-op via the
  `pipeline_runs` table, with all four tasks correctly reporting
  "skipped." Production-grade re-run semantics.
- **DQ hard-fail gate now exists** with proportional fail semantics
  (the ADR-0006 deferred decision landed).
- **Runners remain pure.** No Airflow imports in `src/bronze/run.py`,
  `src/silver/run.py`, `src/gold/run.py`. The wrapper module is the
  only Airflow-aware code; everything else works in any context.
- **Two latent resolver bugs caught and fixed**, with regression
  tests pinning the behaviour.
- **Honest scope choice articulated.** LocalExecutor is the
  deliverable; CeleryExecutor is the named production upgrade path.

**Given up:**

- **Single-machine execution only.** LocalExecutor doesn't scale
  across multiple workers. At the brief's data scale this is fine;
  for production at multi-TB scale we'd need CeleryExecutor or
  KubernetesExecutor.
- **No proper Airflow connections/variables yet.** The DAG uses
  `op_kwargs` for the `raw_root` and `quarantine_threshold_pct`
  values. Production would put these in Airflow Variables or
  Connections with environment-specific overrides.
- **Backfill story is implicit.** The DAG has `catchup=False`. To
  re-run a historical date the operator would use
  `airflow dags backfill ... --start-date ... --end-date ...`
  which works because the runners are idempotent, but the workflow
  isn't documented in this ADR.
- **Email/Slack notifications not wired.** `default_args` has
  `email_on_failure=False`. Production would wire a callback to
  Slack or PagerDuty for critical-task failures. Deferred to a
  future phase.

## Alternatives considered

### CeleryExecutor with separate scheduler/worker containers

Wire up Redis, a scheduler container, a webserver container, one or
more worker containers, and demonstrate distributed task execution.

**Rejected because** it's substantially more infrastructure (5
containers vs 1) for no functional gain at the brief's scale. The
upgrade path is named explicitly so a reviewer understands the
decision is "we deliberately chose not to demo distributed
execution" rather than "we don't know how distributed Airflow works."

### BashOperator with `python -m src.bronze.run`

Wrap the runners as shell commands rather than Python callables.
Closer to how a developer runs them locally; no wrapper module
needed.

**Rejected because** BashOperator surfaces subprocess errors as
opaque exit codes, loses Python-level introspection of return
values (no XCom-friendly summary dicts), and forces all logging
through stdout/stderr capture rather than Airflow's structured
logging. PythonOperator + lazy import is a cleaner separation.

### Bake the DQ gate into Silver

Have Silver raise on its own DQ violations rather than relying on
a downstream task.

**Rejected because** it conflates two concerns. Silver's job is
"build the artifacts"; the gate's job is "decide whether downstream
should consume them." Mixing these makes Silver harder to test
(you'd need to mock the gate logic) and makes the failure mode
ambiguous ("did Silver fail because the transforms broke, or
because too many rows were quarantined?"). The separate-task
design gives clear failure attribution.

### Strict DQ gate (fail on any critical failure)

Fail the gate whenever `critical_failures > 0`, regardless of
quarantine percentage.

**Rejected because** real data has noise. Our sample data has 1
deliberate orphan in 30 appearances (~3.3%) — that's a critical
failure but well within tolerable noise. A strict gate would
fail every daily run for a single bad row. The proportional
threshold is the right tradeoff.

### Catch-up enabled

Allow Airflow to backfill from `start_date` automatically when the
DAG is first unpaused.

**Rejected because** the demo state has accumulated test batches
(`day-1`, `day-2`) that wouldn't make sense in a catchup window.
For production, explicit backfills via `airflow dags backfill`
are preferable — operators see exactly which dates are running
rather than triggering N historical runs by toggling a switch.

## See also

- Implementation:
  - `dags/football_pipeline.py` (the DAG file at repo root)
  - `src/orchestration/airflow_wrappers.py` (the wrapper functions)
  - `src/orchestration/__init__.py` (package marker)
  - `src/dq/report.py` (`read_dq_report` helper for the gate)
  - `requirements-airflow.txt` (optional Airflow dependency)
- Tests:
  - `tests/test_airflow_wrappers.py` (9 wrapper-behaviour tests)
  - `tests/test_airflow_dag.py` (9 DAG-structure tests)
  - `tests/test_bronze_resolver.py::TestChainOfFileGrainSkips` (3
    regression tests for the bugs documented above)
- Related:
  - ADR-0001 (Audit Table Design) — `pipeline_runs` table that
    powers layer-grain idempotency
  - ADR-0003 (Bronze Storage and Partitioning) — file-grain
    idempotency that the chain-following resolver navigates
  - ADR-0006 (DQ Framework Design) — the gate-threshold decision
    deferred from there and landed here
  - ADR-0008 (Cross-Batch Semantics) — the resolver design that
    Slice 8.1b corrected with chain-following and registered_at
    ordering
  - ADR-0009 (Spark Engine Scope and Stub Design) — the same
    scope-discipline pattern (build a stub, articulate the
    production upgrade, defend the choice)
