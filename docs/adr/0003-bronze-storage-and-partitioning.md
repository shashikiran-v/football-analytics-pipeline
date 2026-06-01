# ADR-0003: Bronze Layer Storage and Partitioning

## Status

Accepted — 2026-06-01

## Context

Bronze is the raw landing zone for source data. The brief calls for
"raw ingestion with minimal transformations" stored as Parquet. That
leaves several non-trivial decisions:

- **What format on disk?** Single file per source, partitioned dataset,
  one of the lakehouse table formats (Delta, Iceberg, Hudi)?
- **What partition key, and what naming convention?** Date? Batch ID?
  Source-derived? Prefixed?
- **What does "raw" mean in practice?** Strictly the source bytes, or
  is appending bookkeeping columns acceptable?
- **How is idempotency expressed?** At what grain — batch, layer, file?
- **What happens when one source in a batch fails?** Abort the batch, or
  continue and let the audit reflect the partial failure?

Each of these decisions interacts with downstream layers. Picking
poorly here costs us in Silver, Gold, DQ, and the DAG.

## Decision

### Format: Hive-partitioned Parquet, no table format yet

Bronze stores each source as a Hive-partitioned Parquet dataset:

```
data/lake/bronze/
├── players/
│   ├── batch_id=2026-06-01T15/
│   │   └── <hash>-0.parquet
│   ├── batch_id=2026-06-01T16/
│   │   └── <hash>-0.parquet
│   └── ...
├── games/
│   └── batch_id=.../
└── ...
```

We deliberately do NOT use Delta/Iceberg/Hudi at this stage. Reasons:

- The brief asks for Parquet specifically (§3 Medallion Architecture).
- Plain Parquet + Hive partitioning is fully supported by pyarrow,
  DuckDB, Spark, and every modern query engine without extra dependencies.
- The lakehouse table formats solve problems we don't have yet
  (concurrent writes, ACID guarantees across hundreds of files). The
  metadata DB gives us per-batch idempotency without needing transactional
  Parquet.

This decision is **reversible**. Migrating from raw Parquet to Delta
later is a write-path change in `engine.write_parquet` and a tooling
addition; downstream readers using the engine abstraction would not
change.

### Partition key: `batch_id` (no underscore prefix)

The single bookkeeping column appended to Bronze is `batch_id`. It is
also the partition key. We do NOT prefix it with an underscore.

**This is a non-obvious decision worth flagging.** The naïve choice
would be `_batch_id` — underscore-prefixed to mark it as pipeline
metadata distinct from source columns. We tried that. It silently broke
partition discovery, because:

- pyarrow's `read_table` skips paths beginning with `_` by default
- Spark's partition discovery skips `_*` and `.*` entries (the
  `_SUCCESS` / `_metadata` convention from Hadoop)
- Hive itself treats underscore-prefixed paths as hidden

Using `_batch_id` produced parquet files at the correct paths but made
them invisible to any reader that didn't override the hidden-file
behaviour. The smoke tests caught this immediately; the partition test
failed with "0 rows read back". Renaming to plain `batch_id` resolved it.

We document this loudly in `src/bronze/writer.py` so future-us doesn't
re-introduce the bug.

### Partition granularity: hourly by default, configurable

`config.batch.granularity` controls how the default batch_id is derived
when the caller doesn't supply one:

- `hourly` → `2026-06-01T15` (default)
- `daily`  → `2026-06-01`

The brief asks for "configurable frequency, default hourly" (§5 Delta /
Incremental Load). Hourly is the default; daily is one config edit away;
weekly would be a new entry in the granularity enum. The Kaggle dataset
updates weekly in practice — running hourly against it just means most
runs find unchanged checksums and skip cleanly. That's a feature, not a
bug: it proves the idempotency works.

### What goes in the Parquet file

The Bronze Parquet contains:
1. The source's columns, exactly as the file loader returned them
   (schema-coerced to the registry's declared types)
2. One appended column: `batch_id`

No derived fields, no normalisation, no renames, no PII hashing. Those
all live downstream:

- DQ runs against Bronze data (Phase 4)
- Position/country normalisation happens in Silver (Phase 3)
- PII hashing happens at the Bronze→Silver transition (Phase 10)

This keeps the "raw" contract honest. A reviewer reading a Bronze
parquet sees what the vendor sent, full stop.

### Idempotency layered at two grains

Bronze enforces idempotency at two levels, and they compose:

**Layer-grain** (in `run.py`, backed by `pipeline_runs` from Phase 1):
- Before processing anything, check if `(batch_id, "bronze")` already
  succeeded.
- If yes, the entire layer is a no-op. Re-running a completed batch
  changes nothing.

**File-grain** (in `writer.py`, backed by `file_audit` from Phase 2a):
- For each source, look up whether the file's MD5 has *ever* been
  ingested successfully in any prior batch.
- If yes, register this batch's audit row but short-circuit the write.
  The prior partition's data is already on disk; nothing needs writing.
- The audit row for this batch still transitions through the lifecycle,
  so the timeline remains honest about what happened.

The two mechanisms compose:
- Repeat batch_id → layer-grain fires, no source-level checks even run
- Fresh batch_id with unchanged files → every source individually skipped
- Fresh batch_id, one file changed → that source writes, others skip

### Writer never raises; runner continues on failure

`write_bronze_source` is contractually never-raise. Failures are
captured in `BronzeWriteResult.status='failed'` and in the audit DAO.

The runner aggregates per-source results. A failing source does NOT
abort the batch; the runner continues with the next source. The batch
ends in `failed` status overall only if at least one source failed.

This mirrors how production batch jobs actually behave. Real vendors
sometimes deliver one bad file in a multi-file drop; aborting the
batch punishes the other sources for no reason. Better to land what
landed cleanly, surface the failures loudly, and let humans (or a
retry mechanism) handle the bad ones.

## Consequences

**Gained:**

- Time-travel queries on Bronze become a one-line `WHERE batch_id = ...`
  in any Parquet reader. No table-format dependency required.
- Idempotency proven by tests at both grains. Re-running batches is
  safe; recovery from partial failure is just re-running the same DAG.
- The "raw" contract is enforceable by inspection — a reviewer can
  diff a Bronze parquet against the source CSV and verify byte-level
  fidelity (modulo type coercion and the one appended column).
- Per-source failure semantics: one missing file doesn't propagate
  into a 12-source outage.
- The partition layout works with any modern Parquet-aware tool: DuckDB
  for Phase 5's Gold queries, Spark for Phase 7's optional engine,
  Superset for Phase 10's dashboards.

**Given up:**

- No ACID writes. If two processes wrote to the same partition
  simultaneously they could corrupt each other. We accept this because
  Airflow's LocalExecutor serialises tasks within a DAG run and our
  CLI runner is single-process. Migrating to Delta later would close
  this gap.
- No native schema evolution. If the source schema changes
  meaningfully between batches, the parquet files in older partitions
  have the old schema. A consumer reading across partitions has to
  reconcile. Mitigated by the schema_version_hash recorded in the
  audit DAO — drift is detected and the schema_drift_detected event
  fires, even if Bronze itself doesn't resolve it.
- No partition compaction. Long-running pipelines will accumulate many
  small Parquet files (one per batch per source). For our row volumes
  this is harmless; for production we'd add a periodic compaction job
  in a later phase.

## Alternatives considered

### Single Parquet file per source, no partitioning

`data/lake/bronze/players.parquet`, overwritten each batch.

**Rejected because** every re-run destroys the previous batch's data
on disk. No time-travel queries are possible. Re-running the same
batch is "safe" but only because the data is identical; partial
failures mid-write would corrupt the file.

### Underscore-prefixed `_batch_id`

The conventional naming for "pipeline metadata" columns.

**Rejected because** pyarrow / Spark / Hive all treat underscore-prefixed
paths as hidden. Smoke testing during Phase 2b development surfaced
this immediately when partition discovery returned zero rows. The
naming convention saves a sentence in code comments but costs
ecosystem-wide compatibility. Not worth it.

### Delta Lake from day one

Use the `delta-spark` or `deltalake` Python packages to write Delta
tables instead of plain Parquet.

**Rejected for v1** because:
1. The brief specifies Parquet (§3).
2. Adds ~30 MB of dependencies (deltalake) or a Spark cluster
   (delta-spark).
3. We don't yet have the problem Delta solves (concurrent writers,
   ACID, time-travel guarantees beyond what partitioning gives us).
4. Migrating later is contained to the engine layer's write path.

Worth reconsidering if Phase 7's Spark work proves we want Delta's
ACID guarantees for multi-writer concurrency.

### Source-defined partition keys

Partition by `season` for facts, `country` for clubs, etc. — using
domain-specific columns as the partition key.

**Rejected because** domain partitioning is a *Silver / Gold* concern,
not a Bronze concern. Bronze is the raw landing zone; its purpose is
to preserve fidelity to the source and enable replay by batch.
Domain partitioning gets applied downstream where the access patterns
are known. Pre-applying it in Bronze would couple ingestion to query
patterns.

### Hard-fail the whole batch on first source error

Standard "fail-fast" semantics: first failure aborts everything.

**Rejected because** the assessment's brief includes idempotency,
retries, and structured logging (§9 Airflow Orchestration). All of
these point toward partial-failure resilience. A batch that ran 5 of
6 sources successfully should be re-runnable to pick up just the 6th,
not require re-running the 5 successful ones.

Continue-on-failure is also more honest from an audit perspective:
the file_audit table shows exactly which sources succeeded, failed,
and were skipped, with timestamps and error stages. Fail-fast hides
this — you only know about the first failure.

## See also

- Implementation: `src/bronze/writer.py`, `src/bronze/run.py`
- Tests: `tests/test_bronze.py` (18 tests covering writer per-source,
  runner per-batch, both layers of idempotency, partition layout on
  disk, never-raise contract, continue-on-failure semantics)
- Related ADRs:
  - ADR-0001 (Audit Table Design) — `file_audit` powers file-grain
    idempotency
  - ADR-0002 (Source Registry as a Framework) — drives the per-source
    iteration in `run.py`
- Operational guide: README "Running the pipeline" section
