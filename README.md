# Football Analytics Pipeline

A production-grade, modular ETL pipeline over the Kaggle Player Scores
dataset, built around a Medallion architecture (Bronze → Silver → Gold)
with full Airflow orchestration, Docker packaging, SCD Type 2 historical
tracking, and a Pandas/PySpark dual-engine abstraction selectable via
config.

> **Status: under active development.** This README will fill out as
> phases land. The build is incremental and every phase ships passing
> tests; see [Build progress](#build-progress) below.

---

## Why this design

The brief asks for a list of capabilities. The interesting question is
*how those capabilities compose into something a senior engineer would
actually ship*. Three decisions drive the architecture:

1. **The engine choice is a runtime config, not a code rewrite.**
   A `DataFrameEngine` protocol fronts both Pandas and PySpark
   implementations. Transformations import the protocol, never the
   underlying library. Swapping engines is one line in `config.yaml`.

2. **Idempotency is enforced by the metadata DB, not by hope.**
   Every layer consults `pipeline_runs` before doing work and writes
   its outcome on completion. Re-running a successful batch is a no-op;
   re-running a failed batch resumes from the failed task.

3. **DQ failures quarantine, not crash.**
   ERROR-severity DQ failures route bad rows to `_rejected/` parquet
   and let the clean rows continue downstream. The DQ report is a
   first-class artefact, queryable from SQLite, not a stack trace.

The full architectural rationale lands in this README in Phase 10
(documentation polish). For now, the [Build progress](#build-progress)
section below tracks what's implemented.

---

## Tech stack

| Layer            | Choice                                          |
| ---------------- | ----------------------------------------------- |
| Language         | Python 3.11                                     |
| Compute engines  | Pandas 2.2 **and** PySpark 3.5 (config switch)  |
| Storage format   | Parquet (via PyArrow) across all layers         |
| Query layer      | DuckDB over the Gold parquet                    |
| Metadata         | SQLite (run state, DQ results, SCD watermarks)  |
| Orchestration    | Apache Airflow 2.9, LocalExecutor               |
| BI               | Apache Superset                                 |
| Containerisation | Docker + docker-compose                         |
| CI               | GitHub Actions (pytest + ruff + mypy)           |

---

## Repository layout

```
project/
├── configs/            # config.yaml (engine, paths, DQ behaviour, ...)
├── dags/               # Airflow DAGs (Phase 8)
├── src/
│   ├── engines/        # DataFrameEngine protocol + Pandas/Spark impls
│   ├── ingestion/      # Source loaders (Phase 2)
│   ├── bronze/         # Raw partitioned Parquet writes (Phase 2)
│   ├── silver/         # Star-schema transforms + SCD2 (Phase 3)
│   ├── gold/           # Business aggregations (Phase 5)
│   ├── dq/             # Data-quality checks + quarantine (Phase 4)
│   ├── pii/            # Salted hashing anonymiser (Phase 10)
│   ├── metadata/       # SQLite DAOs: runs, dq_results, watermarks
│   ├── models/         # Pydantic table schemas
│   └── utils/          # Config, structured logging, row hashing
├── tests/              # pytest suites, engine-parametrised
├── scripts/            # Day-2 mutation generator, query helpers
├── data/               # Lake root (mostly gitignored; sample/ committed)
└── docker/             # Dockerfiles (Phase 9)
```

---

## Build progress

| Phase | Scope                                              | Status   |
| ----- | -------------------------------------------------- | -------- |
| 1     | Foundation: config, logging, metadata, engine abstraction + Pandas | ✅ Done |
| 2a    | Source registry framework + audit infrastructure + ADRs | ✅ Done |
| 2b    | Bronze ingestion + sample data + Kaggle manifest    | ✅ Done  |
| 3     | Silver: transforms, star-schema dimensions, SCD Type 2 | ✅ Done  |
| 4     | DQ framework + quarantine + report                 | ✅ Done  |
| 5     | Gold aggregations + DuckDB views                   | ✅ Done  |
| 6     | Day-2 incremental snapshot + SCD2 validation       | ✅ Done  |
| 7     | Spark engine: stub + design doc *(deliberate scope choice, see ADR-0009)* | ✅ Done |
| 8     | Airflow DAG + idempotency wiring                   | ✅ Done  |
| 9     | Docker + docker-compose stack                      | ✅ Done  |
| 10    | PII anonymisation, Superset, CI, README polish     | ⏳ Next  |

### What's in Phase 2a

- **Source registry** (`configs/sources.yaml` + `src/ingestion/registry.py`):
  declarative source definitions for all six Kaggle tables. Adding a new
  dataset is a YAML edit, not a code change. Filtered views
  (`scd2_sources`, `pii_sources`, `incremental_sources`) remove
  conditional plumbing from consumers.
- **File checksums** (`src/utils/checksums.py`): streaming MD5 of file
  contents (constant memory regardless of file size) plus a
  deterministic schema-version hash for drift detection.
- **Audit DAO** (`src/metadata/audit.py`): two-table audit infrastructure
  (mutating row + append-only event log), full file lifecycle from
  `register_file` through `record_silver_complete`, vendor/filesystem
  timestamp split, state-machine enforcement, mark_failed asymmetry,
  and a 6-rule reconciliation engine returning typed findings.
- **ADRs** (`docs/adr/`): Architecture Decision Records documenting the
  decisions that shaped both the registry framework and the audit
  design. See [docs/adr/0000-using-adrs.md](docs/adr/0000-using-adrs.md)
  for the index.

Total test count after Phase 2a: **98 passing** (engine 21, registry 26,
checksums 15, audit 36).

### What's in Phase 2b

- **Sample data** (`data/sample/*.csv`, `scripts/generate_samples.py`):
  six committed CSVs across all Kaggle sources, deterministically
  generated, with edge cases deliberately seeded — one orphan FK in
  appearances (for DQ to catch), position-label variants for the
  normaliser, country-name variants for the ISO normaliser, three
  SCD2-prone players for Phase 6's day-2 demo. Reviewers can run the
  pipeline out-of-the-box without a Kaggle account.
- **Kaggle fetcher** (`scripts/seed_kaggle.py`, `make seed`): downloads
  the full dataset via the Kaggle API and writes a `_manifest.json`
  carrying the dataset's `lastUpdated` timestamp — the authoritative
  vendor provenance flows through to the audit DAO.
- **Vendor manifest reader** (`src/ingestion/manifest.py`): typed,
  version-aware, returns None for "no manifest present" so the
  audit layer's vendor_timestamp_source = 'filesystem_only' path stays
  clean.
- **File loader** (`src/ingestion/file_loader.py`): single chokepoint
  that produces a typed `LoadResult` per source — engine-native
  DataFrame plus a `FileFingerprint` ready for `audit.register_file()`.
  Engine-agnostic; Bronze passes the engine through.
- **Bronze layer** (`src/bronze/writer.py`, `src/bronze/run.py`): writes
  Hive-partitioned Parquet to `data/lake/bronze/<source>/batch_id=<id>/`.
  Layer-grain idempotency (re-running a batch is a no-op) AND file-grain
  idempotency (vendor-resend detection across batches). Continue-on-
  failure semantics — one bad source doesn't abort the batch. Never-raise
  contract on the writer; failures captured in BronzeWriteResult.
- **ADR-0003** documents the Bronze design choices, including the
  underscore-prefix trap caught by smoke testing.

After Phase 2b, the pipeline produces real output for the first time.
You can `python -m src.bronze.run --batch-id $(date -u +%Y-%m-%dT%H)`
and watch six Parquet partitions appear on disk with full audit
lineage in `data/metadata.db`.

Total test count after Phase 2b: **170 passing, 1 skipped**
(adds: samples 13+1, manifest 12, file_loader 17, bronze 18).

### What's in Phase 3

- **Silver transformations** (`src/silver/transforms.py` + reference
  YAMLs): four pure functions implementing the brief's required
  normalisations — position to a defined taxonomy, country to ISO
  3166-1 alpha-2, match outcome from goals, football season from date.
  Engine-agnostic via `engine.with_derived_column`; tolerant of
  null/empty/unknown input (returns sentinels rather than raising).
  Reference data lives in `configs/position_taxonomy.yaml` and
  `configs/country_iso.yaml` — adding new variants is a YAML edit.
- **SCD Type 2 merge** (`src/silver/scd2.py`): hash-based 4-category
  merge engine (NEW / CHANGED / UNCHANGED / HISTORICAL). Engine-agnostic
  via `engine.with_row_hash`. Auto-incremented integer surrogate keys,
  allocated deterministically by natural-key sort order so replays
  yield identical state. The most consequential function in the
  codebase; 17 dedicated tests cover every category and the cardinal
  "historical rows never mutated" rule across three batch cycles.
- **Dimension builders** (`src/silver/dimensions.py`): four builders.
  Type-1 for clubs and competitions (latest snapshot, dedupe on
  natural key). Generated `dim_date` covering 2018–2030 with football
  season derivation. Type-2 `dim_players` driven by registry config —
  `natural_key` and `tracked_columns` come from `sources.yaml`, not
  hardcoded. First-run effective_date is FAR_PAST_DATE (`1900-01-01`)
  so as-of-event fact joins resolve for historical match dates.
- **Fact builders** (`src/silver/facts.py`): `fact_games` (with
  derived outcome, season, date_key) and `fact_appearances` with the
  **as-of-event FK resolution to dim_players** — the differentiating
  use of SCD2. For each appearance, finds the dim_players version
  whose `[effective_date, end_date]` window contains the match date.
  Validated by `test_as_of_event_picks_correct_version`: two dim
  versions for one player, matches on each side of the transfer
  resolve to the correct version.
- **Silver CLI runner** (`src/silver/run.py`): orchestrates Bronze reads,
  dim/fact builds, Silver writes, and audit DAO lifecycle through
  `mark_transforming` → `record_silver_complete`. Layer-grain
  idempotency, continue-on-failure semantics. After this layer, one
  command per layer produces the full pipeline:
  ```bash
  python -m src.bronze.run --batch-id demo-1 --raw-root data/sample
  python -m src.silver.run --batch-id demo-1
  ```
- **ADR-0004 and ADR-0005**: architectural records for the
  transformation strategy and SCD2 implementation. ADR-0005 in
  particular documents every consequential SCD2 decision with
  alternatives rejected — hash-based merge over column-by-column,
  integer surrogate keys over hash-based (with the trade-off
  acknowledged), as-of-event facts over always-current FK,
  source-grain audit attribution over joint or per-artifact.

After Phase 3, the pipeline produces business-ready dimensional data
with full SCD2 history and as-of-event fact joins. Real Parquet on
disk, real audit lineage in SQLite, ready for DQ (Phase 4) and Gold
aggregations (Phase 5).

Total test count after Phase 3: **283 passing, 1 skipped**
(adds: silver_transforms 52, scd2 17, dimensions 17, facts 12,
silver_run 15).

### What's in Phase 4

Phase 4 adds the Data Quality framework, sitting between Bronze and
Silver as a gate. Bad rows never reach Silver — they're quarantined
to `data/lake/_rejected/` with a precise failure reason. The deliberate
orphan `player_id=9999` (seeded back in Phase 2b) is now caught here
rather than reaching `fact_appearances` with a NULL surrogate key.

- **Declarative rules in YAML** (`configs/dq_rules.yaml`): 27 rules
  covering all five brief-mandated rule types (not-null, range,
  unique, foreign_key, schema) across the six sources. Each rule
  declares its severity (`critical` or `warning`); adding a new rule
  is a YAML edit.
- **Typed rule implementations** (`src/dq/rules.py`): five Pydantic
  rule classes with a discriminated union (`Field(discriminator="rule_type")`)
  for the YAML loader. New rule types are Python additions; new
  *instances* are pure YAML. The same framework-style argument from
  ADR-0002 applied to a different layer.
- **DQ runner** (`src/dq/runner.py`): `run_dq_for_source` orchestrates
  rule evaluation for one source. Pre-loads FK lookup sets via
  `build_fk_lookups` for O(1) per-row FK checks. Returns a typed
  `DQResult` with clean rows, failing rows, per-rule outcomes, and
  the `_dq_failure_reason` column composed from critical rule IDs.
- **Quarantine writer** (`src/dq/quarantine.py`): writes failing rows
  to `data/lake/_rejected/<source>/batch_id=<id>/` in the same
  Hive-partitioned format as Bronze and Silver. Reviewers can `cat`
  the directory to see exactly which rows failed which rules.
- **Per-batch JSON report** (`src/dq/report.py`): emits
  `data/dq_reports/<batch_id>.json` with batch summary plus per-source
  rule-level breakdown. The brief's §7 "DQ report per batch"
  requirement made tangible — operators and downstream tooling can
  parse the report rather than scraping logs.
- **Silver runner integration** (`src/silver/run.py`): DQ runs BEFORE
  any dim/fact builder consumes Bronze. Single Bronze read per source;
  clean data flows through to builders; quarantine + audit
  `record_quarantine` for failures. Continue-on-failure preserved.
- **ADR-0006**: documents every Phase 4 design decision with
  alternatives explicitly rejected — DQ-as-gate vs parallel vs after
  Silver; declarative YAML vs DSL vs inline Python; quarantine vs
  silent-drop vs flag-column; critical+warning vs single-severity;
  FK fail-open vs fail-closed.

After Phase 4, the pipeline produces dim/fact tables with NO orphan
FKs by construction. `fact_appearances` has 29 rows (not 30); the
orphan lives in `data/lake/_rejected/appearances/` with the failure
reason intact; `audit.list_batch_files` shows
`bronze=30, rejected=1, silver=29` for the appearances row. Math
reconciles; lineage is preserved; the brief's §7 requirements are
fully covered.

Total test count after Phase 4: **330 passing, 1 skipped**
(adds: dq_rules 21, dq_runner 11, dq_report 9, plus 6 dq_integration
in test_silver_run.py).

### What's in Phase 5

Phase 5 adds the Gold analytical layer with a hybrid storage strategy:
materialised Parquet artifacts (consistent with Bronze/Silver) PLUS
DuckDB views over them for interactive SQL querying. The brief's
five §6 analytical questions are all answered with materialised
Gold tables; each artifact's `gold_row_count` is recorded in the
audit DAO with full source-grain lineage.

- **DuckDB session** (`src/gold/duckdb_session.py`): in-memory
  connection with all Silver dim/fact tables AND Bronze
  `player_valuations` registered as queryable views. After session
  creation, Gold queries reference tables by name with no path
  resolution. Context-managed lifecycle so connections always close
  cleanly.
- **Typed Gold artifacts** (`src/gold/artifacts.py`): five Pydantic
  `GoldArtifact` constants, one per §6 question. SQL lives as
  multi-line Python strings (not YAML — SQL is code; typos should
  fail at import). Metadata around the SQL stays declarative
  (`name`, `sources`, `primary_source`, `description`). New
  artifacts = one constant + one append to `ALL_ARTIFACTS`.
- **The five artifacts:**
  * `top_scorers_by_season` (§6.1) — joins fact_appearances to
    dim_players via SCD2 `player_sk` (player's club AT THE TIME
    of the appearance).
  * `club_season_summary` (§6.2) — unions home + away perspectives
    of fact_games to compute per-club season totals
    (matches_played, wins/draws/losses, goals, points).
  * `top_players_all_time` (§6.3) — lifetime per-player aggregates
    with `goals_per_appearance` as a derived efficiency metric.
  * `player_valuation_rolling_avg` (§6.4) — **DuckDB window function
    showcase.** 90-day rolling AVG of market value via
    `AVG(...) OVER (PARTITION BY player_id ORDER BY date
    ROWS BETWEEN 89 PRECEDING AND CURRENT ROW)`. Reads
    `bronze_player_valuations` directly (no Silver layer per
    ADR-0005). SCD2 as-of-event join expressed in SQL via range
    predicate.
  * `club_performance_metrics` (§6.5) — per-club lifetime metrics
    including clean sheets, win rate, goals per game.
- **Gold builder** (`src/gold/builders.py`): `build_gold_artifact`
  executes SQL via DuckDB, appends `batch_id` partition column,
  writes Hive-partitioned Parquet to `data/lake/gold/<artifact>/`.
- **Gold CLI runner** (`src/gold/run.py`): parallel structure to
  Silver runner. Layer-grain idempotency, single DuckDB session,
  continue-on-failure per-artifact. Calls
  `audit.record_gold_complete` on each artifact's primary Bronze
  source.
- **Audit DAO extensions** (`src/metadata/audit.py`,
  `src/metadata/db.py`): added `gold_row_count` column,
  `record_gold_complete` function, `GOLD_FINISHED` event type, and
  `_apply_migrations` for forward-compatible schema updates.
  Crucially: Gold does NOT add a new lifecycle state — it's an
  analytical view, not a new stage. The `gold_row_count` is set
  without changing file status.
- **ADR-0007**: documents the hybrid storage strategy, DuckDB
  choice, SQL-as-code-not-YAML decision, source-grain audit
  attribution, no-new-lifecycle-state design, and the
  `player_valuations` Bronze→Gold direct lineage. Alternatives
  explicitly rejected.

After Phase 5, the pipeline runs end-to-end in three commands and
produces a complete source-grain lineage record:

```
source                bronze  rejected  silver    gold
appearances               30         1      29      12
clubs                      5         0       5       0
competitions               3         0       3       0
games                      6         0       6       5
player_valuations         18         0       0      18
players                   12         0      12       0
```

The `player_valuations` row shows the architecturally-deliberate
skip-Silver pattern honestly: `silver=0` because no Silver builder
exists, `gold=18` because Gold consumed it directly via the
DuckDB view.

Total test count after Phase 5: **377 passing, 1 skipped**
(adds: gold_duckdb 6, gold_artifacts 28, gold_run 13).

### What's in Phase 6

Phase 6 adds day-2 incremental processing and demonstrates that SCD2
works correctly across batches. The committed `data/sample/day2/`
directory contains a complete second vendor snapshot with deliberate
diffs that exercise every aspect of cross-batch behaviour.

Two genuinely new things came out of Phase 6:

**1. The day-2 test data and the SCD2 cross-batch certificate.** Most
candidates can't demonstrate SCD2 working across batches because they
never run a day-2. This codebase ships with one and pins every aspect
with 24 dedicated tests.

**2. Two real architectural bugs surfaced and fixed.** Day-2 testing is
the first thing in the codebase that exercises multi-batch storage,
and it surfaced issues invisible in single-batch operation:

- **The Bronze→Silver/DQ contract gap.** File-grain idempotency
  (ADR-0003) skips re-writing identical bytes under a new partition.
  Silver and DQ were silently assuming "Bronze data for batch X lives
  under partition `batch_id=X`" — violated on day-2 for the three
  unchanged sources. Fixed via `src/bronze/resolver.py` (the
  cross-batch resolver). See ADR-0008.

- **The destructive parquet writer.** Phase 1's `PandasEngine.write_parquet`
  called `shutil.rmtree(target)` before every write — silently wiping
  all existing batch partitions. Latent since Phase 1, invisible until
  Phase 6. Fixed with partition-aware overwrite semantics:
  partition-only rmtree for partitioned writes. See ADR-0008.

What's now demonstrable:

- **`data/sample/day2/`**: complete second-day vendor snapshot, 6 CSVs.
  Three (`clubs`, `competitions`, `player_valuations`) are byte-identical
  to day-1 — they exercise file-grain idempotency. Three have deliberate
  diffs: Saka transferred (Arsenal→Chelsea, value 120M→130M), Neuer's
  position label changed (`"GK"`→`"Goalkeeper"` — raw change with
  unchanged canonical), and 7 new appearances/games in January 2025.

- **`src/bronze/resolver.py`**: `resolve_bronze_partition()` handles
  the contract gap. When the current-batch partition is absent, follows
  the file's MD5 checksum to find the batch where the data actually
  lives. Used by Silver runner and DQ FK lookup builder.

- **`src/metadata/audit.py`**: added `find_most_recent_ingestion_for_source`
  helper. Used by the resolver.

- **`src/silver/run.py`** + **`src/dq/runner.py`**: both delegate to the
  resolver. DQ also fixes a subtle FK rule bug: missing FK target now
  produces an absent dict key (triggers fail-open), not an empty set
  (which would falsely fail every row).

- **`src/engines/pandas_engine.py`**: partition-aware overwrite. The fix
  knows the specific partition values in the incoming DataFrame and
  only wipes those subdirectories, leaving other batches untouched.

- **The SCD2 certificate**: 14 dedicated tests in
  `tests/test_scd2_day2.py` pinning version counts, immutability of
  historical rows, surrogate-key non-collision, and as-of-event
  resolution across multi-version dim_players.

- **Runner-level integration**: 10 tests in `tests/test_silver_run_day2.py`
  verifying file-grain idempotency at the runner level, audit lineage
  across batches, layer-grain idempotency on re-run, and the
  cross-partition data integrity that caught the writer bug.

- **ADR-0008**: documents the cross-batch resolver, partition-aware
  overwrite semantics, observation-time SCD2 (we use batch timestamp
  as effective_date because no vendor "change date" exists), and
  raw-vs-canonical SCD2 detection (Neuer's case — vendor lineage
  preserved by tracking both columns). Alternatives explicitly rejected.

After Phase 6, the audit table evolution day-1 → day-2 tells the
complete cross-batch story:

```
=== day-1 ===
source                bronze  rejected  silver    gold
appearances               30         1      29      12
clubs                      5         0       5       0
competitions               3         0       3       0
games                      6         0       6       5
player_valuations         18         0       0      18
players                   12         0      12       0

=== day-2 ===
source                bronze  rejected  silver    gold
appearances               35         1      34      12
clubs               (skip)         0       5       0   ← file-grain skip
competitions        (skip)         0       3       0   ← file-grain skip
games                      8         0       8       5
player_valuations   (skip)         0       0      18   ← file-grain skip
players                   12         0      14       0   ← 12 + 2 SCD2 versions
```

The `(skip)` markers show file-grain idempotency working honestly:
Bronze records the audit row but doesn't re-write bytes; Silver still
processes the source via the cross-batch resolver. The `silver=14`
for players on day-2 reflects the SCD2 merge output: 12 unchanged + 2
new versions for Saka and Neuer.

Total test count after Phase 6: **407 passing, 1 skipped**
(adds: bronze_resolver 6, scd2_day2 14, silver_run_day2 10).

### What's in Phase 7

Phase 7 adds the Spark engine stub and the engineering case for why
this codebase implements Pandas to production quality and Spark as a
deliberate stub. The brief allows either engine "or both, with
abstraction." We chose "Pandas fully, with the abstraction proven
real via a contracted Spark stub" — and ADR-0009 articulates why.

The core argument: at this data scale (~9 GB), Pandas runs the full
Bronze→Silver→Gold pipeline in ~60 seconds. Spark would add JVM
startup, cluster management, and 5-10x slower test iteration with no
functional benefit. The abstraction (`src/engines/base.py`) exists
to make adding Spark a contained, contracted task — ~2 weeks of
focused engineering in a single file — when scale, deployment, or
multi-tenancy actually justify it.

What this phase delivers:

- **`src/engines/spark_engine.py`**: a `SparkEngine` class
  implementing every method of the `DataFrameEngine` protocol and
  raising `NotImplementedError` on each. The stub is deliberately
  minimal — no `import pyspark` anywhere, no scaffolded session
  management. A user can instantiate it (proving the factory
  wiring), but every operation refuses to work with a clear message
  pointing at ADR-0009.

- **Factory dispatch already wired** (from Phase 1).
  Switching engines is a one-line config change in
  `configs/config.yaml` (`engine: pandas | spark`) or via the
  `PIPELINE_ENGINE` environment variable. The factory selects the
  right class; the abstraction handles the rest.

- **7 stub tests** in `tests/test_spark_engine_stub.py` proving:
  * The factory returns the right class for each engine kind
  * Both engines satisfy the `DataFrameEngine` protocol (catches
    missing-abstract-method bugs before runtime)
  * The stub is cheap to instantiate (no JVM startup)
  * Representative operations raise `NotImplementedError` with
    messages pointing at ADR-0009
  * `pyspark` is NOT in `sys.modules` after instantiating the stub
    (the zero-dependency claim is enforced by test)

- **ADR-0009** documents the engineering case: why Pandas, why the
  stub exists at all, what a real Spark implementation would look
  like method-by-method, the honest ~2 week cost estimate, and the
  scale/deployment thresholds where flipping the switch would be
  justified. Five alternatives explicitly rejected (build both,
  Spark only, drop the abstraction entirely, more-thorough stub,
  add pyspark to requirements).

The point: this codebase doesn't pretend Spark is "almost done." It
takes a defensible position, names the cost of the missing work, and
proves the abstraction supports the upgrade today.

Total test count after Phase 7: **414 passing, 1 skipped**
(adds: spark_engine_stub 7).

### What's in Phase 8

Phase 8 satisfies the brief's §8 orchestration requirement with a
runnable Airflow DAG that wires the three CLI commands
(`python -m src.bronze.run`, `python -m src.silver.run`,
`python -m src.gold.run`) into a scheduled pipeline with a DQ
hard-fail gate between Silver and Gold.

The DAG runs against Airflow 2.10.3 with `LocalExecutor` —
`airflow standalone` brings up scheduler + webserver + worker as
one process. CeleryExecutor is named explicitly as the production
upgrade path in ADR-0010; we deliberately don't build it for the
deliverable.

Two latent bugs in the cross-batch resolver (ADR-0008) surfaced
during Airflow integration testing — Phase 8's
`data_interval_start` produces `YYYY-MM-DD` batch_ids against an
audit DAO containing the `day-1`/`day-2` batches from Phase 6
testing. Both bugs are documented in ADR-0010 and fixed in
Slice 8.1b with 3 dedicated regression tests:

- **Lexicographic batch_id comparison.** The audit DAO's
  `find_most_recent_ingestion_for_source` used `WHERE batch_id <= ?`
  SQL — string comparison. ASCII `d` (0x64) > `2` (0x32), so
  `"day-1" <= "2026-06-02"` evaluates False. Fix: order/filter by
  `registered_at` (ISO-8601, lexicographically safe).
- **Resolver only walked back one step.** Chains of file-grain skips
  (day-1 wrote bytes → day-2 skipped → 2026-06-02 skipped) defeated
  the resolver. Fix: chain-following via
  `_find_all_batches_with_checksum` returning a list; iterate
  most-recent-first until a partition exists on disk.

What this phase delivers:

- **`dags/football_pipeline.py`**: the DAG. Linear chain
  `bronze → silver → dq_gate → gold`. Daily schedule, 1 retry per
  task, `max_active_runs=1` (prevents partition races),
  `catchup=False`.
- **`src/orchestration/airflow_wrappers.py`**: four PythonOperator
  entrypoints that translate Airflow's run context into our
  `batch_id` semantics. The runners themselves
  (`src/bronze/run.py`, etc.) have no Airflow imports.
- **DQ hard-fail gate** (the ADR-0006 deferred decision now landed):
  fails when `critical_failures > 0 AND quarantine_pct > 5%`.
  Proportional fail — a handful of orphan rows in 10 million
  shouldn't kill the daily run; widespread DQ failure should.
- **`requirements-airflow.txt`**: optional dependency separate
  from core `requirements.txt`. Includes the Apache constraints
  file URL because installing Airflow without constraints almost
  always hits Flask/Werkzeug/SQLAlchemy version conflicts.
- **Resolver chain-following fix** (Slice 8.1b): the cross-batch
  resolver now handles arbitrary chains of file-grain skips and
  arbitrary batch_id formats, with 3 dedicated regression tests in
  `TestChainOfFileGrainSkips`.
- **18 orchestration tests** + 3 regression tests = 21 new tests,
  bringing total to 435.
- **ADR-0010**: documents the LocalExecutor scope choice, the
  CeleryExecutor production upgrade path, the PythonOperator +
  wrapper pattern, the `data_interval_start → batch_id` derivation,
  the DQ gate threshold, and the two bugs surfaced and fixed.

Demonstrable end-to-end idempotency through Airflow: re-triggering
a successful DAG run is a no-op via `pipeline_runs`, with all four
tasks reporting "skipped." Production-grade re-run semantics.

Total test count after Phase 8: **435 passing, 1 skipped**
(adds: airflow_wrappers 9, airflow_dag 9, chain_of_skips 3).

### What's in Phase 9

Phase 9 packages the whole stack into Docker. One command,
`docker compose up`, brings up Airflow with the pipeline code,
dependencies, and DAGs all baked in. A reviewer can launch the
complete demonstration in ~3 minutes (first build) or ~10 seconds
(subsequent runs).

What this phase delivers:

- **`Dockerfile`** extending `apache/airflow:2.10.3-python3.12` —
  pins Airflow + Python versions matching our test suite, installs
  `requirements.txt` inside the image, copies `src/`, `dags/`,
  `configs/`, and `data/sample/` into `/opt/airflow/`. Layer
  ordering optimised for fast rebuilds on source-only changes.
- **`docker-compose.yml`** with a single service running
  `airflow standalone` inside the container. Two bind mounts:
  - `./data → /opt/airflow/data` so the reviewer sees the lake on
    their host filesystem while Airflow runs (tangible demo, not
    hidden in a Docker volume)
  - `./.airflow → /opt/airflow/.airflow` so Airflow's SQLite DB
    and admin password persist across container restarts
- **`.dockerignore`** excludes `.venv/`, runtime state, caches,
  tests, and docs from the build context — keeps the image lean.
- **Five `make docker-*` targets** for ergonomic workflow:
  `docker-build`, `docker-up`, `docker-down`, `docker-logs`,
  `docker-shell`.
- **Cross-environment idempotency proven**: the same `data/metadata.db`
  is read by host-side Airflow runs (Phase 8) AND Docker-container
  Airflow runs (Phase 9). Both correctly recognise prior successful
  batches and skip them, demonstrating that pipeline state is
  genuinely portable across execution environments.
- **ADR-0011** captures the design choices: single-service compose,
  custom Dockerfile, bind mounts, SQLite + SequentialExecutor.
  Also documents four real-world gotchas surfaced during the build:
  `pip install --user` inside a venv (rejected by pip), stale
  `airflow.cfg` from prior host runs leaking absolute Mac paths
  into the container, the SequentialExecutor-vs-LocalExecutor
  correction (Airflow 2.10 forbids LocalExecutor + SQLite), and
  the *inverse* problem — container cfg polluting the host
  filesystem after running Docker, breaking host `pytest tests/`
  until `.airflow/` is wiped. Same root cause in both directions
  (bind mount sharing the cfg between environments).
- **ADR-0010 amended** to acknowledge the executor misnaming —
  evolution of understanding kept visible rather than rewritten.

The Docker stack uses **`SequentialExecutor` + SQLite**, matching
what `airflow standalone` actually runs (which is also what we ran
in Phase 8 — we just incorrectly called it LocalExecutor at the
time). The DAG is a linear chain so there's no parallelism to give
up. Production upgrade path: **LocalExecutor + Postgres** for
parallel branches, or full **CeleryExecutor + Postgres + Redis** for
distributed execution. Same scope-discipline pattern as Spark
(ADR-0009).

Test count unchanged in Phase 9 — Docker is a deployment artifact,
not new runtime code. **435 passing, 1 skipped.**

---

## Running the pipeline

### First-time setup

```bash
# Clone and enter
git clone https://github.com/<your-username>/football-analytics-pipeline.git
cd football-analytics-pipeline

# Set up a virtual environment (Python 3.11+)
python3.11 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements-dev.txt
```

### Verifying the build

```bash
# Run the full test suite (~30 seconds without Airflow; ~90s with)
pytest tests/

# Expected: 435 passed, 1 skipped (with Airflow installed)
# Or:       414 passed, 22 skipped (without Airflow — 21 Airflow tests
#                                    skip cleanly + 1 sample skip)
```

### Full pipeline in three commands

After Phase 5, the complete Bronze → Silver → Gold flow runs end-to-end:

```bash
python -m src.bronze.run --batch-id demo-1 --raw-root data/sample
python -m src.silver.run --batch-id demo-1
python -m src.gold.run    --batch-id demo-1
```

Each layer is idempotent (re-running succeeds as a no-op) and has
continue-on-failure semantics (one source's failure doesn't kill the
batch). The sections below explain each layer in detail.

### Running Bronze end-to-end

The committed sample data lets you run Bronze without a Kaggle account.

```bash
# Run Bronze against the committed samples
python -m src.bronze.run --batch-id demo-1 --raw-root data/sample

# Expected summary at the end:
#   Bronze run summary — batch_id=demo-1
#     status: success
#     total rows: 74
#     per source:
#       competitions       written   rows=3
#       clubs              written   rows=5
#       players            written   rows=12
#       games              written   rows=6
#       appearances        written   rows=30
#       player_valuations  written   rows=18

# Inspect what was produced
find data/lake/bronze -type f -name '*.parquet' | sort
```

### Running Silver end-to-end

After Bronze has populated `data/lake/bronze/` for a given `batch_id`,
Silver builds dimensions and facts on top of it:

```bash
# Run Silver against the Bronze data from the previous command
python -m src.silver.run --batch-id demo-1

# Expected summary at the end:
#   Silver run summary — batch_id=demo-1
#     status: success
#     total rows: 4804         (dim_date dominates; other artifacts ~50 rows)
#     per artifact:
#       dim_clubs         written   rows=5
#       dim_competitions  written   rows=3
#       dim_date          written   rows=4748
#       dim_players       written   rows=12
#       fact_games        written   rows=6
#       fact_appearances  written   rows=30

# Inspect what was produced
find data/lake/silver -maxdepth 2 -type d | sort
```

You should see one directory per Silver artifact, each Hive-partitioned
by `batch_id`. The output mirrors Bronze's layout, plus the four
dimensions and two facts.

### Verifying SCD Type 2 with as-of-event resolution

The single most differentiating piece of the pipeline is SCD Type 2
with fact joins that resolve to the correct version at the time of
the match. After Silver runs, you can verify this directly:

```bash
python -c "
import pandas as pd
fact = pd.read_parquet('data/lake/silver/fact_appearances')
print(f'Total appearances: {len(fact)}')
print(f'Resolved player_sk: {fact[\"player_sk\"].notna().sum()}')
print(f'Unresolved (orphan): {fact[\"player_sk\"].isna().sum()}')
print()
print('Orphan appearance (deliberate seed for DQ to catch):')
print(fact[fact['player_sk'].isna()][['appearance_id', 'player_id', 'date']].to_string(index=False))
"
```

Expected output:
- **30 total appearances**
- **29 resolved player_sk** (every legitimate appearance correctly
  joined to its dim_players version)
- **1 unresolved**: player_id=9999, the deliberate orphan FK we
  seeded in the sample data for DQ to catch

The audit DAO accurately reflects this throughout:

```bash
python -c "
from src.metadata import audit
rows = audit.list_batch_files(batch_id='demo-1')
for r in rows:
    print(f'{r.source_name:20s} {r.status.value:14s} silver_rows={r.silver_row_count}')
"
```

Five sources reach `transformed` status with accurate silver row counts;
`player_valuations` stays at `ingested` (no Silver builder consumes it;
Phase 5's Gold layer queries it directly from Bronze).

### Verifying DQ catches the orphan

The deliberate orphan `player_id=9999` we seeded into the sample
appearances is caught by the DQ framework's FK rule before it can
reach Silver. After Silver runs, the orphan lives in `_rejected/`
with a precise failure reason; `fact_appearances` is clean (29 rows,
not 30).

```bash
# Look at the per-batch DQ report
cat data/dq_reports/demo-1.json | python -m json.tool | head -30
```

Expected: `total_rows_quarantined: 1`,
`sources_with_critical_failures: ["appearances"]`,
the appearances source report shows `rows_in: 30, rows_clean: 29,
rows_quarantined: 1`, and the failing rule is
`foreign_key:appearances.player_id->players.player_id`.

```bash
# Inspect the quarantined row on disk
python -c "
import pandas as pd
df = pd.read_parquet('data/lake/_rejected/appearances')
print(f'Quarantined: {len(df)} row(s)')
print(df[['appearance_id', 'player_id', 'game_id', '_dq_failure_reason']].to_string(index=False))
"
```

Expected: one row, `appearance_id=A08030`, `player_id=9999`, with
failure reason `foreign_key:appearances.player_id->players.player_id`.

```bash
# Confirm fact_appearances is clean (no NULL player_sk)
python -c "
import pandas as pd
fact = pd.read_parquet('data/lake/silver/fact_appearances')
print(f'fact_appearances rows: {len(fact)}')
print(f'player_sk null count: {fact[\"player_sk\"].isna().sum()}')
print(f'orphan player_id=9999 present? {(fact[\"player_id\"] == 9999).any()}')
"
```

Expected: 29 rows, 0 NULL player_sk, orphan not present.

```bash
# Confirm the audit DAO captured the quarantine
python -c "
from src.metadata import audit
rows = audit.list_batch_files(batch_id='demo-1')
for r in rows:
    if r.source_name == 'appearances':
        print(f'appearances: bronze={r.bronze_row_count}, '
              f'rejected={r.rejected_row_count}, silver={r.silver_row_count}')
"
```

Expected: `bronze=30, rejected=1, silver=29`. The math reconciles at
source grain — the property ADR-0001 and ADR-0006 are designed to
preserve.

### Running Gold + querying via SQL

After Silver completes, the Gold runner builds all five §6 analytical
artifacts via DuckDB and materialises them to partitioned Parquet:

```bash
python -m src.gold.run --batch-id demo-1

# Expected summary at the end:
#   Gold run summary — batch_id=demo-1
#     status: success
#     total rows: 52
#     per artifact:
#       top_scorers_by_season         written  rows=12 primary_source=appearances
#       club_season_summary           written  rows=5  primary_source=games
#       top_players_all_time          written  rows=12 primary_source=appearances
#       player_valuation_rolling_avg  written  rows=18 primary_source=player_valuations
#       club_performance_metrics      written  rows=5  primary_source=games
```

The artifacts are materialised at `data/lake/gold/<artifact>/batch_id=<id>/`
in the same Hive-partitioned style as Bronze and Silver. They're
also queryable via interactive SQL through the DuckDB session:

```bash
python -c "
from src.utils.config import get_config
from src.gold.duckdb_session import gold_session

cfg = get_config()
with gold_session(silver_root=cfg.paths.silver, bronze_root=cfg.paths.bronze) as conn:
    print('TOP 5 SCORERS:')
    df = conn.execute('''
        SELECT player_name, position_canonical, club_name_at_event, total_goals
        FROM read_parquet(\"data/lake/gold/top_scorers_by_season/**/*.parquet\")
        ORDER BY total_goals DESC LIMIT 5
    ''').fetchdf()
    print(df.to_string(index=False))
"
```

You should see Bellingham and Lewandowski tied at the top with 4
goals each — the brief's §6.1 question answered.

For the most analytically interesting artifact (the rolling-average
window function):

```bash
python -c "
import pandas as pd
df = pd.read_parquet('data/lake/gold/player_valuation_rolling_avg')
saka = df[df['player_name'] == 'Bukayo Saka'].sort_values('date')
print('=== Bukayo Saka valuation trend (90-day rolling avg) ===')
print(saka[['date','market_value_in_eur','rolling_avg_90d','rolling_sample_count']].to_string(index=False))
"
```

Saka's `rolling_avg_90d` rises monotonically as his market value
increases — proof the DuckDB window function works correctly across
the partition.

### Full lineage from raw vendor data to analytical aggregates

After all three layers run, the audit table tells the complete story:

```bash
python -c "
from src.metadata import audit
rows = audit.list_batch_files(batch_id='demo-1')
print(f'{\"source\":20s} {\"bronze\":>7} {\"rejected\":>9} {\"silver\":>7} {\"gold\":>7}')
for r in rows:
    print(f'{r.source_name:20s} {r.bronze_row_count or 0:7d} {r.rejected_row_count or 0:9d} {r.silver_row_count or 0:7d} {r.gold_row_count or 0:7d}')
"
```

Expected output:

```
source                bronze  rejected  silver    gold
appearances               30         1      29      12
clubs                      5         0       5       0
competitions               3         0       3       0
games                      6         0       6       5
player_valuations         18         0       0      18
players                   12         0      12       0
```

Three things this output proves:

1. The orphan `player_id=9999` was caught (rejected=1 for appearances)
2. Dimensions (`clubs`, `competitions`, `players`) feed into Gold
   artifacts but aren't primary sources (gold=0 — see ADR-0007)
3. `player_valuations` follows the Bronze→Gold direct pattern
   (silver=0, gold=18) deliberately, per ADR-0005

### Demonstrating idempotency

Two complementary mechanisms protect against accidental duplicate work:

```bash
# Layer-grain idempotency: re-running the same batch_id is a no-op
python -m src.bronze.run --batch-id demo-1 --raw-root data/sample
# Expected: status: skipped — already succeeded

# File-grain idempotency: fresh batch_id with unchanged files
# skips every source individually, citing the prior batch
python -m src.bronze.run --batch-id demo-2 --raw-root data/sample
# Expected: every source 'skipped', skip_reason mentions demo-1
```

### Running a day-2 incremental snapshot

The `data/sample/day2/` directory contains a complete second-day
vendor snapshot. Three sources are byte-identical to day-1 (testing
file-grain idempotency) and three have deliberate diffs (testing
SCD2 cross-batch behaviour). Walk through the day-2 demo as follows:

```bash
make clean

# Day 1 — full Bronze, Silver
python -m src.bronze.run --batch-id day-1 --raw-root data/sample
python -m src.silver.run --batch-id day-1

# Day 2 — watch for file-grain skips on unchanged sources
python -m src.bronze.run --batch-id day-2 --raw-root data/sample/day2
# Expected:
#   competitions       skipped   skip_reason=identical checksum already ingested in batch day-1
#   clubs              skipped   skip_reason=identical checksum already ingested in batch day-1
#   players            written   rows=12
#   games              written   rows=8
#   appearances        written   rows=35
#   player_valuations  skipped   skip_reason=identical checksum already ingested in batch day-1

python -m src.silver.run --batch-id day-2
# Expected SCD2 output (dim_players merge):
#   new=0, changed=2, unchanged=10, total_output=14
# The two changes are Saka (transferred to Chelsea, market value up)
# and Neuer (position label changed 'GK'→'Goalkeeper' — raw vendor
# change preserves vendor lineage per ADR-0008).
```

Verify the SCD2 cross-batch story:

```bash
python -c "
import pandas as pd
dim = pd.read_parquet('data/lake/silver/dim_players')
# Cross-partition read returns the full historical view: 12 (day-1) + 14 (day-2) = 26
print(f'Total dim_players rows across all partitions: {len(dim)}')

# Saka has THREE rows visible across partitions:
#   1 from day-1 partition (Arsenal-era, current at the time)
#   2 from day-2 partition (Arsenal-era closed + Chelsea-era current)
saka = dim[dim['player_id'] == 1001][
    ['player_sk','current_club_id','market_value_in_eur',
     'effective_date','end_date','is_current']
]
print('Saka SCD2 versions:')
print(saka.to_string(index=False))
"
```

Expected output: 26 total rows, with Saka showing his career
progression — Arsenal at 120M (closed out at day-2 timestamp),
Chelsea at 130M (current).

This is the SCD2 win expressed at runtime: the same dim_players
table preserves both Saka's historical Arsenal state AND his
current Chelsea state, with surrogate keys that fact_appearances
joins to as-of each appearance's match date. See ADR-0008 for the
observation-time vs event-time discussion of the effective_date
semantics.

### Inspecting the audit trail

The metadata DB at `data/metadata.db` captures every file's lifecycle:

```python
python -c "
from src.metadata import audit
rows = audit.list_batch_files(batch_id='demo-1')
for r in rows:
    print(f'{r.source_name:20s} {r.status.value:12s} '
          f'source={r.source_row_count:>4} bronze={r.bronze_row_count:>4}')
"
```

### Switching engines (Pandas / Spark)

The engine abstraction (`src/engines/base.py`) supports both Pandas
and Spark. Pandas is the production implementation; Spark is a
deliberate stub (see ADR-0009 for the engineering case).

To verify the abstraction works for both engines:

```bash
# Default behaviour — Pandas runs the full pipeline
python -c "
from src.engines.factory import get_engine
engine = get_engine()
print(f'Default engine: {engine.kind}')
"
# Expected: Default engine: pandas

# Switch to Spark via env var; instantiation succeeds, operations refuse
PIPELINE_ENGINE=spark python -c "
from src.utils.config import get_config; get_config.cache_clear()
from src.engines.factory import get_engine; get_engine.cache_clear()
engine = get_engine()
print(f'Configured engine: {engine.kind}')
try:
    engine.read_csv('data/sample/players.csv')
except NotImplementedError as e:
    print(f'Spark stub refuses operations as expected:')
    print(f'  {e}')
"
# Expected: configured engine = spark, operations raise NotImplementedError
# with a message pointing at ADR-0009
```

This proves the abstraction is real — the factory dispatch, config
selection, and protocol layer all support both engines today. Only
the Spark implementation body is missing. See ADR-0009 for the
method-by-method design sketch and the honest ~2-week cost estimate
for a production-quality Spark engine.

### Running the Airflow DAG

Phase 8 wires the three CLI commands into an Airflow DAG with a DQ
hard-fail gate between Silver and Gold. The DAG is at
`dags/football_pipeline.py`; orchestration design choices are
documented in ADR-0010.

Airflow is an optional dependency. Install it on top of the core
requirements:

```bash
# In your existing venv:
pip install -r requirements-airflow.txt \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.3/constraints-3.12.txt"
```

The constraints file is essential — installing Airflow without it
almost always produces Flask/Werkzeug/SQLAlchemy version conflicts
that prevent the DAG from loading.

Launch Airflow standalone (scheduler + webserver + LocalExecutor
worker in one process):

```bash
export AIRFLOW_HOME=$(pwd)/.airflow
export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export PYTHONPATH=$(pwd):$PYTHONPATH       # so dags/ can import src/
airflow standalone
```

On first launch, Airflow prints an auto-generated admin password to
stdout. Look for `Login with username: admin  password: <random>`.
The password persists at `$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated`
(both files are gitignored).

Open `http://localhost:8080`, log in, and:

1. Toggle the `football_analytics_pipeline` DAG **on** (switch at
   the left of the DAG row — paused by default)
2. Click the DAG name to open its detail page
3. Click **▶ Trigger DAG** in the top-right

You should see four tasks turn green in sequence:

```
bronze (~3s) → silver (~3s) → dq_gate (~1s) → gold (~3s)
```

Expected outcomes:

- `bronze`: 6 sources written (or skipped via file-grain idempotency
  if you've run before)
- `silver`: 6 artifacts written
- `dq_gate`: decision=pass (1 critical failure from the orphan, but
  ~3.3% quarantine is below the 5% threshold)
- `gold`: 5 artifacts, 52 total rows

### Proving idempotent re-trigger through Airflow

Trigger the same DAG again. All four tasks turn green again — but
this time each task reports `layer_status: skipped`. The runners
checked `pipeline_runs` in the metadata DB, saw the batch already
succeeded, and declared themselves no-ops without doing work.

This is production-grade re-run semantics: re-triggering after a
worker crash, an operator panic, or a backfill is safe and cheap.
Combined with file-grain idempotency at Bronze (skip identical
bytes across batches) and SCD2's hash-based merge at Silver
(no spurious new versions on unchanged data), the pipeline
supports arbitrary re-runs without data corruption.

### Running with Docker

Phase 9 packages the whole stack into a single Docker image with a
single-service compose file. One command brings up the pipeline.

Prerequisites: Docker Desktop installed and running (the whale icon
in your menu bar should be steady, not animating).

```bash
# 1. First-time setup: build the image (~3-5 minutes — pulls Airflow
#    base + installs requirements.txt). Subsequent builds are ~10s.
make docker-build

# 2. Launch. Streams logs to terminal. Watch for the admin password
#    line: "standalone | Login with username: admin  password: <RANDOM>"
make docker-up

# 3. In your browser: http://localhost:8080
#    Log in with username=admin, password from step 2.
#    Toggle the DAG on, click ▶ Trigger DAG, watch four tasks go green.

# 4. From a SEPARATE terminal, prove the bind mount works:
ls data/lake/gold/top_scorers_by_season/    # data on host while
                                            # container is running

sqlite3 data/metadata.db "SELECT * FROM pipeline_runs"
# Shows the layer-grain success record — same DB that host Airflow
# uses if you ran Phase 8 locally.

# 5. When done: Ctrl+C in the docker-up terminal, then:
make docker-down
```

Three gotchas worth knowing about (each documented in ADR-0011):

1. **If you previously ran local `airflow standalone` from Phase 8**,
   the `.airflow/` directory on disk contains a `airflow.cfg` with
   absolute host paths (`/Users/...`). The container can't write to
   those paths inside its filesystem. Before first `docker compose
   up`, wipe the stale state:

   ```bash
   rm -rf .airflow && mkdir -p .airflow
   ```

2. **The container uses `SequentialExecutor`**, not `LocalExecutor`.
   Airflow 2.10 forbids `LocalExecutor + SQLite` at startup (write
   races on the SQLite file). `SequentialExecutor` is what `airflow
   standalone` uses under the hood anyway — single-process,
   single-task-at-a-time, exactly right for our linear-chain DAG.

3. **Bind-mount permissions** can occasionally trip up on Docker
   Desktop for Mac. If you see permission errors writing to
   `/opt/airflow/.airflow/` inside the container, ensure
   Docker Desktop has full disk access (System Settings → Privacy
   & Security → Full Disk Access → enable Docker.app).

### Switching between host and Docker Airflow

The `./.airflow/` directory is **bind-mounted into the container**.
This is deliberate — it gives the host visibility into Airflow's
state. But it has a side effect: whichever environment (host or
container) writes `airflow.cfg` last wins, and the cfg embeds
*absolute paths* (log directories, etc).

Symptoms when paths leak across environments:

- **Container fails to start** with `PermissionError: '/Users/...'` —
  host paths got into the cfg, container can't write to them.
- **Host `pytest tests/` fails** with `PermissionError: '/opt/airflow'`
  — container paths got into the cfg, host can't write to them.

Same root cause both directions. **Fix is the same**: wipe `.airflow/`
and let the current environment regenerate it cleanly.

#### Going from host Airflow → Docker:

```bash
rm -rf .airflow && mkdir -p .airflow
# Container will regenerate cfg with container-internal paths
docker compose up
```

#### Going from Docker → host Airflow (e.g. to run pytest):

```bash
docker compose down                 # stop the container
rm -rf .airflow && mkdir -p .airflow
export AIRFLOW_HOME=$(pwd)/.airflow # tell host Airflow where to look
airflow db migrate                  # regenerate cfg with host paths
pytest tests/                       # should now pass
```

For a clean reviewer experience, **pick one path (Docker OR host
standalone) and stick with it**. The wipe-between-environments
discipline is only relevant for development iteration.

### Fetching the full Kaggle dataset

To run against the real data (requires a Kaggle API token at
`~/.kaggle/kaggle.json`):

```bash
make seed                                         # downloads data/day1/*.csv + _manifest.json
python -m src.bronze.run --raw-root data/day1     # batch_id auto-derived from UTC now
```

The fetched data carries a `_manifest.json` whose `vendor_last_updated`
field flows into the audit DAO as the authoritative vendor timestamp.

---

## Pandas vs Spark — the choice

The pipeline supports both engines via a single config switch
(`engine: pandas | spark`). The architectural recommendation, justified
in detail in Phase 10's README polish:

* **Default to Pandas for this dataset.** The Kaggle data is ~1–2 GB
  uncompressed. Spark on a single-node Docker container adds JVM
  startup + serialisation overhead with no shuffle benefit; Pandas
  wins on wall-clock time and memory footprint.
* **Switch to Spark when** working-set memory exceeds ~50% of available
  RAM, or fact-table joins exceed ~10M rows, or distributed execution
  becomes available. The abstraction is the *option*; the right answer
  for *this dataset on a laptop* is Pandas.

---

## License

MIT — see [LICENSE](LICENSE).
