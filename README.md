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
| 5     | Gold aggregations + DuckDB views                   | ⏳ Next  |
| 6     | Day-2 incremental snapshot + SCD2 validation       | ⏳       |
| 7     | Spark engine: stub + design doc *(not fully built — cost-aware choice)* | ⏳ |
| 8     | Airflow DAG + idempotency wiring                   | ⏳       |
| 9     | Docker + docker-compose stack                      | ⏳       |
| 10    | PII anonymisation, Superset, CI, README polish     | ⏳       |

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
# Run the full test suite (~3 seconds)
pytest tests/

# Expected: 170 passed, 1 skipped
```

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
