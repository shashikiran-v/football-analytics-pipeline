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
| 3     | Silver: transforms, star-schema dimensions, SCD2   | ⏳ Next  |
| 4     | DQ framework + quarantine + report                 | ⏳       |
| 5     | Gold aggregations + DuckDB views                   | ⏳       |
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
