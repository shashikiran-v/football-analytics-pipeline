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
| 2b    | Bronze ingestion + sample data + Kaggle manifest    | ⏳ Next  |
| 3     | Silver: transforms, star-schema dimensions, SCD2   | ⏳       |
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

---

## Running what exists today (Phase 1)

```bash
# Clone and enter
git clone https://github.com/<your-username>/football-analytics-pipeline.git
cd football-analytics-pipeline

# Set up a virtual environment
python3.11 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements-dev.txt

# Run the test suite (21 tests, ~1 second)
pytest tests/ -v
```

You should see 21 tests pass against the Pandas engine. The same suite
will run against PySpark when the Spark engine lands in Phase 7.

### What you can poke at right now

```python
# Load the typed config
from src.utils.config import load_config
cfg = load_config()
print(cfg.engine, cfg.paths.bronze)

# Initialise the metadata DB
from src.metadata.db import init_db
init_db()                                # creates data/metadata.db

# Try the engine abstraction
from src.engines.factory import get_engine
engine = get_engine()                    # picks Pandas from config
```

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
