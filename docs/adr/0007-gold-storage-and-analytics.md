# ADR-0007: Gold Layer Storage and Analytics Engine

## Status

Accepted — 2026-06-02

## Context

The brief calls for an analytical Gold layer answering five specific
questions (§6):

> - Top scorers per season
> - Season summaries (clubs)
> - Top players by total goals
> - Player valuation trends (rolling average)
> - Club performance metrics

The questions imply both storage decisions (where do the aggregates
live?) and engine decisions (how are they computed?). Several
non-trivial sub-decisions arise within "build Gold":

1. **How is Gold stored?** Materialised Parquet, ephemeral views, or both?
2. **What query engine computes the aggregates?** Pandas/Spark in-process,
   or a SQL engine?
3. **Where do query definitions live?** Python constants, YAML, a DSL?
4. **How does audit attribution work for Gold artifacts** that read from
   multiple Silver/Bronze sources?
5. **Does Gold introduce a new file-lifecycle state** (e.g. AGGREGATED)?
6. **What happens to sources that have no Silver layer** (specifically
   `player_valuations`, which Phase 3 deliberately skipped)?

Each is a real engineering choice. Phase 5 documents the reasoning chain.

## Decision

### Hybrid storage: Materialised Parquet + DuckDB views

Each Gold artifact has TWO presentations:

1. **Materialised Parquet** at `data/lake/gold/<artifact_name>/batch_id=<id>/`,
   built once per batch by the Gold runner. Same Hive-partitioned
   layout as Bronze and Silver — consistent audit/lineage story
   throughout.

2. **DuckDB views** registered at runtime via `gold_session()`. After
   the session opens, an analyst can query Silver dim/fact tables
   AND Bronze `player_valuations` AND materialised Gold artifacts
   using regular SQL:

   ```sql
   SELECT player_name, total_goals
   FROM read_parquet('data/lake/gold/top_scorers_by_season/**/*.parquet')
   ORDER BY total_goals DESC LIMIT 5;
   ```

The two presentations serve different audiences. Materialised Parquet
gives data engineers the same auditable artefact pattern as Bronze
and Silver — partition counts, file-on-disk inspection, audit DAO
row counts. DuckDB views give analysts an interactive SQL interface
they can demonstrate in 30 seconds.

### DuckDB as the analytics engine

The window-function pattern for `player_valuation_rolling_avg`
makes DuckDB the right tool:

```sql
AVG(market_value_in_eur) OVER (
    PARTITION BY player_id
    ORDER BY date
    ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
)
```

Three lines of SQL via window functions. A Pandas equivalent
would need careful sorting + per-player iteration + edge-case
handling for the first 89 observations. DuckDB does it natively
in one expression.

The SCD2 as-of-event join is similarly clean in SQL:

```sql
LEFT JOIN dim_players dp
    ON dp.player_id = v.player_id
    AND CAST(v.date AS VARCHAR) >= dp.effective_date
    AND CAST(v.date AS VARCHAR) <= dp.end_date
```

The same `[effective_date, end_date]` range predicate we
implemented in Python via the in-memory version index in
`src/silver/facts.py` is expressed in SQL with three lines.
**The architectural pattern (as-of-event resolution) is portable
across implementation languages.**

DuckDB also reads Parquet directly via `read_parquet(...)` with
predicate pushdown — no ETL step is needed to load Silver into
DuckDB. The query plan optimises across the parquet schema.

### SQL as Python code, not YAML

Gold artifacts are defined as typed Pydantic `GoldArtifact`
constants in `src/gold/artifacts.py`:

```python
top_scorers_by_season = GoldArtifact(
    name="top_scorers_by_season",
    sql="""SELECT ... FROM fact_appearances ...""",
    sources=["fact_appearances", "dim_players", "dim_clubs"],
    primary_source="appearances",
    description="...",
)
```

The SQL is multi-line Python strings — NOT YAML. The metadata
(`name`, `sources`, `primary_source`, `description`) stays declarative,
but the SQL itself is code.

This is a deliberate departure from ADR-0002's framework-as-config
pattern. SQL is genuinely code: typos in column names should fail
at Python import time (when the module loads and the constant is
constructed), not at runtime when an analyst opens a Superset chart.
We also want syntax highlighting and IDE support for multi-line SQL,
which YAML doesn't give.

The metadata around the SQL stays declarative because that's where
behaviour lives (the runner iterates over `ALL_ARTIFACTS`, the audit
layer attributes to `primary_source`). New artifacts are one new
constant plus one append to `ALL_ARTIFACTS`. **Hybrid pattern,
deliberately chosen.**

### Source-grain audit attribution for Gold artifacts

Each `GoldArtifact` declares a `primary_source` — the Bronze source
the artifact fundamentally represents. The Gold runner calls
`audit.record_gold_complete(source=primary_source, ...)` on completion.

| Artifact | Primary source |
|----------|---------------|
| `top_scorers_by_season` | `appearances` |
| `top_players_all_time` | `appearances` |
| `club_season_summary` | `games` |
| `club_performance_metrics` | `games` |
| `player_valuation_rolling_avg` | `player_valuations` |

This matches the source-grain attribution pattern from ADR-0001 and
ADR-0005. Dimensions joined into the artifact (e.g. `dim_clubs` for
`top_scorers_by_season`) are NOT marked as Gold sources — they
appear in the `sources` list for documentation/lineage, but the
audit row's `gold_row_count` belongs to one source per artifact.

When two artifacts share a primary source (`top_scorers_by_season`
and `top_players_all_time` both → `appearances`), the audit row's
`gold_row_count` is **last-writer-wins**. The per-artifact granularity
is preserved in the materialised parquet directories; the audit row
gives the quick lineage summary, not a per-artifact ledger.

### Gold does NOT introduce a new lifecycle state

I deliberately did NOT add `AGGREGATING` or `AGGREGATED` to
`FileStatus`. The state machine remains:

```
registered -> ingesting -> ingested -> transforming -> transformed
                                                    -> failed
```

Gold is an analytical view *over* `TRANSFORMED` data, not a new
lifecycle stage. The data's "lifecycle" is complete at `TRANSFORMED`;
Gold derives aggregates without re-stating the file's status.

`record_gold_complete()` therefore:
- DOES set `gold_row_count` on the primary source's audit row
- DOES emit a `GOLD_FINISHED` event to the timeline
- Does NOT change file status

This keeps the state machine focused on data lineage (what happened
to the source file) while still recording Gold's contribution
(how many aggregate rows resulted from this source).

### `player_valuations` Bronze→Gold direct lineage

`player_valuations` was deliberately given no Silver layer in Phase 3
(per ADR-0005: its shape is already aggregation-ready; adding a
redundant Silver copy would have no transformations). Phase 5's
`player_valuation_rolling_avg` reads it directly from the Bronze
view (`bronze_player_valuations`) via DuckDB.

The audit row's lineage now reads:

```
player_valuations:  bronze=18  rejected=0  silver=NULL  gold=18  status=INGESTED
```

The `silver_row_count IS NULL` and `status=INGESTED` honestly reflect
that this source never reached Silver; the `gold_row_count=18` honestly
reflects that Gold did consume it. **The brief's lineage requirement
is satisfied for a non-standard data flow** — Bronze→Gold direct,
documented in the audit DAO without forcing the source through an
unnecessary Silver layer.

## Consequences

**Gained:**

- **Brief's §6 fully covered.** Five Gold artifacts, all five
  analytical questions answered, all materialised to disk and
  queryable via SQL.
- **Analyst-facing payoff.** A reviewer can open a Python REPL,
  call `gold_session()`, and run interactive SQL against the
  whole pipeline. *That's* the Medallion architecture paying off
  — not just files on disk, but queryable analytics.
- **Window functions and range joins in SQL.** The rolling-average
  and SCD2 as-of-event patterns are expressed in the SQL idiom
  designed for them. Same correctness as the Python equivalent at
  a fraction of the code.
- **One command per layer.** Three commands now produce the full
  pipeline (`bronze.run`, `silver.run`, `gold.run`), each idempotent.
- **Full source-grain lineage in audit DAO.** Every source's row
  count is recorded at every stage it touched. The `player_valuations`
  Bronze→Gold direct flow is captured honestly.
- **Hybrid storage gives BOTH audit consistency AND SQL accessibility.**

**Given up:**

- **Last-writer-wins on shared primary sources.** When two artifacts
  share a primary source, the audit row reflects the last write.
  Per-artifact granularity lives in the materialised parquet, not in
  the audit DAO. We accept this; a per-artifact audit table would
  be scope creep for the current value it delivers.
- **In-memory DuckDB session, no persistent analytical store.** The
  DuckDB connection is `:memory:` — analytics queries hit Parquet
  on disk via `read_parquet`. For our scale this is right; for
  hyperscale we'd switch to a file-backed DuckDB or DuckLake.
  Documented as a future scaling path.
- **Minimal migration system.** `_apply_migrations` in `db.py`
  handles the `gold_row_count` ALTER TABLE specifically. For a
  real warehouse we'd use Alembic or similar. Documented in the
  function docstring as proportional to current scope.
- **No Gold incremental builds.** Each batch rebuilds every artifact
  from scratch. For our row counts this is negligible (sub-second);
  for production with millions of rows we'd add per-artifact
  incremental refresh patterns. Out of scope for the brief.
- **No cross-batch Gold semantics.** Each batch's Gold partition
  reflects only that batch's Silver state. There's no "rolling Gold
  view across all batches" yet — though queries that need it can
  use `read_parquet('data/lake/gold/<artifact>/**/*.parquet')` to
  scan all partitions.

## Alternatives considered

### Materialised Parquet only (no DuckDB views)

Build the artifacts as Parquet files; analysts use Pandas to query
them. No DuckDB dependency.

**Rejected because** the rolling-average window function would be
substantially more complex in Pandas (sorting, partition iteration,
boundary handling). The analyst-facing payoff is also weaker — `SELECT *
FROM top_scorers LIMIT 10` is a 30-second demo; "read this parquet
in pandas and groupby" is not.

### DuckDB views only (no materialisation)

Define Gold artifacts as DuckDB views; queries scan Silver Parquet
each time. No materialisation step.

**Rejected because** it loses parity with Bronze/Silver's auditable
pattern — no Parquet directory to inspect, no row count in the audit
DAO, no partition-on-disk for a reviewer to grep. The "files on disk"
property of the Medallion architecture is genuinely valuable for
auditability, and matches reviewer expectations.

### Pandas/Spark engine, no SQL

Compute aggregates in Python via the existing engine abstraction
(`engine.with_derived_column` etc.). Engine-agnostic across Pandas
and Spark.

**Rejected because** SQL is unambiguously the right tool for window
functions and range joins. We have an engine abstraction for the
ETL layers (Bronze, Silver) where pure-Python operations make
sense; analytics is a different problem with a different idiomatic
tool. **Using the right tool for the job rather than dogmatically
applying one paradigm.**

### SQL definitions in YAML

Each artifact's SQL lives in a YAML file. Same framework-as-config
pattern as ADR-0002.

**Rejected because** the YAML loader's failure mode for a column-name
typo is opaque (loader succeeds; runtime DuckDB error). The Python-
constant approach fails at import. SQL is code; the metadata around
it is config.

### Generic SQL DSL parser

Define a higher-level DSL that compiles to SQL.

**Rejected immediately.** Substantial codebase for marginal value.
The five artifacts are clearly expressed in plain SQL; a DSL would
add cognitive load without removing real complexity.

### Adding an `AGGREGATED` lifecycle state

Extend `FileStatus` with `AGGREGATING → AGGREGATED` to track Gold's
contribution as a lifecycle stage.

**Rejected because** the state machine should describe what
happened to the data, not what happened to derived views of the data.
A source's "lifecycle" is genuinely complete at `TRANSFORMED`;
Gold uses the result. Tracking gold_row_count and emitting a
GOLD_FINISHED event captures the contribution without polluting
the state semantics.

### Joint-source audit attribution for Gold

Update audit rows for every source consumed by a Gold artifact
(e.g. `top_scorers_by_season` updates `appearances`, `players`,
`clubs` audit rows).

**Rejected because** it pollutes the audit story. A Gold artifact
that takes 12 rows from appearances and 5 from clubs would record
`gold_row_count=12` on appearances and `gold_row_count=5` on clubs
— but the 5 isn't really "what Gold did with clubs," it's "how many
distinct clubs participated." Source-grain attribution to ONE
primary source per artifact is honest about what the row count
means.

### Building a Silver layer for `player_valuations`

Force the source through a Silver builder for symmetry with the
others.

**Rejected because** the Silver layer would have no transformations
to apply — `player_valuations` is already in the right shape for
the rolling-average aggregation. Adding a Silver layer would be a
redundant copy with no business value. The audit DAO honestly
reflects "this source skipped Silver" via the NULL `silver_row_count`
and `INGESTED` status, with the `gold_row_count` showing it was
consumed by Gold.

## See also

- Implementation:
  - `src/gold/duckdb_session.py` (connection lifecycle + view registration)
  - `src/gold/artifacts.py` (five typed GoldArtifact constants)
  - `src/gold/builders.py` (`build_gold_artifact` — execute SQL + materialise)
  - `src/gold/run.py` (CLI runner with audit integration)
  - `src/metadata/audit.py` (`record_gold_complete`, `GOLD_FINISHED` event)
  - `src/metadata/db.py` (`gold_row_count` schema column + migration)
- Tests:
  - `tests/test_gold_duckdb.py` (6 session/view tests)
  - `tests/test_gold_artifacts.py` (28 artifact correctness tests)
  - `tests/test_gold_run.py` (13 runner integration tests)
- Related:
  - ADR-0001 (Audit Table Design) — source-grain attribution pattern
  - ADR-0002 (Source Registry as a Framework) — framework-as-config
    pattern that this ADR deliberately departs from for SQL
  - ADR-0005 (SCD2 Implementation) — as-of-event resolution pattern
    expressed in SQL here for `player_valuation_rolling_avg`
