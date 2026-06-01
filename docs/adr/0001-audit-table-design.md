# ADR-0001: Audit Table Design

## Status

Accepted — 2026-05-31

## Context

Production data pipelines must answer questions like:

- Did yesterday's `players.csv` actually arrive from the vendor?
- Is the file we received today the same one we processed yesterday, or
  did the vendor re-send a corrected version?
- How many rows did the source provide, and how many made it to Silver?
  If there's a gap, where did the missing rows go?
- When was each file processed, by which batch, with what outcome?
- If a downstream analyst reports a discrepancy in Gold, can we trace
  the lineage back to specific source files?

Without explicit answers to these questions, "the pipeline lost data" is
indistinguishable from "the data was never there." Regulatory environments
(pharma, banking, insurance) treat that ambiguity as a critical operational
risk. Outside regulated environments, it still costs human time to
investigate every reported discrepancy.

The `pipeline_runs` table already tracks idempotency at **layer grain**
("did Bronze succeed for batch X?"), but layer grain is too coarse:
- It can't distinguish 100% success from 99% success with one bad file
- It can't drive file-level idempotency ("we already ingested these
  exact bytes last week — skip")
- It can't support reconciliation queries that compare row counts across
  stages

We need a separate table at **file grain**.

## Decision

We add **two complementary tables** to the metadata DB, both owned by
the `audit` DAO:

### `file_audit` — mutating, one row per (batch_id, source_file_path)

Always reflects the current state of a file's journey through the
pipeline. Stores:

- **File fingerprint:** size, MD5 checksum, schema version hash
- **Provenance timestamps:** vendor's last-modified (when available)
  AND filesystem mtime (always known), plus an enum recording which
  source we got the vendor timestamp from
- **Row counts at each stage:** source, Bronze, Silver, rejected
- **Status and timing:** lifecycle state, started/finished/registered
  timestamps, error message and the stage that raised on failure

Primary key: `(batch_id, source_file_path)`. SQLite enforces this; no
two rows can describe the same file in the same batch.

### `file_audit_events` — append-only, one row per state transition

Captures the forensic timeline. Every state-changing DAO function
writes a row here in the **same transaction** as the mutating-row
update. Stored:

- `event_id` (autoincrement, ordering anchor)
- `batch_id`, `source_file_path` (foreign key back to file_audit)
- `event_type` (one of nine enum values: registered, ingest_started,
  ingest_finished, dq_completed, silver_started, silver_finished,
  reconciled, failed, schema_drift_detected, vendor_timestamp_unavailable)
- `event_payload` (JSON blob carrying context — counts, error messages,
  schema diffs)
- `occurred_at`

### Atomicity guarantee

Every state-changing function in the DAO opens an explicit transaction
(`BEGIN`), writes both the `file_audit` UPDATE and the
`file_audit_events` INSERT, and commits. On any exception, it rolls
back. The two tables can never drift apart.

### Reconciliation engine

A separate function `reconcile_batch(batch_id)` walks every file and
returns a list of `ReconciliationFinding` objects classified as
`CRITICAL` or `WARN`. Rules:

| Code                       | Severity | Trigger                                           |
| -------------------------- | -------- | ------------------------------------------------- |
| `bronze_inflated`          | CRITICAL | `bronze_rows > source_rows` (invented rows)       |
| `row_count_drift`          | CRITICAL | `silver_rows ≠ bronze_rows − rejected_rows`        |
| `complete_silver_loss`     | CRITICAL | `silver_rows == 0 AND bronze_rows > 0`             |
| `high_reject_rate`         | WARN     | `rejected_rows / bronze_rows > 5%`                 |
| `empty_source_file`        | WARN     | `bronze_rows == 0`                                 |
| `non_terminal_status`      | WARN     | File didn't reach `transformed` or `failed`        |

The function returns findings; it does not raise. The DAG task that
calls `reconcile_batch` decides what to do — Phase 8 will fail the
task on CRITICAL findings and warn on WARN.

### Source-grain attribution for DQ and Silver

DQ checks operate on Bronze *tables*, not individual files. A source
with multiple shard files would lose per-file reject attribution.
The DAO functions `record_quarantine(source_name, rejected_row_count)`
and `record_silver_complete(source_name, silver_row_count)` therefore
attribute counts at **source grain**.

For the Kaggle dataset (one file per source), this is moot. For
multi-file vendors, this is a documented limitation — Phase 4+ could
add a `_rejected/source_file_path` column to enable per-file attribution
if a real consumer needs it.

### Vendor timestamp handling

We split timestamps into two columns:

- `source_modified_at_vendor` — vendor's authoritative last-modified
  time (from Kaggle API's `lastUpdated`, or HTTP `Last-Modified` header)
- `source_modified_at_filesystem` — file's mtime on our disk (always
  known)

Plus a third column `vendor_timestamp_source` recording how we obtained
the vendor timestamp (`manifest`, `http_header`, or `filesystem_only`
when no vendor signal was available).

When no vendor timestamp is available, `register_file` records
`vendor_timestamp_source = 'filesystem_only'` AND emits a
`vendor_timestamp_unavailable` event. The dashboard can surface this as
"incomplete provenance" — operators see clearly which files have real
vendor provenance vs only filesystem-time.

## Consequences

**Gained:**

- File-grain idempotency: `find_previous_successful_ingestion(checksum)`
  can skip files we've already processed even across batches.
- Vendor-resend detection: same filename + different MD5 raises
  `AuditConflictError`; same filename + same MD5 is silently idempotent.
- Forensic timeline: every state change is auditable in chronological
  order, regardless of how the current state evolved.
- Reconciliation as a first-class deliverable: row-count math across
  stages is a 6-rule engine, not ad-hoc comparison code in the DAG.
- Schema drift detection groundwork: schema hash is computed and stored;
  drift events have a payload column for diff data.
- Honest provenance: `vendor_timestamp_source` makes it explicit whether
  we trust the timestamp or are relying on filesystem mtime.

**Given up:**

- Two tables to maintain instead of one. Mitigated by the DAO owning
  both atomically — callers see one API surface.
- Storage cost. Estimated ~30 MB/year of event rows at hourly cadence
  for the Kaggle dataset; negligible.
- Per-file attribution for DQ rejects in multi-file sources. Documented
  limitation, not a blocker for the current dataset.

## Alternatives considered

### Single-row design (no event log)

Keep only `file_audit`, mutate the row through its lifecycle. Cheaper
to write and query, simpler API.

**Rejected because:** the row only remembers its *current* state. When
something goes wrong, you can't replay the timeline to understand *how*
it got there. The forensic value of the event log is exactly the
"how did we get here" answer.

### Event-sourced only (no mutating row)

Drop `file_audit`, derive current state by replaying the events. Pure
event-sourcing pattern.

**Rejected because:** every "current state" query becomes an aggregation
over the event log. For a dashboard query like "show all files currently
in `failed` status", that's ~10× more SQL than the mutating-row design.
The 30 MB/year storage saving wasn't worth it.

### Two separate timestamp columns without a source enum

`source_modified_at_vendor` nullable, `source_modified_at_filesystem`
always populated, no enum column.

**Rejected because:** the dashboard would have to infer which timestamp
is authoritative ("is vendor_at NULL? then it's filesystem-only"). The
explicit enum value makes the meaning unambiguous in queries and joins.

### Single combined timestamp column

One column, document that it's "vendor when known, filesystem when not."

**Rejected because:** loses information. Vendor timestamp and filesystem
timestamp are different facts; collapsing them loses the ability to
detect "the vendor said it was modified at T, but our file mtime is T+3h,
suggesting a slow download — was it interrupted?" That's a real
operational signal.

### Postgres for the metadata DB

Use a real RDBMS instead of SQLite.

**Rejected for now** because SQLite has zero ops cost (no extra service
in docker-compose), is file-based and trivially inspectable
(`sqlite3 data/metadata.db`), and is fully adequate for the row volumes
we generate (a few hundred per batch). The DAO pattern means swapping
to Postgres later is a connection-string change — the abstraction
preserves optionality.

## See also

- Implementation: `src/metadata/audit.py`
- Schema: `src/metadata/db.py`
- Tests: `tests/test_audit.py` (36 tests covering lifecycle, state
  machine, reconciliation rules, vendor-timestamp branches)
- Related: ADR-0002 (Source Registry as a Framework) — the source
  registry produces the `FileFingerprint` that `register_file` consumes
