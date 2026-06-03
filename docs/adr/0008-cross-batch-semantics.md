# ADR-0008: Cross-Batch Semantics

## Status

Accepted — 2026-06-03

## Context

Phase 6 introduced day-2 testing — running the pipeline a second time
against a fresh vendor snapshot containing deliberate changes (a player
transfer, raw position label change, and new matchday data).

Day-2 testing exposed four design decisions that hadn't been forced by
single-batch operation:

1. **Where does Silver/DQ find Bronze data when file-grain idempotency
   skips re-writing it?** Phase 2b's idempotency (ADR-0003) was correct
   in isolation, but it created an implicit downstream contract:
   *"Silver and DQ must always find Bronze data for source X under
   partition `batch_id=X`."* That contract was undocumented and silently
   broken on day-2 for the three unchanged sources.

2. **What does "overwrite" mean for a partitioned Silver table?**
   The Phase 1 engine writer called `shutil.rmtree(target)` before
   every write — wiping all existing batch partitions on each Silver
   run. This was a latent bug, invisible until Phase 6 first exercised
   multi-batch storage.

3. **What timestamp should anchor a new SCD2 version's effective_date?**
   Phase 3 implicitly used the batch timestamp. Day-2 made the
   alternative — using a vendor-supplied "change date" — visible as a
   real choice rather than an oversight.

4. **Should SCD2 produce a new version when only the RAW vendor label
   changes but the canonical projection is unchanged?** Phase 3's
   hash-based detection includes raw columns; we just hadn't seen it
   exercised until Neuer's "GK"→"Goalkeeper" change in day-2 data.

Each of these is a real cross-batch concern. Phase 6 documents the
reasoning chain.

## Decision

### Cross-batch Bronze resolver

A new module `src/bronze/resolver.py` provides
`resolve_bronze_partition(bronze_root, source_name, batch_id)` —
returns the partition path that actually contains Bronze data for
this source/batch.

The resolver checks three cases in order:

1. **Current-batch partition exists on disk.** Return it directly.
   The common case for sources that changed in this batch.

2. **Current-batch audit row exists but no disk data.** This is the
   file-grain-skip signature: Bronze recorded an audit row claiming
   ingestion but skipped re-writing identical bytes. The resolver
   follows the file's MD5 checksum to find a *different* batch where
   the data was actually written, and returns that partition path.

3. **Neither exists.** Return `None`. Callers decide what to do
   (Silver runner records a per-artifact failure; DQ FK lookup
   builder triggers fail-open on the absent rule key).

The resolver is consulted by both:

- **`src/silver/run.py`'s `_read_bronze`** — Silver dim/fact builders
  now work transparently regardless of whether Bronze freshly wrote
  the source or skipped it via file-grain idempotency.

- **`src/dq/runner.py`'s `build_fk_lookups`** — DQ's FK target sources
  are reachable across batches. Crucially: missing FK target sources
  produce an *absent dict key* (which triggers the FK rule's fail-open
  semantics), NOT an empty set (which would falsely fail every row).

This elevates the previously-implicit contract to an explicit
single-function abstraction. New consumers of cross-batch Bronze data
just call the resolver.

### Partition-aware overwrite semantics

`PandasEngine.write_parquet()` now distinguishes between two write modes:

- **Non-partitioned writes**: full-target rmtree before write (existing
  behaviour). Single-file outputs.

- **Partitioned writes**: rmtree ONLY the specific partition
  subdirectories present in the incoming DataFrame. Other batches'
  partitions on disk are left untouched. Then pyarrow's
  `existing_data_behavior="overwrite_or_ignore"` handles the partition
  contents.

The pre-fix behaviour silently destroyed prior batches' partitions on
every Silver run — a bug latent since Phase 1, invisible until day-2.
The fix is partition-aware: it knows the specific partition values in
the incoming DataFrame and only wipes those subdirectories.

This is NOT just "don't rmtree at all." A naive fix that always skips
rmtree would create a *different* bug: re-running the same batch_id
would append duplicates because pyarrow's overwrite_or_ignore doesn't
guarantee removal of files outside the new dataset's partition
structure. The correct semantics is "wipe the specific partitions we're
about to write, leave others alone."

Multi-column partitioning isn't used in this codebase yet; for that
case the implementation falls back to full-target wipe with an
explanatory comment, as a deliberate scope choice.

### Observation-time SCD2

When a new SCD2 version is produced for a tracked column change, its
`effective_date` is set to the **batch run timestamp**, not the date
the change actually occurred in the business world.

For Saka in day-2: we observe that he's now at Chelsea (club_id=3).
We don't know *when* he transferred — the Kaggle dataset doesn't
provide change-date fields. The batch timestamp is the best proxy
we have for "when we observed this change."

The consequence is that fact_appearances dated 2025-01-18 (after a
hypothetical real-world transfer date but before the day-2 batch run)
will resolve to the **Arsenal-era version**, not the Chelsea-era one.
The as-of-event invariant still holds: every fact's resolved
`player_sk` window contains its match date. The resolution is just
relative to *when we observed the change*, not *when the change
happened*.

This is observation-time SCD2. The alternative — event-time SCD2 —
would split versions by the vendor-supplied change date. We can't
implement event-time SCD2 without a change-date field. Documenting
the choice means a future engineer (or reviewer) knows this is a
deliberate modelling choice, not an oversight.

### Raw-vs-canonical SCD2 change detection

Phase 3's `dim_players` builder includes BOTH the raw vendor column
(`position`) and the canonical normalised column (`position_canonical`)
in the SCD2 hash. The natural-key + tracked-columns config in
`sources.yaml` lists `position` as a tracked column; the canonical
projection is additive, not replacement.

The deliberate test case: Neuer's day-2 raw `position` changes from
`"GK"` to `"Goalkeeper"`. Both normalise to canonical `"Goalkeeper"`.

The decision: **a raw change DOES produce a new SCD2 version, even
when the canonical projection is unchanged.**

Rationale: preserving vendor lineage. A future audit might ask "what
exact label did the vendor send us on this date?" An answer of
"canonical=Goalkeeper" loses information; an answer of "raw=GK,
canonical=Goalkeeper" preserves it. SCD2 is the dimension where
historical fidelity lives.

Analytics that don't care about the raw distinction can query
`position_canonical` from the current row (is_current=TRUE) and get
a stable answer. Auditors can join to the historical versions and
see the actual labels.

This sometimes produces "noise" SCD2 versions (a vendor changes
their label scheme without changing meaning). For the brief's
scope this is the right trade-off. A future enhancement could
add a `track_changes_in: [canonical_only | raw_or_canonical]`
config option per source.

## Consequences

**Gained:**

- **Cross-batch storage works correctly.** Bronze and Silver
  partitions for all prior batches survive subsequent runs. The
  full Medallion architecture genuinely supports multi-batch
  history, not just single-batch demos.

- **File-grain idempotency's downstream contract is explicit.**
  The resolver is the documented bridge between Bronze's skip
  semantics and Silver/DQ's read semantics. New consumers consult
  one function rather than re-implementing the logic.

- **The SCD2 cross-batch behaviour is pinned with 24 tests.** Most
  candidates can't demonstrate that SCD2 works correctly across
  batches because they never run a day-2. This codebase
  demonstrates it explicitly.

- **The observation-time vs event-time choice is now articulated
  rather than implicit.** A reviewer or future engineer reads the
  ADR and knows the model.

- **Raw vendor lineage is preserved in dim_players.** Auditors get
  full historical fidelity; analysts get canonical stability.
  Both audiences served from the same dimension.

**Given up:**

- **The resolver's checksum-following logic only works for sources
  that produce stable checksums.** A vendor that re-orders rows
  within a CSV would produce different checksums for semantically-
  identical files; the resolver would treat them as fresh and the
  file-grain skip wouldn't trigger. This is acceptable for our
  vendor (Kaggle delivers stable file orderings); production
  pipelines facing chaotic vendors would need a content-aware
  checksum strategy.

- **Observation-time SCD2 means historical analytics with vendor-
  supplied change dates won't be accurate.** If a future
  requirement needs event-time SCD2 (e.g. "show me Saka's transfer
  date precisely"), we'd need to extend the SCD2 builder to
  accept an `effective_date_from_column` config option. Out of
  scope for the brief.

- **Raw-vs-canonical SCD2 detection produces noise versions** when
  vendors change labels without changing meaning. For the brief's
  scope this is acceptable; production would likely add the
  `track_changes_in` config option.

- **The partition-aware overwrite fix doesn't help non-Pandas
  engines yet.** Spark's writer has different semantics — but
  since Spark is stubbed (Phase 7), this is a deferred concern.

- **Cross-batch full reads see ALL historical versions including
  closed ones.** Analyst-facing queries reading `dim_players` root
  must filter `is_current=True` for "current state" semantics.
  This is standard SCD2 behaviour but worth flagging for new
  consumers.

## Alternatives considered

### Per-batch Silver full reload from Bronze (no cross-batch state)

Treat each Silver run as completely independent: read all current-
batch Bronze data, produce a fresh dim_players from scratch (no
SCD2 merge against prior state). Each batch's Silver tables
contain only that batch's data.

**Rejected because** it abandons SCD2's whole purpose. The point of
SCD2 is to preserve historical state across batches. A per-batch
fresh-build dimension is just a snapshot, not a slowly-changing
dimension.

### Eager Bronze re-write under new batch partition (no file-grain idempotency)

Drop the file-grain skip. Every batch always rewrites Bronze under
its own partition, even for byte-identical files. Eliminates the
contract gap entirely.

**Rejected because** it defeats the I/O savings of file-grain
idempotency, which becomes substantial at full-Kaggle-dataset
scale (~9 GB of CSV). The resolver pattern preserves both
benefits — skip the write, resolve the read.

### Cross-batch symbolic links / hardlinks

When file-grain idempotency triggers, write a symbolic link under
the new batch's partition pointing at the original batch's
parquet file. Silver/DQ read transparently without needing a
resolver.

**Rejected because** symlinks break ergonomic things like
`pd.read_parquet(dim_root)` cross-partition reads (which would
see both the link and the target as duplicate rows). And they're
platform-dependent (Windows). The resolver approach keeps the
disk layout clean and explicit.

### Event-time SCD2 with synthetic change dates

When SCD2 produces a new version, attempt to infer the real
change date from related data (e.g. for a transfer, look at the
first appearance in the new club).

**Rejected because** this inference is fragile (what if the new
club hasn't played a match yet?) and conflates business logic
with audit logic. Observation-time SCD2 is honest about what we
know: the batch timestamp is when we observed the change.

### Canonical-only SCD2 hash (ignore raw column changes)

Hash only the canonical-projected columns; treat raw label changes
as no-ops.

**Rejected because** it loses vendor lineage. A reviewer asking
"what label did the vendor send us on date X?" has no answer.
The current decision preserves the lineage; analytics consumers
just query the canonical column.

### Adding a `track_changes_in: canonical_only` config option

Per-source toggle: `position.track_changes_in: canonical_only`
would make the SCD2 hash ignore raw changes for that column.

**Considered but deferred.** Worth adding when there's a real
production need; YAGNI for the brief's scope. The current
behaviour (track raw + canonical) is correct for the default;
the toggle would be a future enhancement when noise versions
become a real problem.

## See also

- Implementation:
  - `src/bronze/resolver.py` (cross-batch resolver)
  - `src/metadata/audit.py` (`find_most_recent_ingestion_for_source`)
  - `src/silver/run.py` (`_read_bronze` uses the resolver)
  - `src/dq/runner.py` (`build_fk_lookups` uses the resolver,
    absent-key triggers fail-open)
  - `src/engines/pandas_engine.py` (partition-aware overwrite)
  - `data/sample/day2/` (day-2 sample data with deliberate diffs)
- Tests:
  - `tests/test_bronze_resolver.py` (6 resolver tests)
  - `tests/test_scd2_day2.py` (14 SCD2 cross-batch tests)
  - `tests/test_silver_run_day2.py` (10 runner-level integration tests)
- Related:
  - ADR-0003 (Bronze Storage and Partitioning) — file-grain
    idempotency that this ADR's resolver makes explicit
  - ADR-0005 (SCD Type 2 Implementation) — the Phase 3 SCD2 design
    that this ADR's tests certify works correctly across batches
  - ADR-0006 (DQ Framework Design) — the FK rule's fail-open
    semantics that this ADR's missing-key fix correctly triggers
