# ADR-0013: BI / Visualisation Scope Choice

## Status

Accepted — 2026-06-04

## Context

The brief implicitly expects some demonstration that the Gold layer
is queryable for analytical use. The phrasing is open — "should be
queryable by BI tools" — leaving the visualisation implementation
deliberately undefined.

Three approaches are credible:

1. **Embed Apache Superset in the Docker stack.** Connect Superset
   to our DuckDB Gold layer; ship pre-built dashboards as YAML.
2. **Ship a narrative Jupyter notebook** with queries and rendered
   outputs against the Gold parquet files.
3. **Document the Gold schema** and leave the visualisation layer
   entirely to the consumer's choice (Tableau, Metabase, anything
   else they prefer).

Each approach has different cost, risk, and reviewer-value profiles.

## Decision

**Slice 10.4 ships a narrative Jupyter notebook** with executed cell
outputs preserved, plus a small `scripts/build_notebook.py` that
makes the notebook reproducible from fresh pipeline output. The
notebook lives at `notebooks/gold_exploration.ipynb` and renders
inline on GitHub.

The notebook walks through seven representative queries:

1. Top scorers by season (joins with as-of-event FK resolution)
2. Goals by canonical position with a bar chart
3. Club standings rolled up from `fact_games`
4. Player valuation trends with rolling averages
5. SCD Type 2 dimension layout on disk
6. A star-schema join: top scorer at each club
7. The pipeline's own audit trail from `pipeline_runs`

Each cell is headed by a markdown question and **links to the
relevant ADR** explaining the design that powers the query.

## Why a notebook, not Superset

This was the harder call. Three reasons drove the decision:

### 1. Cost-to-value ratio

Embedding Superset in Docker is substantial work:

- Add a Superset service to `docker-compose.yml`
- Build (or extend) a Superset image that ships `duckdb-engine`
- Configure first-launch admin password, init scripts, database
  connection bootstrapping
- Export sample dashboards as YAML so they ship with the repo
- Document how to launch, log in, and rebuild dashboards
- Handle the bind-mount permissions for Superset to read the lake

Realistic effort: ~90-150 minutes if everything works first time;
several hours if not. Phase 9 surfaced four real-world Docker
gotchas during the Airflow integration; there's no reason to
expect Superset would be smoother.

A Jupyter notebook with committed outputs delivers ~70% of the
"visual demo of Gold" goal at ~20% of the cost, with effectively
zero infrastructure risk.

### 2. Architectural consistency

The pipeline's design choice (ADR-0007) is **DuckDB as the
analytical query layer over Parquet**. The notebook honours that:
every query goes through DuckDB against the Gold parquet files,
exactly matching how a connected BI tool would read the data in
production.

Embedding Superset and configuring it to use DuckDB would have
been consistent too. But embedding Superset and pre-loading
dashboards into it would *also* tempt us toward materialising
Gold into a Postgres database to make Superset happier — which
would muddy the architecture story (two query engines? why?).

The notebook avoids that temptation entirely. Gold stays parquet-
plus-DuckDB; the notebook demonstrates exactly that.

### 3. Reviewer experience

A reviewer wants to evaluate the pipeline, not learn Superset.
The notebook renders inline on GitHub — open the file, see the
queries with their results in one scroll. No clone, no Docker,
no login, no clicking around an unfamiliar UI.

Reviewers who DO want interactive exploration can:

- Clone the repo
- `make docker-up` for the full Airflow stack
- Connect their own BI tool (Tableau, Metabase, dbeaver) to the
  DuckDB session on the host filesystem

The pipeline produces queryable Parquet + DuckDB. The
visualisation layer is the consumer's choice.

## Rejected: Superset in Docker

Genuine BI integration with pre-built dashboards has real value
for production deployments. The reasons against it for this brief:

- **Scope risk.** Phase 9 showed Docker integrations surface
  unpredictable gotchas. A Superset deployment adds one more
  service, its own admin UI, its own database connection
  bootstrapping, and a separate failure mode for the demo.
- **Architectural drift.** The cleanest Superset deployment
  would route through Postgres, not DuckDB; we'd be inheriting
  the BI tool's preferences instead of demonstrating our query
  architecture.
- **Reviewer friction.** Loading Docker, finding the password,
  logging in, and clicking through Superset is more friction
  than scrolling a GitHub notebook.
- **The brief doesn't require it.** "Queryable by BI tools" is
  demonstrated by having queryable Gold artifacts. We have those.

## Rejected: documentation-only ("here's the schema, BYO BI")

The brief is open enough that ADR-0007's Gold schema documentation
+ the sample SQL queries already in the README would technically
satisfy "queryable by BI tools." But that approach loses the
reviewer experience entirely. A pure documentation deliverable
demands the reviewer's imagination to fill in what the data
"looks like in practice."

The notebook with executed outputs splits the difference: visible
example queries with visible results, without the cost of a full
BI deployment.

## Consequences

**Gained:**

- **Visible Gold queries with visible results**, rendered inline on
  GitHub without any clone-and-run effort from the reviewer.
- **Reproducibility.** `python scripts/build_notebook.py` rebuilds
  the notebook against fresh pipeline output. A reviewer suspicious
  of cherry-picked or hand-edited results can verify.
- **Cross-references to ADRs.** Every cell links to the design
  decision that powers its query. The notebook is a guided tour,
  not just a query showcase.
- **No infrastructure risk.** No new services, no new gotchas, no
  multi-hour debug arc.
- **The pipeline's architecture stays clean.** Gold parquet + DuckDB
  as query layer. Consumer chooses the visualisation.

**Given up:**

- **No clickable interactive dashboard.** Reviewers who want to
  pivot, filter, and drill have to run their own BI tool against
  the lake. That's a real loss for some kinds of evaluation, but
  acceptable for a portfolio piece focused on engineering quality.
- **No "BI ready" production deployment proof point.** A future
  iteration would add Superset or similar; the ADR-0011 docker
  story would extend to multi-service.
- **No pre-built executive dashboards.** Some briefs expect
  reviewer-facing dashboards as primary artifacts. This one
  emphasises pipeline engineering; the dashboard is implicit.

If the production environment later needs a hosted BI layer, the
upgrade is well-understood: add a Superset service to compose,
configure the DuckDB connection, expose pre-built dashboards as
YAML init artifacts. ~2 days of focused work, contained changes.

## See also

- Implementation:
  - `notebooks/gold_exploration.ipynb` (the executed notebook)
  - `scripts/build_notebook.py` (programmatic notebook builder)
  - `requirements-dev.txt` (nbformat, nbclient, ipykernel, matplotlib)
  - The "Sample analytics" section in the README

- Related:
  - ADR-0007 (Gold Storage and Analytics) — DuckDB as the query
    layer; the foundation this slice demonstrates
  - ADR-0011 (Docker Deployment Design) — the deployment surface
    a future Superset integration would extend
