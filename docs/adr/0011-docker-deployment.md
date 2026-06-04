# ADR-0011: Docker Deployment Design

## Status

Accepted — 2026-06-04

## Context

The brief (§8) calls for a Docker-based deployment. ADR-0010
established Airflow orchestration with `airflow standalone` for the
local-development experience; Phase 9 packages that into a
containerised stack that a reviewer can launch in one command.

Five sub-questions to answer:

1. **Compose file structure** — single file or split?
2. **Image strategy** — pull official Airflow image as-is, or build a
   custom image extending it?
3. **Storage strategy** — named Docker volumes or bind mounts for the
   pipeline's lake and metadata?
4. **Airflow metadata DB** — stay on SQLite, or add Postgres?
5. **Executor choice** — and how does this reconcile with ADR-0010's
   claim of LocalExecutor?

Phase 9 also surfaced **three real-world Docker gotchas** that no
amount of code review would have caught — each is documented at the
end of this ADR as a "lessons learned" section.

## Decision

### Single `docker-compose.yml` with one service

One container running `airflow standalone` (scheduler + webserver +
worker as a single process), one bind mount for `./data`, one bind
mount for `./.airflow`, port 8080 exposed.

This is the minimum-viable-stack for the brief's requirement. It
doesn't pretend to be production: production would split scheduler /
webserver / worker into separate containers with Postgres as the
metadata store and Redis as the broker for CeleryExecutor. We name
that explicitly as the upgrade path rather than building it.

**Rejected: split compose files** (an `airflow.yml` + `pipeline.yml`
overlay pattern). Production-shaped but adds cognitive overhead for
no functional gain at this scale. A reviewer should be able to read
one file and understand the whole deployment.

### Custom Dockerfile extending `apache/airflow:2.10.3-python3.12`

Rather than pulling the official image and mounting our `src/` as a
volume, we build a custom image that bakes the pipeline code in. The
image gets tagged `football-analytics-pipeline:dev`.

Three reasons:

1. **Volume-mounting Python source is fragile.** Every Mac/Linux/Windows
   path quirk becomes a debugging session. Baking source in is the
   production pattern and avoids the class of "but it worked locally"
   bugs.
2. **A Dockerfile IS the artifact.** Reviewers want to see how the
   image is built: which base, which deps, which env, which entrypoint.
   A Dockerfile makes that visible; volume-mounted source hides it.
3. **`pip install -r requirements.txt` inside the image** locks the
   exact Pandas/PyArrow/DuckDB versions our tests pinned. Mounting
   source from the host would still need a `pip install` somewhere; a
   build-time install is the cleanest place.

**Layer ordering matters.** `requirements.txt` is copied and installed
before `src/`, `dags/`, `configs/`, `data/sample/`. Docker's layer
cache means source-only changes (every commit) don't trigger a
~90-second pip install rebuild — only the affected layers rebuild.
First-time build is ~3-5 minutes; rebuilds with no requirements
changes are ~10 seconds.

### Bind mounts for `./data` and `./.airflow`

`./data:/opt/airflow/data` — the pipeline's lake, DQ reports, and
SQLite metadata DB live on the host filesystem. While the container
is running, `ls data/lake/gold/top_scorers_by_season/` on the host
shows the parquet files the container just wrote. **Demos are about
visibility.** A named Docker volume would hide the data; a bind
mount makes it tangible.

`./.airflow:/opt/airflow/.airflow` — Airflow's SQLite DB, task logs,
and auto-generated admin password persist across container restarts.
The host can read `.airflow/simple_auth_manager_passwords.json.generated`
if the password scrolls off the terminal.

`AIRFLOW_HOME=/opt/airflow/.airflow` is set explicitly in the compose
file. Without this Airflow defaults `AIRFLOW_HOME=/opt/airflow` —
which is a read-only image layer, and Airflow can't write its DB
there. **This is a non-obvious gotcha** that catches first-time
integrators.

**Rejected: named Docker volumes** (`pipeline_data:/app/data`). They
persist correctly across restarts, but they're opaque to the host
filesystem — the reviewer can't browse them from their terminal. Loss
of demo visibility outweighs the marginal "production cleanliness"
gain.

### SQLite for Airflow metadata; LocalExecutor + Postgres as production upgrade

`airflow standalone` uses SQLite at `$AIRFLOW_HOME/airflow.db`. We
keep that. The metadata DB persists via the `./.airflow` bind mount.

Adding Postgres would mean a second service in `docker-compose.yml`,
plus an init step (`CREATE DATABASE airflow; CREATE USER ...`), plus
`AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` configuration, plus
`airflow db migrate` for the Postgres schema. That's ~30 minutes of
infrastructure work for no functional gain when our DAG is a linear
chain of four tasks running once per day.

**Rejected: Postgres for the demo.** It IS the right choice when
moving to LocalExecutor + concurrent task execution, but that's the
production upgrade path, not the deliverable.

### `SequentialExecutor`, NOT `LocalExecutor`

This is the **most important correction** in this ADR.

ADR-0010 claimed "LocalExecutor." That was wrong — `airflow standalone`
actually runs `SequentialExecutor` under the hood. Phase 9 surfaced
the mismatch when our Docker compose tried to set
`AIRFLOW__CORE__EXECUTOR=LocalExecutor` and Airflow 2.10 refused to
start with:

```
AirflowConfigException: error: cannot use SQLite with the LocalExecutor
```

Airflow 2.10 added an explicit guard against this combination because
LocalExecutor runs multiple worker processes, and SQLite's
single-writer model races when multiple processes try to write
concurrently. The combination has always been fragile; Airflow now
refuses it at startup rather than silently corrupting data.

**Two ways out:**

1. **Switch to SequentialExecutor** (matches SQLite, single-process,
   no parallelism). Functionally identical for our DAG because it's
   a linear chain — no parallelism to give up.
2. **Add Postgres** (unlocks LocalExecutor). Per Decision D above,
   we deliberately don't.

We chose option 1. ADR-0010 has been amended to reflect this; the
description there now correctly says "SequentialExecutor (the
single-process executor that `airflow standalone` uses under the
hood)" rather than the misleading "LocalExecutor."

The production upgrade path is now precise: **LocalExecutor +
Postgres**, which enables concurrent task execution if we ever build
a DAG with parallel branches. For the linear chain we have, the
upgrade isn't needed.

## Three gotchas surfaced during Phase 9 build

### Gotcha 1: `pip install --user` inside a venv

First Dockerfile draft used `RUN pip install --user -r requirements.txt`.
The build failed at step 3:

```
ERROR: Can not perform a '--user' install.
User site-packages are not visible in this virtualenv.
```

The `apache/airflow` image runs everything inside its bundled venv
(`/home/airflow/.local/`). When inside a venv, `pip install --user`
is meaningless and explicitly rejected — `--user` is for system pip,
not venv pip.

**Fix:** Drop the `--user` flag. Plain `pip install` writes to the
venv's site-packages, which is exactly what we want.

### Gotcha 2: Bind-mounted `.airflow/` with stale host paths

First `docker compose up` failed with this in the logs:

```
PermissionError: [Errno 13] Permission denied: '/Users'
```

Diagnosis: the bind-mounted `.airflow/` directory contained an
`airflow.cfg` from previous local-standalone testing. That cfg
embedded the host's absolute paths (`/Users/shashikiran/...`) into
config keys like `base_log_folder`. When the container mounted the
directory, Airflow read the cfg and tried to write logs to
`/Users/shashikiran/...` **inside the container** — which doesn't
exist there.

**Fix:** Wipe `.airflow/` before first container launch (`rm -rf
.airflow; mkdir -p .airflow`). The container then writes fresh
config with container-correct paths.

**Long-term protection:** This is now documented in the README's
Docker walkthrough, and `make docker-up` could be extended to wipe
stale config automatically (deferred — explicit `rm -rf .airflow` is
clearer for now than implicit destruction).

### Gotcha 3: SequentialExecutor vs LocalExecutor (covered above)

ADR-0010 misnamed the executor. Docker integration testing surfaced
the mismatch. Both ADRs now reflect reality.

### Gotcha 4: bidirectional cfg pollution via the bind mount

Gotcha 2 above is *host config polluting the container*. After running
the Docker stack successfully, the **inverse** bug shows up: container
config pollutes the host.

Symptom: running `pytest tests/` on the host (after Docker had been
running) fails with:

```
PermissionError: [Errno 13] Permission denied: '/opt/airflow'
```

The host's Airflow can't create `/opt/airflow/.airflow/logs/...` —
because `/opt/airflow` is the *container's* path, not the Mac's. The
container wrote a fresh `airflow.cfg` via the bind mount, baked with
container-internal paths, and that cfg now persists on the host
filesystem. Next time the host Python venv's Airflow reads it, it
gets the container paths.

This is the SAME ROOT CAUSE as Gotcha 2, just in the other direction.
The `./.airflow` bind mount makes the cfg visible to both environments,
and whichever runs most recently wins. **The bind mount cuts both
ways.**

**Fix (same as Gotcha 2):** Wipe `.airflow/` when switching environments:

```bash
docker compose down                # if Docker was running
rm -rf .airflow && mkdir -p .airflow
export AIRFLOW_HOME=$(pwd)/.airflow # for the host
airflow db migrate                  # regenerate cfg with host paths
```

**Long-term options:**

1. **Discipline** (current): document the wipe in the README. Reviewers
   doing a clean demo pick one path (Docker OR host) and don't switch.
2. **Separate AIRFLOW_HOME directories** for each environment.
   Host uses `~/.airflow-host/`; container uses `./.airflow/`. They
   never share the cfg file.
3. **Stop running host Airflow entirely.** Once Docker works, the
   local `airflow standalone` workflow is redundant.

For the deliverable, we chose option 1 — bind-mount visibility is a
real demo benefit, and the wipe is one command. Future iterations
could adopt option 2.

### Pattern

Phase 6 surfaced three bugs (resolver gap, partition-destructive
writer, Makefile gap). Phase 8 surfaced two more (lexicographic
batch_id, single-step resolver). Phase 9 surfaces four more
(pip --user, stale host cfg in container, executor mismatch,
container cfg polluting host). **The pattern continues: every
infrastructure-level integration finds latent issues that pure
unit testing missed.** This is exactly what end-to-end testing
is for.

## Consequences

**Gained:**

- **One-command launch.** `make docker-build && make docker-up`
  brings up the full pipeline in ~3 minutes (first time) or ~10
  seconds (subsequent runs).
- **Bind-mount visibility.** Reviewer can browse the lake on their
  host filesystem while Airflow is running. Tangible, not abstract.
- **Persistent state across restarts.** Stopping and restarting the
  container preserves the metadata DB, audit DAO, and lake. No
  re-runs required to demo idempotency.
- **Cross-environment idempotency proven.** The same `data/metadata.db`
  is read by host-side Airflow runs AND Docker-container Airflow
  runs. Both correctly agree on which batches succeeded.
- **A Dockerfile that reviewers can read** — base, deps, env, copy
  layout all explicit. Production-pattern artifact.
- **Three documented gotchas** that future contributors don't need to
  rediscover. The README and this ADR name them with diagnoses.

**Given up:**

- **No production-grade executor.** SequentialExecutor runs one task
  at a time. For our linear DAG that's fine; for a parallel DAG
  we'd need LocalExecutor + Postgres (the upgrade path).
- **No high availability.** Single container running scheduler +
  webserver + worker means any crash stops the whole pipeline. The
  Phase 8 retry semantics partially compensate, but production
  would split these into separate containers with restart policies.
- **No secrets management.** The auto-generated admin password is
  fine for a demo; production would use Airflow's secrets backends
  (Vault, AWS Secrets Manager, etc).
- **No observability stack.** Production would wire Airflow to
  Prometheus/Grafana, structured log shipping (e.g. to Datadog or
  CloudWatch), and metrics dashboards. Deferred.

## Alternatives considered

### Use the official `apache/airflow` image with `src/` volume-mounted

`docker run -v ./src:/opt/airflow/src apache/airflow:2.10.3 ...`.
No custom Dockerfile.

**Rejected because** volume-mounting Python source is a known source
of path/permission/import issues that don't reproduce reliably. Also,
without a build step we'd have nowhere clean to install the pipeline's
core deps (`pandas`, `duckdb`, etc) — they'd need to be installed at
runtime via the entrypoint, which is slow and breaks airgap.

### Postgres + LocalExecutor instead of SQLite + SequentialExecutor

Add a Postgres service, configure `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`
to point at it, use LocalExecutor for true parallelism.

**Rejected because** our DAG is a strict linear chain. There's
nothing to parallelise. The extra service is added complexity for
no functional gain. We name this as the production upgrade path
where the DAG actually needs parallel branches.

### CeleryExecutor with Redis broker + separate worker

Full distributed Airflow: scheduler, webserver, Redis, multiple
workers, Postgres. 5+ containers.

**Rejected because** at the brief's data scale (~9 GB max), there's
nothing to distribute. Same scope-discipline pattern as Spark in
ADR-0009 — we build what's needed, name the upgrade explicitly.

### Multi-stage Dockerfile

Build the venv in a builder stage, copy into a slim runtime stage.
Reduces image size from ~2 GB to ~1.5 GB.

**Rejected because** the base `apache/airflow` image is already
~1.5 GB and we can't easily strip it. Multi-stage would save ~10%
for ~30 minutes of Dockerfile complexity. Not worth it for a
deliverable that prioritises clarity over byte-optimisation.

### Skip Docker entirely

ADR-0010 already established `airflow standalone` as the local-dev
experience. Could argue Docker adds nothing the brief truly needs.

**Rejected because** the brief explicitly mentions §8 deployment
requirements, and a "Docker-compose-up-and-go" demonstration is
table stakes for any modern data engineering portfolio. The
1-command launch IS the deliverable's user experience.

## See also

- Implementation:
  - `Dockerfile` (the image build)
  - `docker-compose.yml` (the service definition)
  - `.dockerignore` (build context filter)
  - `Makefile` (`make docker-build`, `make docker-up`, etc)
- Related:
  - ADR-0001 (Audit Table Design) — the `pipeline_runs` table that
    powers cross-environment idempotency (host Airflow + Docker
    Airflow read the same DB)
  - ADR-0010 (Airflow Orchestration Design) — amended to reflect
    SequentialExecutor; LocalExecutor + Postgres named as production
    upgrade path
  - The README section "Running with Docker" — operator-level
    walkthrough with the three gotchas documented inline
