# =====================================================================
# Football Analytics Pipeline — Docker image
# =====================================================================
# Extends the official Airflow image with:
#   * Our pipeline source (src/, dags/, configs/, data/sample/)
#   * Our core Python dependencies (requirements.txt — pandas, duckdb,
#     pyarrow, structlog, pydantic, etc.)
#
# Design choices (see ADR-0011):
#
#  - Base image: apache/airflow:2.10.3-python3.12
#    Same Airflow version as requirements-airflow.txt and same Python
#    version that the test suite runs against. Avoids constraints-file
#    surprises that the README warns about.
#
#  - Non-root user: Airflow's image runs as user `airflow` (uid 50000).
#    We install dependencies as that user (not root) so files don't end
#    up with mismatched ownership when the container writes to bind
#    mounts. The `AIRFLOW_HOME=/opt/airflow` and PYTHONPATH must be
#    set after the COPY for the airflow user to import `src`.
#
#  - Layer ordering: requirements first (rarely changes), then source
#    (changes every commit). Docker's layer cache makes rebuilds fast
#    when only source changes.
#
#  - What is NOT copied: data/lake, data/dq_reports, data/metadata.db
#    (runtime state — provided via the bind-mounted ./data directory
#    per docker-compose.yml). .airflow/ (host-only standalone state).
#    .pytest_cache, __pycache__, .venv (developer cruft). All listed
#    in .dockerignore.
# =====================================================================

FROM apache/airflow:2.10.3-python3.12

# Set the airflow user explicitly for the copy/install steps.
USER airflow

# ---- Python dependencies (cached layer) -----------------------------
# Copy ONLY the requirements file first. This means subsequent source
# changes don't invalidate the (expensive) pip install layer.
COPY --chown=airflow:root requirements.txt /opt/airflow/requirements.txt

# Install core pipeline deps. We use --no-cache-dir to keep the image
# slim — pip cache layers are wasted bytes in a deployed image.
#
# Note: we do NOT install requirements-airflow.txt because the base
# image already provides Airflow 2.10.3. Installing it again would
# resolve the same packages we already have.
#
# We do NOT use --user here because the apache/airflow base image
# runs everything inside its own bundled venv at /home/airflow/.local/
# (the `airflow` user's site-packages). Plain `pip install` writes
# into that venv, which is what we want. `pip install --user` is for
# system-pip and is rejected inside any venv.
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

# ---- Source code (rebuilt every commit) -----------------------------
# Layout inside the container:
#   /opt/airflow/dags/        — Airflow auto-discovers DAGs here
#   /opt/airflow/src/         — our pipeline package
#   /opt/airflow/configs/     — YAML config files
#   /opt/airflow/data/sample/ — committed sample data (production
#                                replaces this via the bind mount)
#
# We use COPY --chown so the airflow user owns these directories at
# runtime — required for any task that writes (e.g. metadata DB
# initialisation) when the bind mount maps to ./data.
COPY --chown=airflow:root src/        /opt/airflow/src/
COPY --chown=airflow:root dags/       /opt/airflow/dags/
COPY --chown=airflow:root configs/    /opt/airflow/configs/
COPY --chown=airflow:root data/sample/ /opt/airflow/data/sample/

# ---- Environment ----------------------------------------------------
# PYTHONPATH lets `from src.bronze.run import run_bronze` work in the
# DAG file. Without this Airflow's scheduler fails to parse the DAG
# with "No module named 'src'" — same gotcha as the local standalone
# setup documented in ADR-0010.
ENV PYTHONPATH=/opt/airflow:${PYTHONPATH}

# Airflow's default DAGs folder is /opt/airflow/dags, which we
# populated above. Explicit declaration here both documents the
# choice and protects against future image-base changes.
ENV AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/dags
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False

# Default SequentialExecutor — single-process task execution that works
# with SQLite. This is what `airflow standalone` runs under the hood
# (it does NOT run LocalExecutor as the name suggests). Airflow 2.10
# now refuses LocalExecutor + SQLite at startup because the combination
# can race on writes; SequentialExecutor is the right pairing.
#
# Functionally identical for our DAG: the pipeline is a strict linear
# chain (bronze → silver → dq_gate → gold), so there's no parallelism
# to give up. LocalExecutor + Postgres is the named production upgrade
# path (see ADR-0010 and ADR-0011) when concurrent task execution
# becomes valuable.
ENV AIRFLOW__CORE__EXECUTOR=SequentialExecutor

# WORKDIR matters for the runners — they use relative paths like
# `data/lake/...` which resolve against `os.getcwd()`. Without this
# the bind mount at /opt/airflow/data wouldn't be found.
WORKDIR /opt/airflow
