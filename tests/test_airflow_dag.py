"""
Tests for the football_pipeline DAG structure.

Parses the DAG via Airflow's DagBag and validates:
  * The DAG parses without errors
  * The expected task IDs are present
  * The task dependencies form the right chain
  * Configuration (schedule, default_args, tags) is as expected

These tests skip when Airflow isn't installed — Airflow is an optional
dependency per ADR-0010.
"""

from __future__ import annotations

from pathlib import Path

import pytest


def _airflow_available() -> bool:
    try:
        import airflow  # noqa: F401
        return True
    except ImportError:
        return False


pytestmark = pytest.mark.skipif(
    not _airflow_available(),
    reason="Airflow not installed (it's an optional dependency)",
)


DAGS_FOLDER = Path(__file__).resolve().parents[1] / "dags"
DAG_ID = "football_analytics_pipeline"


@pytest.fixture(scope="module")
def dagbag():
    """Parse all DAGs in the dags/ folder via Airflow's DagBag.

    Module-scoped because parsing is expensive (~1s) and the DAG bag
    is immutable across tests in this file.

    Ensures Airflow's metadata DB is initialised — DagBag internally
    queries for run state, which fails on a fresh install with
    `no such table: dag`. We run `airflow db migrate` once to make
    sure tests are independent of prior Airflow initialisation.
    """
    # Initialise the Airflow metadata DB. This is idempotent — safe to
    # call repeatedly. Without this, tests fail on a fresh install
    # with sqlite3.OperationalError: no such table: dag.
    import subprocess
    subprocess.run(
        ["airflow", "db", "migrate"],
        check=False,  # already-initialised DB returns non-zero noise
        capture_output=True,
    )
    from airflow.models import DagBag
    return DagBag(dag_folder=str(DAGS_FOLDER), include_examples=False)


# ---------------------------------------------------------------------------
# Parse / import
# ---------------------------------------------------------------------------


class TestDagParses:
    def test_no_import_errors(self, dagbag):
        """The DAG file must parse without any import or syntax errors.
        Airflow's DagBag captures import failures into a dict; an empty
        dict means everything imported cleanly."""
        assert dagbag.import_errors == {}, (
            f"DAG import errors: {dagbag.import_errors}"
        )

    def test_dag_is_registered(self, dagbag):
        assert DAG_ID in dagbag.dag_ids


# ---------------------------------------------------------------------------
# Task structure
# ---------------------------------------------------------------------------


class TestTaskStructure:
    def test_expected_tasks_present(self, dagbag):
        dag = dagbag.get_dag(DAG_ID)
        task_ids = {t.task_id for t in dag.tasks}
        assert task_ids == {"bronze", "silver", "dq_gate", "gold"}

    def test_linear_chain_dependencies(self, dagbag):
        """The pipeline is a strict linear chain:
        bronze → silver → dq_gate → gold. No branching, no parallelism."""
        dag = dagbag.get_dag(DAG_ID)

        bronze = dag.get_task("bronze")
        silver = dag.get_task("silver")
        dq_gate = dag.get_task("dq_gate")
        gold = dag.get_task("gold")

        # bronze has no upstream, silver downstream
        assert bronze.upstream_task_ids == set()
        assert bronze.downstream_task_ids == {"silver"}

        # silver between bronze and dq_gate
        assert silver.upstream_task_ids == {"bronze"}
        assert silver.downstream_task_ids == {"dq_gate"}

        # dq_gate between silver and gold
        assert dq_gate.upstream_task_ids == {"silver"}
        assert dq_gate.downstream_task_ids == {"gold"}

        # gold is terminal
        assert gold.upstream_task_ids == {"dq_gate"}
        assert gold.downstream_task_ids == set()


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


class TestDagConfig:
    def test_schedule_is_daily(self, dagbag):
        dag = dagbag.get_dag(DAG_ID)
        # Airflow 2.4+ exposes schedule_interval or timetable.
        # @daily resolves to "@daily" or a timedelta(days=1) depending
        # on version; both are valid daily schedules.
        schedule = getattr(dag, "schedule_interval", None) or getattr(
            dag, "schedule", None
        )
        assert schedule == "@daily" or str(schedule) == "@daily"

    def test_default_retries_is_one(self, dagbag):
        """Per ADR-0010: one retry is the right default. Runners are
        idempotent so retries are safe; more than one risks masking
        real issues behind churn."""
        dag = dagbag.get_dag(DAG_ID)
        for task in dag.tasks:
            assert task.retries == 1, (
                f"Task {task.task_id} has retries={task.retries}, "
                f"expected 1 (per ADR-0010)"
            )

    def test_max_active_runs_one(self, dagbag):
        """Only one DAG run at a time. Prevents two batches from racing
        on the same Bronze/Silver/Gold partition. Combined with the
        runners' layer-grain idempotency this is belt-and-braces."""
        dag = dagbag.get_dag(DAG_ID)
        assert dag.max_active_runs == 1

    def test_catchup_disabled(self, dagbag):
        """No historical backfill on first deploy — operators trigger
        backfills manually with explicit run params if needed."""
        dag = dagbag.get_dag(DAG_ID)
        assert dag.catchup is False

    def test_tags_present(self, dagbag):
        dag = dagbag.get_dag(DAG_ID)
        assert "football" in dag.tags
        assert "medallion" in dag.tags
        assert "scd2" in dag.tags
