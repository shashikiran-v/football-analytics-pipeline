"""
Football analytics pipeline DAG.

A daily DAG that orchestrates the full Bronze → Silver → DQ gate → Gold
flow for one batch. The batch_id is derived from Airflow's
`data_interval_start` (per ADR-0010); this gives same-day reruns natural
idempotency via the audit DAO's layer-grain skip semantics.

Pipeline shape
--------------

    bronze ──► silver ──► dq_gate ──► gold

Failure semantics
-----------------
* `bronze` failure aborts the whole DAG run. Downstream tasks don't run.
* `silver` failure aborts the run. Downstream tasks don't run.
* `dq_gate` failure aborts the run before Gold builds. This is the
  ADR-0006 hard-fail decision now landed: critical DQ failures
  affecting > 5% of rows stop the pipeline before downstream consumers
  see partial data.
* `gold` failure marks the DAG run failed but doesn't block other
  DAGs from running tomorrow.

Retry behaviour
---------------
Each task retries once, then propagates failure. The runners are
idempotent (re-running the same batch_id is a no-op via pipeline_runs)
so transient failures get one safe retry before paging anyone.

To install
----------
This DAG file is auto-discovered by Airflow when its parent directory
(`dags/`) is configured as `AIRFLOW__CORE__DAGS_FOLDER`. The
docker-compose stack from Phase 9 wires this up; for local development,
set the env var and run `airflow standalone`.

See ADR-0010 for the full orchestration design discussion.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.orchestration.airflow_wrappers import (
    dq_gate_task,
    run_bronze_task,
    run_gold_task,
    run_silver_task,
)

# Default args shared by every task in the DAG.
# Per ADR-0010: one retry per task is the right default. Runners are
# idempotent so retries are safe; more than one risks masking real
# problems behind churn.
default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="football_analytics_pipeline",
    description=(
        "Daily Bronze → Silver → DQ gate → Gold flow over the Kaggle "
        "football dataset. batch_id derives from data_interval_start; "
        "see ADR-0010 for design rationale."
    ),
    default_args=default_args,
    start_date=datetime(2024, 11, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["football", "medallion", "scd2"],
) as dag:
    bronze = PythonOperator(
        task_id="bronze",
        python_callable=run_bronze_task,
        op_kwargs={"raw_root": "data/sample"},
    )

    silver = PythonOperator(
        task_id="silver",
        python_callable=run_silver_task,
    )

    dq_gate = PythonOperator(
        task_id="dq_gate",
        python_callable=dq_gate_task,
        op_kwargs={"quarantine_threshold_pct": 5.0},
    )

    gold = PythonOperator(
        task_id="gold",
        python_callable=run_gold_task,
    )

    # Pipeline shape: a strict linear chain.
    # The DQ gate sits between Silver (which writes the report) and Gold
    # (which consumes Silver). Per ADR-0010 the gate is a separate task
    # rather than baked into Silver — keeping failure modes clear.
    bronze >> silver >> dq_gate >> gold
