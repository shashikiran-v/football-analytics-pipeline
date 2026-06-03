"""
Airflow-aware wrappers around our pipeline runners.

Purpose
-------
The runners in src/{bronze,silver,gold}/run.py have NO Airflow knowledge —
they accept a `batch_id` string and return summary dataclasses. That
isolation is deliberate: the runners can be invoked from a CLI, a notebook,
a test, or Airflow without coupling.

This module provides the thin translation layer that Airflow uses:

  1. Maps Airflow's `data_interval_start` (a pendulum datetime) to our
     `batch_id` (YYYY-MM-DD string).
  2. Calls the underlying runner.
  3. Translates the runner's summary into Airflow's expected return values:
     - Push selected metrics to XCom for downstream tasks
     - Raise AirflowException if the runner reports a failed status
       (so Airflow marks the task FAILED rather than SUCCESS)

Why this module exists rather than calling runners directly
-----------------------------------------------------------
A PythonOperator that imports `run_bronze` directly would mostly work,
but two things are awkward:

  * The runners take keyword arguments. PythonOperator's `op_kwargs`
    works, but Airflow's `data_interval_start` is a pendulum.DateTime,
    not the str the runners expect. Translation has to happen somewhere.

  * The runners return GoldRunSummary / SilverRunSummary / BronzeRunSummary
    objects. Airflow doesn't natively know that "layer_status == failed"
    means the task should fail. We need to raise an exception.

Putting the translation here keeps the runners pure (no Airflow imports)
and keeps the DAG file simple (just `task = PythonOperator(...,
python_callable=run_bronze_task)`).

See ADR-0010 for the orchestration design discussion.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from src.bronze.run import run_bronze
from src.dq.report import read_dq_report
from src.gold.run import run_gold
from src.silver.run import run_silver
from src.utils.config import get_config
from src.utils.logging import get_logger


log = get_logger(__name__)


# Airflow is imported lazily inside the task functions so this module
# can be imported in non-Airflow contexts (e.g. by tests that want to
# check the function signatures without installing Airflow).


def _batch_id_from_context(context: dict[str, Any]) -> str:
    """
    Derive our pipeline's batch_id from Airflow's run context.

    Uses `data_interval_start` (the scheduled start of the data
    window — for a daily DAG this is the date being processed).
    Format: YYYY-MM-DD.

    Why not `dag_run.run_id`: run_id is a long string like
    'scheduled__2026-06-03T00:00:00+00:00' which would be a fine
    primary key but is unwieldy as a partition value and bakes the
    schedule string into the lake's directory layout. The interval
    start gives us a stable, human-readable batch_id.

    Why not `execution_date`: deprecated in Airflow 2.2+, replaced
    by data_interval_start.
    """
    data_interval_start = context.get("data_interval_start")
    if data_interval_start is None:
        # Manual trigger without an interval (e.g. tests calling the
        # wrapper directly); fall back to today's date.
        from datetime import date
        return date.today().isoformat()
    # pendulum.DateTime supports strftime
    return data_interval_start.strftime("%Y-%m-%d")


def run_bronze_task(raw_root: str | None = None, **context: Any) -> dict[str, Any]:
    """
    Airflow PythonOperator entrypoint for the Bronze layer.

    Args:
        raw_root: Override the raw-data directory. Defaults to
                  `data/sample` for the sample-data DAG; production
                  DAGs override to the Kaggle-fetched directory.
        context:  Airflow context kwargs (data_interval_start, etc.).

    Returns:
        Dict pushed to XCom with summary metrics. Downstream tasks
        can read this via Jinja templating or directly from ti.xcom_pull.

    Raises:
        AirflowException if Bronze reports failure (any source failed).
    """
    from airflow.exceptions import AirflowException

    batch_id = _batch_id_from_context(context)
    raw_path = Path(raw_root) if raw_root else Path("data/sample")

    log.info(
        "airflow_bronze_task_started",
        batch_id=batch_id,
        raw_root=str(raw_path),
    )

    summary = run_bronze(batch_id=batch_id, raw_root=raw_path)

    result = {
        "batch_id": batch_id,
        "layer_status": summary.layer_status,
        "sources_written": sum(1 for r in summary.results if r.status == "written"),
        "sources_skipped": sum(1 for r in summary.results if r.status == "skipped"),
        "sources_failed": sum(1 for r in summary.results if r.status == "failed"),
        "total_rows": summary.total_rows if hasattr(summary, "total_rows") else None,
    }

    if summary.layer_status == "failed":
        raise AirflowException(
            f"Bronze layer failed for batch_id={batch_id}: "
            f"{result['sources_failed']} source(s) failed"
        )

    return result


def run_silver_task(**context: Any) -> dict[str, Any]:
    """
    Airflow PythonOperator entrypoint for the Silver layer.

    Note: this task does NOT include the DQ hard-fail gate. That's a
    separate downstream task (`dq_gate_task`) per ADR-0006: keeping the
    Silver runner forgiving on quarantine and adding the gate as an
    explicit downstream decision point keeps the failure modes clear.
    """
    from airflow.exceptions import AirflowException

    batch_id = _batch_id_from_context(context)

    log.info("airflow_silver_task_started", batch_id=batch_id)

    summary = run_silver(batch_id=batch_id)

    result = {
        "batch_id": batch_id,
        "layer_status": summary.layer_status,
        "artifacts_written": sum(
            1 for r in summary.results if r.status == "written"
        ),
        "artifacts_failed": sum(
            1 for r in summary.results if r.status == "failed"
        ),
    }

    if summary.layer_status == "failed":
        raise AirflowException(
            f"Silver layer failed for batch_id={batch_id}: "
            f"{result['artifacts_failed']} artifact(s) failed"
        )

    return result


def dq_gate_task(
    quarantine_threshold_pct: float = 5.0,
    **context: Any,
) -> dict[str, Any]:
    """
    DQ hard-fail gate (ADR-0006 deferred decision now landed).

    Reads the DQ report for the current batch and fails the task if:

      * `critical_failures > 0` (any critical DQ rule failed at least
        one row), AND
      * `quarantine_pct > quarantine_threshold_pct` (the share of rows
        quarantined exceeds the configurable threshold)

    Both conditions must be true. A handful of orphan rows in a
    10-million-row table shouldn't kill the daily pipeline; widespread
    DQ failure indicates a real upstream problem and should stop
    downstream Gold execution.

    Args:
        quarantine_threshold_pct: % of rows quarantined above which
            the gate fails. Default 5%. Configurable per environment.

    Returns:
        Dict with the gate decision and the metrics that drove it.

    Raises:
        AirflowException if both fail conditions are met.
    """
    from airflow.exceptions import AirflowException

    batch_id = _batch_id_from_context(context)
    cfg = get_config()

    report_path = cfg.paths.dq_reports / f"{batch_id}.json"
    if not report_path.exists():
        # No DQ report → no DQ ran → treat as pass-through with a warning.
        # The Silver task would have failed if DQ was supposed to run
        # and didn't; reaching the gate without a report means an empty
        # batch or a manual trigger sequence.
        log.warning(
            "dq_gate_no_report",
            batch_id=batch_id,
            expected_path=str(report_path),
        )
        return {
            "batch_id": batch_id,
            "decision": "pass_no_report",
            "critical_failures": 0,
            "quarantine_pct": 0.0,
            "threshold_pct": quarantine_threshold_pct,
        }

    report = read_dq_report(report_path)
    rows_quarantined = report.get("rows_quarantined_total", 0)
    rows_in = report.get("rows_in_total", 0)
    critical_failures = report.get("critical_failures_total", 0)

    quarantine_pct = (
        (rows_quarantined / rows_in * 100.0) if rows_in > 0 else 0.0
    )

    log.info(
        "airflow_dq_gate_evaluated",
        batch_id=batch_id,
        critical_failures=critical_failures,
        rows_quarantined=rows_quarantined,
        rows_in=rows_in,
        quarantine_pct=round(quarantine_pct, 2),
        threshold_pct=quarantine_threshold_pct,
    )

    fail_decision = (
        critical_failures > 0 and quarantine_pct > quarantine_threshold_pct
    )

    result = {
        "batch_id": batch_id,
        "decision": "fail" if fail_decision else "pass",
        "critical_failures": critical_failures,
        "rows_quarantined": rows_quarantined,
        "rows_in": rows_in,
        "quarantine_pct": round(quarantine_pct, 2),
        "threshold_pct": quarantine_threshold_pct,
    }

    if fail_decision:
        raise AirflowException(
            f"DQ gate failed for batch_id={batch_id}: "
            f"{critical_failures} critical failures and "
            f"{quarantine_pct:.2f}% of rows quarantined "
            f"(threshold: {quarantine_threshold_pct}%). "
            f"See {report_path} for details."
        )

    return result


def run_gold_task(**context: Any) -> dict[str, Any]:
    """
    Airflow PythonOperator entrypoint for the Gold layer.
    """
    from airflow.exceptions import AirflowException

    batch_id = _batch_id_from_context(context)

    log.info("airflow_gold_task_started", batch_id=batch_id)

    summary = run_gold(batch_id=batch_id)

    result = {
        "batch_id": batch_id,
        "layer_status": summary.layer_status,
        "artifacts_written": sum(
            1 for r in summary.results if r.status == "written"
        ),
        "artifacts_failed": sum(
            1 for r in summary.results if r.status == "failed"
        ),
        "total_rows": summary.total_rows,
    }

    if summary.layer_status == "failed":
        raise AirflowException(
            f"Gold layer failed for batch_id={batch_id}: "
            f"{result['artifacts_failed']} artifact(s) failed"
        )

    return result
