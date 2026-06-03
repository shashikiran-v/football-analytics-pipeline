"""
Tests for src.orchestration.airflow_wrappers.

Two flavours of test:
  1. Tests that DON'T need Airflow installed — exercising the wrappers
     via direct function calls when Airflow's exception machinery
     isn't required (the happy paths).
  2. Tests that DO need Airflow — exercising the failure paths where
     wrappers raise AirflowException. These skip cleanly when Airflow
     isn't installed.

The split exists because Airflow is a heavy install dependency (~150 MB,
postgres/celery extras). Per ADR-0010, the project ships Airflow as an
optional extra rather than a core dependency, so tests must work either
way.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from src.bronze.run import run_bronze
from src.metadata.db import init_db
from src.orchestration.airflow_wrappers import (
    _batch_id_from_context,
    dq_gate_task,
    run_bronze_task,
    run_gold_task,
    run_silver_task,
)
from src.silver.run import run_silver


SAMPLES_DIR = Path(__file__).resolve().parents[1] / "data" / "sample"


# ---------------------------------------------------------------------------
# Helper: airflow availability for conditional tests
# ---------------------------------------------------------------------------


def _airflow_available() -> bool:
    try:
        import airflow.exceptions  # noqa: F401
        return True
    except ImportError:
        return False


airflow_required = pytest.mark.skipif(
    not _airflow_available(),
    reason="Airflow not installed (it's an optional dependency)",
)


@pytest.fixture
def fresh_db():
    init_db()


# ---------------------------------------------------------------------------
# batch_id derivation
# ---------------------------------------------------------------------------


class TestBatchIdDerivation:
    def test_from_data_interval_start(self):
        """A real Airflow context provides data_interval_start as a
        pendulum.DateTime. We test with a stand-in object that has the
        same strftime contract."""
        from datetime import datetime
        context = {"data_interval_start": datetime(2026, 6, 3, 0, 0, 0)}
        assert _batch_id_from_context(context) == "2026-06-03"

    def test_fallback_when_no_data_interval(self):
        """A manual trigger without a data interval falls back to
        today's date — useful for ad-hoc reruns where Airflow's
        scheduled interval doesn't exist."""
        from datetime import date
        result = _batch_id_from_context({})
        # Should be today's date in YYYY-MM-DD format
        assert result == date.today().isoformat()


# ---------------------------------------------------------------------------
# Bronze task — Airflow-required for failure cases
# ---------------------------------------------------------------------------


class TestBronzeTask:
    @airflow_required
    def test_happy_path_returns_summary_dict(self, fresh_db):
        """Bronze task against the sample data returns a summary dict
        usable for downstream XCom or assertions."""
        from datetime import datetime
        result = run_bronze_task(
            raw_root=str(SAMPLES_DIR),
            data_interval_start=datetime(2026, 6, 3, 0, 0, 0),
        )
        assert result["batch_id"] == "2026-06-03"
        assert result["layer_status"] == "success"
        assert result["sources_written"] == 6
        assert result["sources_failed"] == 0

    @airflow_required
    def test_raises_airflow_exception_on_layer_failure(self, fresh_db):
        """If Bronze reports failed status, the wrapper must raise
        AirflowException so Airflow marks the task FAILED."""
        from airflow.exceptions import AirflowException
        from datetime import datetime
        # Force a Bronze failure by pointing at a non-existent raw_root.
        # Every source will fail; layer_status will be 'failed'.
        with pytest.raises(AirflowException, match="Bronze layer failed"):
            run_bronze_task(
                raw_root="/nonexistent/path",
                data_interval_start=datetime(2026, 6, 3, 0, 0, 0),
            )


# ---------------------------------------------------------------------------
# Silver task
# ---------------------------------------------------------------------------


class TestSilverTask:
    @airflow_required
    def test_happy_path(self, fresh_db):
        """Silver after Bronze succeeds → wrapper returns success dict."""
        from datetime import datetime
        # Need Bronze data first
        run_bronze(batch_id="2026-06-03", raw_root=SAMPLES_DIR)
        result = run_silver_task(
            data_interval_start=datetime(2026, 6, 3, 0, 0, 0),
        )
        assert result["batch_id"] == "2026-06-03"
        assert result["layer_status"] == "success"
        assert result["artifacts_written"] >= 1
        assert result["artifacts_failed"] == 0


# ---------------------------------------------------------------------------
# DQ gate task — the ADR-0006 deferred decision
# ---------------------------------------------------------------------------


class TestDqGateTask:
    @airflow_required
    def test_gate_passes_with_low_quarantine_share(self, fresh_db):
        """The sample data has 1 quarantined row out of 30 appearances
        (~3.3%). With threshold 5%, the gate should pass."""
        from datetime import datetime
        run_bronze(batch_id="2026-06-03", raw_root=SAMPLES_DIR)
        run_silver(batch_id="2026-06-03")
        result = dq_gate_task(
            quarantine_threshold_pct=5.0,
            data_interval_start=datetime(2026, 6, 3, 0, 0, 0),
        )
        assert result["decision"] == "pass"
        # The sample has 1 orphan in appearances (critical failure) but
        # only 1/30 = ~3.3% of appearances quarantined, which is below
        # the 5% threshold.
        assert result["critical_failures"] >= 1
        assert result["quarantine_pct"] < 5.0

    @airflow_required
    def test_gate_fails_when_threshold_exceeded(self, fresh_db):
        """Force a fail by setting a very low threshold (any quarantine
        triggers the gate)."""
        from airflow.exceptions import AirflowException
        from datetime import datetime
        run_bronze(batch_id="2026-06-03", raw_root=SAMPLES_DIR)
        run_silver(batch_id="2026-06-03")
        with pytest.raises(AirflowException, match="DQ gate failed"):
            dq_gate_task(
                quarantine_threshold_pct=0.1,  # very strict
                data_interval_start=datetime(2026, 6, 3, 0, 0, 0),
            )

    @airflow_required
    def test_gate_passes_when_no_report_present(self, fresh_db):
        """If no DQ report exists for the batch, the gate treats it as
        a pass with a 'pass_no_report' decision — covers manual-trigger
        sequences where Silver may not have run."""
        from datetime import datetime
        result = dq_gate_task(
            quarantine_threshold_pct=5.0,
            data_interval_start=datetime(2026, 6, 3, 0, 0, 0),
        )
        assert result["decision"] == "pass_no_report"
        assert result["critical_failures"] == 0


# ---------------------------------------------------------------------------
# Gold task
# ---------------------------------------------------------------------------


class TestGoldTask:
    @airflow_required
    def test_happy_path(self, fresh_db):
        """Gold after Silver succeeds → wrapper returns success dict
        with all 5 artifacts written."""
        from datetime import datetime
        run_bronze(batch_id="2026-06-03", raw_root=SAMPLES_DIR)
        run_silver(batch_id="2026-06-03")
        result = run_gold_task(
            data_interval_start=datetime(2026, 6, 3, 0, 0, 0),
        )
        assert result["batch_id"] == "2026-06-03"
        assert result["layer_status"] == "success"
        assert result["artifacts_written"] == 5
        assert result["artifacts_failed"] == 0
        assert result["total_rows"] > 0
