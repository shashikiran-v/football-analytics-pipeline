"""
Tests for src.dq.report.

The report module is a serialiser, not an evaluator — these tests
verify the typed builders and JSON output structure, not DQ logic
itself (that's in test_dq_runner.py).
"""

from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path

import pandas as pd
import pytest

from src.dq.report import (
    DQBatchReport,
    DQBatchSummary,
    DQRuleReport,
    DQSourceReport,
    build_batch_report,
    build_source_report,
    write_report,
)
from src.dq.runner import DQResult, DQRuleOutcome
from src.engines.pandas_engine import PandasEngine


@pytest.fixture
def engine():
    return PandasEngine()


# ---------------------------------------------------------------------------
# build_source_report — DQResult -> DQSourceReport
# ---------------------------------------------------------------------------


class TestBuildSourceReport:
    def test_clean_source_has_zero_quarantined(self, engine):
        result = DQResult(
            source_name="clubs",
            rows_in=5,
            clean_rows=pd.DataFrame({"club_id": [1, 2, 3, 4, 5]}),
            failing_rows=None,
            outcomes=[
                DQRuleOutcome(
                    rule_id="not_null:clubs:club_id",
                    severity="critical",
                    rows_evaluated=5, rows_failed=0,
                ),
            ],
        )
        report = build_source_report(result=result, engine=engine)
        assert report.source_name == "clubs"
        assert report.rows_in == 5
        assert report.rows_clean == 5
        assert report.rows_quarantined == 0
        assert len(report.rules) == 1
        assert report.rules[0].pass_rate == 1.0

    def test_source_with_failures_attributes_correctly(self, engine):
        result = DQResult(
            source_name="appearances",
            rows_in=30,
            clean_rows=pd.DataFrame({"x": list(range(29))}),
            failing_rows=pd.DataFrame({"x": [99]}),
            outcomes=[
                DQRuleOutcome(
                    rule_id="foreign_key:appearances.player_id->players.player_id",
                    severity="critical",
                    rows_evaluated=30, rows_failed=1,
                ),
            ],
        )
        report = build_source_report(result=result, engine=engine)
        assert report.rows_in == 30
        assert report.rows_clean == 29
        assert report.rows_quarantined == 1

    def test_pass_rate_computed_per_rule(self, engine):
        result = DQResult(
            source_name="appearances",
            rows_in=10,
            clean_rows=pd.DataFrame({"x": list(range(7))}),
            failing_rows=pd.DataFrame({"x": [99, 99, 99]}),
            outcomes=[
                DQRuleOutcome(
                    rule_id="critical_rule",
                    severity="critical",
                    rows_evaluated=10, rows_failed=3,
                ),
                DQRuleOutcome(
                    rule_id="warning_rule",
                    severity="warning",
                    rows_evaluated=10, rows_failed=1,
                ),
            ],
        )
        report = build_source_report(result=result, engine=engine)
        assert len(report.rules) == 2
        assert report.rules[0].pass_rate == 0.7      # 7/10
        assert report.rules[1].pass_rate == 0.9      # 9/10


# ---------------------------------------------------------------------------
# build_batch_report — aggregates source reports
# ---------------------------------------------------------------------------


def _make_source_report(
    *,
    source_name: str,
    rows_in: int,
    rows_quarantined: int = 0,
    critical_failed: int = 0,
    warning_failed: int = 0,
) -> DQSourceReport:
    """Helper to build a DQSourceReport for batch-level tests."""
    rules: list[DQRuleReport] = []
    if critical_failed > 0:
        rules.append(DQRuleReport(
            rule_id=f"critical:{source_name}",
            severity="critical",
            rows_evaluated=rows_in,
            rows_failed=critical_failed,
            pass_rate=round((rows_in - critical_failed) / rows_in, 6) if rows_in else 1.0,
        ))
    if warning_failed > 0:
        rules.append(DQRuleReport(
            rule_id=f"warning:{source_name}",
            severity="warning",
            rows_evaluated=rows_in,
            rows_failed=warning_failed,
            pass_rate=round((rows_in - warning_failed) / rows_in, 6) if rows_in else 1.0,
        ))
    return DQSourceReport(
        source_name=source_name,
        rows_in=rows_in,
        rows_clean=rows_in - rows_quarantined,
        rows_quarantined=rows_quarantined,
        rules=rules,
    )


class TestBuildBatchReport:
    def test_clean_batch_has_zero_failures(self):
        sources = [
            _make_source_report(source_name="clubs", rows_in=5),
            _make_source_report(source_name="games", rows_in=10),
        ]
        report = build_batch_report(batch_id="b1", source_reports=sources)
        assert report.summary.total_rows_in == 15
        assert report.summary.total_rows_clean == 15
        assert report.summary.total_rows_quarantined == 0
        assert report.summary.critical_failure_count == 0
        assert report.summary.warning_count == 0
        assert report.summary.sources_with_critical_failures == []

    def test_critical_failures_aggregated(self):
        sources = [
            _make_source_report(source_name="clubs", rows_in=5),
            _make_source_report(
                source_name="appearances", rows_in=30,
                rows_quarantined=1, critical_failed=1,
            ),
        ]
        report = build_batch_report(batch_id="b1", source_reports=sources)
        assert report.summary.total_rows_in == 35
        assert report.summary.total_rows_clean == 34
        assert report.summary.total_rows_quarantined == 1
        assert report.summary.critical_failure_count == 1
        assert report.summary.sources_with_critical_failures == ["appearances"]

    def test_warnings_counted_separately(self):
        sources = [
            _make_source_report(
                source_name="players", rows_in=12,
                warning_failed=2,
            ),
        ]
        report = build_batch_report(batch_id="b1", source_reports=sources)
        # Warning rules don't quarantine rows — total_rows_clean = total_rows_in
        assert report.summary.total_rows_clean == 12
        assert report.summary.total_rows_quarantined == 0
        assert report.summary.critical_failure_count == 0
        assert report.summary.warning_count == 1
        assert report.summary.sources_with_critical_failures == []


# ---------------------------------------------------------------------------
# write_report — JSON serialisation
# ---------------------------------------------------------------------------


class TestWriteReport:
    def test_writes_json_at_expected_path(self, tmp_path):
        report = DQBatchReport(
            batch_id="test-batch",
            generated_at="2026-01-01T00:00:00Z",
            summary=DQBatchSummary(
                total_rows_in=0, total_rows_clean=0, total_rows_quarantined=0,
                critical_failure_count=0, warning_count=0,
                sources_with_critical_failures=[],
            ),
            sources=[],
        )
        output_path = write_report(report=report, output_dir=tmp_path)
        assert output_path == tmp_path / "test-batch.json"
        assert output_path.is_file()

    def test_json_round_trips_cleanly(self, tmp_path):
        sources = [_make_source_report(source_name="clubs", rows_in=5)]
        report = build_batch_report(batch_id="rt", source_reports=sources)
        output_path = write_report(report=report, output_dir=tmp_path)
        with output_path.open() as f:
            data = json.load(f)
        assert data["batch_id"] == "rt"
        assert data["summary"]["total_rows_in"] == 5
        assert data["sources"][0]["source_name"] == "clubs"

    def test_creates_output_dir_if_missing(self, tmp_path):
        nested = tmp_path / "does" / "not" / "exist"
        report = DQBatchReport(
            batch_id="b1",
            generated_at="2026-01-01T00:00:00Z",
            summary=DQBatchSummary(
                total_rows_in=0, total_rows_clean=0, total_rows_quarantined=0,
                critical_failure_count=0, warning_count=0,
                sources_with_critical_failures=[],
            ),
            sources=[],
        )
        output_path = write_report(report=report, output_dir=nested)
        assert nested.is_dir()
        assert output_path.is_file()
