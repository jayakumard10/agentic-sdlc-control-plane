"""Tests for the Test Executor node: the retry -> fallback state-transition

bookkeeping and the route_hint disambiguation it owns.
"""

from __future__ import annotations

from pathlib import Path

from agentic_control_plane import tools
from agentic_control_plane.nodes.test_executor import _extract_failure_summaries
from agentic_control_plane.nodes.test_executor import test_executor as run_test_executor
from agentic_control_plane.state import CoderOutput, GraphState


def test_passing_tests_set_route_hint_proceed_with_no_counter_change(tmp_path: Path):
    tools.write_code_files(tmp_path, {"tests/test_x.py": "def test_pass():\n    assert True\n"})
    state = GraphState(scenario_type="brownfield", requirement_raw="x", coder=CoderOutput(attempt_number=1))
    result = run_test_executor(state, workspace=tmp_path)
    assert result["test"].passed is True
    assert result["route_hint"] == "proceed"
    assert "retry_count" not in result
    assert "fallback_triggered" not in result


def test_failing_tests_below_limit_increment_retry_count(tmp_path: Path):
    tools.write_code_files(tmp_path, {"tests/test_x.py": "def test_fail():\n    assert False\n"})
    state = GraphState(
        scenario_type="brownfield",
        requirement_raw="x",
        coder=CoderOutput(attempt_number=1),
        retry_count=0,
        retry_limit=3,
    )
    result = run_test_executor(state, workspace=tmp_path)
    assert result["test"].passed is False
    assert result["retry_count"] == 1
    assert result["route_hint"] == "retry"
    assert any(f.startswith("FAILED") for f in result["test"].failures)


def test_failing_tests_at_limit_triggers_fallback_without_further_retry_increment(tmp_path: Path):
    tools.write_code_files(tmp_path, {"tests/test_x.py": "def test_fail():\n    assert False\n"})
    state = GraphState(
        scenario_type="brownfield",
        requirement_raw="x",
        coder=CoderOutput(attempt_number=4),
        retry_count=3,
        retry_limit=3,
    )
    result = run_test_executor(state, workspace=tmp_path)
    assert result["fallback_triggered"] is True
    assert result["route_hint"] == "fallback_attempt"
    assert "retry_count" not in result


def test_fallback_attempt_failure_leaves_counters_untouched(tmp_path: Path):
    tools.write_code_files(tmp_path, {"tests/test_x.py": "def test_fail():\n    assert False\n"})
    state = GraphState(
        scenario_type="brownfield",
        requirement_raw="x",
        coder=CoderOutput(attempt_number=5),
        retry_count=3,
        retry_limit=3,
        fallback_triggered=True,
    )
    result = run_test_executor(state, workspace=tmp_path)
    assert "retry_count" not in result
    assert "fallback_triggered" not in result
    assert result["route_hint"] == "rollback"


def test_extract_failure_summaries_reports_timeout_distinctly():
    assert _extract_failure_summaries("irrelevant stdout", timed_out=True) == [
        "pytest run timed out"
    ]


def test_extract_failure_summaries_extracts_failed_lines():
    stdout = "F\n=== FAILURES ===\nFAILED tests/test_x.py::test_thing - AssertionError\n"
    failures = _extract_failure_summaries(stdout, timed_out=False)
    assert failures == ["FAILED tests/test_x.py::test_thing - AssertionError"]


def test_extract_failure_summaries_falls_back_when_no_failed_lines_present():
    failures = _extract_failure_summaries("some collection error, no FAILED lines", timed_out=False)
    assert failures == ["pytest reported a non-zero exit with no FAILED summary lines"]
