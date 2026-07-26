"""Unit tests for graph.py's wiring-only helpers (_rollback, routing functions)

in isolation, complementing the full end-to-end coverage in
test_graph_integration.py. Covers the one rollback branch integration testing
doesn't reach: the git operation itself failing (corrupted/invalid target commit),
not just "no commit to roll back to".
"""

from __future__ import annotations

import logging
from pathlib import Path

import pytest

from agentic_control_plane import tools
from agentic_control_plane.graph import (
    _rollback,
    _route_after_clarifier,
    _route_after_coder,
    _route_after_planner,
    _route_after_release_gate,
    _route_after_sync,
)
from agentic_control_plane.state import CoderOutput, GateRecord, GraphState


def test_rollback_to_invalid_commit_safe_stops(tmp_path: Path, caplog: pytest.LogCaptureFixture):
    tools.write_code_files(tmp_path, {"app/ok.py": "x = 1\n"})
    tools.git_commit_all(tmp_path, "initial")

    state = GraphState(
        scenario_type="brownfield",
        requirement_raw="x",
        coder=CoderOutput(commit_sha_before="0000000000000000000000000000000000dead"),
    )
    with caplog.at_level(logging.ERROR, logger="agentic_control_plane.graph"):
        result = _rollback(state, workspace=tmp_path)
    assert result["safe_stop"] is True
    assert result["run_status"] == "failed"
    assert result["rollback_count"] == 1
    assert "rollback failed" in result["events"][0].detail
    assert any("revert" in record.message and "failed" in record.message for record in caplog.records)


def test_rollback_with_no_known_good_commit_logs_error(caplog: pytest.LogCaptureFixture):
    state = GraphState(scenario_type="brownfield", requirement_raw="x")
    with caplog.at_level(logging.ERROR, logger="agentic_control_plane.graph"):
        _rollback(state, workspace=Path("."))
    assert any("no known-good commit" in record.message for record in caplog.records)


def test_rollback_to_known_good_commit_logs_info(tmp_path: Path, caplog: pytest.LogCaptureFixture):
    tools.write_code_files(tmp_path, {"app/ok.py": "x = 1\n"})
    sha = tools.git_commit_all(tmp_path, "initial")
    state = GraphState(
        scenario_type="brownfield",
        requirement_raw="x",
        coder=CoderOutput(commit_sha_before=sha),
    )
    with caplog.at_level(logging.INFO, logger="agentic_control_plane.graph"):
        result = _rollback(state, workspace=tmp_path)
    assert "safe_stop" not in result
    assert any("Rolled back" in record.message for record in caplog.records)


def test_route_after_clarifier_greenfield_skips_codebase_reasoner():
    state = GraphState(scenario_type="greenfield", requirement_raw="x")
    assert _route_after_clarifier(state) == "architecture_design"


def test_route_after_clarifier_brownfield_goes_to_codebase_reasoner():
    state = GraphState(scenario_type="brownfield", requirement_raw="x")
    assert _route_after_clarifier(state) == "codebase_reasoner"


def test_route_after_planner_routes_to_replanner_when_conflict_unresolved():
    state = GraphState(
        scenario_type="ambiguous", requirement_raw="x", replanning_triggered=True
    )
    assert _route_after_planner(state) == "replanner"


def test_route_after_planner_routes_to_coder_once_replanning_resolved():
    state = GraphState(
        scenario_type="ambiguous",
        requirement_raw="x",
        replanning_triggered=True,
        gates={"replanning_approval": GateRecord(gate_type="replanning_approval", status="approved")},
    )
    assert _route_after_planner(state) == "coder"


def test_route_after_coder_safe_stop_goes_straight_to_end():
    state = GraphState(scenario_type="brownfield", requirement_raw="x", safe_stop=True)
    assert _route_after_coder(state) == ["end"]


def test_route_after_coder_normal_fans_out_to_test_and_docs():
    state = GraphState(scenario_type="brownfield", requirement_raw="x")
    assert _route_after_coder(state) == ["test_executor", "documentation"]


def test_route_after_sync_hints():
    base = {"scenario_type": "brownfield", "requirement_raw": "x"}
    assert _route_after_sync(GraphState(**base, route_hint="retry")) == "coder"
    assert _route_after_sync(GraphState(**base, route_hint="fallback_attempt")) == "coder"
    assert _route_after_sync(GraphState(**base, route_hint="rollback")) == "rollback"
    assert _route_after_sync(GraphState(**base, route_hint="proceed")) == "release_gate"


def test_route_after_release_gate_approved_ends_rejected_rolls_back():
    approved = GraphState(
        scenario_type="brownfield",
        requirement_raw="x",
        gates={"merge_release_approval": GateRecord(gate_type="merge_release_approval", status="approved")},
    )
    rejected = GraphState(
        scenario_type="brownfield",
        requirement_raw="x",
        gates={"merge_release_approval": GateRecord(gate_type="merge_release_approval", status="rejected")},
    )
    assert _route_after_release_gate(approved) == "end"
    assert _route_after_release_gate(rejected) == "rollback"
