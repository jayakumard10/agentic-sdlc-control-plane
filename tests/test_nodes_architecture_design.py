"""Tests for the Architecture/Design node's three heuristic branches."""

from __future__ import annotations

from agentic_control_plane.nodes.architecture_design import _format_file_list, architecture_design
from agentic_control_plane.state import CodebaseImpact, GraphState


def test_format_file_list_caps_at_max_and_appends_count():
    many = [f"app/file{i}.py" for i in range(10)]
    text = _format_file_list(many)
    assert text == (
        "app/file0.py, app/file1.py, app/file2.py, app/file3.py, app/file4.py, and 5 more"
    )


def test_format_file_list_no_cap_needed_when_short():
    assert _format_file_list(["a.py", "b.py"]) == "a.py, b.py"


def test_greenfield_branch_frames_as_net_new():
    state = GraphState(
        scenario_type="greenfield",
        requirement_raw="add export",
        requirement_clarified="Add a bulk export endpoint",
    )
    result = architecture_design(state)
    assert "Greenfield" in result["architecture_design"].summary
    assert len(result["events"]) == 2


def test_brownfield_branch_scopes_to_impacted_files():
    state = GraphState(
        scenario_type="brownfield",
        requirement_raw="race",
        requirement_clarified="fix race condition in click counter",
        codebase_impact=CodebaseImpact(
            skipped=False, impacted_modules=["app/analytics.py"], impacted_apis=["app/main.py"]
        ),
    )
    result = architecture_design(state)
    assert "app/analytics.py" in result["architecture_design"].summary
    assert result["architecture_design"].api_schema_changes == [
        "Review signature stability of: app/main.py"
    ]


def test_no_impact_found_branch_treats_as_net_new():
    state = GraphState(
        scenario_type="ambiguous",
        requirement_raw="reliable",
        requirement_clarified="improve reliability",
        codebase_impact=CodebaseImpact(skipped=False, impacted_modules=[], impacted_apis=[]),
    )
    result = architecture_design(state)
    assert "No impacted files" in result["architecture_design"].summary
