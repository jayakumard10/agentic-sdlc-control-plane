"""Tests for the Release Readiness Gate node: guardrail enforcement, the

merge_release_approval gate, and the real git commit on approval.
"""

from __future__ import annotations

from functools import partial
from pathlib import Path

from langgraph.graph import END, START, StateGraph
from langgraph.types import Command

from agentic_control_plane import tools
from agentic_control_plane.checkpointer import build_memory_checkpointer
from agentic_control_plane.nodes.release_gate import release_gate
from agentic_control_plane.state import CoderOutput, GraphState


def _build_graph(workspace: Path):
    graph = StateGraph(GraphState)
    graph.add_node("release_gate", partial(release_gate, workspace=workspace))
    graph.add_edge(START, "release_gate")
    graph.add_edge("release_gate", END)
    return graph.compile(checkpointer=build_memory_checkpointer())


def test_clean_code_approved_commits_and_completes(tmp_path: Path):
    tools.write_code_files(tmp_path, {"app/ok.py": "x = 1\n"})
    compiled = _build_graph(tmp_path)
    config = {"configurable": {"thread_id": "rg-clean"}}
    compiled.invoke(
        GraphState(
            scenario_type="brownfield",
            requirement_raw="x",
            requirement_clarified="clean change",
            coder=CoderOutput(code_files={"app/ok.py": "x = 1\n"}),
        ),
        config=config,
    )
    resumed = compiled.invoke(
        Command(resume={"status": "approved", "decided_by": "human"}), config=config
    )
    assert resumed["run_status"] == "completed"
    assert resumed["gates"]["merge_release_approval"].status == "approved"
    assert resumed["finished_at"] is not None
    assert tools.git_current_commit(tmp_path) is not None


def test_guardrail_violation_forces_rejection_despite_approve(tmp_path: Path):
    """Defense in depth: a guardrail violation blocks the merge even if the human

    decision says "approved", unless override_guardrails is explicitly set.
    """
    tools.write_code_files(tmp_path, {"app/bad.py": "eval(x)\n"})
    compiled = _build_graph(tmp_path)
    config = {"configurable": {"thread_id": "rg-violation"}}
    compiled.invoke(
        GraphState(
            scenario_type="brownfield",
            requirement_raw="x",
            requirement_clarified="risky change",
            coder=CoderOutput(code_files={"app/bad.py": "eval(x)\n"}),
        ),
        config=config,
    )
    resumed = compiled.invoke(
        Command(resume={"status": "approved", "decided_by": "human"}), config=config
    )
    assert resumed["gates"]["merge_release_approval"].status == "rejected"
    assert resumed.get("run_status") != "completed"
    assert tools.git_current_commit(tmp_path) is None


def test_explicit_override_allows_merge_despite_violation(tmp_path: Path):
    tools.write_code_files(tmp_path, {"app/bad2.py": "eval(x)\n"})
    compiled = _build_graph(tmp_path)
    config = {"configurable": {"thread_id": "rg-override"}}
    compiled.invoke(
        GraphState(
            scenario_type="brownfield",
            requirement_raw="x",
            requirement_clarified="risky but reviewed",
            coder=CoderOutput(code_files={"app/bad2.py": "eval(x)\n"}),
        ),
        config=config,
    )
    resumed = compiled.invoke(
        Command(resume={"status": "approved", "decided_by": "human", "override_guardrails": True}),
        config=config,
    )
    assert resumed["gates"]["merge_release_approval"].status == "approved"
    assert resumed["run_status"] == "completed"
    assert resumed["guardrail_violations"][0].overridden_by_human is True
    assert tools.git_current_commit(tmp_path) is not None


def test_explicit_rejection_is_honored_with_no_violations(tmp_path: Path):
    tools.write_code_files(tmp_path, {"app/ok2.py": "y = 2\n"})
    compiled = _build_graph(tmp_path)
    config = {"configurable": {"thread_id": "rg-reject"}}
    compiled.invoke(
        GraphState(
            scenario_type="brownfield",
            requirement_raw="x",
            requirement_clarified="clean but rejected",
            coder=CoderOutput(code_files={"app/ok2.py": "y = 2\n"}),
        ),
        config=config,
    )
    resumed = compiled.invoke(
        Command(resume={"status": "rejected", "decided_by": "human"}), config=config
    )
    assert resumed["gates"]["merge_release_approval"].status == "rejected"
    assert tools.git_current_commit(tmp_path) is None
