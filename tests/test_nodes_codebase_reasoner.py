"""Tests for the Codebase Reasoner node: keyword-based impact scanning, the

brownfield-only codebase_impact_review gate, and the deterministic re-planning
conflict check used by the ambiguous scenario.
"""

from __future__ import annotations

import pytest
from functools import partial
from pathlib import Path

from langgraph.graph import END, START, StateGraph
from langgraph.types import Command

from agentic_control_plane.checkpointer import build_memory_checkpointer
from agentic_control_plane.nodes.codebase_reasoner import _detect_replanning_conflict, codebase_reasoner
from agentic_control_plane.state import GraphState


def _seed_files(tmp_path: Path) -> None:
    """A deliberately generic stand-in service.

    This repo is domain-agnostic, so its own test data must be too - a fixture
    modelled on one particular tenant would put that tenant's concepts into this
    repo just as surely as production code would.
    """
    svc = tmp_path / "svc"
    svc.mkdir()
    (svc / "counters.py").write_text(
        "counter = 0\ndef increment_counter():\n    global counter\n    counter += 1\n",
        encoding="utf-8",
    )
    (svc / "main.py").write_text(
        'from framework import App\napp = App()\n\n@app.get("/{item}")\n'
        "def handle(item: str):\n    increment_counter()\n    return {}\n",
        encoding="utf-8",
    )
    (svc / "unrelated.py").write_text("def add(a, b):\n    return a + b\n", encoding="utf-8")


def _build_graph(workspace: Path):
    graph = StateGraph(GraphState)
    graph.add_node("codebase_reasoner", partial(codebase_reasoner, workspace=workspace))
    graph.add_edge(START, "codebase_reasoner")
    graph.add_edge("codebase_reasoner", END)
    return graph.compile(checkpointer=build_memory_checkpointer())


def test_scan_classifies_modules_vs_apis_using_posix_paths(tmp_path: Path):
    """Regression test: Path.relative_to(...) stringifies with backslashes on

    Windows, which would corrupt matching once fixtures replay inside Linux
    containers. Found via smoke-testing during Phase 2.
    """
    _seed_files(tmp_path)
    compiled = _build_graph(tmp_path)
    config = {"configurable": {"thread_id": "cr-scan"}}

    result = compiled.invoke(
        GraphState(
            scenario_type="brownfield",
            requirement_raw="race condition",
            requirement_clarified="fix the race condition in the counter for concurrent handlers",
        ),
        config=config,
    )
    payload = result["__interrupt__"][0].value
    assert payload["impacted_modules"] == ["svc/counters.py"]
    assert payload["impacted_apis"] == ["svc/main.py"]
    assert "svc/unrelated.py" not in payload["impacted_modules"]


def test_brownfield_gates_on_codebase_impact_review(tmp_path: Path):
    _seed_files(tmp_path)
    compiled = _build_graph(tmp_path)
    config = {"configurable": {"thread_id": "cr-brownfield"}}

    compiled.invoke(
        GraphState(
            scenario_type="brownfield",
            requirement_raw="x",
            requirement_clarified="fix the counter",
        ),
        config=config,
    )
    resumed = compiled.invoke(
        Command(resume={"status": "approved", "decided_by": "human"}), config=config
    )
    assert resumed["gates"]["codebase_impact_review"].status == "approved"
    assert resumed["codebase_impact"].skipped is False


def test_ambiguous_runs_analysis_without_gating(tmp_path: Path):
    _seed_files(tmp_path)
    compiled = _build_graph(tmp_path)
    config = {"configurable": {"thread_id": "cr-ambiguous"}}

    result = compiled.invoke(
        GraphState(
            scenario_type="ambiguous",
            requirement_raw="x",
            requirement_clarified="improve reliability of the handler and counter path",
        ),
        config=config,
    )
    assert "__interrupt__" not in result
    assert result["gates"] == {}
    assert result["codebase_impact"].impacted_modules == ["svc/counters.py"]


def test_detect_replanning_conflict_finds_a_configured_marker(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setenv("REPLANNING_CONFLICT_MARKERS", "throttling")
    (tmp_path / "svc").mkdir()
    (tmp_path / "svc" / "throttling.py").write_text("def allow(k): return True\n", encoding="utf-8")

    reason = _detect_replanning_conflict(tmp_path)

    assert reason is not None
    assert "throttling.py" in reason


def test_detect_replanning_conflict_none_when_marker_module_absent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setenv("REPLANNING_CONFLICT_MARKERS", "throttling")
    (tmp_path / "svc").mkdir()
    (tmp_path / "svc" / "main.py").write_text("x = 1\n", encoding="utf-8")

    assert _detect_replanning_conflict(tmp_path) is None


def test_detect_replanning_conflict_is_inert_with_no_markers_configured(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """The default. This repo is domain-agnostic and cannot know what "already

    exists" means for an arbitrary service, so an unconfigured deployment must not
    invent a conflict - it proceeds without re-planning rather than guessing.
    """
    monkeypatch.delenv("REPLANNING_CONFLICT_MARKERS", raising=False)
    (tmp_path / "svc").mkdir()
    (tmp_path / "svc" / "throttling.py").write_text("x = 1\n", encoding="utf-8")

    assert _detect_replanning_conflict(tmp_path) is None


def test_detect_replanning_conflict_accepts_several_comma_separated_markers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setenv("REPLANNING_CONFLICT_MARKERS", " caching , throttling ")
    (tmp_path / "svc").mkdir()
    (tmp_path / "svc" / "caching.py").write_text("x = 1\n", encoding="utf-8")

    reason = _detect_replanning_conflict(tmp_path)

    assert reason is not None
    assert "caching" in reason


def test_ambiguous_scenario_sets_replanning_triggered_when_conflict_found(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setenv("REPLANNING_CONFLICT_MARKERS", "throttling")
    (tmp_path / "svc").mkdir()
    (tmp_path / "svc" / "throttling.py").write_text("x = 1\n", encoding="utf-8")
    compiled = _build_graph(tmp_path)
    config = {"configurable": {"thread_id": "cr-conflict"}}

    result = compiled.invoke(
        GraphState(
            scenario_type="ambiguous",
            requirement_raw="x",
            requirement_clarified="make the service more reliable",
        ),
        config=config,
    )
    assert result["replanning_triggered"] is True
    assert "throttling.py" in result["replanning_reason"]


def test_greenfield_impact_defaults_stay_untouched_when_node_not_invoked():
    """Greenfield skips this node entirely via a graph-level conditional edge

    (tested in test_graph_integration.py) - this just confirms the default state
    shape a skipped run would carry forward.
    """
    state = GraphState(scenario_type="greenfield", requirement_raw="x")
    assert state.codebase_impact.skipped is True
