"""Tests for the Decomposer/Planner node: task dependency structure, the

greenfield-only plan_approval gate, and the revised-plan behavior that makes
dynamic re-planning a real, inspectable state difference.
"""

from __future__ import annotations

from langgraph.graph import END, START, StateGraph
from langgraph.types import Command

from agentic_control_plane.checkpointer import build_memory_checkpointer
from agentic_control_plane.nodes.decomposer_planner import decomposer_planner
from agentic_control_plane.state import GateRecord, GraphState


def _build_graph():
    graph = StateGraph(GraphState)
    graph.add_node("decomposer_planner", decomposer_planner)
    graph.add_edge(START, "decomposer_planner")
    graph.add_edge("decomposer_planner", END)
    return graph.compile(checkpointer=build_memory_checkpointer())


def test_initial_plan_dependency_structure():
    compiled = _build_graph()
    config = {"configurable": {"thread_id": "dp-initial"}}
    compiled.invoke(
        GraphState(scenario_type="greenfield", requirement_raw="x", requirement_clarified="add an export endpoint"),
        config=config,
    )
    resumed = compiled.invoke(
        Command(resume={"status": "approved", "decided_by": "human"}), config=config
    )
    tasks = {task.id: task for task in resumed["tasks"]}
    assert tasks["T1"].depends_on == []
    assert tasks["T2"].depends_on == ["T1"]
    assert tasks["T3"].depends_on == ["T1"]
    assert tasks["T4"].depends_on == ["T2", "T3"]


def test_greenfield_gates_on_plan_approval():
    compiled = _build_graph()
    config = {"configurable": {"thread_id": "dp-gate"}}
    result = compiled.invoke(
        GraphState(scenario_type="greenfield", requirement_raw="x", requirement_clarified="x"),
        config=config,
    )
    assert result["__interrupt__"][0].value["gate_type"] == "plan_approval"


def test_brownfield_does_not_gate():
    result = decomposer_planner(
        GraphState(scenario_type="brownfield", requirement_raw="x", requirement_clarified="x")
    )
    assert result["gates"] == {}
    assert [task.id for task in result["tasks"]] == ["T1", "T2", "T3", "T4"]


def test_revised_plan_after_replanning_approval_adds_reconciliation_task():
    state = GraphState(
        scenario_type="ambiguous",
        requirement_raw="x",
        requirement_clarified="make it more reliable",
        replanning_triggered=True,
        replanning_reason="Existing implementation of 'throttling' found at 'svc/throttling.py'",
        gates={
            "replanning_approval": GateRecord(
                gate_type="replanning_approval", status="approved", decided_by="human"
            )
        },
    )
    result = decomposer_planner(state)
    task_ids = [task.id for task in result["tasks"]]
    assert task_ids == ["T0", "T1", "T2", "T3", "T4"]
    assert "Reconcile" in result["tasks"][0].description
    assert "throttling.py" in result["tasks"][0].description
    assert "revised" in result["tasks"][1].description.lower()


def test_rejected_replanning_gate_keeps_original_plan():
    state = GraphState(
        scenario_type="ambiguous",
        requirement_raw="x",
        requirement_clarified="make it more reliable",
        replanning_triggered=True,
        replanning_reason="conflict",
        gates={
            "replanning_approval": GateRecord(
                gate_type="replanning_approval", status="rejected", decided_by="human"
            )
        },
    )
    result = decomposer_planner(state)
    assert [task.id for task in result["tasks"]] == ["T1", "T2", "T3", "T4"]
