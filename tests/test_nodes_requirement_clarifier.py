"""Tests for the Requirement Clarifier node: ambiguity detection and the

greenfield-only clarification_approval gate.
"""

from __future__ import annotations

from langgraph.graph import END, START, StateGraph
from langgraph.types import Command

from agentic_control_plane.checkpointer import build_memory_checkpointer
from agentic_control_plane.nodes.requirement_clarifier import _detect_ambiguities, requirement_clarifier
from agentic_control_plane.state import GraphState


def test_detect_ambiguities_flags_vague_qualifiers():
    ambiguities = _detect_ambiguities("make the service more reliable")
    assert any("reliable" in a for a in ambiguities)
    assert any("more" in a for a in ambiguities)


def test_detect_ambiguities_flags_short_requirements():
    ambiguities = _detect_ambiguities("add export")
    assert any("short" in a for a in ambiguities)


def test_detect_ambiguities_clear_requirement_has_none():
    ambiguities = _detect_ambiguities(
        "add a POST /shorten endpoint that accepts a long_url and returns a short_code"
    )
    assert ambiguities == []


def _build_single_node_graph():
    graph = StateGraph(GraphState)
    graph.add_node("requirement_clarifier", requirement_clarifier)
    graph.add_edge(START, "requirement_clarifier")
    graph.add_edge("requirement_clarifier", END)
    return graph.compile(checkpointer=build_memory_checkpointer())


def test_greenfield_pauses_at_clarification_gate_and_resumes():
    compiled = _build_single_node_graph()
    config = {"configurable": {"thread_id": "rc-greenfield"}}

    result = compiled.invoke(
        GraphState(scenario_type="greenfield", requirement_raw="add export"), config=config
    )
    assert "__interrupt__" in result
    assert result["__interrupt__"][0].value["gate_type"] == "clarification_approval"

    resumed = compiled.invoke(
        Command(
            resume={
                "status": "approved",
                "clarified_requirement": "Add a bulk export endpoint",
                "decided_by": "human",
            }
        ),
        config=config,
    )
    assert resumed["requirement_clarified"] == "Add a bulk export endpoint"
    assert resumed["gates"]["clarification_approval"].status == "approved"
    assert [event.event_type for event in resumed["events"]] == [
        "node_start",
        "gate_decision",
        "node_end",
    ]


def test_brownfield_does_not_gate():
    compiled = _build_single_node_graph()
    config = {"configurable": {"thread_id": "rc-brownfield"}}

    result = compiled.invoke(
        GraphState(scenario_type="brownfield", requirement_raw="fix the race condition"),
        config=config,
    )
    assert "__interrupt__" not in result
    assert result["gates"] == {}
    assert result["requirement_clarified"] == "fix the race condition"


def test_ambiguous_does_not_gate_but_still_detects_ambiguity():
    compiled = _build_single_node_graph()
    config = {"configurable": {"thread_id": "rc-ambiguous"}}

    result = compiled.invoke(
        GraphState(scenario_type="ambiguous", requirement_raw="make the service more reliable"),
        config=config,
    )
    assert "__interrupt__" not in result
    assert len(result["ambiguities"]) > 0
