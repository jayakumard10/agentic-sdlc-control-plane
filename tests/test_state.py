"""Tests for GraphState: required fields, defaults, and the additive reducers.

Checkpoint-serde tests live in test_checkpointer.py - they exercise
checkpointer.py, not state.py, and keeping them here made this module depend on
a Postgres reachability probe it has no other reason to need.
"""

from __future__ import annotations

import pytest
from langgraph.graph import END, START, StateGraph
from pydantic import ValidationError

from agentic_control_plane.state import AuditEvent, CoderOutput, GraphState


def test_graph_state_requires_scenario_type():
    state = GraphState(scenario_type="brownfield", requirement_raw="x")
    assert state.scenario_type == "brownfield"
    assert state.mode == "replay"
    assert state.run_status == "running"
    assert state.retry_count == 0
    assert state.retry_limit == 3
    assert state.events == []
    assert state.coder_attempts == []
    assert state.finished_at is None


def test_events_field_uses_additive_reducer_under_parallel_writes():
    """The Test Executor / Documentation parallel fan-out both append to events -

    confirm LangGraph actually merges concurrent writes via operator.add rather than
    one branch's update silently clobbering the other's.
    """

    def branch_a(state: GraphState) -> dict:
        return {"events": [AuditEvent(node="a", event_type="node_start", detail="a")]}

    def branch_b(state: GraphState) -> dict:
        return {"events": [AuditEvent(node="b", event_type="node_start", detail="b")]}

    graph = StateGraph(GraphState)
    graph.add_node("a", branch_a)
    graph.add_node("b", branch_b)
    graph.add_edge(START, "a")
    graph.add_edge(START, "b")
    graph.add_edge("a", END)
    graph.add_edge("b", END)
    compiled = graph.compile()

    result = compiled.invoke(GraphState(scenario_type="greenfield", requirement_raw="x"))

    assert len(result["events"]) == 2
    assert {event.node for event in result["events"]} == {"a", "b"}


def test_coder_attempts_field_uses_additive_reducer():
    """Same pattern, different field: every Coder invocation across a retry loop

    must stay visible in coder_attempts, not just the latest one.
    """

    def first_attempt(state: GraphState) -> dict:
        output = CoderOutput(attempt_number=1, rationale="first")
        return {"coder": output, "coder_attempts": [output]}

    def second_attempt(state: GraphState) -> dict:
        output = CoderOutput(attempt_number=2, rationale="second")
        return {"coder": output, "coder_attempts": [output]}

    graph = StateGraph(GraphState)
    graph.add_node("first", first_attempt)
    graph.add_node("second", second_attempt)
    graph.add_edge(START, "first")
    graph.add_edge("first", "second")
    graph.add_edge("second", END)
    compiled = graph.compile()

    result = compiled.invoke(GraphState(scenario_type="brownfield", requirement_raw="x"))

    assert len(result["coder_attempts"]) == 2
    assert [attempt.attempt_number for attempt in result["coder_attempts"]] == [1, 2]
    assert result["coder"].attempt_number == 2


def test_scenario_type_is_constrained_to_the_three_literal_values():
    """Inbound events carry scenario_type, and GraphState keeps

    its definition as-is. An unrecognized value from a producer must fail loudly at
    the state boundary rather than flow into routing as an unhandled case.
    """
    with pytest.raises(ValidationError):
        GraphState(scenario_type="not-a-scenario", requirement_raw="x")
