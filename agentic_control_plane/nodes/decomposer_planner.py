"""Decomposer/Planner node: the *what/when* - task dependency graph, sequencing.

Kept distinct from Architecture/Design (the *how*). Produces a small, deterministic
task list whose dependency edges mirror the graph's own
Coder -> (Test Executor, Documentation) -> Release Gate topology, giving an
explicit, inspectable decomposition artifact rather than leaving it implicit in the
fixed graph structure. Gates on plan_approval only for greenfield, per the
gate-placement distribution.

This node also produces the ambiguous scenario's *revised* plan: when it runs after
the Re-planner's replanning_approval gate has already been approved, it inserts a
reconciliation task ahead of the implementation task instead of just re-emitting the
original list - this is what makes "dynamic re-planning when upstream outputs
change" a real, inspectable difference in state rather than a relabeled no-op.
"""

from __future__ import annotations

import time

from langgraph.types import interrupt

from agentic_control_plane.state import AuditEvent, GateRecord, GraphState, Task


def decomposer_planner(state: GraphState) -> dict:
    start = time.monotonic()

    change_summary = state.architecture_design.summary or state.requirement_clarified

    replanning_gate = state.gates.get("replanning_approval")
    is_revision = replanning_gate is not None and replanning_gate.status == "approved"

    if is_revision:
        tasks = [
            Task(
                id="T0",
                description=f"Reconcile with existing functionality: {state.replanning_reason}",
                depends_on=[],
            ),
            Task(id="T1", description=f"Implement (revised): {change_summary}", depends_on=["T0"]),
            Task(id="T2", description="Write/update unit tests for the change", depends_on=["T1"]),
            Task(
                id="T3",
                description="Update the target service's documentation for the change",
                depends_on=["T1"],
            ),
            Task(
                id="T4",
                description="Run guardrail checks and prepare for release",
                depends_on=["T2", "T3"],
            ),
        ]
    else:
        tasks = [
            Task(id="T1", description=f"Implement: {change_summary}", depends_on=[]),
            Task(id="T2", description="Write/update unit tests for the change", depends_on=["T1"]),
            Task(
                id="T3",
                description="Update the target service's documentation for the change",
                depends_on=["T1"],
            ),
            Task(
                id="T4",
                description="Run guardrail checks and prepare for release",
                depends_on=["T2", "T3"],
            ),
        ]

    events = [
        AuditEvent(
            node="decomposer_planner",
            event_type="node_start",
            detail=f"decomposed into {len(tasks)} tasks{' (revised plan)' if is_revision else ''}",
        )
    ]

    gates = dict(state.gates)
    if state.scenario_type == "greenfield":
        decision = interrupt(
            {
                "gate_type": "plan_approval",
                "tasks": [task.model_dump() for task in tasks],
            }
        )
        status = decision.get("status", "approved")
        gates["plan_approval"] = GateRecord(
            gate_type="plan_approval",
            status=status,
            decision_payload=str(decision),
            decided_by=decision.get("decided_by", "human"),
            replayed_from_fixture=decision.get("replayed_from_fixture", False),
        )
        events.append(
            AuditEvent(
                node="decomposer_planner",
                event_type="gate_decision",
                detail="plan_approval",
                decision=status,
            )
        )

    events.append(
        AuditEvent(
            node="decomposer_planner",
            event_type="node_end",
            detail="plan ready",
            latency_ms=(time.monotonic() - start) * 1000,
        )
    )

    return {"tasks": tasks, "gates": gates, "events": events}
