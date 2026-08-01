"""Re-planner node: the ambiguous scenario's mid-flight re-planning checkpoint.

Triggered when Codebase Reasoner (ambiguous scenario only) finds a deterministic
conflict: a vague requirement whose likely planned interpretation would duplicate
a module the workspace already contains (see that node's
REPLANNING_CONFLICT_MARKERS). Surfaces the conflict and the initial task list at the
replanning_approval gate, then hands control back to Decomposer/Planner
(graph.py routes the edge) to regenerate the plan. Decomposer/Planner
remains the one node that actually reasons about *what tasks, in what order*;
this node's job is strictly the human checkpoint on top of that reasoning, per the
node-boundary rule that keeps these distinct.
"""

from __future__ import annotations

import time

from langgraph.types import interrupt

from agentic_control_plane.state import AuditEvent, GateRecord, GraphState


def replanner(state: GraphState) -> dict:
    start = time.monotonic()

    decision = interrupt(
        {
            "gate_type": "replanning_approval",
            "reason": state.replanning_reason,
            "initial_tasks": [task.model_dump() for task in state.tasks],
        }
    )
    status = decision.get("status", "approved")

    gates = dict(state.gates)
    gates["replanning_approval"] = GateRecord(
        gate_type="replanning_approval",
        status=status,
        decision_payload=str(decision),
        decided_by=decision.get("decided_by", "human"),
        replayed_from_fixture=decision.get("replayed_from_fixture", False),
    )

    events = [
        AuditEvent(
            node="replanner",
            event_type="node_start",
            detail=f"surfacing re-planning conflict: {state.replanning_reason}",
        ),
        AuditEvent(
            node="replanner",
            event_type="gate_decision",
            detail="replanning_approval",
            decision=status,
        ),
        AuditEvent(
            node="replanner",
            event_type="node_end",
            detail=(
                "handing back to decomposer_planner for a revised plan"
                if status == "approved"
                else "human kept the original plan, no revision"
            ),
            latency_ms=(time.monotonic() - start) * 1000,
        ),
    ]

    return {"gates": gates, "events": events}
