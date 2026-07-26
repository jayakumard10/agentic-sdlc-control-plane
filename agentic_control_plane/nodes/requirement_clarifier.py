"""Requirement Clarifier node: parses raw intent, surfaces ambiguity, gates on greenfield.

Every gated node follows the same shape regardless of live/replay mode: it calls
`interrupt(payload)`, which pauses the graph and surfaces `payload` to whatever is
driving execution. In live mode that's a human at a terminal prompt; in replay mode
it's the scenario runner resuming with the recorded decision from the fixture via
`Command(resume=...)`. The node itself never branches on `state.mode` for this -
that's what keeps live and replay executing the identical code path.
"""

from __future__ import annotations

import time

from langgraph.types import interrupt

from agentic_control_plane.state import AuditEvent, GateRecord, GraphState

_AMBIGUITY_MARKERS = ("reliable", "better", "improve", "faster", "modernize", "clean up", "more")


def _detect_ambiguities(requirement_raw: str) -> list[str]:
    lowered = requirement_raw.lower()
    found = [f"vague qualifier: '{marker}'" for marker in _AMBIGUITY_MARKERS if marker in lowered]
    if not found and len(requirement_raw.split()) < 6:
        found.append("requirement is very short and may be underspecified")
    return found


def requirement_clarifier(state: GraphState) -> dict:
    start = time.monotonic()
    ambiguities = _detect_ambiguities(state.requirement_raw)
    clarified = state.requirement_raw.strip()

    events = [
        AuditEvent(
            node="requirement_clarifier",
            event_type="node_start",
            detail=f"parsing requirement ({len(state.requirement_raw)} chars)",
        )
    ]

    gates = dict(state.gates)
    if state.scenario_type == "greenfield":
        decision = interrupt(
            {
                "gate_type": "clarification_approval",
                "requirement_raw": state.requirement_raw,
                "ambiguities": ambiguities,
                "proposed_clarification": clarified,
            }
        )
        status = decision.get("status", "approved")
        clarified = decision.get("clarified_requirement", clarified)
        gates["clarification_approval"] = GateRecord(
            gate_type="clarification_approval",
            status=status,
            decision_payload=str(decision),
            decided_by=decision.get("decided_by", "human"),
            replayed_from_fixture=decision.get("replayed_from_fixture", False),
        )
        events.append(
            AuditEvent(
                node="requirement_clarifier",
                event_type="gate_decision",
                detail="clarification_approval",
                decision=status,
            )
        )

    events.append(
        AuditEvent(
            node="requirement_clarifier",
            event_type="node_end",
            detail=f"clarified requirement, {len(ambiguities)} ambiguities surfaced",
            latency_ms=(time.monotonic() - start) * 1000,
        )
    )

    return {
        "requirement_clarified": clarified,
        "ambiguities": ambiguities,
        "gates": gates,
        "events": events,
    }
