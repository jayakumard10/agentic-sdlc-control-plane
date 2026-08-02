"""Release Readiness Gate node: guardrail checks + the merge/deploy human-approval checkpoint.

*Is* the merge/deploy gate, not a separate concept layered on top of it. Runs the
three concrete guardrails (unsafe calls, DDL changes, secret-shaped strings) against
Coder's generated code, surfaces any findings at the merge_release_approval gate,
and - this is deliberate defense in depth, not just recording what the human said -
treats the merge as blocked whenever violations exist and the human did not
explicitly set override_guardrails, regardless of the raw approve/reject status they
sent. On a real approval, commits code and docs together as one real commit in the
target app's own nested git history. On rejection, leaves the workspace uncommitted
for graph.py's routing to send to rollback.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from pathlib import Path

from langgraph.types import interrupt

from agentic_control_plane import tools
from agentic_control_plane.state import AuditEvent, GateRecord, GraphState, GuardrailViolation


def release_gate(state: GraphState, workspace: Path) -> dict:
    start = time.monotonic()

    findings = tools.evaluate_guardrails(state.coder.code_files)
    violations = [
        GuardrailViolation(rule=finding.rule, location=f"{finding.file}:{finding.line}")
        for finding in findings
    ]

    events = [
        AuditEvent(
            node="release_gate",
            event_type="node_start",
            detail=f"{len(violations)} guardrail finding(s) before merge review",
        )
    ]
    for violation in violations:
        events.append(
            AuditEvent(
                node="release_gate",
                event_type="guardrail_violation",
                detail=f"{violation.rule} at {violation.location}",
            )
        )

    decision = interrupt(
        {
            "gate_type": "merge_release_approval",
            "violations": [v.model_dump() for v in violations],
            "requirement": state.requirement_clarified,
            "files_changed": sorted(
                set(state.coder.code_files) | set(state.documentation.doc_files)
            ),
        }
    )
    raw_status = decision.get("status", "approved")
    overridden = bool(decision.get("override_guardrails", False))
    if overridden:
        for violation in violations:
            violation.overridden_by_human = True

    status = "rejected" if (violations and not overridden) else raw_status

    gates = dict(state.gates)
    gates["merge_release_approval"] = GateRecord(
        gate_type="merge_release_approval",
        status=status,
        decision_payload=str(decision),
        decided_by=decision.get("decided_by", "human"),
        replayed_from_fixture=decision.get("replayed_from_fixture", False),
    )
    events.append(
        AuditEvent(
            node="release_gate",
            event_type="gate_decision",
            detail="merge_release_approval",
            decision=status,
        )
    )

    updates: dict = {"gates": gates, "guardrail_violations": violations, "events": events}

    if status == "approved":
        commit_sha = tools.git_commit_all(
            workspace,
            f"[{state.scenario_type}] {state.requirement_clarified or state.requirement_raw}",
        )
        updates["run_status"] = "completed"
        updates["finished_at"] = datetime.now(timezone.utc)
        # Recorded in state, not only in the audit line below, so the worker can put it
        # on the outcome event. Delivering it anywhere is the worker's job, not this
        # node's: the graph decides whether to release, the worker knows the remote.
        updates["commit_sha_after"] = commit_sha
        events.append(
            AuditEvent(
                node="release_gate",
                event_type="node_end",
                detail=f"committed {commit_sha[:8] if commit_sha else '(no changes)'}",
                latency_ms=(time.monotonic() - start) * 1000,
            )
        )
    else:
        events.append(
            AuditEvent(
                node="release_gate",
                event_type="node_end",
                detail="release rejected, not committed",
                latency_ms=(time.monotonic() - start) * 1000,
            )
        )

    return updates
