"""Codebase Reasoner node: brownfield/ambiguous impact analysis over the target app.

Deliberately a lightweight static scan (keyword matching against file contents),
not an LLM call - this node answers "what looks relevant", while actual code
understanding and generation is the LLM-powered Coder node's job. Skipped entirely
on greenfield via an explicit conditional edge in graph.py (no existing code to
reason about), so in practice this node only ever runs for brownfield and ambiguous.
The codebase-impact-review gate only fires for brownfield, per the gate-placement
distribution; ambiguous still gets the analysis, just without pausing on it.

For the ambiguous scenario specifically, this node also runs a second, deterministic
check: does the workspace already contain a module implementing something the vague
requirement is likely to be planned as adding? A concrete, scripted conflict (not
something an LLM has to notice) that triggers the Re-planner node. Scripted
deliberately so replay stays reproducible rather than depending on an LLM
rediscovering the same conflict every run.

Which module names count as a conflict is deployment configuration, not platform
logic: this repo is domain-agnostic and cannot know what "already exists" means for
an arbitrary tenant service. REPLANNING_CONFLICT_MARKERS supplies them as a
comma-separated list; with none set, the check is simply inert and the ambiguous
path proceeds without re-planning.
"""

from __future__ import annotations

import os
import re
import time
from pathlib import Path

from langgraph.types import interrupt

from agentic_control_plane.state import AuditEvent, CodebaseImpact, GateRecord, GraphState

_STOPWORDS = {
    "the", "a", "an", "and", "or", "to", "of", "in", "on", "for", "is", "are",
    "that", "this", "with", "make", "add", "fix", "it", "be", "more",
}
# Matches the route-decorator style used by the mainstream Python web frameworks.
# A framework-shaped heuristic, deliberately not a domain-shaped one - it says
# nothing about what any particular service does.
_ROUTE_PATTERN = re.compile(r"@\w+\.(get|post|put|delete|patch)\(")


def _replanning_conflict_markers() -> list[str]:
    raw = os.environ.get("REPLANNING_CONFLICT_MARKERS", "")
    return [marker.strip().lower() for marker in raw.split(",") if marker.strip()]


def _extract_keywords(text: str) -> list[str]:
    words = re.findall(r"[a-zA-Z][a-zA-Z_]{3,}", text.lower())
    seen: list[str] = []
    for word in words:
        if word not in _STOPWORDS and word not in seen:
            seen.append(word)
    return seen


def _scan_workspace(workspace: Path, keywords: list[str]) -> tuple[list[str], list[str]]:
    """Return (impacted_modules, impacted_apis) as relative paths whose content

    matches at least one keyword. Files containing route decorators are classified
    as APIs, everything else as modules.
    """
    impacted_modules: list[str] = []
    impacted_apis: list[str] = []
    if not workspace.is_dir():
        return impacted_modules, impacted_apis

    for path in sorted(workspace.rglob("*.py")):
        try:
            content = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue
        lowered = content.lower()
        if not any(keyword in lowered for keyword in keywords):
            continue
        # POSIX-style separators regardless of host OS: fixtures captured on this
        # (Windows) machine still need to match paths inside the Linux containers
        # that replay them.
        relative = path.relative_to(workspace).as_posix()
        if _ROUTE_PATTERN.search(content):
            impacted_apis.append(relative)
        else:
            impacted_modules.append(relative)
    return impacted_modules, impacted_apis


def _detect_replanning_conflict(workspace: Path) -> str | None:
    markers = _replanning_conflict_markers()
    if not markers or not workspace.is_dir():
        return None
    for path in sorted(workspace.rglob("*.py")):
        stem = path.stem.lower()
        for marker in markers:
            if marker in stem:
                relative = path.relative_to(workspace).as_posix()
                return (
                    f"Existing implementation of '{marker}' found at '{relative}'; "
                    "the planned interpretation of this requirement risks "
                    "duplicating it rather than reusing it."
                )
    return None


def codebase_reasoner(state: GraphState, workspace: Path) -> dict:
    start = time.monotonic()
    keywords = _extract_keywords(state.requirement_clarified or state.requirement_raw)
    impacted_modules, impacted_apis = _scan_workspace(workspace, keywords)

    shown_keywords = keywords[:6]
    summary = (
        f"Scanned {workspace} for keywords {shown_keywords}"
        f"{'...' if len(keywords) > len(shown_keywords) else ''}: "
        f"{len(impacted_modules)} module(s), {len(impacted_apis)} API file(s) impacted."
    )

    impact = CodebaseImpact(
        skipped=False,
        impacted_modules=impacted_modules,
        impacted_apis=impacted_apis,
        summary=summary,
    )

    events = [AuditEvent(node="codebase_reasoner", event_type="node_start", detail=summary)]

    gates = dict(state.gates)
    if state.scenario_type == "brownfield":
        decision = interrupt(
            {
                "gate_type": "codebase_impact_review",
                "impacted_modules": impacted_modules,
                "impacted_apis": impacted_apis,
                "summary": summary,
            }
        )
        status = decision.get("status", "approved")
        gates["codebase_impact_review"] = GateRecord(
            gate_type="codebase_impact_review",
            status=status,
            decision_payload=str(decision),
            decided_by=decision.get("decided_by", "human"),
            replayed_from_fixture=decision.get("replayed_from_fixture", False),
        )
        events.append(
            AuditEvent(
                node="codebase_reasoner",
                event_type="gate_decision",
                detail="codebase_impact_review",
                decision=status,
            )
        )

    updates: dict = {"codebase_impact": impact, "gates": gates}

    final_detail = "impact analysis complete"
    if state.scenario_type == "ambiguous":
        replanning_reason = _detect_replanning_conflict(workspace)
        if replanning_reason:
            updates["replanning_triggered"] = True
            updates["replanning_reason"] = replanning_reason
            final_detail = f"impact analysis complete; re-planning conflict detected: {replanning_reason}"

    events.append(
        AuditEvent(
            node="codebase_reasoner",
            event_type="node_end",
            detail=final_detail,
            latency_ms=(time.monotonic() - start) * 1000,
        )
    )
    updates["events"] = events

    return updates
