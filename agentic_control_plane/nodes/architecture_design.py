"""Architecture/Design node: the *how* - component shape, API/schema design, data changes.

Deterministic heuristic reasoning over the clarified requirement and (when present)
the Codebase Reasoner's impact analysis - not an LLM call. Kept distinct from
Decomposer/Planner, which reasons about *what tasks, in what order*, not *how the
system should look*. No gate: this node isn't in the five-gate distribution, its
output flows straight into planning. Runs identically in live and replay mode -
only the Coder node's output is fixture-replayed.
"""

from __future__ import annotations

import time

from agentic_control_plane.state import ArchitectureDesign, AuditEvent, GraphState

# Codebase Reasoner's keyword scan is deliberately simple and can over-match (common
# words coincidentally appearing in docstrings/comments across many files) - capping
# how many file names get spelled out keeps the downstream Coder prompt bounded
# regardless of how imprecise any single match happens to be.
_MAX_FILES_LISTED = 5


def _format_file_list(paths: list[str]) -> str:
    shown = paths[:_MAX_FILES_LISTED]
    text = ", ".join(shown)
    if len(paths) > _MAX_FILES_LISTED:
        text += f", and {len(paths) - _MAX_FILES_LISTED} more"
    return text


def architecture_design(state: GraphState) -> dict:
    start = time.monotonic()

    if state.codebase_impact.skipped:
        summary = (
            f"Greenfield: introduce new component(s) to satisfy "
            f"'{state.requirement_clarified}'. No impact analysis was run, so integrate "
            "against whatever entry points and data-access layer the workspace already "
            "provides rather than assuming a particular structure."
        )
        api_schema_changes = [f"New endpoint(s) to support: {state.requirement_clarified}"]
    elif state.codebase_impact.impacted_apis or state.codebase_impact.impacted_modules:
        touched = state.codebase_impact.impacted_modules + state.codebase_impact.impacted_apis
        summary = (
            f"Change scoped to existing file(s): {_format_file_list(touched)}. Modify in "
            f"place to satisfy '{state.requirement_clarified}' without changing public API "
            "signatures unless the impacted file is itself an API module."
        )
        api_schema_changes = [
            f"Review signature stability of: {api}"
            for api in state.codebase_impact.impacted_apis[:_MAX_FILES_LISTED]
        ]
    else:
        summary = (
            f"No impacted files were identified by the Codebase Reasoner for "
            f"'{state.requirement_clarified}'; treating as a net-new addition."
        )
        api_schema_changes = []

    design = ArchitectureDesign(summary=summary, api_schema_changes=api_schema_changes)

    events = [
        AuditEvent(node="architecture_design", event_type="node_start", detail="designing"),
        AuditEvent(
            node="architecture_design",
            event_type="node_end",
            detail=summary,
            latency_ms=(time.monotonic() - start) * 1000,
        ),
    ]

    return {"architecture_design": design, "events": events}
