"""Documentation node: generates docs for the *target app*, not the orchestrator itself.

Runs in parallel with Test Executor - both consume Coder's output independently and
synchronize before Release Readiness Gate. Deterministic templating over the
generated code and the architecture/task-plan context, not an LLM call. Writes into
the same shared workspace Coder wrote code into, so Release Gate commits code and
docs together as one real, inspectable change.
"""

from __future__ import annotations

import time
from pathlib import Path

from agentic_control_plane import tools
from agentic_control_plane.state import AuditEvent, DocumentationOutput, GraphState


def _summarize_code_files(code_files: dict[str, str]) -> str:
    if not code_files:
        return "## Generated files\n\n(none)"
    lines = ["## Generated files", ""]
    for path in sorted(code_files):
        file_lines = code_files[path].splitlines()
        first_line = file_lines[0].lstrip("# ").strip() if file_lines else ""
        suffix = f" - {first_line}" if first_line.startswith(("#", '"""')) else ""
        lines.append(f"- `{path}`{suffix}")
    return "\n".join(lines)


def documentation(state: GraphState, workspace: Path) -> dict:
    start = time.monotonic()

    task_lines = [
        f"- [{task.id}] {task.description} (depends on: {', '.join(task.depends_on) or 'none'})"
        for task in state.tasks
    ]

    readme = "\n".join(
        [
            f"# Change: {state.requirement_clarified or state.requirement_raw}",
            "",
            f"**Scenario:** {state.scenario_type}",
            "",
            "## Design",
            state.architecture_design.summary,
            "",
            _summarize_code_files(state.coder.code_files),
            "",
            "## Tasks",
            *task_lines,
        ]
    )

    doc_files = {f"docs/CHANGE_{state.run_id[:8]}.md": readme}
    tools.write_code_files(workspace, doc_files)

    events = [
        AuditEvent(
            node="documentation", event_type="node_start", detail="generating target-app docs"
        ),
        AuditEvent(
            node="documentation",
            event_type="node_end",
            detail=f"wrote {len(doc_files)} doc file(s)",
            latency_ms=(time.monotonic() - start) * 1000,
        ),
    ]

    return {"documentation": DocumentationOutput(doc_files=doc_files), "events": events}
