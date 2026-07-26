"""Tests for the Documentation node: target-app doc generation and writing into

the shared workspace.
"""

from __future__ import annotations

from pathlib import Path

from agentic_control_plane.nodes.documentation import documentation
from agentic_control_plane.state import ArchitectureDesign, CoderOutput, GraphState, Task


def test_documentation_summarizes_design_files_and_tasks(tmp_path: Path):
    state = GraphState(
        scenario_type="brownfield",
        requirement_raw="fix race",
        requirement_clarified="fix the race condition in the click counter",
        architecture_design=ArchitectureDesign(summary="Guard the counter increment with a lock."),
        coder=CoderOutput(code_files={"app/analytics.py": "# thread-safe counter\n"}),
        tasks=[
            Task(id="T1", description="Implement fix", depends_on=[]),
            Task(id="T2", description="Write tests", depends_on=["T1"]),
        ],
    )
    result = documentation(state, workspace=tmp_path)
    doc_files = result["documentation"].doc_files
    assert len(doc_files) == 1
    content = next(iter(doc_files.values()))

    assert "fix the race condition" in content
    assert "app/analytics.py" in content
    assert "[T1]" in content and "[T2]" in content
    assert "depends on: T1" in content


def test_documentation_writes_file_to_workspace(tmp_path: Path):
    state = GraphState(scenario_type="greenfield", requirement_raw="x", requirement_clarified="x")
    result = documentation(state, workspace=tmp_path)
    key = next(iter(result["documentation"].doc_files))
    assert (tmp_path / key).is_file()


def test_documentation_handles_no_generated_files(tmp_path: Path):
    state = GraphState(scenario_type="greenfield", requirement_raw="x", requirement_clarified="x")
    result = documentation(state, workspace=tmp_path)
    content = next(iter(result["documentation"].doc_files.values()))
    assert "(none)" in content


def test_documentation_events_have_start_and_end(tmp_path: Path):
    state = GraphState(scenario_type="greenfield", requirement_raw="x", requirement_clarified="x")
    result = documentation(state, workspace=tmp_path)
    assert [e.event_type for e in result["events"]] == ["node_start", "node_end"]
