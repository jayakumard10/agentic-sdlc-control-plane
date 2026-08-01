"""End-to-end integration tests for the fully compiled StateGraph.

Every governance path - retry, fallback, rollback, safe-stop, dynamic re-planning -
driven through the real compiled graph rather than by calling nodes directly.

Node-level tests cannot establish that routing is correct, because routing is the
thing they stub out: a conditional edge that sends a brownfield run down the
greenfield branch passes every node test in the suite. These drive the compiled
graph so the edges are exercised by the same machinery that runs in production.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from langgraph.types import Command

from agentic_control_plane import tools
from agentic_control_plane.checkpointer import build_memory_checkpointer
from agentic_control_plane.graph import build_graph
from agentic_control_plane.state import GraphState


def _drive(compiled, initial_state: GraphState, thread_id: str) -> tuple[dict, list[str]]:
    """Invoke the graph and auto-approve every gate it hits, recording which gate

    types fired along the way.
    """
    config = {"configurable": {"thread_id": thread_id}}
    result = compiled.invoke(initial_state, config=config)
    gates_seen: list[str] = []
    steps = 0
    while "__interrupt__" in result and steps < 20:
        steps += 1
        payload = result["__interrupt__"][0].value
        gates_seen.append(payload["gate_type"])
        result = compiled.invoke(
            Command(resume={"status": "approved", "decided_by": "replayed", "replayed_from_fixture": True}),
            config=config,
        )
    return result, gates_seen


def _write_fixture(fixtures_dir: Path, scenario_type: str, attempts: list[dict]) -> None:
    scenario_dir = fixtures_dir / scenario_type
    scenario_dir.mkdir(parents=True, exist_ok=True)
    (scenario_dir / "transcript.json").write_text(
        json.dumps({"attempts": attempts}), encoding="utf-8"
    )


@pytest.fixture()
def fixtures_dir(tmp_path: Path) -> Path:
    d = tmp_path / "fixtures"
    d.mkdir()
    return d


@pytest.fixture()
def workspace(tmp_path: Path) -> Path:
    ws = tmp_path / "workspace"
    ws.mkdir()
    (ws / "tests").mkdir()
    (ws / "tests" / "test_placeholder.py").write_text(
        "def test_ok():\n    assert True\n", encoding="utf-8"
    )
    return ws


def test_greenfield_happy_path(workspace: Path, fixtures_dir: Path):
    _write_fixture(
        fixtures_dir,
        "greenfield",
        [{"attempt_number": 1, "code_files": {"svc/feature.py": "def feature(): return b''\n"}, "rationale": "ok"}],
    )
    compiled = build_graph(workspace, fixtures_dir, build_memory_checkpointer())
    result, gates = _drive(
        compiled,
        GraphState(scenario_type="greenfield", requirement_raw="add a new endpoint", mode="replay"),
        "gf-happy",
    )
    assert result["run_status"] == "completed"
    assert set(gates) == {"clarification_approval", "plan_approval", "merge_release_approval"}


def test_brownfield_deliberate_failure_then_retry_recovers(workspace: Path, fixtures_dir: Path):
    (workspace / "svc").mkdir()
    (workspace / "svc" / "counters.py").write_text("counter = 0\n", encoding="utf-8")
    _write_fixture(
        fixtures_dir,
        "brownfield",
        [
            {
                "attempt_number": 1,
                "code_files": {
                    "svc/counters.py": "counter = 0\ndef increment():\n    global counter\n    counter += 1\n",
                    "tests/test_counters.py": (
                        "import svc.counters as m\n\n"
                        "def test_increment_is_correct():\n"
                        "    m.counter = 0\n"
                        "    m.increment()\n"
                        "    assert m.counter == 999  # deliberately wrong, forces a failure\n"
                    ),
                },
                "rationale": "first attempt: deliberately wrong assertion to force a failure",
            },
            {
                "attempt_number": 2,
                "code_files": {
                    "svc/counters.py": "counter = 0\ndef increment():\n    global counter\n    counter += 1\n",
                    "tests/test_counters.py": (
                        "import svc.counters as m\n\n"
                        "def test_increment_is_correct():\n"
                        "    m.counter = 0\n"
                        "    m.increment()\n"
                        "    assert m.counter == 1\n"
                    ),
                },
                "rationale": "second attempt: corrected assertion, now passes",
            },
        ],
    )
    compiled = build_graph(workspace, fixtures_dir, build_memory_checkpointer())
    result, gates = _drive(
        compiled,
        GraphState(
            scenario_type="brownfield",
            requirement_raw="fix counter",
            requirement_clarified="fix the click counter increment logic",
            mode="replay",
        ),
        "bf-retry",
    )
    assert result["run_status"] == "completed"
    assert result["retry_count"] == 1
    assert result["coder"].attempt_number == 2
    assert len(result["coder_attempts"]) == 2


def test_ambiguous_replanning_produces_revised_plan(
    workspace: Path, fixtures_dir: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setenv("REPLANNING_CONFLICT_MARKERS", "throttling")
    (workspace / "svc").mkdir()
    (workspace / "svc" / "throttling.py").write_text("def is_allowed(k): return True\n", encoding="utf-8")
    _write_fixture(
        fixtures_dir,
        "ambiguous",
        [{"attempt_number": 1, "code_files": {"svc/revised.py": "def reuse(): pass\n"}, "rationale": "ok"}],
    )
    compiled = build_graph(workspace, fixtures_dir, build_memory_checkpointer())
    result, gates = _drive(
        compiled,
        GraphState(
            scenario_type="ambiguous",
            requirement_raw="reliable",
            requirement_clarified="make the service more reliable by adding throttling",
            mode="replay",
        ),
        "amb-replan",
    )
    assert "replanning_approval" in gates
    assert result["run_status"] == "completed"
    assert [task.id for task in result["tasks"]] == ["T0", "T1", "T2", "T3", "T4"]


def test_safe_stop_on_missing_fixture_never_reaches_release_gate(workspace: Path, fixtures_dir: Path):
    compiled = build_graph(workspace, fixtures_dir, build_memory_checkpointer())
    result, gates = _drive(
        compiled,
        GraphState(scenario_type="brownfield", requirement_raw="x", requirement_clarified="x", mode="replay"),
        "ss-missing",
    )
    assert result["run_status"] == "failed"
    assert result["safe_stop"] is True
    assert "merge_release_approval" not in gates


def test_guardrail_rejection_with_no_prior_commit_safe_stops(workspace: Path, fixtures_dir: Path):
    _write_fixture(
        fixtures_dir, "brownfield", [{"attempt_number": 1, "code_files": {"svc/bad.py": "eval(x)\n"}, "rationale": "violation"}]
    )
    compiled = build_graph(workspace, fixtures_dir, build_memory_checkpointer())
    result, gates = _drive(
        compiled,
        GraphState(scenario_type="brownfield", requirement_raw="x", requirement_clarified="x", mode="replay"),
        "rb-nogit",
    )
    assert result["gates"]["merge_release_approval"].status == "rejected"
    assert result["rollback_count"] == 1
    assert result["run_status"] == "failed"
    assert result["safe_stop"] is True


def test_guardrail_rejection_with_prior_commit_rolls_back_for_real(workspace: Path, fixtures_dir: Path):
    (workspace / "svc").mkdir()
    (workspace / "svc" / "good.py").write_text("x = 1\n", encoding="utf-8")
    good_sha = tools.git_commit_all(workspace, "known-good commit")

    _write_fixture(
        fixtures_dir, "brownfield", [{"attempt_number": 1, "code_files": {"svc/bad.py": "eval(x)\n"}, "rationale": "violation"}]
    )
    compiled = build_graph(workspace, fixtures_dir, build_memory_checkpointer())
    result, gates = _drive(
        compiled,
        GraphState(scenario_type="brownfield", requirement_raw="x", requirement_clarified="x", mode="replay"),
        "rb-realgit",
    )
    assert result["rollback_count"] == 1
    assert result["safe_stop"] is False
    assert result["run_status"] == "failed"
    assert not (workspace / "svc" / "bad.py").exists()
    assert tools.git_current_commit(workspace) == good_sha


def test_fallback_exhaustion_rolls_back(workspace: Path, fixtures_dir: Path):
    (workspace / "svc").mkdir()
    (workspace / "svc" / "seed.py").write_text("x = 1\n", encoding="utf-8")
    seed_sha = tools.git_commit_all(workspace, "seed")
    (workspace / "tests" / "test_placeholder.py").write_text(
        "def test_never_passes():\n    assert False\n", encoding="utf-8"
    )

    attempts = [
        {"attempt_number": n, "code_files": {"svc/attempt.py": f"n = {n}\n"}, "rationale": f"attempt {n}, still broken"}
        for n in range(1, 4)
    ]
    attempts.append(
        {
            "attempt_number": 4,
            "fallback": True,
            "code_files": {"svc/attempt.py": "n = 999  # fallback, still broken\n"},
            "rationale": "fallback attempt, still fails",
        }
    )
    _write_fixture(fixtures_dir, "brownfield", attempts)

    compiled = build_graph(workspace, fixtures_dir, build_memory_checkpointer())
    result, gates = _drive(
        compiled,
        GraphState(scenario_type="brownfield", requirement_raw="x", requirement_clarified="x", mode="replay"),
        "fb-exhaust",
    )
    assert result["retry_count"] == 3
    assert result["fallback_triggered"] is True
    assert result["coder"].attempt_number == 4
    assert result["rollback_count"] == 1
    assert result["safe_stop"] is False
    assert tools.git_current_commit(workspace) == seed_sha
