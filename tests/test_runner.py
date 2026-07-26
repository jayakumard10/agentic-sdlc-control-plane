"""Tests for the non-blocking run worker: park, resume, idempotency and TTL.

The central property here is that starting a run *returns* at a gate rather than
waiting at one. Everything else - resuming from a fresh checkpointer, rejecting a
duplicate, expiring a run nobody answered - follows from parked state living in the
checkpointer rather than in whichever process happened to start the run.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from agentic_control_plane import runner, tools, workspace
from agentic_control_plane.checkpointer import build_memory_checkpointer
from agentic_control_plane.state import GraphState


@pytest.fixture()
def env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A workspaces root and a fixtures dir, both scoped to this test."""
    monkeypatch.setenv("WORKSPACES_ROOT", str(tmp_path / "workspaces"))
    monkeypatch.setenv("FIXTURES_DIR", str(tmp_path / "fixtures"))
    monkeypatch.delenv("PARKED_RUN_TTL_HOURS", raising=False)
    return tmp_path


def _seed_workspace(run_id: str) -> Path:
    """A workspace with a passing test suite, so a run can reach the release gate."""
    ws = workspace.workspace_for(run_id)
    tools.write_code_files(
        ws,
        {
            "svc/main.py": "def handle():\n    return 1\n",
            "tests/test_main.py": "from svc.main import handle\n\ndef test_handle():\n    assert handle() == 1\n",
        },
    )
    tools.git_commit_all(ws, "baseline")
    return ws


def _write_fixture(scenario_type: str, code_files: dict[str, str]) -> None:
    directory = runner.fixtures_dir() / scenario_type
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "transcript.json").write_text(
        json.dumps(
            {
                "scenario_type": scenario_type,
                "attempts": [
                    {"attempt_number": 1, "code_files": code_files, "rationale": "ok"}
                ],
            }
        ),
        encoding="utf-8",
    )


def _brownfield_state() -> GraphState:
    return GraphState(
        scenario_type="brownfield",
        requirement_raw="fix the counter",
        requirement_clarified="fix the counter",
        mode="replay",
    )


def test_start_run_returns_at_the_first_gate_instead_of_waiting(env: Path):
    """The property the whole design rests on: starting a run hands control back at

    a gate. If this blocked, a consumer's poll loop would block with it.
    """
    _seed_workspace("run-park")
    _write_fixture("brownfield", {"svc/added.py": "x = 1\n"})
    checkpointer = build_memory_checkpointer()

    result = runner.start_run("run-park", _brownfield_state(), checkpointer)

    assert result.parked is True
    assert result.is_terminal is False
    assert result.gate_type == "codebase_impact_review"
    assert result.gate_payload is not None


def test_a_parked_run_is_resumable_and_a_finished_one_is_not(env: Path):
    _seed_workspace("run-resumable")
    _write_fixture("brownfield", {"svc/added.py": "x = 1\n"})
    checkpointer = build_memory_checkpointer()

    runner.start_run("run-resumable", _brownfield_state(), checkpointer)
    assert runner.is_resumable("run-resumable", checkpointer) is True

    result = runner.resume_run(
        "run-resumable", {"status": "approved", "decided_by": "human"}, checkpointer
    )
    while result.parked:
        result = runner.resume_run(
            "run-resumable", {"status": "approved", "decided_by": "human"}, checkpointer
        )

    assert result.is_terminal is True
    assert runner.is_resumable("run-resumable", checkpointer) is False


def test_run_reaches_a_completed_terminal_state_through_its_gates(env: Path):
    _seed_workspace("run-complete")
    _write_fixture("brownfield", {"svc/added.py": "x = 1\n"})
    checkpointer = build_memory_checkpointer()

    result = runner.start_run("run-complete", _brownfield_state(), checkpointer)
    gates_seen = []
    while result.parked:
        gates_seen.append(result.gate_type)
        result = runner.resume_run(
            "run-complete", {"status": "approved", "decided_by": "human"}, checkpointer
        )

    assert "codebase_impact_review" in gates_seen
    assert "merge_release_approval" in gates_seen
    assert result.terminal_state == "completed"


def test_a_rejected_release_gate_ends_the_run_without_completing(env: Path):
    _seed_workspace("run-rejected")
    _write_fixture("brownfield", {"svc/added.py": "x = 1\n"})
    checkpointer = build_memory_checkpointer()

    result = runner.start_run("run-rejected", _brownfield_state(), checkpointer)
    while result.parked:
        decision = (
            {"status": "rejected", "decided_by": "human"}
            if result.gate_type == "merge_release_approval"
            else {"status": "approved", "decided_by": "human"}
        )
        result = runner.resume_run("run-rejected", decision, checkpointer)

    assert result.terminal_state in ("failed", "safe_stop")


def test_already_known_distinguishes_a_started_run_from_an_unseen_one(env: Path):
    _seed_workspace("run-known")
    _write_fixture("brownfield", {"svc/added.py": "x = 1\n"})
    checkpointer = build_memory_checkpointer()

    assert runner.already_known("run-known", checkpointer) is False

    runner.start_run("run-known", _brownfield_state(), checkpointer)

    assert runner.already_known("run-known", checkpointer) is True
    assert runner.already_known("never-started", checkpointer) is False


def test_is_resumable_is_false_for_a_run_that_was_never_started(env: Path):
    """Reconciliation relies on this: a workspace whose run the checkpointer has

    never heard of is an orphan from a crash between clone and first checkpoint.
    """
    checkpointer = build_memory_checkpointer()
    assert runner.is_resumable("never-started", checkpointer) is False


def test_resuming_an_unknown_run_raises_rather_than_starting_a_new_one(env: Path):
    checkpointer = build_memory_checkpointer()
    with pytest.raises(KeyError):
        runner.resume_run("never-started", {"status": "approved"}, checkpointer)


def test_resuming_a_finished_run_raises(env: Path):
    _seed_workspace("run-done")
    _write_fixture("brownfield", {"svc/added.py": "x = 1\n"})
    checkpointer = build_memory_checkpointer()

    result = runner.start_run("run-done", _brownfield_state(), checkpointer)
    while result.parked:
        result = runner.resume_run(
            "run-done", {"status": "approved", "decided_by": "human"}, checkpointer
        )

    with pytest.raises(ValueError):
        runner.resume_run("run-done", {"status": "approved"}, checkpointer)


def test_missing_fixture_safe_stops_rather_than_crashing(env: Path):
    """The shipped default: no fixtures mounted. The run must end in a defined

    terminal state with a reason, not raise out of the worker.
    """
    _seed_workspace("run-nofixture")
    checkpointer = build_memory_checkpointer()

    result = runner.start_run("run-nofixture", _brownfield_state(), checkpointer)
    while result.parked:
        result = runner.resume_run(
            "run-nofixture", {"status": "approved", "decided_by": "human"}, checkpointer
        )

    assert result.terminal_state == "safe_stop"


def test_parked_since_reports_a_timestamp_only_while_parked(env: Path):
    _seed_workspace("run-parked-at")
    _write_fixture("brownfield", {"svc/added.py": "x = 1\n"})
    checkpointer = build_memory_checkpointer()

    assert runner.parked_since("run-parked-at", checkpointer) is None

    runner.start_run("run-parked-at", _brownfield_state(), checkpointer)
    since = runner.parked_since("run-parked-at", checkpointer)

    assert since is not None
    assert (datetime.now(timezone.utc) - since) < timedelta(minutes=5)


def test_find_stale_runs_expires_only_runs_parked_past_the_ttl(env: Path):
    _seed_workspace("run-ttl")
    _write_fixture("brownfield", {"svc/added.py": "x = 1\n"})
    checkpointer = build_memory_checkpointer()
    runner.start_run("run-ttl", _brownfield_state(), checkpointer)

    now = datetime.now(timezone.utc)

    assert runner.find_stale_runs(["run-ttl"], checkpointer, now=now) == []
    assert runner.find_stale_runs(
        ["run-ttl"], checkpointer, now=now + timedelta(hours=25)
    ) == ["run-ttl"]


def test_ttl_is_configurable(env: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("PARKED_RUN_TTL_HOURS", "1")
    _seed_workspace("run-short-ttl")
    _write_fixture("brownfield", {"svc/added.py": "x = 1\n"})
    checkpointer = build_memory_checkpointer()
    runner.start_run("run-short-ttl", _brownfield_state(), checkpointer)

    now = datetime.now(timezone.utc) + timedelta(hours=2)

    assert runner.find_stale_runs(["run-short-ttl"], checkpointer, now=now) == ["run-short-ttl"]


def test_find_stale_runs_ignores_runs_that_are_not_parked(env: Path):
    checkpointer = build_memory_checkpointer()
    assert runner.find_stale_runs(["never-started"], checkpointer) == []


def _postgres_reachable() -> bool:
    import os

    import psycopg

    from agentic_control_plane.checkpointer import _postgres_conn_string

    if not os.environ.get("POSTGRES_USER"):
        return False
    try:
        with psycopg.connect(_postgres_conn_string(), connect_timeout=3):
            return True
    except psycopg.OperationalError:
        return False


@pytest.mark.skipif(
    not _postgres_reachable(), reason="requires PostgreSQL reachable via POSTGRES_* env vars"
)
def test_gate_interrupt_persists_and_resumes_across_a_fresh_connection(env: Path):
    """The property that makes a human gate usable, tested against real Postgres.

    A run parks at a gate under one checkpointer connection, that connection is
    closed entirely, and a second independent one resumes the run to completion.
    Nothing about the parked run lives in the process that started it - which is why
    a consumer rebalance, restart or redeploy in between changes nothing, and why
    the poll loop is free to return immediately.

    MemorySaver would pass every other test in this module and fail this one.
    """
    from agentic_control_plane.checkpointer import build_postgres_checkpointer

    run_id = f"durability-{datetime.now(timezone.utc).timestamp()}"
    _seed_workspace(run_id)
    _write_fixture("brownfield", {"svc/added.py": "x = 1\n"})

    with build_postgres_checkpointer() as checkpointer:
        result = runner.start_run(run_id, _brownfield_state(), checkpointer)
        assert result.parked is True
        first_gate = result.gate_type

    # Everything above is now out of scope: connection closed, no in-process state.
    with build_postgres_checkpointer() as fresh:
        assert runner.is_resumable(run_id, fresh) is True
        assert runner.parked_since(run_id, fresh) is not None

        result = runner.resume_run(run_id, {"status": "approved", "decided_by": "human"}, fresh)
        while result.parked:
            result = runner.resume_run(
                run_id, {"status": "approved", "decided_by": "human"}, fresh
            )

    assert first_gate == "codebase_impact_review"
    assert result.terminal_state == "completed"
