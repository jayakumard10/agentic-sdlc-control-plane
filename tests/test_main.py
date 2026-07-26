"""Tests for the entrypoint's startup ordering and shutdown behaviour.

Kafka and Postgres are not involved here: what is being checked is the wiring that
decides *when* things happen relative to each other, which is where an entrypoint
usually goes wrong.
"""

from __future__ import annotations

import threading
from pathlib import Path

import pytest

from agentic_control_plane import consumer, main, runner, tools, workspace
from agentic_control_plane.checkpointer import build_memory_checkpointer


@pytest.fixture()
def env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv("WORKSPACES_ROOT", str(tmp_path / "workspaces"))
    monkeypatch.setenv("FIXTURES_DIR", str(tmp_path / "fixtures"))
    monkeypatch.setenv("AUDIT_LOG_PATH", str(tmp_path / "audit" / "runs.jsonl"))
    return tmp_path


def test_main_refuses_to_start_without_a_broker_configured(monkeypatch: pytest.MonkeyPatch):
    """Failing at boot beats appearing healthy and silently consuming nothing."""
    monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS", raising=False)
    with pytest.raises(SystemExit):
        main.main()


def test_reconcile_removes_orphans_but_keeps_a_resumable_run(env: Path):
    """Reconciliation runs before consumption starts, so it can assume every

    workspace it sees predates this process.
    """
    checkpointer = build_memory_checkpointer()
    for run_id in ("orphan-a", "orphan-b"):
        ws = workspace.workspace_for(run_id)
        tools.write_code_files(ws, {"svc/x.py": "x = 1\n"})

    main._reconcile_workspaces(checkpointer)

    assert not workspace.workspace_for("orphan-a").exists()
    assert not workspace.workspace_for("orphan-b").exists()


def test_reconcile_keeps_the_workspace_of_a_run_still_parked(env: Path, monkeypatch):
    checkpointer = build_memory_checkpointer()
    ws = workspace.workspace_for("parked-run")
    tools.write_code_files(ws, {"svc/x.py": "x = 1\n"})
    monkeypatch.setattr(runner, "is_resumable", lambda run_id, _cp: run_id == "parked-run")

    main._reconcile_workspaces(checkpointer)

    assert workspace.workspace_for("parked-run").exists()


def test_audit_log_path_is_configurable(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("AUDIT_LOG_PATH", "/tmp/custom/audit.jsonl")
    assert main.audit_log_path() == Path("/tmp/custom/audit.jsonl")


def test_sweep_loop_survives_a_failing_pass(env: Path, monkeypatch: pytest.MonkeyPatch):
    """A sweep thread that dies on its first bad pass stops expiring parked runs

    forever, silently. That exact failure has happened in this platform before.
    """
    worker = consumer.Worker(build_memory_checkpointer())
    calls: list[int] = []

    def _explode():
        calls.append(1)
        raise RuntimeError("checkpointer unavailable")

    monkeypatch.setattr(worker, "sweep_stale_runs", _explode)
    monkeypatch.setattr(main, "SWEEP_INTERVAL_SECONDS", 0.01)

    stop = threading.Event()
    thread = threading.Thread(target=main._run_sweep_loop, args=(worker, stop), daemon=True)
    thread.start()
    threading.Event().wait(0.2)
    stop.set()
    thread.join(timeout=2)

    assert not thread.is_alive()
    assert len(calls) > 1, "the loop must keep sweeping after a failed pass"


def test_drain_reports_work_still_queued_at_shutdown(
    env: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
):
    """The loss window is logged as an error at shutdown rather than passed over."""
    monkeypatch.setattr(main, "SHUTDOWN_DRAIN_SECONDS", 0.05)
    worker = consumer.Worker(build_memory_checkpointer())
    worker.submit(consumer.TriggerWork("queued", "brownfield", "u", "main", "r"))

    with caplog.at_level("ERROR", logger="agentic_control_plane.main"):
        main._drain(worker)

    assert any("still queued" in record.message for record in caplog.records)


def test_drain_returns_promptly_when_nothing_is_queued(env: Path):
    worker = consumer.Worker(build_memory_checkpointer())
    main._drain(worker)
    assert worker.queue.empty()
