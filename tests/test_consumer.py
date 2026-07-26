"""Tests for message validation, work hand-off, and the worker's own decisions.

The property under test throughout is separation: a poll loop validates and hands
off, and everything expensive or slow happens somewhere the loop cannot observe.
"""

from __future__ import annotations

import json
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import pytest

from agentic_control_plane import consumer, events, runner, tools, workspace
from agentic_control_plane.checkpointer import build_memory_checkpointer


def _envelope(
    *,
    event_type: str = "drift-detected",
    correlation_id: str = "run-1",
    repo_url: str = "https://github.com/example/target.git",
    branch: str = "main",
    scenario_type: str = "brownfield",
    metrics: dict | None = None,
    payload: dict | None = None,
) -> str:
    return json.dumps(
        {
            "schema_version": "1.0",
            "event_id": str(uuid4()),
            "correlation_id": correlation_id,
            "tenant": "default",
            "service": "agentic-sdlc-mlops",
            "event_type": event_type,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "producer": {"service": "agentic-sdlc-mlops", "instance_id": "probe"},
            "git_target": {"repo_url": repo_url, "branch": branch, "commit_sha": None},
            "scenario_type": scenario_type,
            "metrics": metrics
            if metrics is not None
            else {
                "metric_name": "p95_latency_ms",
                "reference_value": 42.1,
                "current_value": 61.8,
                "relative_delta_pct": 46.8,
                "threshold_pct": 20.0,
            },
            "payload": payload or {},
        }
    )


# --- trigger parsing -------------------------------------------------------


def test_parse_trigger_extracts_the_run_target():
    work = consumer.parse_trigger(_envelope(correlation_id="run-abc"))

    assert work.run_id == "run-abc"
    assert work.repo_url == "https://github.com/example/target.git"
    assert work.branch == "main"
    assert work.scenario_type == "brownfield"


def test_run_id_is_the_producers_correlation_id_so_redelivery_dedupes():
    """The producer derives correlation_id deterministically from the drift

    condition, so the same condition always yields the same run_id here. That is
    what makes an at-least-once redelivery a no-op instead of a second run.
    """
    first = consumer.parse_trigger(_envelope(correlation_id="stable-id"))
    second = consumer.parse_trigger(_envelope(correlation_id="stable-id"))

    assert first.run_id == second.run_id


def test_requirement_text_is_built_from_whatever_metric_the_event_names():
    """No fixed vocabulary: a new metric from a new producer needs no change here."""
    work = consumer.parse_trigger(
        _envelope(metrics={"metric_name": "queue_depth", "current_value": 900})
    )

    assert "queue_depth" in work.requirement
    assert "900" in work.requirement


def test_requirement_text_survives_an_event_with_no_metrics():
    work = consumer.parse_trigger(_envelope(metrics={}))
    assert work.requirement


def test_parse_trigger_rejects_malformed_json():
    with pytest.raises(consumer.InvalidMessage):
        consumer.parse_trigger(b"{not json")


def test_parse_trigger_rejects_an_envelope_missing_required_fields():
    with pytest.raises(consumer.InvalidMessage):
        consumer.parse_trigger(json.dumps({"schema_version": "1.0"}))


def test_parse_trigger_rejects_the_wrong_event_type_on_the_topic():
    with pytest.raises(consumer.InvalidMessage):
        consumer.parse_trigger(_envelope(event_type="run-outcome"))


def test_parse_trigger_rejects_an_unrecognized_scenario_type():
    """scenario_type flows straight into GraphState, which constrains it. Rejecting

    here keeps a bad value from becoming an unhandled routing case later.
    """
    raw = json.loads(_envelope())
    raw["scenario_type"] = "not-a-scenario"
    with pytest.raises(consumer.InvalidMessage):
        consumer.parse_trigger(json.dumps(raw))


# --- decision parsing ------------------------------------------------------


def test_parse_decision_normalizes_approve_to_approved():
    """The event contract's worked example uses "approve"; GateRecord's status

    vocabulary uses "approved". Normalizing here keeps both correct.
    """
    work = consumer.parse_decision(
        _envelope(
            event_type="gate-decision",
            correlation_id="run-xyz",
            payload={"gate_type": "merge_release_approval", "decision": "approve"},
        )
    )

    assert work.run_id == "run-xyz"
    assert work.decision["status"] == "approved"


def test_parse_decision_accepts_the_already_normalized_form():
    work = consumer.parse_decision(
        _envelope(event_type="gate-decision", payload={"status": "rejected"})
    )
    assert work.decision["status"] == "rejected"


def test_parse_decision_carries_the_guardrail_override_through():
    work = consumer.parse_decision(
        _envelope(
            event_type="gate-decision",
            payload={"decision": "approve", "override_guardrails": True},
        )
    )
    assert work.decision["override_guardrails"] is True


def test_a_username_in_decided_by_maps_to_provenance_not_straight_through():
    """Regression, found by a real end-to-end run and by nothing else.

    The event contract's `decided_by` is an identity - its own worked example
    carries a username. GraphState's is provenance, constrained to human/replayed.
    Passing the wire value through failed GateRecord validation for every username
    that is not literally "human", which is all of them.
    """
    work = consumer.parse_decision(
        _envelope(
            event_type="gate-decision",
            payload={"decision": "approve", "decided_by": "jayakumard10"},
        )
    )

    assert work.decision["decided_by"] == "human"
    assert work.decision["decided_by_identity"] == "jayakumard10"


def test_the_resulting_decision_actually_builds_a_valid_gate_record():
    """The assertion that would have caught the above before a live run did.

    Validating the shape parse_decision produces against the model it is destined
    for, rather than against what the parser happens to emit.
    """
    from agentic_control_plane.state import GateRecord

    work = consumer.parse_decision(
        _envelope(
            event_type="gate-decision",
            payload={"decision": "approve", "decided_by": "some-operator"},
        )
    )

    record = GateRecord(
        gate_type="merge_release_approval",
        status=work.decision["status"],
        decision_payload=str(work.decision),
        decided_by=work.decision["decided_by"],
    )

    assert record.decided_by == "human"
    assert "some-operator" in record.decision_payload


def test_a_replayed_decision_keeps_its_replayed_provenance():
    work = consumer.parse_decision(
        _envelope(event_type="gate-decision", payload={"decision": "approve", "decided_by": "replayed"})
    )
    assert work.decision["decided_by"] == "replayed"


def test_clarified_requirement_is_absent_rather_than_none_when_not_supplied():
    """Regression: the gate nodes read this with .get(key, current_value), so a

    present-but-None key overwrites the clarified requirement with None, which
    GraphState rejects. Absence and None are not interchangeable here.
    """
    work = consumer.parse_decision(
        _envelope(event_type="gate-decision", payload={"decision": "approve"})
    )

    assert "clarified_requirement" not in work.decision


def test_clarified_requirement_is_carried_through_when_supplied():
    work = consumer.parse_decision(
        _envelope(
            event_type="gate-decision",
            payload={"decision": "approve", "clarified_requirement": "do the narrower thing"},
        )
    )

    assert work.decision["clarified_requirement"] == "do the narrower thing"


def test_parse_decision_rejects_a_payload_with_no_usable_status():
    with pytest.raises(consumer.InvalidMessage):
        consumer.parse_decision(_envelope(event_type="gate-decision", payload={"note": "hi"}))


def test_parse_decision_rejects_the_wrong_event_type():
    with pytest.raises(consumer.InvalidMessage):
        consumer.parse_decision(_envelope(event_type="drift-detected"))


# --- poll loop -------------------------------------------------------------


class _FakeMessage:
    def __init__(self, value: str, topic: str = "mlops.drift-detected.v1", offset: int = 0):
        self.value = value.encode("utf-8") if isinstance(value, str) else value
        self.topic = topic
        self.offset = offset


class _FakeConsumer:
    def __init__(self, batches: list[dict]):
        self._batches = list(batches)
        self.commits = 0

    def poll(self, timeout_ms=None):
        if self._batches:
            return self._batches.pop(0)
        return {}

    def commit(self):
        self.commits += 1


def _drain(fake_consumer, parse, worker):
    """Run the poll loop until the fake consumer is exhausted."""
    stop = threading.Event()

    def _stop_when_empty():
        while fake_consumer._batches:
            pass
        stop.set()

    thread = threading.Thread(target=_stop_when_empty, daemon=True)
    thread.start()
    consumer.poll_loop(fake_consumer, parse, worker, stop)
    thread.join(timeout=2)


def test_poll_loop_hands_valid_messages_to_the_worker(monkeypatch: pytest.MonkeyPatch):
    worker = consumer.Worker(build_memory_checkpointer())
    fake = _FakeConsumer([{"p0": [_FakeMessage(_envelope(correlation_id="run-q"))]}])

    _drain(fake, consumer.parse_trigger, worker)

    assert worker.queue.qsize() == 1
    assert worker.queue.get().run_id == "run-q"
    assert fake.commits == 1


def test_poll_loop_sends_a_poison_message_to_the_dlq_and_commits_past_it(
    monkeypatch: pytest.MonkeyPatch,
):
    """A poison message must never block its partition: the offset advances past it

    and the payload is preserved on the DLQ for inspection.
    """
    forwarded: list[tuple] = []
    monkeypatch.setattr(
        events, "publish_to_dlq", lambda raw, topic, err: forwarded.append((raw, topic, err))
    )
    worker = consumer.Worker(build_memory_checkpointer())
    fake = _FakeConsumer([{"p0": [_FakeMessage("{not json", offset=7)]}])

    _drain(fake, consumer.parse_trigger, worker)

    assert worker.queue.qsize() == 0
    assert len(forwarded) == 1
    assert fake.commits == 1


def test_poll_loop_keeps_going_after_a_poison_message(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(events, "publish_to_dlq", lambda *a, **k: None)
    worker = consumer.Worker(build_memory_checkpointer())
    fake = _FakeConsumer(
        [
            {
                "p0": [
                    _FakeMessage("{not json", offset=1),
                    _FakeMessage(_envelope(correlation_id="run-after"), offset=2),
                ]
            }
        ]
    )

    _drain(fake, consumer.parse_trigger, worker)

    assert worker.queue.qsize() == 1
    assert worker.queue.get().run_id == "run-after"


def test_submit_reports_failure_rather_than_blocking_when_the_queue_is_full():
    """The poll loop must never block on a full queue - that would be the same

    stall the worker split exists to prevent, arriving by a different route.
    """
    worker = consumer.Worker(build_memory_checkpointer())
    for i in range(consumer.WORK_QUEUE_MAXSIZE):
        assert worker.submit(consumer.TriggerWork(str(i), "brownfield", "u", "main", "r")) is True

    assert worker.submit(consumer.TriggerWork("overflow", "brownfield", "u", "main", "r")) is False


# --- worker ----------------------------------------------------------------


@pytest.fixture()
def worker_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("WORKSPACES_ROOT", str(tmp_path / "workspaces"))
    monkeypatch.setenv("FIXTURES_DIR", str(tmp_path / "fixtures"))
    monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS", raising=False)
    monkeypatch.delenv("ORCHESTRATOR_MODE", raising=False)
    events._reset_for_tests()
    yield tmp_path
    events._reset_for_tests()


@pytest.fixture()
def origin(tmp_path: Path) -> Path:
    repo = tmp_path / "origin"
    tools.write_code_files(
        repo,
        {
            "svc/main.py": "def handle():\n    return 1\n",
            "tests/test_main.py": "from svc.main import handle\n\ndef test_handle():\n    assert handle() == 1\n",
        },
    )
    tools.git_commit_all(repo, "initial")
    import subprocess

    subprocess.run(["git", "branch", "-M", "main"], cwd=repo, check=True, capture_output=True)
    return repo


def _published(monkeypatch: pytest.MonkeyPatch) -> list:
    captured: list = []
    monkeypatch.setattr(events, "publish_run_outcome", captured.append)
    return captured


def test_worker_clones_and_parks_at_the_first_gate(
    worker_env: Path, origin: Path, monkeypatch: pytest.MonkeyPatch
):
    outcomes = _published(monkeypatch)
    checkpointer = build_memory_checkpointer()
    worker = consumer.Worker(checkpointer)

    worker.handle_trigger(
        consumer.TriggerWork("run-w1", "brownfield", str(origin), "main", "fix it")
    )

    assert runner.is_resumable("run-w1", checkpointer) is True
    assert workspace.workspace_for("run-w1").exists()
    assert outcomes == [], "a parked run has no outcome yet"


def test_worker_treats_a_redelivered_trigger_as_a_noop(
    worker_env: Path, origin: Path, monkeypatch: pytest.MonkeyPatch
):
    """At-least-once delivery must not produce a second run for the same drift."""
    _published(monkeypatch)
    checkpointer = build_memory_checkpointer()
    worker = consumer.Worker(checkpointer)
    work = consumer.TriggerWork("run-dupe", "brownfield", str(origin), "main", "fix it")

    worker.handle_trigger(work)
    first_snapshot = runner.snapshot_for("run-dupe", checkpointer).created_at

    worker.handle_trigger(work)
    second_snapshot = runner.snapshot_for("run-dupe", checkpointer).created_at

    assert first_snapshot == second_snapshot


def test_worker_publishes_clone_failed_without_starting_a_run(
    worker_env: Path, monkeypatch: pytest.MonkeyPatch
):
    outcomes = _published(monkeypatch)
    checkpointer = build_memory_checkpointer()
    worker = consumer.Worker(checkpointer)

    worker.handle_trigger(
        consumer.TriggerWork("run-badclone", "brownfield", "/nope/not/a/repo", "main", "fix")
    )

    assert len(outcomes) == 1
    assert outcomes[0].payload["terminal_state"] == "clone_failed"
    assert runner.already_known("run-badclone", checkpointer) is False


def test_worker_resumes_a_parked_run_and_publishes_the_outcome(
    worker_env: Path, origin: Path, monkeypatch: pytest.MonkeyPatch
):
    outcomes = _published(monkeypatch)
    (worker_env / "fixtures" / "brownfield").mkdir(parents=True)
    (worker_env / "fixtures" / "brownfield" / "transcript.json").write_text(
        json.dumps(
            {
                "scenario_type": "brownfield",
                "attempts": [
                    {"attempt_number": 1, "code_files": {"svc/added.py": "x = 1\n"}, "rationale": "ok"}
                ],
            }
        ),
        encoding="utf-8",
    )
    checkpointer = build_memory_checkpointer()
    worker = consumer.Worker(checkpointer)
    worker.handle_trigger(
        consumer.TriggerWork("run-resume", "brownfield", str(origin), "main", "fix it")
    )

    while runner.is_resumable("run-resume", checkpointer):
        worker.handle_decision(
            consumer.DecisionWork("run-resume", {"status": "approved", "decided_by": "human"})
        )

    assert len(outcomes) == 1
    assert outcomes[0].payload["terminal_state"] == "completed"
    assert outcomes[0].correlation_id == "run-resume"


def test_workspace_is_cleaned_up_on_a_terminal_state(
    worker_env: Path, origin: Path, monkeypatch: pytest.MonkeyPatch
):
    _published(monkeypatch)
    checkpointer = build_memory_checkpointer()
    worker = consumer.Worker(checkpointer)
    worker.handle_trigger(
        consumer.TriggerWork("run-cleanup", "brownfield", str(origin), "main", "fix it")
    )
    assert workspace.workspace_for("run-cleanup").exists()

    while runner.is_resumable("run-cleanup", checkpointer):
        worker.handle_decision(
            consumer.DecisionWork("run-cleanup", {"status": "approved", "decided_by": "human"})
        )

    assert not workspace.workspace_for("run-cleanup").exists()


def test_a_node_failure_becomes_a_reported_failed_run_not_a_vanished_one(
    worker_env: Path, origin: Path, monkeypatch: pytest.MonkeyPatch
):
    """Regression, found by a real run. Two bugs compounded here.

    handle_decision caught KeyError/ValueError around the whole resume call, so the
    same builtins raised from inside a graph node were logged as "already reached a
    terminal state" and dropped. And a run that died mid-node was left with no
    pending work but a run_status of "running" - neither resumable nor terminal - so
    nothing published an outcome and the next startup reconciled its workspace away.

    A governed system may end a run in failure. It may not lose one.
    """
    outcomes = _published(monkeypatch)
    checkpointer = build_memory_checkpointer()
    worker = consumer.Worker(checkpointer)
    worker.handle_trigger(
        consumer.TriggerWork("run-explode", "brownfield", str(origin), "main", "fix it")
    )
    monkeypatch.setattr(
        runner,
        "resume_run",
        lambda *_a, **_k: (_ for _ in ()).throw(ValueError("a node blew up")),
    )

    worker.handle_decision(consumer.DecisionWork("run-explode", {"status": "approved"}))

    assert len(outcomes) == 1
    assert outcomes[0].payload["terminal_state"] == "failed"
    assert "a node blew up" in outcomes[0].payload["detail"]
    assert not workspace.workspace_for("run-explode").exists()


def test_a_failure_starting_a_run_is_reported_too(
    worker_env: Path, origin: Path, monkeypatch: pytest.MonkeyPatch
):
    outcomes = _published(monkeypatch)
    worker = consumer.Worker(build_memory_checkpointer())
    monkeypatch.setattr(
        runner, "start_run", lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("boom"))
    )

    worker.handle_trigger(
        consumer.TriggerWork("run-startfail", "brownfield", str(origin), "main", "fix it")
    )

    assert outcomes[0].payload["terminal_state"] == "failed"
    assert "boom" in outcomes[0].payload["detail"]


def test_poll_loop_survives_a_transient_client_error(monkeypatch: pytest.MonkeyPatch):
    """Regression, found by a real run: a selector error inside the Kafka client

    propagated out of poll(), out of the loop, and terminated the process. A whole
    control plane stopped by one bad file descriptor.
    """
    monkeypatch.setattr(consumer, "POLL_RETRY_BACKOFF_SECONDS", 0.01)
    worker = consumer.Worker(build_memory_checkpointer())

    class _FlakyConsumer:
        def __init__(self):
            self.calls = 0
            self.commits = 0

        def poll(self, timeout_ms=None):
            self.calls += 1
            if self.calls == 1:
                raise ValueError("Invalid file descriptor: -1")
            if self.calls == 2:
                return {"p0": [_FakeMessage(_envelope(correlation_id="run-after-flake"))]}
            return {}

        def commit(self):
            self.commits += 1

    flaky = _FlakyConsumer()
    stop = threading.Event()

    def _stop_soon():
        while flaky.calls < 3:
            pass
        stop.set()

    thread = threading.Thread(target=_stop_soon, daemon=True)
    thread.start()
    consumer.poll_loop(flaky, consumer.parse_trigger, worker, stop)
    thread.join(timeout=2)

    assert worker.queue.qsize() == 1
    assert worker.queue.get().run_id == "run-after-flake"


def test_poll_loop_gives_up_after_repeated_failures(monkeypatch: pytest.MonkeyPatch):
    """Retrying forever would hide a permanently broken consumer behind a quiet loop."""
    monkeypatch.setattr(consumer, "POLL_RETRY_BACKOFF_SECONDS", 0.001)
    monkeypatch.setattr(consumer, "MAX_CONSECUTIVE_POLL_FAILURES", 3)
    worker = consumer.Worker(build_memory_checkpointer())

    class _BrokenConsumer:
        def poll(self, timeout_ms=None):
            raise OSError("socket is gone")

        def commit(self):  # pragma: no cover - never reached
            pass

    with pytest.raises(OSError):
        consumer.poll_loop(_BrokenConsumer(), consumer.parse_trigger, worker, threading.Event())


def test_a_decision_for_an_unknown_run_is_ignored_not_fatal(
    worker_env: Path, monkeypatch: pytest.MonkeyPatch
):
    """A decision may arrive for a run this deployment never had. Ignoring it beats

    crashing the worker on someone else's message.
    """
    outcomes = _published(monkeypatch)
    worker = consumer.Worker(build_memory_checkpointer())

    worker.handle_decision(consumer.DecisionWork("never-heard-of-it", {"status": "approved"}))

    assert outcomes == []


def test_a_second_decision_for_a_finished_run_is_ignored(
    worker_env: Path, origin: Path, monkeypatch: pytest.MonkeyPatch
):
    outcomes = _published(monkeypatch)
    checkpointer = build_memory_checkpointer()
    worker = consumer.Worker(checkpointer)
    worker.handle_trigger(
        consumer.TriggerWork("run-twice", "brownfield", str(origin), "main", "fix it")
    )
    while runner.is_resumable("run-twice", checkpointer):
        worker.handle_decision(
            consumer.DecisionWork("run-twice", {"status": "approved", "decided_by": "human"})
        )
    outcome_count = len(outcomes)

    worker.handle_decision(consumer.DecisionWork("run-twice", {"status": "approved"}))

    assert len(outcomes) == outcome_count


def test_sweep_expires_a_run_parked_past_the_ttl_and_reports_it(
    worker_env: Path, origin: Path, monkeypatch: pytest.MonkeyPatch
):
    """A human decision that never arrives has to become visible rather than

    leaving a run stuck forever holding a workspace.
    """
    outcomes = _published(monkeypatch)
    checkpointer = build_memory_checkpointer()
    worker = consumer.Worker(checkpointer)
    worker.handle_trigger(
        consumer.TriggerWork("run-stale", "brownfield", str(origin), "main", "fix it")
    )

    assert worker.sweep_stale_runs(now=datetime.now(timezone.utc)) == []

    stale = worker.sweep_stale_runs(now=datetime.now(timezone.utc) + timedelta(hours=25))

    assert stale == ["run-stale"]
    assert outcomes[-1].payload["terminal_state"] == "stale"
    assert not workspace.workspace_for("run-stale").exists()


def test_worker_loop_survives_a_failing_work_item(
    worker_env: Path, monkeypatch: pytest.MonkeyPatch
):
    """One bad run must not take the worker thread down with it."""
    worker = consumer.Worker(build_memory_checkpointer())
    monkeypatch.setattr(
        worker, "handle_trigger", lambda _w: (_ for _ in ()).throw(RuntimeError("boom"))
    )
    stop = threading.Event()
    worker.submit(consumer.TriggerWork("bad", "brownfield", "u", "main", "r"))

    thread = threading.Thread(target=worker.run_forever, args=(stop,), daemon=True)
    thread.start()
    worker.queue.join()
    stop.set()
    thread.join(timeout=3)

    assert not thread.is_alive()
