"""Tests for outcome publishing and DLQ forwarding against a fake producer.

No real broker: what matters here is the envelope shape, the partition key, and that
a broker problem never propagates into the caller. Real-broker behaviour is covered
by the functional verification run instead, for the same reason the other services
in this platform draw the line there.
"""

from __future__ import annotations

import json

import pytest

from agentic_control_plane import events


class _FakeFuture:
    def __init__(self) -> None:
        self.errback = None

    def add_errback(self, fn):
        self.errback = fn


class _FakeProducer:
    def __init__(self, raise_on_send: bool = False) -> None:
        self.sent: list[tuple[str, str, str]] = []
        self.futures: list[_FakeFuture] = []
        self.raise_on_send = raise_on_send
        self.flushed = False

    def send(self, topic, key=None, value=None):
        if self.raise_on_send:
            raise RuntimeError("broker unreachable")
        self.sent.append((topic, key, value))
        future = _FakeFuture()
        self.futures.append(future)
        return future

    def flush(self, timeout=None):
        self.flushed = True


@pytest.fixture(autouse=True)
def clean_module_state():
    events._reset_for_tests()
    yield
    events._reset_for_tests()


@pytest.fixture()
def fake_producer(monkeypatch: pytest.MonkeyPatch) -> _FakeProducer:
    producer = _FakeProducer()
    monkeypatch.setattr(events, "_get_producer", lambda: producer)
    return producer


def _outcome(**overrides):
    kwargs = {
        "run_id": "0190f7d1-2a3b-7c4d-8e5f-6a7b8c9d0e1f",
        "terminal_state": "completed",
        "scenario_type": "brownfield",
        "repo_url": "https://github.com/example/target.git",
        "branch": "main",
        "commit_sha": "a1b2c3d4e5f60718293a4b5c6d7e8f9012345678",
    }
    kwargs.update(overrides)
    return events.build_run_outcome(**kwargs)


def test_outcome_envelope_matches_the_shared_contract():
    envelope = _outcome(detail="run completed")

    assert envelope.schema_version == "1.0"
    assert envelope.service == "agentic-sdlc-control-plane"
    assert envelope.event_type == "run-outcome"
    assert envelope.tenant == "default"
    assert envelope.payload["terminal_state"] == "completed"
    assert envelope.git_target.branch == "main"


def test_correlation_id_is_the_run_id_which_is_the_thread_id():
    """One identifier spans the trigger, the checkpoint, the decision and the

    outcome. A different correlation id here would break the trace at its last hop.
    """
    envelope = _outcome(run_id="run-abc")
    assert envelope.correlation_id == "run-abc"


def test_outcome_is_keyed_by_run_id_so_a_run_stays_ordered(fake_producer: _FakeProducer):
    events.publish_run_outcome(_outcome(run_id="run-key"))

    topic, key, _value = fake_producer.sent[0]
    assert topic == "control-plane.run-outcome.v1"
    assert key == "run-key"


def test_published_value_round_trips_as_the_envelope(fake_producer: _FakeProducer):
    events.publish_run_outcome(_outcome(terminal_state="safe_stop"))

    _topic, _key, value = fake_producer.sent[0]
    decoded = json.loads(value)
    assert decoded["payload"]["terminal_state"] == "safe_stop"
    assert decoded["event_type"] == "run-outcome"


@pytest.mark.parametrize(
    "terminal_state", ["completed", "failed", "safe_stop", "clone_failed", "stale"]
)
def test_every_terminal_state_is_publishable(fake_producer: _FakeProducer, terminal_state: str):
    """Including 'stale' and 'clone_failed' - the two that exist specifically so a

    run that never got anywhere is still visible rather than silently absent.
    """
    events.publish_run_outcome(_outcome(terminal_state=terminal_state))

    _topic, _key, value = fake_producer.sent[0]
    assert json.loads(value)["payload"]["terminal_state"] == terminal_state


def test_publish_is_a_noop_when_no_broker_is_configured(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS", raising=False)
    events.publish_run_outcome(_outcome())
    assert events.publish_failures == 0


def test_a_send_failure_is_counted_and_never_raised(monkeypatch: pytest.MonkeyPatch):
    """A broker outage must not turn a completed run into a failed one. The run

    completed; the outcome event reports that, it is not part of it.
    """
    monkeypatch.setattr(events, "_get_producer", lambda: _FakeProducer(raise_on_send=True))

    events.publish_run_outcome(_outcome())

    assert events.publish_failures == 1


def test_an_async_delivery_failure_is_counted(fake_producer: _FakeProducer):
    """send() succeeding only means the message was queued. Delivery can still fail

    afterwards, which arrives through the errback rather than as an exception.
    """
    events.publish_run_outcome(_outcome())
    assert events.publish_failures == 0

    errback = fake_producer.futures[0].errback
    assert errback is not None, "publish must register an errback on the send future"
    errback(RuntimeError("delivery timed out"))

    assert events.publish_failures == 1


def test_dlq_forwards_the_raw_payload_and_the_reason(fake_producer: _FakeProducer):
    events.publish_to_dlq(b"{not valid json", "mlops.drift-detected.v1", "JSONDecodeError")

    topic, key, value = fake_producer.sent[0]
    report = json.loads(value)
    assert topic == "control-plane.dlq.v1"
    assert key == "mlops.drift-detected.v1"
    assert report["raw_value"] == "{not valid json"
    assert report["error"] == "JSONDecodeError"
    assert report["source_topic"] == "mlops.drift-detected.v1"


def test_dlq_report_is_not_itself_envelope_validated(fake_producer: _FakeProducer):
    """A message reaches the DLQ precisely because it failed the envelope contract.

    Validating the report against that same contract would discard the evidence.
    """
    events.publish_to_dlq('{"schema_version": "99.0"}', "some.topic.v1", "unsupported version")

    _topic, _key, value = fake_producer.sent[0]
    report = json.loads(value)
    assert "schema_version" not in report
    assert report["raw_value"] == '{"schema_version": "99.0"}'


def test_dlq_handles_undecodable_bytes_without_raising(fake_producer: _FakeProducer):
    events.publish_to_dlq(b"\xff\xfe not utf-8", "some.topic.v1", "UnicodeDecodeError")

    _topic, _key, value = fake_producer.sent[0]
    assert json.loads(value)["error"] == "UnicodeDecodeError"


def test_flush_is_safe_when_no_producer_was_ever_built():
    events.flush()
