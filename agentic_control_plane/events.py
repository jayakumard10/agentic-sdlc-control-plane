"""Publishes run-outcome events and forwards poison messages to the DLQ.

Mirrors the bounded, best-effort producer pattern the other services in this
platform converged on: constructing a KafkaProducer against an unreachable broker
blocks indefinitely, so construction happens on a background thread the caller joins
with a short bound. Publish failures are logged and counted, never raised - a broker
outage must not turn a completed run into a failed one, because the run really did
complete and the outcome event is a report of that fact, not part of it.
"""

from __future__ import annotations

import logging
import os
import socket
import threading
from datetime import datetime, timezone
from uuid import uuid4

from agentic_events import EventEnvelope, GitTarget, Producer

logger = logging.getLogger(__name__)

SERVICE_NAME = "agentic-sdlc-control-plane"
RUN_OUTCOME_TOPIC = "control-plane.run-outcome.v1"
GATE_DECISION_TOPIC = "control-plane.gate-decision.v1"
DLQ_TOPIC = "control-plane.dlq.v1"

_PRODUCER_INIT_JOIN_TIMEOUT_S = 1.0

_instance_id = socket.gethostname()
_producer = None
_producer_init_failed = False
_producer_init_thread: threading.Thread | None = None
_producer_init_lock = threading.Lock()
publish_failures = 0


def _construct_producer(bootstrap_servers: str) -> None:
    global _producer, _producer_init_failed
    try:
        from kafka import KafkaProducer

        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: v.encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k is not None else None,
            api_version=(2, 5, 0),
            request_timeout_ms=2000,
            max_block_ms=2000,
            retries=3,
        )
    except Exception:
        logger.exception("failed to initialize Kafka producer; outcome publishing disabled")
        _producer_init_failed = True
        return
    _producer = producer


def _get_producer():
    global _producer_init_thread, _producer_init_failed

    if _producer is not None or _producer_init_failed:
        return _producer

    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
    if not bootstrap_servers:
        _producer_init_failed = True
        logger.info("KAFKA_BOOTSTRAP_SERVERS not set; outcome publishing disabled")
        return None

    with _producer_init_lock:
        if _producer_init_thread is None:
            _producer_init_thread = threading.Thread(
                target=_construct_producer, args=(bootstrap_servers,), daemon=True
            )
            _producer_init_thread.start()

    _producer_init_thread.join(timeout=_PRODUCER_INIT_JOIN_TIMEOUT_S)
    return _producer


def _on_send_error(exc: BaseException) -> None:
    global publish_failures
    publish_failures += 1
    logger.warning("publish failed: %s", exc)


def build_run_outcome(
    *,
    run_id: str,
    terminal_state: str,
    scenario_type: str,
    repo_url: str,
    branch: str,
    commit_sha: str | None,
    detail: str = "",
    metrics: dict | None = None,
) -> EventEnvelope:
    """The envelope reporting how a run ended.

    correlation_id is the run_id, which is also the LangGraph thread_id - one
    identifier across the trigger that started the run, the checkpoint that holds it,
    the decision that resumes it, and this outcome.
    """
    return EventEnvelope(
        event_id=uuid4(),
        correlation_id=run_id,
        service=SERVICE_NAME,
        event_type="run-outcome",
        timestamp=datetime.now(timezone.utc),
        producer=Producer(service=SERVICE_NAME, instance_id=_instance_id),
        git_target=GitTarget(repo_url=repo_url, branch=branch, commit_sha=commit_sha),
        scenario_type=scenario_type,
        metrics=metrics or {},
        payload={"terminal_state": terminal_state, "detail": detail},
    )


def publish_run_outcome(envelope: EventEnvelope) -> None:
    """Publish an outcome, keyed by run_id so a run's events stay ordered."""
    producer = _get_producer()
    if producer is None:
        return
    try:
        future = producer.send(
            RUN_OUTCOME_TOPIC,
            key=envelope.correlation_id,
            value=envelope.model_dump_json(),
        )
        future.add_errback(_on_send_error)
    except Exception as exc:  # pragma: no cover - defensive, mirrors add_errback path
        _on_send_error(exc)


def publish_to_dlq(raw_value: bytes | str, source_topic: str, error: str) -> None:
    """Forward a message that could not be processed, with the reason it failed.

    Sent as a raw JSON object rather than an EventEnvelope: the whole reason a
    message lands here is that it did not satisfy the envelope contract, so
    validating the report against that same contract would drop the evidence.
    """
    producer = _get_producer()
    if producer is None:
        return
    if isinstance(raw_value, bytes):
        raw_value = raw_value.decode("utf-8", errors="replace")
    import json

    report = json.dumps(
        {
            "source_topic": source_topic,
            "error": error,
            "failed_at": datetime.now(timezone.utc).isoformat(),
            "reported_by": {"service": SERVICE_NAME, "instance_id": _instance_id},
            "raw_value": raw_value,
        }
    )
    try:
        future = producer.send(DLQ_TOPIC, key=source_topic, value=report)
        future.add_errback(_on_send_error)
    except Exception as exc:  # pragma: no cover - defensive
        _on_send_error(exc)


def flush(timeout_seconds: float = 5.0) -> None:
    """Block until queued sends complete. Called on shutdown, not per message."""
    if _producer is not None:
        try:
            _producer.flush(timeout=timeout_seconds)
        except Exception:
            logger.warning("producer flush did not complete cleanly", exc_info=True)


def _reset_for_tests() -> None:
    """Clear module state between tests. Not used at runtime."""
    global _producer, _producer_init_failed, _producer_init_thread, publish_failures
    _producer = None
    _producer_init_failed = False
    _producer_init_thread = None
    publish_failures = 0
