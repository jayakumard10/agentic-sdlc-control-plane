"""Kafka consumption: two independent read paths, and a worker neither one waits on.

**Trigger path** - pattern-subscribes to every `*.drift-detected.v{n}` topic, so a
new producing service starts triggering runs without a code change here. Discovery
of a newly auto-created topic is bounded by `metadata.max.age.ms`: the 300000ms
default would mean up to five minutes between a drift being detected and a run
starting, which is not a useful reaction time, so it is tuned to 30000ms. The cost
is broker-side metadata requests scaling with (consumers / interval) - negligible at
a handful of consumers, which is what this platform has.

**Decision path** - a separate consumer on the decisions topic, in its own consumer
group, correlating each decision to a parked run by `thread_id == run_id`. It is a
distinct read path rather than a branch inside the trigger loop precisely so that
resuming a run is never something the trigger loop does.

**The worker** - both poll loops do the same two things and then return: validate the
message, and hand the work to a queue. Neither one executes a graph, clones a repo,
or waits for a human. Actual execution happens on a single worker thread.

That worker is single-threaded on purpose. It owns the checkpointer exclusively, so
there is no question about whether a checkpointer connection is safe to share across
threads - a question this platform has already been bitten by once, with a different
library, by assuming the answer rather than checking it. Runs therefore execute
serially, which is the right trade at this scale and is a bounded change to make
later (a pool of workers, each with its own checkpointer) if it stops being.

**The gap this leaves, stated plainly**: offsets are committed once work is enqueued,
not once it completes. A crash with items still queued loses those triggers. The
alternative - committing only after completion - puts a clone and a full graph
execution inside the poll loop, which is the thing the design exists to prevent. The
window is bounded by the queue size, and drift is a recurring condition: if it
persists, the next detection publishes the same deterministic correlation_id and the
lost run starts then. A durable hand-off table is the upgrade path if that stops
being acceptable.
"""

from __future__ import annotations

import json
import logging
import queue
import threading
from dataclasses import dataclass
from datetime import datetime, timezone

from agentic_events import EventEnvelope

from agentic_control_plane import events, runner, workspace
from agentic_control_plane.state import GraphState
from agentic_control_plane.telemetry import TelemetrySink

logger = logging.getLogger(__name__)

TRIGGER_TOPIC_PATTERN = r".*\.drift-detected\.v[0-9]+"
TRIGGER_GROUP = "control-plane-triggers"
DECISION_GROUP = "control-plane-decisions"
METADATA_MAX_AGE_MS = 30_000

# Bounded: see the module docstring's note on the loss window. A larger queue would
# widen it for no benefit, since the worker is serial anyway.
WORK_QUEUE_MAXSIZE = 32


class InvalidMessage(ValueError):
    """A message that cannot be processed no matter how many times it is retried."""


@dataclass
class TriggerWork:
    run_id: str
    scenario_type: str
    repo_url: str
    branch: str
    requirement: str


@dataclass
class DecisionWork:
    run_id: str
    decision: dict


def build_trigger_consumer(bootstrap_servers: str):
    from kafka import KafkaConsumer

    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        group_id=TRIGGER_GROUP,
        metadata_max_age_ms=METADATA_MAX_AGE_MS,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    consumer.subscribe(pattern=TRIGGER_TOPIC_PATTERN)
    logger.info("Subscribed to pattern %s as group %s", TRIGGER_TOPIC_PATTERN, TRIGGER_GROUP)
    return consumer


def build_decision_consumer(bootstrap_servers: str):
    from kafka import KafkaConsumer

    consumer = KafkaConsumer(
        events.GATE_DECISION_TOPIC,
        bootstrap_servers=bootstrap_servers,
        group_id=DECISION_GROUP,
        metadata_max_age_ms=METADATA_MAX_AGE_MS,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    logger.info("Subscribed to %s as group %s", events.GATE_DECISION_TOPIC, DECISION_GROUP)
    return consumer


def _describe_drift(envelope: EventEnvelope) -> str:
    """Turn drift metrics into the requirement text a run starts from.

    Built from whatever metric the event names rather than from a fixed vocabulary,
    so a new metric from a new producer needs no change here.
    """
    metrics = envelope.metrics or {}
    metric_name = metrics.get("metric_name", "an operational metric")
    reference = metrics.get("reference_value")
    current = metrics.get("current_value")
    delta = metrics.get("relative_delta_pct")
    threshold = metrics.get("threshold_pct")

    # Each part is included on its own merit. Requiring reference and current
    # together dropped the current value outright for any producer that reports one
    # without the other, which is exactly the not-yet-invented metric shape this is
    # supposed to accommodate.
    parts: list[str] = []
    if reference is not None:
        parts.append(f"reference {reference}")
    if current is not None:
        parts.append(f"current {current}")
    if delta is not None:
        parts.append(f"relative change {delta}%")
    if threshold is not None:
        parts.append(f"threshold {threshold}%")

    text = f"Operational drift detected in {metric_name}"
    if parts:
        text += ": " + ", ".join(parts)
    return text + ". Investigate the regression and remediate it."


def parse_trigger(raw_value: bytes | str) -> TriggerWork:
    """Validate a drift event and derive the work it implies.

    Raises InvalidMessage for anything that will fail identically on every retry.
    """
    try:
        decoded = raw_value.decode("utf-8") if isinstance(raw_value, bytes) else raw_value
        envelope = EventEnvelope.model_validate_json(decoded)
    except Exception as exc:
        raise InvalidMessage(f"{type(exc).__name__}: {exc}") from exc

    if envelope.event_type != "drift-detected":
        raise InvalidMessage(
            f"unexpected event_type {envelope.event_type!r} on a drift-detected topic"
        )

    return TriggerWork(
        # thread_id == run_id == the producer's correlation_id, which it derives
        # deterministically from the drift condition. A redelivery of the same
        # condition therefore arrives with a run_id already in the checkpointer.
        run_id=envelope.correlation_id,
        scenario_type=envelope.scenario_type,
        repo_url=envelope.git_target.repo_url,
        branch=envelope.git_target.branch,
        requirement=_describe_drift(envelope),
    )


def parse_decision(raw_value: bytes | str) -> DecisionWork:
    """Validate a gate decision and extract the run it resumes."""
    try:
        decoded = raw_value.decode("utf-8") if isinstance(raw_value, bytes) else raw_value
        envelope = EventEnvelope.model_validate_json(decoded)
    except Exception as exc:
        raise InvalidMessage(f"{type(exc).__name__}: {exc}") from exc

    if envelope.event_type != "gate-decision":
        raise InvalidMessage(
            f"unexpected event_type {envelope.event_type!r} on the decisions topic"
        )

    payload = envelope.payload or {}
    status = payload.get("decision") or payload.get("status")
    if status not in ("approve", "approved", "reject", "rejected", "edited"):
        raise InvalidMessage(f"decision payload has no usable status: {payload!r}")

    normalized = {
        "approve": "approved",
        "reject": "rejected",
    }.get(status, status)

    return DecisionWork(
        run_id=envelope.correlation_id,
        decision={
            "status": normalized,
            "decided_by": payload.get("decided_by", "human"),
            "override_guardrails": bool(payload.get("override_guardrails", False)),
            "comment": payload.get("comment", ""),
            "clarified_requirement": payload.get("clarified_requirement"),
        },
    )


class Worker:
    """Executes queued work. The only thing that touches the checkpointer.

    Kept separate from the poll loops so that neither the length of a run nor the
    length of a human's attention span is ever observable from a poll loop.
    """

    def __init__(self, checkpointer, audit_sink: TelemetrySink | None = None) -> None:
        self.checkpointer = checkpointer
        self.audit_sink = audit_sink
        self.queue: queue.Queue = queue.Queue(maxsize=WORK_QUEUE_MAXSIZE)
        # Enough to publish an outcome for a run whose workspace is already gone.
        self._targets: dict[str, tuple[str, str, str]] = {}

    def submit(self, work) -> bool:
        """Enqueue work without blocking. False if the queue is full."""
        try:
            self.queue.put_nowait(work)
            return True
        except queue.Full:
            logger.error("work queue is full, dropping %r", work)
            return False

    def handle_trigger(self, work: TriggerWork) -> None:
        if runner.already_known(work.run_id, self.checkpointer):
            logger.info(
                "Run %s already exists; treating redelivered trigger as a no-op",
                work.run_id,
            )
            return

        self._targets[work.run_id] = (work.repo_url, work.branch, work.scenario_type)

        try:
            _path, commit_sha_before = workspace.clone_for_run(
                work.run_id, work.repo_url, work.branch
            )
        except workspace.CloneError as exc:
            logger.error("Clone failed for run %s: %s", work.run_id, exc)
            self._publish_outcome(work.run_id, "clone_failed", detail=str(exc), commit_sha=None)
            return

        initial = GraphState(
            scenario_type=work.scenario_type,
            requirement_raw=work.requirement,
            mode="live" if _is_live_mode() else "replay",
        )
        result = runner.start_run(work.run_id, initial, self.checkpointer)
        self._after_slice(work.run_id, result, commit_sha_before)

    def handle_decision(self, work: DecisionWork) -> None:
        try:
            result = runner.resume_run(work.run_id, work.decision, self.checkpointer)
        except KeyError:
            logger.warning(
                "Decision for unknown run %s; ignoring (it may belong to another "
                "deployment, or its checkpoint may have been pruned)",
                work.run_id,
            )
            return
        except ValueError:
            logger.warning(
                "Decision for run %s which already reached a terminal state; ignoring",
                work.run_id,
            )
            return
        self._after_slice(work.run_id, result, None)

    def sweep_stale_runs(self, now: datetime | None = None) -> list[str]:
        """Expire runs parked past the TTL, so a decision that never comes is visible.

        Candidates come from the workspaces on disk: a parked run always has one, and
        it avoids enumerating every thread the checkpointer has ever held.
        """
        candidates = workspace.existing_run_ids()
        stale = runner.find_stale_runs(candidates, self.checkpointer, now=now)
        for run_id in stale:
            logger.warning("Run %s exceeded the parked-run TTL; marking stale", run_id)
            self._publish_outcome(
                run_id, "stale", detail="parked past the TTL with no decision", commit_sha=None
            )
            workspace.cleanup(run_id)
        return stale

    def _after_slice(self, run_id: str, result: runner.RunResult, commit_sha: str | None) -> None:
        self._record_audit(result)
        if result.parked:
            logger.info(
                "Run %s parked at %s awaiting a decision on %s",
                run_id,
                result.gate_type,
                events.GATE_DECISION_TOPIC,
            )
            return
        self._publish_outcome(
            run_id, result.terminal_state or "failed", detail=result.detail, commit_sha=commit_sha
        )
        workspace.cleanup(run_id)

    def _record_audit(self, result: runner.RunResult) -> None:
        """Append this slice's audit events to the JSONL sink.

        The same AuditEvent list the graph already threads through state, projected
        to a durable file - including the gate payload of a parked run, which is how
        a reviewer sees what they are being asked to approve.
        """
        if self.audit_sink is None:
            return
        state_events = result.values.get("events") if result.values else None
        if not state_events:
            return
        try:
            for line in self.audit_sink.flush_new_events(state_events):
                logger.info("audit %s", line)
        except Exception:
            logger.exception("failed to write audit events; continuing")

    def _publish_outcome(
        self, run_id: str, terminal_state: str, detail: str, commit_sha: str | None
    ) -> None:
        repo_url, branch, scenario_type = self._targets.get(
            run_id, ("unknown", "unknown", "brownfield")
        )
        envelope = events.build_run_outcome(
            run_id=run_id,
            terminal_state=terminal_state,
            scenario_type=scenario_type,
            repo_url=repo_url,
            branch=branch,
            commit_sha=commit_sha,
            detail=detail,
        )
        events.publish_run_outcome(envelope)
        self._targets.pop(run_id, None)

    def run_forever(self, stop_event: threading.Event) -> None:
        while not stop_event.is_set():
            try:
                work = self.queue.get(timeout=1.0)
            except queue.Empty:
                continue
            try:
                if isinstance(work, TriggerWork):
                    self.handle_trigger(work)
                elif isinstance(work, DecisionWork):
                    self.handle_decision(work)
                else:  # pragma: no cover - defensive
                    logger.error("unknown work item %r", work)
            except Exception:
                logger.exception("work item failed; continuing with the next")
            finally:
                self.queue.task_done()


def _is_live_mode() -> bool:
    import os

    return os.environ.get("ORCHESTRATOR_MODE", "replay").lower() == "live"


def poll_loop(consumer, parse, worker: Worker, stop_event: threading.Event) -> None:
    """Read, validate, hand off, commit, repeat. Never executes anything.

    A message that fails validation goes straight to the DLQ rather than through a
    bounded retry. Retrying is worth doing when a failure might not recur; a payload
    that does not satisfy the envelope contract fails identically every time, so
    retrying it three times only delays the offset commit and holds up the partition
    behind a message that will never succeed.
    """
    while not stop_event.is_set():
        batches = consumer.poll(timeout_ms=1000)
        for _partition, messages in batches.items():
            for message in messages:
                try:
                    work = parse(message.value)
                except InvalidMessage as exc:
                    logger.error(
                        "Poison message on %s at offset %s: %s",
                        message.topic,
                        message.offset,
                        exc,
                    )
                    events.publish_to_dlq(message.value, message.topic, str(exc))
                else:
                    worker.submit(work)
        if batches:
            # Committed past the batch once it is queued, including any message
            # forwarded to the DLQ - a poison message must never block its partition.
            consumer.commit()


def utcnow() -> datetime:
    return datetime.now(timezone.utc)
