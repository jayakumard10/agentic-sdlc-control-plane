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

**A full queue is backpressure, not loss.** The queue is bounded and the worker is
serial, so it can fill. When it does, the message is left uncommitted, its partition is
rewound to it and paused, and it is redelivered once the worker drains. See `poll_loop`.

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

from agentic_control_plane import events, heartbeat, publish, runner, workspace
from agentic_control_plane.inbox import payload_of
from agentic_control_plane.state import GraphState
from agentic_control_plane.telemetry import TelemetrySink, render_console_line

logger = logging.getLogger(__name__)

TRIGGER_TOPIC_PATTERN = r".*\.drift-detected\.v[0-9]+"
TRIGGER_GROUP = "control-plane-triggers"
DECISION_GROUP = "control-plane-decisions"
METADATA_MAX_AGE_MS = 30_000

# Bounded: see the module docstring's note on the loss window. A larger queue would
# widen it for no benefit, since the worker is serial anyway.
WORK_QUEUE_MAXSIZE = 32

# Partitions are resumed once the queue has drained to here, not the moment a single
# slot frees. Resuming at the first free slot would fetch a batch, fill up, and pause
# again on nearly every cycle; a low-water mark makes the pause/resume cycle
# proportional to how much the worker has actually got through.
WORK_QUEUE_RESUME_THRESHOLD = WORK_QUEUE_MAXSIZE // 2

# A poll failure is retried rather than fatal, but not indefinitely: a consumer that
# cannot poll at all should fail loudly instead of looping quietly forever.
MAX_CONSECUTIVE_POLL_FAILURES = 10
POLL_RETRY_BACKOFF_SECONDS = 2.0


class InvalidMessage(ValueError):
    """A message that cannot be processed no matter how many times it is retried."""


@dataclass
class TriggerWork:
    run_id: str
    scenario_type: str
    repo_url: str
    branch: str
    requirement: str
    # Row id in the durable inbox, set when the work is recorded and used to forget it
    # once finished. None when no inbox is configured, as in tests.
    inbox_id: int | None = None


@dataclass
class DecisionWork:
    run_id: str
    decision: dict
    inbox_id: int | None = None


_WORK_KINDS = {"trigger": TriggerWork, "decision": DecisionWork}


def _kind_of(work) -> str:
    return "trigger" if isinstance(work, TriggerWork) else "decision"


@dataclass
class _RunTarget:
    """What an outcome event needs, kept for the life of a run.

    commit_sha_before is captured once, at clone time, and a run reaches its
    terminal state on a later slice - after a gate, in a different call, possibly in
    a different process. Holding it here is what stops it being lost in between.
    """

    repo_url: str
    branch: str
    scenario_type: str
    commit_sha_before: str | None = None


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

    # The event contract and GraphState both call a field `decided_by` and mean
    # different things by it. On the wire it is an identity - the contract's own
    # worked example carries a username. In GraphState it is provenance, constrained
    # to whether a real human or a replayed fixture resolved the gate. Passing the
    # wire value straight through fails GateRecord validation for every username
    # that is not the literal string "human", which is all of them.
    identity = payload.get("decided_by", "human")
    provenance = "replayed" if identity == "replayed" else "human"

    decision = {
        "status": normalized,
        "decided_by": provenance,
        # Kept, not discarded: who approved a gate is exactly what an audit trail
        # exists to record. It reaches GateRecord.decision_payload with the rest of
        # the decision.
        "decided_by_identity": identity,
        "override_guardrails": bool(payload.get("override_guardrails", False)),
        "comment": payload.get("comment", ""),
    }
    # Only when actually supplied. A present-but-None key would override the
    # clarified requirement with None, which GraphState rejects - the gate nodes
    # read this with .get(key, current_value), so absence and None are not the same.
    if payload.get("clarified_requirement") is not None:
        decision["clarified_requirement"] = payload["clarified_requirement"]

    return DecisionWork(run_id=envelope.correlation_id, decision=decision)


class Worker:
    """Executes queued work. The only thing that touches the checkpointer.

    Kept separate from the poll loops so that neither the length of a run nor the
    length of a human's attention span is ever observable from a poll loop.
    """

    def __init__(self, checkpointer, audit_sink: TelemetrySink | None = None, inbox=None) -> None:
        self.checkpointer = checkpointer
        self.audit_sink = audit_sink
        # Optional so tests can run without Postgres. The running control plane always
        # has one - without it, a crash with items queued loses them (ADR 0010).
        self.inbox = inbox
        self.queue: queue.Queue = queue.Queue(maxsize=WORK_QUEUE_MAXSIZE)
        # Enough to publish an outcome for a run whose workspace is already gone:
        # repo, branch, scenario_type, and the commit the clone started from.
        self._targets: dict[str, _RunTarget] = {}

    def submit(self, work) -> bool:
        """Record the work durably, then enqueue it. False if it cannot be accepted.

        A False here is backpressure, not loss: the caller must leave the offset
        uncommitted and rewind, so the message is redelivered once the worker has
        drained. See `poll_loop`.

        Recording comes first so there is no moment where the work is queued but not
        durable. A failure to record is also a refusal - accepting work this process
        could not write down, and then committing the offset for it, is the exact loss
        this exists to prevent.
        """
        if self.inbox is not None:
            try:
                work.inbox_id = self.inbox.record(
                    _kind_of(work), work.run_id, payload_of(work)
                )
            except Exception:
                logger.exception(
                    "could not record %s durably; refusing it so Kafka redelivers",
                    work.run_id,
                )
                return False
        try:
            self.queue.put_nowait(work)
            return True
        except queue.Full:
            # Recorded but not queued, so forget it again - it is about to be
            # redelivered, and leaving the row would replay it a second time at startup.
            self._forget(work)
            logger.warning(
                "work queue is full (%d items); applying backpressure rather than "
                "accepting %r",
                self.queue.qsize(),
                work.run_id,
            )
            return False

    def _forget(self, work) -> None:
        if self.inbox is not None and getattr(work, "inbox_id", None) is not None:
            self.inbox.discard(work.inbox_id)
            work.inbox_id = None

    def restore_pending(self) -> int:
        """Put back work the previous process accepted but did not finish.

        Called once at startup, before the poll loops begin, so recovered work is
        handled ahead of anything newly consumed - the order it was accepted in.
        """
        if self.inbox is None:
            return 0
        restored = 0
        for row_id, kind, payload in self.inbox.pending():
            work_class = _WORK_KINDS.get(kind)
            if work_class is None:  # pragma: no cover - defensive
                logger.error("unknown work kind %r in inbox row %s; dropping", kind, row_id)
                self.inbox.discard(row_id)
                continue
            try:
                work = work_class(**payload, inbox_id=row_id)
            except TypeError:
                logger.exception(
                    "inbox row %s does not match %s; dropping", row_id, work_class.__name__
                )
                self.inbox.discard(row_id)
                continue
            try:
                self.queue.put_nowait(work)
            except queue.Full:
                # More unfinished work than the queue holds. The rest stays in the
                # inbox and is restored on a later start rather than being dropped.
                logger.warning(
                    "queue full while restoring; %d item(s) remain in the inbox",
                    self.inbox.depth() - restored,
                )
                break
            restored += 1
        if restored:
            logger.warning("Restored %d unfinished work item(s) from the inbox", restored)
        else:
            logger.info("No unfinished work to restore")
        return restored

    def has_capacity(self) -> bool:
        """True once the queue has drained enough to resume fetching."""
        return self.queue.qsize() <= WORK_QUEUE_RESUME_THRESHOLD

    def handle_trigger(self, work: TriggerWork) -> None:
        if runner.already_known(work.run_id, self.checkpointer):
            logger.info(
                "Run %s already exists; treating redelivered trigger as a no-op",
                work.run_id,
            )
            return

        target = _RunTarget(work.repo_url, work.branch, work.scenario_type)
        self._targets[work.run_id] = target

        try:
            _path, target.commit_sha_before = workspace.clone_for_run(
                work.run_id, work.repo_url, work.branch
            )
        except workspace.CloneError as exc:
            logger.error("Clone failed for run %s: %s", work.run_id, exc)
            self._publish_outcome(work.run_id, "clone_failed", detail=str(exc))
            return

        initial = GraphState(
            scenario_type=work.scenario_type,
            requirement_raw=work.requirement,
            mode="live" if _is_live_mode() else "replay",
        )
        try:
            result = runner.start_run(work.run_id, initial, self.checkpointer)
        except Exception as exc:
            self._fail_run(work.run_id, exc)
            return
        self._after_slice(work.run_id, result)

    def handle_decision(self, work: DecisionWork) -> None:
        # Deliberately narrow. An earlier version caught KeyError/ValueError, which
        # also swallowed the same builtins raised from inside graph execution and
        # logged a real failure as "already terminal" - the run then sat parked
        # forever with no error anywhere. Only the two precondition failures are
        # benign; anything else has to propagate to the worker loop and be logged
        # with its traceback.
        try:
            result = runner.resume_run(work.run_id, work.decision, self.checkpointer)
        except runner.UnknownRunError:
            logger.warning(
                "Decision for unknown run %s; ignoring (it may belong to another "
                "deployment, or its checkpoint may have been pruned)",
                work.run_id,
            )
            return
        except runner.RunAlreadyTerminalError:
            logger.warning(
                "Decision for run %s which already reached a terminal state; ignoring",
                work.run_id,
            )
            return
        except Exception as exc:
            self._fail_run(work.run_id, exc)
            return
        self._after_slice(work.run_id, result)

    def _fail_run(self, run_id: str, exc: Exception) -> None:
        """Report a run that died inside graph execution, and clean up after it.

        Found by a real run and by nothing else: when a node raised, the exception
        propagated to the worker loop, which logged it and moved on. The run was
        left with no pending work but a run_status of "running" - neither resumable
        nor terminal - so no outcome was ever published and the next startup
        reconciled its workspace away. The run simply vanished, which is the one
        outcome a governed system must not produce.
        """
        logger.exception("Run %s failed during graph execution", run_id)
        self._publish_outcome(run_id, "failed", detail=f"{type(exc).__name__}: {exc}")
        workspace.cleanup(run_id)

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
                run_id, "stale", detail="parked past the TTL with no decision"
            )
            workspace.cleanup(run_id)
        return stale

    def _after_slice(self, run_id: str, result: runner.RunResult) -> None:
        self._record_audit(result)
        if result.parked:
            logger.info(
                "Run %s parked at %s awaiting a decision on %s",
                run_id,
                result.gate_type,
                events.GATE_DECISION_TOPIC,
            )
            return
        # Delivery first: cleanup below removes the workspace holding the commit.
        delivery = self._deliver_if_completed(run_id, result)
        self._publish_outcome(
            run_id,
            result.terminal_state or "failed",
            detail=result.detail,
            extra_payload=delivery,
        )
        workspace.cleanup(run_id)

    def _record_audit(self, result: runner.RunResult) -> None:
        """Record this slice's audit events to both durable sinks.

        The same AuditEvent list the graph already threads through state, projected
        to a hash-chained file - including the gate payload of a parked run, which is
        how a reviewer sees what they are being asked to approve - and published to
        the audit topic.

        Two sinks because they fail differently. The file is local and complete but
        writable by whoever holds the volume; the topic is out of reach of that
        writer but only as durable as the broker's retention. Neither is sufficient
        evidence alone, and the pair disagreeing is itself worth knowing. See
        docs/adr/0008.
        """
        if self.audit_sink is None:
            return
        state_events = result.values.get("events") if result.values else None
        if not state_events:
            return
        try:
            new_events = self.audit_sink.flush_new_events(result.run_id, state_events)
        except Exception:
            logger.exception("failed to write audit events; continuing")
            return
        for event in new_events:
            logger.info("audit %s", render_console_line(event))
        self._publish_audit(result.run_id, new_events)

    def _publish_audit(self, run_id: str, new_events: list) -> None:
        """Publish audit records to the topic, best-effort.

        A run whose broker is unreachable still completes and still has its local
        audit file; what is lost is the independent copy, which is a degraded
        guarantee rather than a failed run.
        """
        target = self._targets.get(run_id)
        if target is None:
            # A slice with no target on record - a decision for a run this process did
            # not start. The file sink still has the events; only the topic copy needs
            # the git context the envelope requires.
            return
        for event in new_events:
            try:
                events.publish_audit_event(
                    events.build_audit_event(
                        run_id=run_id,
                        scenario_type=target.scenario_type,
                        repo_url=target.repo_url,
                        branch=target.branch,
                        commit_sha=target.commit_sha_before,
                        event=json.loads(event.model_dump_json()),
                    )
                )
            except Exception:
                logger.exception("failed to publish an audit event; continuing")

    def _deliver_if_completed(self, run_id: str, result: runner.RunResult) -> dict:
        """Publish an approved change, and report what happened to it.

        Runs between the terminal state and workspace cleanup, which is the only
        window where the commit still exists on disk. Only a `completed` run has
        anything to deliver: every other terminal state either produced no commit or
        produced one the gate rejected.

        Returns the fields the outcome event should carry. Never raises - a delivery
        failure is reported on the event, not turned into a failed run, because the
        change was generated, tested and approved regardless of whether the push
        landed. See docs/adr/0012.
        """
        payload: dict = {}
        commit_sha_after = (result.values or {}).get("commit_sha_after")
        if commit_sha_after:
            payload["commit_sha_after"] = commit_sha_after
        if result.terminal_state != "completed":
            return payload

        target = self._targets.get(run_id)
        if target is None:
            logger.warning("Run %s completed with no target on record; not publishing", run_id)
            return payload

        outcome = publish.publish_change(
            workspace=workspace.workspace_for(run_id),
            run_id=run_id,
            repo_url=target.repo_url,
            base_branch=target.branch,
            requirement=(result.values or {}).get("requirement_clarified", ""),
        )
        payload.update(outcome.as_payload())
        return payload

    def _publish_outcome(
        self, run_id: str, terminal_state: str, detail: str, extra_payload: dict | None = None
    ) -> None:
        """Read the target from the run's record rather than taking it as arguments.

        The commit SHA used to be threaded through the call chain, which lost it for
        every run that passed through a gate: it is captured at clone time, but the
        terminal state is reached on a later slice whose caller had no access to it,
        and so passed None. Every completed run reported a null commit.
        """
        target = self._targets.get(run_id) or _RunTarget("unknown", "unknown", "brownfield")
        envelope = events.build_run_outcome(
            run_id=run_id,
            terminal_state=terminal_state,
            scenario_type=target.scenario_type,
            repo_url=target.repo_url,
            branch=target.branch,
            commit_sha=target.commit_sha_before,
            detail=detail,
            extra_payload=extra_payload,
        )
        events.publish_run_outcome(envelope)
        self._targets.pop(run_id, None)
        # Retire the run's audit cursor alongside its target. Both are per-run state
        # on a process-lifetime object, and a cursor left behind here is what made
        # every run after the first go unaudited - see docs/adr/0011.
        if self.audit_sink is not None:
            self.audit_sink.forget(run_id)

    def run_forever(self, stop_event: threading.Event) -> None:
        while not stop_event.is_set():
            # Every pass, idle included: an idle worker and a dead one are the same
            # shape from outside, and only this tells them apart.
            heartbeat.touch()
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
                # Forget it either way. A failed run has already been reported as
                # terminal (ADR 0003), so replaying it at the next startup would
                # publish a second outcome for a run that has one.
                self._forget(work)
                self.queue.task_done()


def _is_live_mode() -> bool:
    import os

    return os.environ.get("ORCHESTRATOR_MODE", "replay").lower() == "live"


def _resume_drained_partitions(consumer, worker: Worker) -> None:
    """Resume partitions paused by backpressure, once the worker has caught up."""
    paused = consumer.paused()
    if not paused or not worker.has_capacity():
        return
    consumer.resume(*paused)
    logger.info(
        "work queue drained to %d item(s); resumed %d paused partition(s)",
        worker.queue.qsize(),
        len(paused),
    )


def poll_loop(consumer, parse, worker: Worker, stop_event: threading.Event) -> None:
    """Read, validate, hand off, commit, repeat. Never executes anything.

    A message that fails validation goes straight to the DLQ rather than through a
    bounded retry. Retrying is worth doing when a failure might not recur; a payload
    that does not satisfy the envelope contract fails identically every time, so
    retrying it three times only delays the offset commit and holds up the partition
    behind a message that will never succeed.

    **A full work queue is backpressure, not loss.** The worker is serial and the queue
    is bounded, so it can fill. When it does, the message that could not be accepted is
    left uncommitted *and* the partition is rewound to it, because `poll()` has already
    advanced the in-memory position past it - skipping the commit alone would drop the
    message for the life of this process and only redeliver it after a restart. The
    partition is then paused so the loop does not spin re-fetching work it cannot take,
    and resumed once the worker has drained. Processing of that partition stops at the
    first rejected message: accepting anything behind it would put the redelivered
    message out of order relative to work already queued.
    """
    consecutive_failures = 0
    while not stop_event.is_set():
        _resume_drained_partitions(consumer, worker)
        try:
            batches = consumer.poll(timeout_ms=1000)
        except Exception:
            # A client-side error must not take the service down. Observed for real:
            # a transient selector error inside the Kafka client propagated out of
            # poll(), out of this loop, and terminated the process - a whole control
            # plane stopped by one bad file descriptor. Retried with a bounded
            # tolerance so a genuinely broken consumer still surfaces rather than
            # spinning silently forever.
            consecutive_failures += 1
            logger.exception(
                "consumer poll failed (%d/%d consecutive)",
                consecutive_failures,
                MAX_CONSECUTIVE_POLL_FAILURES,
            )
            if consecutive_failures >= MAX_CONSECUTIVE_POLL_FAILURES:
                logger.error("consumer poll failed %d times in a row; giving up", consecutive_failures)
                raise
            stop_event.wait(POLL_RETRY_BACKOFF_SECONDS)
            continue
        consecutive_failures = 0

        accepted_everything = True
        for partition, messages in batches.items():
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
                    if not worker.submit(work):
                        # Rewind to the message we could not take, then stop fetching
                        # this partition until the worker drains. Both are required:
                        # the rewind because poll() has already moved the position past
                        # it, the pause so this loop does not spin on work it cannot
                        # accept.
                        consumer.seek(partition, message.offset)
                        consumer.pause(partition)
                        logger.warning(
                            "backpressure on %s: rewound to offset %s and paused",
                            message.topic,
                            message.offset,
                        )
                        accepted_everything = False
                        break
        if batches and accepted_everything:
            # Committed past the batch once it is queued, including any message
            # forwarded to the DLQ - a poison message must never block its partition.
            #
            # Skipped entirely when anything was rejected, rather than committing the
            # partitions that were fine. Those messages stay uncommitted and are
            # redelivered after a crash, which is harmless: a redelivered trigger
            # carries a run_id the checkpointer already knows and is a no-op. The
            # alternative - per-partition offsets - buys precision this platform has no
            # use for at one partition per topic, in exchange for coupling this module
            # to the client's offset types.
            consumer.commit()


def utcnow() -> datetime:
    return datetime.now(timezone.utc)
