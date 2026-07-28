"""Entrypoint: `python -m agentic_control_plane.main`.

Startup order matters and is deliberate:

1. Open the checkpointer. Everything else depends on it, and failing here should
   fail loudly at boot rather than on the first message.
2. Reconcile orphaned workspaces, before any new run can create one. Running this
   after consumption starts would risk deleting a workspace a run had just made.
3. Start the worker, so the queue has a consumer before anything can fill it.
4. Start the decision consumer and the TTL sweep.
5. Enter the trigger poll loop on the main thread.

Shutdown reverses it: signal every loop to stop, let in-flight work drain, flush
queued outcome events, and only then let the checkpointer context close.
"""

from __future__ import annotations

import logging
import os
import signal
import threading
import time
from pathlib import Path

from agentic_control_plane import consumer, events, inbox, runner, workspace
from agentic_control_plane.checkpointer import _postgres_conn_string, build_postgres_checkpointer
from agentic_control_plane.logging_config import configure_logging
from agentic_control_plane.telemetry import TelemetrySink

logger = logging.getLogger(__name__)

SWEEP_INTERVAL_SECONDS = 300
SHUTDOWN_DRAIN_SECONDS = 30


def audit_log_path() -> Path:
    """Outside the workspaces root, deliberately - see docs/adr/0009.

    The default used to be `/workspaces/.audit/runs.jsonl`, which put the audit trail
    inside the directory startup reconciliation sweeps for orphaned run workspaces. It
    was deleted on every restart.
    """
    return Path(os.environ.get("AUDIT_LOG_PATH", "/var/audit/runs.jsonl"))


def _run_sweep_loop(worker: consumer.Worker, stop_event: threading.Event) -> None:
    """Expire runs parked past the TTL, on an interval.

    Wrapped so one failing pass logs and waits for the next rather than killing the
    thread - a thread that dies silently is the failure mode this platform has
    already hit once.
    """
    while not stop_event.is_set():
        try:
            worker.sweep_stale_runs()
        except Exception:
            logger.exception("stale-run sweep failed; retrying next interval")
        stop_event.wait(SWEEP_INTERVAL_SECONDS)


def _reconcile_workspaces(checkpointer) -> None:
    removed = workspace.reconcile(lambda run_id: runner.is_resumable(run_id, checkpointer))
    if removed:
        logger.warning("Reconciled %d orphaned workspace(s): %s", len(removed), ", ".join(removed))
    else:
        logger.info("No orphaned workspaces to reconcile")


def main() -> None:
    configure_logging()

    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
    if not bootstrap_servers:
        raise SystemExit("KAFKA_BOOTSTRAP_SERVERS must be set")

    stop_event = threading.Event()

    def _handle_signal(signum, _frame):
        logger.info("Received signal %s, shutting down", signum)
        stop_event.set()

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    with build_postgres_checkpointer() as checkpointer:
        _reconcile_workspaces(checkpointer)

        work_inbox = inbox.Inbox(_postgres_conn_string())
        work_inbox.setup()

        worker = consumer.Worker(
            checkpointer, audit_sink=TelemetrySink(audit_log_path()), inbox=work_inbox
        )
        # Before the poll loops start, so work the previous process accepted and did
        # not finish is handled ahead of anything newly consumed.
        worker.restore_pending()

        threads = [
            threading.Thread(
                target=worker.run_forever, args=(stop_event,), name="worker", daemon=True
            ),
            threading.Thread(
                target=_run_sweep_loop, args=(worker, stop_event), name="ttl-sweep", daemon=True
            ),
        ]

        decision_consumer = consumer.build_decision_consumer(bootstrap_servers)
        threads.append(
            threading.Thread(
                target=consumer.poll_loop,
                args=(decision_consumer, consumer.parse_decision, worker, stop_event),
                name="decisions",
                daemon=True,
            )
        )

        for thread in threads:
            thread.start()

        trigger_consumer = consumer.build_trigger_consumer(bootstrap_servers)
        logger.info("Control plane ready; polling for triggers")
        try:
            consumer.poll_loop(
                trigger_consumer, consumer.parse_trigger, worker, stop_event
            )
        finally:
            stop_event.set()
            _drain(worker)
            for thread in threads:
                thread.join(timeout=5)
            for kafka_consumer in (trigger_consumer, decision_consumer):
                try:
                    kafka_consumer.close()
                except Exception:
                    logger.warning("consumer did not close cleanly", exc_info=True)
            events.flush()
            logger.info("Shutdown complete")


def _drain(worker: consumer.Worker) -> None:
    """Give queued work a bounded chance to finish before the checkpointer closes.

    Work still queued at shutdown is the loss window documented in consumer.py.
    Draining shrinks it; it does not close it, and pretending otherwise would be
    worse than saying so.
    """
    deadline = time.monotonic() + SHUTDOWN_DRAIN_SECONDS
    while not worker.queue.empty() and time.monotonic() < deadline:
        time.sleep(0.5)
    remaining = worker.queue.qsize()
    if remaining:
        logger.error(
            "Shutting down with %d work item(s) still queued; these triggers are lost "
            "and will be re-delivered only if the drift condition recurs",
            remaining,
        )


if __name__ == "__main__":
    main()
