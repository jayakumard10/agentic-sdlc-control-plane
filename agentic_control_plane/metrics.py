"""Reliability metrics: success rate, retry/rollback frequency, MTTR, e2e latency.

Computed in two stages: `summarize_run` reduces one completed `GraphState`'s audit
trail into a `RunSummary`; `compute_metrics` aggregates a list of `RunSummary`
objects (one or more runs, e.g. the three captured scenarios) into `RunMetrics`.
Kept independent of persistence — callers load whatever `GraphState`s they have
(from the Postgres checkpointer or from a saved fixture) and pass them in.
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel

from agentic_control_plane.state import GraphState, RunMetrics, RunStatus


class RunSummary(BaseModel):
    """Minimal per-run facts needed for aggregate reliability metrics."""

    run_id: str
    run_status: RunStatus
    retry_count: int
    rollback_count: int
    started_at: datetime
    finished_at: datetime | None
    first_failure_at: datetime | None = None
    recovered_at: datetime | None = None


def summarize_run(state: GraphState) -> RunSummary:
    """Reduce a run's audit trail to the facts `compute_metrics` needs.

    MTTR is derived from the first `test_executor` failure event and the next
    `test_executor` success event after it, matching the brownfield scenario's
    deliberate failure -> retry -> recovery arc.
    """
    first_failure_at: datetime | None = None
    recovered_at: datetime | None = None
    for event in state.events:
        if event.node != "test_executor" or event.event_type != "node_end":
            continue
        detail = event.detail.lower()
        if "fail" in detail and first_failure_at is None:
            first_failure_at = event.timestamp
        elif "pass" in detail and first_failure_at is not None and recovered_at is None:
            recovered_at = event.timestamp

    return RunSummary(
        run_id=state.run_id,
        run_status=state.run_status,
        retry_count=state.retry_count,
        rollback_count=state.rollback_count,
        started_at=state.started_at,
        finished_at=state.finished_at,
        first_failure_at=first_failure_at,
        recovered_at=recovered_at,
    )


def compute_metrics(runs: list[RunSummary]) -> RunMetrics:
    if not runs:
        return RunMetrics()

    total = len(runs)
    passed = sum(1 for run in runs if run.run_status == "completed")
    success_rate = passed / total

    # Every run makes at least one Coder invocation, plus one per retry.
    coder_invocations = sum(run.retry_count + 1 for run in runs)
    total_retries = sum(run.retry_count for run in runs)
    retry_frequency = total_retries / coder_invocations if coder_invocations else 0.0

    total_rollbacks = sum(run.rollback_count for run in runs)
    rollback_frequency = total_rollbacks / total

    mttrs = [
        (run.recovered_at - run.first_failure_at).total_seconds()
        for run in runs
        if run.first_failure_at is not None and run.recovered_at is not None
    ]
    mttr_seconds = sum(mttrs) / len(mttrs) if mttrs else None

    latencies = [
        (run.finished_at - run.started_at).total_seconds()
        for run in runs
        if run.finished_at is not None
    ]
    e2e_latency_seconds = sum(latencies) / len(latencies) if latencies else None

    return RunMetrics(
        success_rate=success_rate,
        retry_frequency=retry_frequency,
        rollback_frequency=rollback_frequency,
        mttr_seconds=mttr_seconds,
        e2e_latency_seconds=e2e_latency_seconds,
    )
