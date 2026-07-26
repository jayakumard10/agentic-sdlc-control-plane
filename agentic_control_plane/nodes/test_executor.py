"""Test Executor node: runs pytest against Coder output and drives the bounded retry loop.

Owns the retry -> fallback state-transition bookkeeping (governance model, §6 of the
plan): on failure, increments retry_count while under the bounded limit; once the
limit is reached, flips fallback_triggered so the next Coder invocation uses a
reduced-scope prompt; if the fallback attempt also fails, counters are left as-is.

Also sets route_hint explicitly for graph.py's routing (Phase 3): "fallback_triggered
is True and the test failed" is ambiguous on its own between "about to try the
fallback attempt" and "the fallback attempt just failed too" - this node has the
full before/after context to disambiguate at the moment it happens, so the router
downstream never has to guess from raw counters.
"""

from __future__ import annotations

import time
from pathlib import Path

from agentic_control_plane import tools
from agentic_control_plane.state import AuditEvent, GraphState, TestResult

_LOG_TAIL_CHARS = 4000


def _extract_failure_summaries(pytest_stdout: str, timed_out: bool) -> list[str]:
    if timed_out:
        return ["pytest run timed out"]
    failures = [
        line.strip() for line in pytest_stdout.splitlines() if line.strip().startswith("FAILED ")
    ]
    return failures or ["pytest reported a non-zero exit with no FAILED summary lines"]


def test_executor(state: GraphState, workspace: Path) -> dict:
    start = time.monotonic()
    run = tools.run_pytest_sandboxed(workspace, timeout_seconds=60)
    failures = [] if run.passed else _extract_failure_summaries(run.stdout, run.timed_out)

    test_result = TestResult(
        passed=run.passed,
        failures=failures,
        attempt_number=state.coder.attempt_number,
        logs=(run.stdout + run.stderr)[-_LOG_TAIL_CHARS:],
    )

    updates: dict = {"test": test_result}
    events = [
        AuditEvent(
            node="test_executor",
            event_type="node_start",
            detail=f"running pytest for attempt {state.coder.attempt_number}",
        )
    ]

    if run.passed:
        summary = "tests passed"
        updates["route_hint"] = "proceed"
    elif state.fallback_triggered:
        summary = "tests failed (fallback attempt also failed)"
        updates["route_hint"] = "rollback"
    elif state.retry_count < state.retry_limit:
        updates["retry_count"] = state.retry_count + 1
        summary = f"tests failed, retrying (attempt {state.retry_count + 2} of {state.retry_limit + 1})"
        updates["route_hint"] = "retry"
        events.append(AuditEvent(node="test_executor", event_type="retry", detail=summary))
    else:
        updates["fallback_triggered"] = True
        summary = "retries exhausted, falling back to reduced-scope prompt"
        updates["route_hint"] = "fallback_attempt"
        events.append(AuditEvent(node="test_executor", event_type="fallback", detail=summary))

    events.append(
        AuditEvent(
            node="test_executor",
            event_type="node_end",
            detail=summary,
            latency_ms=(time.monotonic() - start) * 1000,
        )
    )
    updates["events"] = events
    return updates
