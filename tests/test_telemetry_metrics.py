"""Tests for telemetry.py (dual JSONL/console logging) and metrics.py (reliability

metrics computed from the audit trail).
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

from agentic_control_plane.metrics import compute_metrics, summarize_run
from agentic_control_plane.state import AuditEvent, GraphState
from agentic_control_plane.telemetry import TelemetrySink, render_console_line, render_console_trace


def test_telemetry_sink_flushes_only_new_events(tmp_path: Path):
    sink = TelemetrySink(tmp_path / "events.jsonl")

    events1 = [AuditEvent(node="a", event_type="node_start", detail="start")]
    lines1 = sink.flush_new_events(events1)
    assert len(lines1) == 1
    assert (tmp_path / "events.jsonl").read_text().count("\n") == 1

    events2 = events1 + [AuditEvent(node="a", event_type="node_end", detail="end")]
    lines2 = sink.flush_new_events(events2)
    assert len(lines2) == 1
    assert (tmp_path / "events.jsonl").read_text().strip().count("\n") + 1 == 2

    # no new events since the last flush -> nothing written, nothing rendered
    assert sink.flush_new_events(events2) == []


def test_render_console_line_includes_decision_and_latency():
    event = AuditEvent(
        node="release_gate",
        event_type="gate_decision",
        detail="merge_release_approval",
        decision="approved",
        latency_ms=42.5,
    )
    line = render_console_line(event)
    assert "release_gate" in line
    assert "decision=approved" in line
    assert "42ms" in line


def test_render_console_trace_joins_multiple_events():
    events = [
        AuditEvent(node="a", event_type="node_start", detail="x"),
        AuditEvent(node="b", event_type="node_end", detail="y"),
    ]
    trace = render_console_trace(events)
    assert trace.count("\n") == 1
    assert "a" in trace and "b" in trace


def _make_run(
    run_status: str,
    retry_count: int,
    rollback_count: int,
    started_at: datetime,
    finished_at: datetime | None,
    events: list[AuditEvent] | None = None,
) -> GraphState:
    return GraphState(
        scenario_type="brownfield",
        requirement_raw="x",
        run_status=run_status,
        retry_count=retry_count,
        rollback_count=rollback_count,
        started_at=started_at,
        finished_at=finished_at,
        events=events or [],
    )


def test_summarize_run_extracts_first_failure_and_recovery():
    t0 = datetime.now(timezone.utc)
    events = [
        AuditEvent(
            node="test_executor",
            event_type="node_end",
            detail="tests failed: 1 failure",
            timestamp=t0 + timedelta(seconds=5),
        ),
        AuditEvent(
            node="test_executor",
            event_type="node_end",
            detail="tests passed",
            timestamp=t0 + timedelta(seconds=35),
        ),
    ]
    state = _make_run("completed", 1, 0, t0, t0 + timedelta(seconds=60), events)
    summary = summarize_run(state)
    assert summary.first_failure_at is not None
    assert summary.recovered_at is not None
    assert (summary.recovered_at - summary.first_failure_at).total_seconds() == 30.0


def test_summarize_run_no_failures_leaves_mttr_fields_none():
    t0 = datetime.now(timezone.utc)
    state = _make_run("completed", 0, 0, t0, t0 + timedelta(seconds=10))
    summary = summarize_run(state)
    assert summary.first_failure_at is None
    assert summary.recovered_at is None


def test_compute_metrics_matches_hand_calculated_values():
    t0 = datetime.now(timezone.utc)
    fail_event = AuditEvent(
        node="test_executor",
        event_type="node_end",
        detail="tests failed: 1 failure",
        timestamp=t0 + timedelta(seconds=5),
    )
    pass_event = AuditEvent(
        node="test_executor",
        event_type="node_end",
        detail="tests passed",
        timestamp=t0 + timedelta(seconds=35),
    )
    runs = [
        _make_run("completed", 1, 0, t0, t0 + timedelta(seconds=60), [fail_event, pass_event]),
        _make_run("completed", 0, 0, t0, t0 + timedelta(seconds=40)),
        _make_run("failed", 3, 1, t0, t0 + timedelta(seconds=90)),
    ]
    summaries = [summarize_run(state) for state in runs]
    metrics = compute_metrics(summaries)

    assert metrics.success_rate == 2 / 3
    # 4 total retries across (1+1) + (0+1) + (3+1) = 7 coder invocations
    assert metrics.retry_frequency == 4 / 7
    assert metrics.rollback_frequency == 1 / 3
    assert metrics.mttr_seconds == 30.0
    assert metrics.e2e_latency_seconds == (60 + 40 + 90) / 3


def test_compute_metrics_empty_input_returns_all_none():
    metrics = compute_metrics([])
    assert metrics.success_rate is None
    assert metrics.retry_frequency is None
    assert metrics.rollback_frequency is None
    assert metrics.mttr_seconds is None
    assert metrics.e2e_latency_seconds is None
