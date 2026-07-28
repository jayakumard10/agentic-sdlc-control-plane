"""Tests for telemetry.py (dual JSONL/console logging) and metrics.py (reliability

metrics computed from the audit trail).
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from agentic_control_plane.metrics import compute_metrics, summarize_run
from agentic_control_plane.state import AuditEvent, GraphState
from agentic_control_plane.telemetry import (
    TelemetrySink,
    _record_digest,
    render_console_line,
    render_console_trace,
    verify_chain,
)


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


def _write_events(path: Path, count: int) -> TelemetrySink:
    sink = TelemetrySink(path)
    sink.flush_new_events(
        [AuditEvent(node=f"n{i}", event_type="node_end", detail=f"detail {i}") for i in range(count)]
    )
    return sink


def _lines(path: Path) -> list[str]:
    return path.read_text(encoding="utf-8").strip().splitlines()


def test_a_clean_audit_log_verifies(tmp_path: Path):
    path = tmp_path / "events.jsonl"
    _write_events(path, 5)

    result = verify_chain(path)

    assert result.ok
    assert result.records_checked == 5


def test_a_missing_audit_log_verifies_as_an_empty_chain(tmp_path: Path):
    """Nothing written yet is a different condition from something was tampered with."""
    result = verify_chain(tmp_path / "never-written.jsonl")

    assert result.ok
    assert result.records_checked == 0


def test_editing_a_record_is_detected(tmp_path: Path):
    """The property the chain exists for. Before this, the audit trail was plain

    appended JSONL on a writable volume: a gate decision could be edited after the
    fact - who approved it, or whether it was approved at all - and nothing anywhere
    could tell. For a platform whose claim is governed, auditable change, that is the
    load-bearing record.
    """
    path = tmp_path / "events.jsonl"
    _write_events(path, 4)

    lines = _lines(path)
    tampered = json.loads(lines[1])
    tampered["event"]["decision"] = "approved"  # a rejection quietly becomes an approval
    lines[1] = json.dumps(tampered, sort_keys=True, separators=(",", ":"))
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    result = verify_chain(path)

    assert not result.ok
    assert result.broken_at_line == 2
    assert "digest" in result.reason


def test_recomputing_the_digest_of_an_edited_record_is_still_detected(tmp_path: Path):
    """Editing a record and fixing up its own hash does not make the edit consistent:

    the next record still carries the digest of what came before, and that is what
    fails. Defeating the chain means rewriting every record after the edit, which is
    the limitation ADR 0008 names rather than hides.
    """
    path = tmp_path / "events.jsonl"
    _write_events(path, 4)

    lines = _lines(path)
    tampered = json.loads(lines[1])
    tampered["event"]["detail"] = "something else entirely"
    tampered["hash"] = _record_digest(
        tampered["seq"], tampered["prev_hash"], tampered["event"]
    )
    lines[1] = json.dumps(tampered, sort_keys=True, separators=(",", ":"))
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    result = verify_chain(path)

    assert not result.ok
    assert result.broken_at_line == 3, "the break surfaces at the record after the edit"
    assert "prev_hash" in result.reason


def test_removing_a_record_from_the_middle_is_detected(tmp_path: Path):
    path = tmp_path / "events.jsonl"
    _write_events(path, 5)

    lines = _lines(path)
    del lines[2]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    result = verify_chain(path)

    assert not result.ok
    assert result.broken_at_line == 3


def test_truncating_the_tail_is_not_detected_by_the_chain_alone(tmp_path: Path):
    """A limitation, asserted so it stays a known one.

    Each record proves only that it follows its predecessor; nothing in the file says
    how long the file should be. Dropping records off the end therefore leaves a
    shorter chain that still verifies. Detecting that needs an anchor outside the file
    - the Kafka copy of the same events - and this test exists so the gap is visible in
    the suite rather than discovered later by someone assuming otherwise.
    """
    path = tmp_path / "events.jsonl"
    _write_events(path, 5)

    lines = _lines(path)
    path.write_text("\n".join(lines[:3]) + "\n", encoding="utf-8")

    result = verify_chain(path)

    assert result.ok, "the chain cannot see what is no longer there"
    assert result.records_checked == 3


def test_a_restart_extends_the_existing_chain(tmp_path: Path):
    """A new sink over an existing file must continue the chain, not start a new one.

    Restarts are ordinary - the container is restartable by design and a parked run can
    be resumed by a different process. If each restart began again at genesis, every
    audit log would be a series of disconnected segments and verification would report
    a break for something that never happened.
    """
    path = tmp_path / "events.jsonl"
    _write_events(path, 3)

    second_sink = TelemetrySink(path)
    second_sink.flush_new_events(
        [AuditEvent(node="after-restart", event_type="node_start", detail="resumed")]
    )

    result = verify_chain(path)

    assert result.ok
    assert result.records_checked == 4
    assert [json.loads(line)["seq"] for line in _lines(path)] == [0, 1, 2, 3]


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
