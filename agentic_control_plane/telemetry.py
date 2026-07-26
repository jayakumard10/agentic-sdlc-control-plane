"""Dual telemetry output — JSON-lines audit log and a derived console trace.

Both sinks read from the same `AuditEvent` list on `GraphState.events`, so there is
one event source and two projections rather than two independently maintained logs.
"""

from __future__ import annotations

from pathlib import Path

from agentic_control_plane.state import AuditEvent


def render_console_line(event: AuditEvent) -> str:
    ts = event.timestamp.strftime("%H:%M:%S")
    line = f"[{ts}] {event.node:<24} {event.event_type:<18} {event.detail}"
    if event.decision:
        line += f" | decision={event.decision}"
    if event.latency_ms is not None:
        line += f" | {event.latency_ms:.0f}ms"
    return line


def render_console_trace(events: list[AuditEvent]) -> str:
    return "\n".join(render_console_line(event) for event in events)


class TelemetrySink:
    """Appends new `AuditEvent`s to a JSONL file as they arrive during a run.

    Tracks how many events have already been flushed so repeated calls with the
    growing `state.events` list (as LangGraph threads state through nodes) only
    ever write and render the events that are actually new.
    """

    def __init__(self, jsonl_path: Path) -> None:
        self.jsonl_path = jsonl_path
        self.jsonl_path.parent.mkdir(parents=True, exist_ok=True)
        self._emitted_count = 0

    def flush_new_events(self, events: list[AuditEvent]) -> list[str]:
        new_events = events[self._emitted_count :]
        if not new_events:
            return []
        with self.jsonl_path.open("a", encoding="utf-8") as f:
            for event in new_events:
                f.write(event.model_dump_json() + "\n")
        self._emitted_count = len(events)
        return [render_console_line(event) for event in new_events]
