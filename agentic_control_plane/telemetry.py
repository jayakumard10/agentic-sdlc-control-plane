"""Dual telemetry output - a tamper-evident JSON-lines audit log and a console trace.

Both sinks read from the same `AuditEvent` list on `GraphState.events`, so there is
one event source and two projections rather than two independently maintained logs.

**The audit log is hash-chained.** Each line carries the SHA-256 of the line before it,
so editing or removing a record invalidates every record after it and `verify_chain`
reports where. Appending to a file is not by itself an integrity property: the file
sits on a writable volume, and "append-only" was a description of how this code uses
it, not a guarantee about what could happen to it. For a platform whose claim is
governed, auditable change, an audit trail nobody can check is the wrong artefact to
take on trust.

What a chain does and does not buy is set out in ADR 0008. In short: it detects a
record altered or removed anywhere but the end. Two things it does not detect, both
because a chain only ever proves that record N follows record N-1:

- **Truncation of the tail.** Dropping the last few lines leaves a shorter chain that
  still verifies, since nothing in the file asserts how long it should be.
- **A wholesale rewrite.** Anyone who can rewrite every line can rebuild a consistent
  chain from scratch.

Both need an anchor outside the file, which is what publishing the same events to
Kafka is for: the broker holds an independent copy that whoever can write to this
volume cannot reach.
"""

from __future__ import annotations

import hashlib
import json
import logging
from pathlib import Path
from typing import NamedTuple

from agentic_control_plane.state import AuditEvent

logger = logging.getLogger(__name__)

# The `prev_hash` of the first record in a chain. A fixed, recognisable value rather
# than an empty string, so a genuinely first record is distinguishable from a record
# whose predecessor's hash went missing.
GENESIS_HASH = "0" * 64


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


def _canonical(payload: dict) -> str:
    """One byte-for-byte serialisation, used when writing and when verifying.

    Sorted keys and no incidental whitespace, so the digest depends on the content
    and not on how the JSON happened to be formatted. Writer and verifier must call
    this same function or a chain would fail to verify the moment either side's
    serialisation drifted.
    """
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def _record_digest(seq: int, prev_hash: str, event: dict) -> str:
    return hashlib.sha256(
        f"{seq}\n{prev_hash}\n{_canonical(event)}".encode("utf-8")
    ).hexdigest()


class ChainVerification(NamedTuple):
    """The result of checking an audit log end to end."""

    ok: bool
    records_checked: int
    broken_at_line: int | None = None
    reason: str = ""


def verify_chain(jsonl_path: Path) -> ChainVerification:
    """Walk an audit log and report the first record that does not hold.

    Checks three things per record, which between them cover alteration, removal, and
    reordering: the sequence number follows its predecessor, `prev_hash` matches the
    previous record's digest, and the digest recomputed from the record's own contents
    matches the one stored on it.

    A missing file verifies as an empty chain rather than a failure - nothing has been
    written yet is a different condition from something has been tampered with.
    """
    if not jsonl_path.exists():
        return ChainVerification(True, 0)

    expected_seq = 0
    expected_prev = GENESIS_HASH
    checked = 0

    with jsonl_path.open(encoding="utf-8") as handle:
        for line_no, raw in enumerate(handle, start=1):
            raw = raw.strip()
            if not raw:
                continue
            try:
                record = json.loads(raw)
            except json.JSONDecodeError as exc:
                return ChainVerification(False, checked, line_no, f"not valid JSON: {exc}")

            missing = {"seq", "prev_hash", "hash", "event"} - record.keys()
            if missing:
                return ChainVerification(
                    False, checked, line_no, f"record is missing {sorted(missing)}"
                )

            if record["seq"] != expected_seq:
                return ChainVerification(
                    False,
                    checked,
                    line_no,
                    f"expected seq {expected_seq}, found {record['seq']} "
                    "- a record was removed or reordered",
                )

            if record["prev_hash"] != expected_prev:
                return ChainVerification(
                    False,
                    checked,
                    line_no,
                    "prev_hash does not match the previous record's digest",
                )

            recomputed = _record_digest(record["seq"], record["prev_hash"], record["event"])
            if recomputed != record["hash"]:
                return ChainVerification(
                    False, checked, line_no, "record contents do not match its own digest"
                )

            checked += 1
            expected_seq += 1
            expected_prev = record["hash"]

    return ChainVerification(True, checked)


def _resume_chain(jsonl_path: Path) -> tuple[int, str]:
    """Pick up an existing chain, so a restart extends it rather than starting a new one.

    A fresh sink on an existing file must continue from the last record's digest.
    Starting again at genesis would leave a seam that verification correctly reports as
    a break - which is the right outcome for a damaged file, and the wrong one for an
    ordinary process restart.
    """
    if not jsonl_path.exists():
        return 0, GENESIS_HASH

    last_line = ""
    with jsonl_path.open(encoding="utf-8") as handle:
        for raw in handle:
            if raw.strip():
                last_line = raw.strip()

    if not last_line:
        return 0, GENESIS_HASH

    try:
        record = json.loads(last_line)
        return int(record["seq"]) + 1, str(record["hash"])
    except (json.JSONDecodeError, KeyError, TypeError, ValueError):
        # The tail is unreadable, so the chain cannot be continued from it. Starting a
        # new segment at genesis is deliberate: it leaves a seam `verify_chain` reports
        # at exactly this point, which is what an operator needs to see. Silently
        # renumbering to look continuous would hide the damage.
        logger.error(
            "audit log tail at %s is unreadable; starting a new chain segment. "
            "verify_chain will report a break here.",
            jsonl_path,
        )
        return 0, GENESIS_HASH


class TelemetrySink:
    """Appends new `AuditEvent`s to a hash-chained JSONL file as a run progresses.

    Tracks how many events have already been flushed so repeated calls with the
    growing `state.events` list (as LangGraph threads state through nodes) only
    ever write and render the events that are actually new.

    **The flushed-count is keyed by run, and that is load-bearing.** Two kinds of
    state live here and they have different lifetimes. The chain itself - `_seq` and
    `_prev_hash` - is per *file*: one process appends to one trail, and those must
    advance monotonically across every run or the chain breaks. The flushed-count is
    per *run*: each run threads its own `GraphState` with its own `events` list that
    starts empty.

    Conflating the two silently dropped every run after the first. One sink is built
    per process, so a single counter left over from run 1 was applied to run 2's
    events list - `events[17:]` against a list of 17 - and wrote nothing. The run
    completed, published its outcome, logged cleanly, and `verify_chain` reported the
    file intact, because a chain proves record N follows N-1 and cannot detect records
    that were never written at all. See docs/adr/0011.
    """

    def __init__(self, jsonl_path: Path) -> None:
        self.jsonl_path = jsonl_path
        self.jsonl_path.parent.mkdir(parents=True, exist_ok=True)
        self._emitted_counts: dict[str, int] = {}
        self._seq, self._prev_hash = _resume_chain(jsonl_path)

    def forget(self, run_id: str) -> None:
        """Drop a finished run's cursor. Called when the worker retires the run.

        Without this the map grows for the life of the process. Forgetting a run that
        is not finished would re-write its events from the start, so this belongs with
        the worker's other terminal cleanup and nowhere else.
        """
        self._emitted_counts.pop(run_id, None)

    def flush_new_events(self, run_id: str, events: list[AuditEvent]) -> list[AuditEvent]:
        """Persist this run's not-yet-written events, and return them.

        Returns the events rather than rendered lines so the caller can decide what
        else they are for - the worker also publishes each one to Kafka, and a second
        projection of the same list is exactly what this module exists to avoid.
        """
        new_events = events[self._emitted_counts.get(run_id, 0) :]
        if not new_events:
            return []
        with self.jsonl_path.open("a", encoding="utf-8") as handle:
            for event in new_events:
                # Through the model's own JSON first, so datetimes and enums serialise
                # the way the rest of the platform sees them rather than via str().
                payload = json.loads(event.model_dump_json())
                digest = _record_digest(self._seq, self._prev_hash, payload)
                handle.write(
                    _canonical(
                        {
                            "seq": self._seq,
                            "prev_hash": self._prev_hash,
                            "hash": digest,
                            "event": payload,
                        }
                    )
                    + "\n"
                )
                self._seq += 1
                self._prev_hash = digest
        self._emitted_counts[run_id] = len(events)
        return new_events
