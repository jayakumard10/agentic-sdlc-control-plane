"""Tests for the worker liveness signal.

The property under test is the one a process check cannot give you: that a worker
which has stopped turning is distinguishable from one that is merely idle. The
sibling repository's ADR 0002 is the reason this exists - a thread died on
construction and the container reported healthy the whole time it ran.
"""

from __future__ import annotations

import os
import queue
import threading
import time
from pathlib import Path

import pytest

from agentic_control_plane import consumer, heartbeat


def test_an_unconfigured_heartbeat_writes_nothing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Off by default, so the suite never needs a writable heartbeat path.

    A signal that has to be stubbed out to run the tests is one that gets deleted.
    """
    monkeypatch.delenv("HEARTBEAT_PATH", raising=False)
    heartbeat.touch()
    assert list(tmp_path.iterdir()) == []

    ok, reason = heartbeat.check()
    assert ok is False
    assert "not set" in reason


def test_a_fresh_heartbeat_passes_and_a_stale_one_does_not(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    beat = tmp_path / "nested" / "worker.heartbeat"
    monkeypatch.setenv("HEARTBEAT_PATH", str(beat))
    monkeypatch.setenv("HEARTBEAT_MAX_AGE_SECONDS", "30")

    heartbeat.touch()
    assert beat.exists(), "touch must create the parent directory it was given"
    ok, _ = heartbeat.check()
    assert ok is True

    # Age it past the limit rather than sleeping through it.
    stale = time.time() - 120
    os.utime(beat, (stale, stale))
    ok, reason = heartbeat.check()
    assert ok is False
    assert "over the" in reason


def test_a_missing_heartbeat_is_not_treated_as_healthy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """Startup and death look identical here, and both should hold the container
    out of service until a pass has actually completed."""
    monkeypatch.setenv("HEARTBEAT_PATH", str(tmp_path / "never-written"))
    ok, reason = heartbeat.check()
    assert ok is False
    assert "does not exist" in reason


def test_a_heartbeat_that_cannot_be_written_does_not_take_the_worker_down(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """The file is a report about the loop, never a dependency of it."""
    unwritable = tmp_path / "a-file"
    unwritable.write_text("not a directory", encoding="utf-8")
    monkeypatch.setenv("HEARTBEAT_PATH", str(unwritable / "worker.heartbeat"))

    heartbeat.touch()  # must not raise


def test_an_unparseable_max_age_falls_back_rather_than_failing(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("HEARTBEAT_MAX_AGE_SECONDS", "soon")
    assert heartbeat.max_age_seconds() == float(heartbeat.DEFAULT_MAX_AGE_SECONDS)


def test_an_unset_max_age_uses_the_default(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("HEARTBEAT_MAX_AGE_SECONDS", raising=False)
    assert heartbeat.max_age_seconds() == float(heartbeat.DEFAULT_MAX_AGE_SECONDS)


def test_the_healthcheck_entrypoint_exits_nonzero_when_stale(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
):
    """This is the contract Compose actually depends on.

    The healthcheck runs `python -m agentic_control_plane.heartbeat` and reads only
    the exit status, so a check that reported a problem and still returned 0 would
    leave the container marked healthy while saying it was not.
    """
    beat = tmp_path / "worker.heartbeat"
    monkeypatch.setenv("HEARTBEAT_PATH", str(beat))
    monkeypatch.setenv("HEARTBEAT_MAX_AGE_SECONDS", "30")

    heartbeat.touch()
    assert heartbeat.main() == 0

    stale = time.time() - 120
    os.utime(beat, (stale, stale))
    assert heartbeat.main() == 1
    assert "over the" in capsys.readouterr().out


def test_the_idle_worker_loop_keeps_the_heartbeat_fresh(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """The point of the whole mechanism: idle is not silent.

    An idle worker writes nothing to any log and holds no work, so from outside it is
    indistinguishable from a thread that died on its first pass. This drives the real
    loop with an empty queue and asserts the file moves anyway.
    """
    beat = tmp_path / "worker.heartbeat"
    monkeypatch.setenv("HEARTBEAT_PATH", str(beat))

    worker = object.__new__(consumer.Worker)
    worker.queue = queue.Queue()

    stop_event = threading.Event()
    thread = threading.Thread(target=consumer.Worker.run_forever, args=(worker, stop_event))
    thread.start()
    try:
        deadline = time.time() + 5
        while not beat.exists() and time.time() < deadline:
            time.sleep(0.05)
        assert beat.exists(), "an idle worker still has to report that it is turning"
    finally:
        stop_event.set()
        thread.join(timeout=5)
