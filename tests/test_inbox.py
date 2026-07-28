"""Tests for the durable work hand-off.

The property under test is the one ADR 0005 documented as a known gap and ADR 0010
closes: work accepted from Kafka survives the process that accepted it. Offsets are
committed on enqueue, so anything the worker had in hand when it died is not coming
back from the broker - it has to come back from here.

These need a real Postgres and skip without one, for the same reason the checkpointer's
durability tests do: an in-memory stand-in would assert that the code runs, not that the
work survives.

They share a database with a possibly-running control plane, so every test scopes itself
to run ids it created and cleans up only those. A `TRUNCATE` here would delete the live
service's pending work, which is precisely the failure this module exists to prevent.
"""

from __future__ import annotations

import os
import uuid

import psycopg
import pytest

from agentic_control_plane import consumer
from agentic_control_plane.checkpointer import _postgres_conn_string, build_memory_checkpointer
from agentic_control_plane.inbox import Inbox


def _postgres_reachable() -> bool:
    if not os.environ.get("POSTGRES_USER"):
        return False
    try:
        with psycopg.connect(_postgres_conn_string(), connect_timeout=3):
            return True
    except psycopg.OperationalError:
        return False


pytestmark = pytest.mark.skipif(
    not _postgres_reachable(), reason="needs a reachable Postgres; see the README"
)


@pytest.fixture
def work_inbox():
    """A real inbox, with only this test's own rows removed afterwards."""
    box = Inbox(_postgres_conn_string())
    box.setup()
    prefix = f"test-{uuid.uuid4().hex[:8]}"
    yield box, prefix
    with psycopg.connect(_postgres_conn_string(), connect_timeout=3) as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM work_inbox WHERE run_id LIKE %s", (f"{prefix}%",))


def _trigger(prefix: str, suffix: str = "1") -> consumer.TriggerWork:
    return consumer.TriggerWork(
        run_id=f"{prefix}-{suffix}",
        scenario_type="brownfield",
        repo_url="https://example.invalid/repo.git",
        branch="main",
        requirement="something drifted",
    )


def _mine(box: Inbox, prefix: str) -> list[tuple]:
    return [row for row in box.pending() if row[2]["run_id"].startswith(prefix)]


def test_accepted_work_survives_the_process_that_accepted_it(work_inbox):
    """The headline property. Offsets are committed once work is enqueued, so a crash

    with items in memory used to lose them outright - the broker considers them handled
    and will not send them again. Now the next process finds them.
    """
    box, prefix = work_inbox
    dying = consumer.Worker(build_memory_checkpointer(), inbox=box)

    assert dying.submit(_trigger(prefix, "a")) is True
    assert dying.submit(_trigger(prefix, "b")) is True
    # The process dies here: the queue is in-memory and goes with it.
    del dying

    restarted = consumer.Worker(build_memory_checkpointer(), inbox=box)
    restored = restarted.restore_pending()

    assert restored >= 2
    recovered = []
    while not restarted.queue.empty():
        recovered.append(restarted.queue.get().run_id)
    assert f"{prefix}-a" in recovered
    assert f"{prefix}-b" in recovered


def test_finished_work_is_not_replayed(work_inbox):
    """A run that completed must not start again on the next boot.

    It has already published a terminal outcome, so replaying it would report a second
    one for the same run.
    """
    box, prefix = work_inbox
    worker = consumer.Worker(build_memory_checkpointer(), inbox=box)
    work = _trigger(prefix, "done")
    worker.submit(work)
    assert _mine(box, prefix), "recorded while in flight"

    worker._forget(work)  # what run_forever does once the item is handled

    assert _mine(box, prefix) == [], "forgotten once finished"


def test_work_refused_for_a_full_queue_leaves_nothing_behind(work_inbox):
    """Backpressure must not also become a replay.

    The row is written before the enqueue attempt, so a refusal has to undo it -
    otherwise the message is redelivered by Kafka *and* restored from the inbox, and
    the same drift starts twice.
    """
    box, prefix = work_inbox
    worker = consumer.Worker(build_memory_checkpointer(), inbox=box)
    for i in range(consumer.WORK_QUEUE_MAXSIZE):
        assert worker.submit(_trigger(prefix, f"fill{i}")) is True

    overflow = _trigger(prefix, "overflow")
    assert worker.submit(overflow) is False

    remaining = {row[2]["run_id"] for row in _mine(box, prefix)}
    assert f"{prefix}-overflow" not in remaining, "a refused item must leave no row"
    assert len(remaining) == consumer.WORK_QUEUE_MAXSIZE, "accepted items are still recorded"


def test_work_that_cannot_be_recorded_is_refused(work_inbox):
    """If the durable write fails, the work must not be accepted.

    Enqueuing anyway would commit the Kafka offset for work this process cannot write
    down - the exact loss the inbox exists to prevent, arriving through the failure
    path instead of the crash path.
    """
    _box, prefix = work_inbox

    class _BrokenInbox:
        def record(self, *_a, **_k):
            raise psycopg.OperationalError("connection refused")

        def discard(self, *_a, **_k):  # pragma: no cover - not reached
            raise AssertionError("nothing to discard")

    worker = consumer.Worker(build_memory_checkpointer(), inbox=_BrokenInbox())

    assert worker.submit(_trigger(prefix, "unrecordable")) is False
    assert worker.queue.empty(), "refused work must not be queued"


def test_restore_preserves_the_order_work_was_accepted_in(work_inbox):
    """Decisions are keyed by thread_id precisely so an earlier one cannot be

    overtaken by a later one for the same parked run. Recovery has to honour that too.
    """
    box, prefix = work_inbox
    worker = consumer.Worker(build_memory_checkpointer(), inbox=box)
    for i in range(5):
        worker.submit(_trigger(prefix, f"{i}"))

    restarted = consumer.Worker(build_memory_checkpointer(), inbox=box)
    restarted.restore_pending()

    order = []
    while not restarted.queue.empty():
        run_id = restarted.queue.get().run_id
        if run_id.startswith(prefix):
            order.append(run_id)
    assert order == [f"{prefix}-{i}" for i in range(5)]


def test_a_worker_without_an_inbox_still_works(work_inbox):
    """Tests and any deployment without Postgres keep the previous behaviour,

    rather than the inbox becoming a hard dependency of enqueueing at all.
    """
    _box, prefix = work_inbox
    worker = consumer.Worker(build_memory_checkpointer())

    assert worker.submit(_trigger(prefix, "no-inbox")) is True
    assert worker.restore_pending() == 0
