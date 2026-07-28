"""Durable hand-off between the poll loops and the worker.

Offsets are committed once work is enqueued rather than once it completes, because
committing on completion would put a clone and a full graph run inside the poll loop -
the thing the worker split exists to prevent (ADR 0005). That leaves a window: a crash
with items still on the in-memory queue loses those triggers, since Kafka has been told
they were handled.

This closes it. Each validated work item is written here *before* it reaches the queue,
and deleted once the worker is done with it. Anything still present at startup is work
the previous process accepted and did not finish, so it is put back on the queue. The
write is a single bounded INSERT into this service's own Postgres, which is nothing like
a graph run, so it does not reintroduce the problem the split solved.

**A connection per operation, deliberately.** The two poll loops and the worker would
otherwise share one, and whether a psycopg connection is safe to use concurrently is
exactly the class of question ADR 0005 declines to answer by testing. Connecting to a
local Postgres costs a few milliseconds against a poll loop that is already doing
network I/O to a broker, and this platform's volume is drift events, not a firehose. A
pool is the bounded upgrade if that stops being true.

**Failing to record is refusal, not a warning.** If the write fails, `record` raises and
the caller treats the item as un-acceptable - the offset stays uncommitted and Kafka
redelivers. Recording work as durable when it is not would be worse than backpressure.
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict

import psycopg

logger = logging.getLogger(__name__)

# Short, so an unreachable Postgres surfaces as refusal quickly rather than stalling a
# poll loop that must keep calling poll() to hold its partitions.
CONNECT_TIMEOUT_SECONDS = 3

SCHEMA = """
CREATE TABLE IF NOT EXISTS work_inbox (
    id BIGSERIAL PRIMARY KEY,
    kind TEXT NOT NULL,
    run_id TEXT NOT NULL,
    payload JSONB NOT NULL,
    enqueued_at TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""


class Inbox:
    """Work accepted from Kafka but not yet finished, held in Postgres."""

    def __init__(self, conn_string: str) -> None:
        self.conn_string = conn_string

    def _connect(self):
        return psycopg.connect(self.conn_string, connect_timeout=CONNECT_TIMEOUT_SECONDS)

    def setup(self) -> None:
        """Create the table if absent. Idempotent, called on every startup."""
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(SCHEMA)

    def record(self, kind: str, run_id: str, payload: dict) -> int:
        """Persist one work item and return its row id. Raises if it cannot."""
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO work_inbox (kind, run_id, payload) VALUES (%s, %s, %s) RETURNING id",
                (kind, run_id, json.dumps(payload)),
            )
            return cur.fetchone()[0]

    def discard(self, row_id: int) -> None:
        """Forget a work item the worker has finished with, or never queued.

        Failure here is logged rather than raised: the work itself is done, and the
        cost of a leftover row is that it is replayed at the next startup - where
        a redelivered trigger is already a no-op, since its run is in the checkpointer.
        Raising would turn a bookkeeping problem into a failed run.
        """
        try:
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute("DELETE FROM work_inbox WHERE id = %s", (row_id,))
        except Exception:
            logger.exception(
                "could not discard inbox row %s; it will be replayed at next startup", row_id
            )

    def pending(self) -> list[tuple[int, str, dict]]:
        """Work the previous process accepted and did not finish, oldest first."""
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT id, kind, payload FROM work_inbox ORDER BY id")
            return [(row[0], row[1], row[2]) for row in cur.fetchall()]

    def depth(self) -> int:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM work_inbox")
            return cur.fetchone()[0]


def payload_of(work) -> dict:
    """The work item's own fields, without the row id that identifies it here."""
    fields = asdict(work)
    fields.pop("inbox_id", None)
    return fields
