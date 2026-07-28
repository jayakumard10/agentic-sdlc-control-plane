# 0010 - Durable work hand-off

## Context

[ADR 0005](0005-single-threaded-worker-and-when-offsets-commit.md) committed offsets once work was
enqueued rather than once it completed, because committing on completion puts a clone and a full
graph run inside the poll loop - the thing the worker split exists to prevent. It named the
resulting window plainly: a crash with items still on the in-memory queue loses those triggers,
because Kafka has already been told they were handled.

It bounded that with three arguments, and named the fix: *"write the trigger to a table in this
service's own Postgres inside the poll loop, commit the offset, and have the worker read from
there."* An external architecture review reached the same conclusion independently and put it first
on its must-fix list, on the grounds that a governed change platform cannot silently lose a trigger.

One of ADR 0005's three mitigations has since turned out to be false, which sharpens the case. It
argued that drift recurs, so a lost trigger would be re-started by the next detection carrying the
same run id. The producing repository generated a random `correlation_id` per detection, so it
never would have (mlops ADR 0006). That is fixed, but the loss window should not have been resting
on a property of another repository in the first place.

[ADR 0007](0007-a-full-work-queue-is-backpressure-not-loss.md) closed the other, worse, loss path -
a full queue dropping work and committing past it. This closes the one that remained.

## Decision

Work is written to a `work_inbox` table in this service's own Postgres **before** it reaches the
in-memory queue, and deleted once the worker has finished with it. Anything still present at
startup is work a previous process accepted and did not finish, so it is put back on the queue
before the poll loops start.

The in-memory queue stays. It is what makes hand-off immediate; the table is what makes it
survivable. A single bounded `INSERT` is nothing like a graph run, so this does not reintroduce the
problem the worker split solved.

**Recording comes first.** There is no moment where work is queued but not durable. The ordering
matters more than it looks: the other order leaves a window where the process can die holding work
it has already told Kafka it handled, which is the exact failure being fixed.

**Failing to record is a refusal.** If the write fails, `submit` returns `False` and the poll loop
treats it as backpressure - the offset stays uncommitted and Kafka redelivers. Accepting work this
process could not write down, and then committing its offset, would be the same loss arriving
through the failure path instead of the crash path.

**A refusal undoes its own row.** Work recorded but not queued is deleted again, so a message that
is about to be redelivered is not also restored from the inbox and run twice.

**Failing to discard is logged, not raised.** By then the work is done. A leftover row costs one
replay at the next startup, where a redelivered trigger is already a no-op because its run is in the
checkpointer. Raising would turn bookkeeping into a failed run.

**A connection per operation.** The two poll loops and the worker would otherwise share one, and
whether a psycopg connection is safe to use concurrently is exactly the class of question ADR 0005
declines to answer by testing. Connecting to a local Postgres costs a few milliseconds against a
poll loop already doing network I/O to a broker. A pool is the bounded upgrade if this platform's
volume ever justifies it.

## Consequences

Work accepted from Kafka survives the process that accepted it. Measured against the running
container: fourteen drift events published, the container killed with `SIGKILL` two seconds later
while the worker was still draining, **twelve unfinished items found in the table**, and
`Restored 12 unfinished work item(s) from the inbox` on the next start - with Kafka at zero lag
throughout, so none of it came back from the broker. Before this, those twelve were gone.

**A duplicate window replaces the loss window, and that is the trade.** Offsets are committed after
the work is recorded and queued, so a crash in between means the item is both in the inbox and
still unread by Kafka - it is restored *and* redelivered. Both copies carry the same `run_id`, so
the second is a no-op against the checkpointer. Trading a silent loss for a deduplicated duplicate
is the right direction; claiming exactly-once would not be true.

The table is unbounded in principle. In practice it holds at most what the queue holds plus what is
in flight, because every path that adds a row also removes it, and the queue is capped at 32. A row
that outlives its work - through a failed discard - is removed at the next startup by being
replayed and then finished.

Startup now depends on Postgres being reachable before the poll loops begin, which was already true
for the checkpointer, so this adds no new failure mode. It does mean the inbox and the checkpoint
store share a database and a lifetime; that is deliberate, since restoring work whose run state had
been lost would restore nothing useful.
