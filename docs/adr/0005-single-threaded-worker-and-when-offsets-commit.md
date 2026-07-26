# 0005 - A single-threaded worker, and when offsets commit

## Context

A gate can park a run for as long as a human takes to answer. Kafka's `max.poll.interval.ms`
defaults to five minutes, after which a consumer that has not polled is considered dead and its
partitions are reassigned - taking every other in-flight run on those partitions with it. So the
poll loop cannot execute runs, and this shapes the whole component.

Two questions follow: where does execution happen, and when is an offset committed.

## Decision

**Execution happens on a worker thread, and the poll loops only validate and enqueue.** Neither
loop clones a repo, runs a graph, or waits for a decision. Run duration is fully decoupled from
`max.poll.interval.ms`.

**The worker is single-threaded and owns the checkpointer exclusively.** This is the part worth
justifying, because it costs throughput. Sharing a checkpointer across threads raises a question
about the safety of its underlying connection that would need to be answered by testing, not by
reading. This platform has already shipped a bug from assuming a library's concurrency model
matched its documentation - a second connection to the same store that the library rejected at
runtime, discovered only in a live container. One thread makes the question not arise. Runs execute
serially, which is right at this platform's volume, and the upgrade is bounded: a pool of workers
each holding its own checkpointer, if and when serial execution is the constraint.

**Offsets commit once work is enqueued, not once it completes.** Committing on completion would put
a clone and a full graph run back inside the poll loop, which is the thing this design exists to
prevent.

## Consequences

The commit choice leaves a real gap, stated rather than glossed: if the process dies with items
still on the queue, those triggers are lost. Three things bound it.

The queue is small - 32 items - and the worker is serial, so a larger queue would widen the window
for no benefit. Shutdown drains the queue for up to thirty seconds and logs an error naming the
number of items lost if it cannot finish. And drift is a recurring condition: the producer derives
each event's correlation id deterministically from the drift condition itself, so if the condition
persists the next detection arrives with the same run id and starts the run that was lost. A
transient drift that resolves on its own is the case that would be genuinely dropped, and that is
the case least in need of a run.

The upgrade path, if that stops being acceptable, is a durable hand-off: write the trigger to a
table in this service's own Postgres inside the poll loop, commit the offset, and have the worker
read from there. That is a fast bounded write, unlike a graph run, so it does not reintroduce the
original problem. It was not built now because it adds a table and a recovery path to maintain for
a window the recurrence property already largely covers.

Serial execution means a long run delays every queued run behind it. At one worker with a 32-item
queue this is visible as latency, not loss, and the sweep and TTL machinery is unaffected because
it runs on its own thread.
