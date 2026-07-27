# 0007 - A full work queue is backpressure, not loss

## Context

An external architecture review flagged that the worker "can log and drop work when its in-memory
queue fills", citing the drop path in `consumer.py`, and read it as the loss window that
[ADR 0005](0005-single-threaded-worker-and-when-offsets-commit.md) already documents and bounds.

It was not that window. It was a second one, undocumented, and worse.

ADR 0005 describes losing *queued* items when the process dies. That is bounded by three things: a
small queue, a shutdown drain, and the recurrence of drift. None of them apply here. Reading the
call site:

```python
worker.submit(work)     # returns False when the queue is full - discarded
...
consumer.commit()       # commits the whole batch regardless
```

`submit` correctly reported that it could not accept the work. Nothing checked. The offset was then
committed past the message, so Kafka would never redeliver it. A drift signal entered the platform
and left no trace: no run, no redelivery, and — because a run that never starts cannot reach a
terminal state — **no outcome event either**. It was invisible in the audit trail, which is the one
place a governed-change platform cannot afford a silent gap.

This directly contradicts design principle **P6, "a run is never lost."** A documented, bounded
trade-off is a design decision. A stated invariant the code does not hold is a defect, and it puts
every other claim in the design document in question.

The overload case is also exactly when it would bite hardest: the queue fills because triggers are
arriving faster than the serial worker drains them, so the messages being dropped are the ones from
the busiest period.

## Decision

A rejected message is left uncommitted, rewound, and redelivered.

**Both halves are required, and the second is the non-obvious one.** Skipping the commit alone does
not work: `poll()` has already advanced the consumer's in-memory position past the message, so the
next poll returns the *following* message. The rejected one would be skipped for the life of the
process and only reappear after a restart or rebalance. So the partition is explicitly rewound with
`seek(partition, message.offset)`.

**The partition is then paused.** Without that, the loop re-fetches the same message on every
iteration and spins against a queue it cannot drain. It resumes once the worker has drained to a
low-water mark of half the queue, rather than on the first free slot — resuming at one free slot
would fetch a batch, fill, and pause again on nearly every cycle.

**Processing of a partition stops at the first rejected message.** Accepting anything behind it
would put the redelivered message out of order relative to work already queued, which for a
partition keyed by `thread_id` would mean a later gate decision overtaking an earlier one for the
same parked run.

**The commit is skipped for the whole batch when anything is rejected**, rather than committing the
partitions that were fine. Those messages stay uncommitted and are redelivered after a crash, which
is harmless — a redelivered trigger carries a `run_id` the checkpointer already knows and is
handled as a no-op. Per-partition offsets would be more precise, at the cost of coupling this module
to the client's offset types, and buy nothing at one partition per topic.

## Consequences

P6 holds again for this path. A full queue costs latency instead of a run.

The remaining window is the one ADR 0005 documents and bounds: a crash with items already accepted
onto the queue. That is genuinely narrower than what existed before — it needs a process death, not
merely a busy period — and the durable hand-off table remains the upgrade path for closing it.

The regression test drives the queue past capacity and asserts every trigger is handed over exactly
once and in order, across as many pause/resume cycles as it takes. It was verified against the
pre-fix code first: it fails there, logging 21 consecutively dropped triggers. A regression test
that has never been seen to fail is an assertion about the author's confidence, not about the code.

Testing this needed a fake consumer with a real *position*, because the existing one returns canned
batches and has no notion of one. Against that fake the rewind is invisible — a test using it would
pass whether or not `seek` were ever called. Where the behaviour under test is about a client's
internal state machine, the double has to model the part of that machine the behaviour depends on,
or the test only asserts that the code runs.

The general point is narrower than "check return values". `submit` was written to report refusal,
and documented as doing so, and tested as doing so — the test asserting `submit(...) is False`
passed throughout. What was missing was any test of what the *caller* does with that answer. A
function's contract being honoured proves nothing about the call site honouring it.
