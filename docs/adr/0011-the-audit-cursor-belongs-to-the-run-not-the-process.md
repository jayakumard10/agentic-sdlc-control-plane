# 0011 - The audit cursor belongs to the run, not the process

## Context

`TelemetrySink` is built once, in `main.py`, and handed to the `Worker` for the life of the
process. It appends `AuditEvent`s to the hash-chained trail as a run progresses, and it tracks how
many of them it has already written so that repeated calls with a growing `state.events` list -
LangGraph threads the same list through every node - only ever write what is new.

That counter was a single integer on the sink. But each run threads its **own** `GraphState`, with
its own `events` list starting empty. So the counter left over from one run was applied to the
next one's list:

```
run 1: events grows 0 -> 17, counter ends at 17
run 2: events grows 0 -> 17, new_events = events[17:] -> []
```

**Every run after the first went unaudited.** Not truncated, not partial - absent. The audit trail
of a container that had served fifty runs contained the first one.

Where a later run produced *more* events than the high-water mark, the outcome was worse than
nothing: only the tail past that mark was written, producing a partial record of a run whose
beginning is missing, inside a chain with no gap in it.

Nothing detected this, and the list of things that did not detect it is the point:

- **The run completed.** It cloned, generated, tested, passed its gates, committed, and published
  a `run-outcome` with the right `commit_sha`.
- **The logs were clean.** Audit lines are rendered from the return value of `flush_new_events`,
  which was correctly empty, so nothing looked wrong or even quiet.
- **`verify_chain` returned `ok=True`.** This is the sharp part. A hash chain proves that record N
  follows record N-1. It cannot prove that records exist. `telemetry.py` already documented
  truncation of the tail as a case a chain cannot detect (ADR 0008); this is that case, reached
  through a different door - not records removed after the fact, but records never written.
- **The unit suite passed.** Every audit test built one worker and drove one run through it. The
  fixture encoded the same assumption as the code: that a sink only ever sees a single run. No
  test catches an assumption it shares with the code (the same shape as ADR 0009).

Found by running two real runs against one container and reading the trail afterwards, rather than
by reasoning about the code: the file held `seq` 0-16 with timestamps spanning only the first run,
while the second had completed and published minutes later.

## Decision

**Two kinds of state live on the sink, and they have different lifetimes. Separate them.**

The chain itself - `_seq` and `_prev_hash` - is per *file*. One process appends to one trail and
those must advance monotonically across every run, or restarts would produce disconnected segments
and verification would report breaks for events that never happened (ADR 0008's resume behaviour).
That state stays where it is.

The flushed-count is per *run*. It becomes `dict[run_id, int]`, and `flush_new_events` takes the
`run_id` it is flushing for.

A cursor is retired by `TelemetrySink.forget(run_id)`, called by the worker where it already
retires the run's `_RunTarget` - the one place that knows a run is finished. Retiring a cursor for
a run still in flight would re-write its events from the beginning, so this belongs with the other
terminal cleanup and nowhere else.

Rejected: **building a sink per run.** It works - `_resume_chain` exists precisely so a new sink
can pick the chain up from the file - but it re-reads the whole trail on every run to recover state
the process already had correctly, and it treats the chain as per-run when the chain is the one
thing here that genuinely is not.

## Consequences

Every run is audited, including after a restart, and the chain remains continuous across all of
them.

The cursor map is bounded by runs in flight rather than by runs served, because `forget` is wired
to terminal cleanup. A run that never reaches a terminal state leaks one small integer, which is
the same leak already accepted for `_targets`.

Two regression tests, and the shape of them matters more than their presence. Both drive **two**
runs where every previous audit test drove one:

- `test_a_second_run_through_the_same_sink_is_audited` - the unit case, including a shorter second
  run, which is the case a high-water mark swallows whole.
- `test_every_run_is_audited_not_only_the_first` - the same property through the real `Worker`,
  because the defect lived in the wiring between a process-lifetime object and per-run state, and a
  test of the sink alone would not have found it.

Both were confirmed to fail against the previous behaviour before being kept.

The platform-wide rule this produces, added to the practice document: **an object whose lifetime is
the process must not hold state whose lifetime is one unit of work.** Where it must, that state is
keyed by the unit and retired with it. The failure is silent by construction, because the leftover
value is a plausible one.

And the narrower lesson about this specific control: **a hash chain is evidence about the records
that are present, and no evidence at all about the ones that should be.** Verifying the trail was
never sufficient; the check that actually catches this class is comparing runs *served* against
runs *recorded* - the same "compare what was written against what was published" rule that mlops
ADR 0007 arrived at from the other direction.
