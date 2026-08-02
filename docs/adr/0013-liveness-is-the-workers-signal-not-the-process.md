# 0013 — Liveness is the worker's signal, not the process's

## Context

Neither worker container in this platform had a healthcheck. Every other service —
Postgres, Kafka, MLflow, the tenant API — had one, so the gap read as an oversight of
the two most important containers rather than a decision.

The argument for leaving them out is that a crashed process is already handled:
`restart: unless-stopped` brings it back, and Docker knows the process is gone
without being told. That argument is wrong in a specific way this platform has
already paid for.

In `agentic-sdlc-mlops`, the background drift thread died with a `TypeError` on every
start ([its ADR 0002](https://github.com/jayakumard10/agentic-sdlc-mlops/blob/main/docs/adr/0002-extract-thread-construction-for-testability.md)).
The process stayed up, because a thread that dies does not raise into the process that
started it. The container reported healthy for as long as it ran, and the only reason
anyone found out was reading its logs for an unrelated purpose.

A process check would have reported healthy too. The two states that need
distinguishing — the worker is idle, and the worker is gone — look identical from
outside the process, and identical to `pgrep`.

## Decision

The worker writes a heartbeat file on every pass of its loop, idle included, and the
container healthcheck fails when that file stops moving.

- `Worker.run_forever` calls `heartbeat.touch()` at the top of each iteration. The
  loop already wakes at least once a second on `queue.get(timeout=1.0)`, so an idle
  worker keeps the file fresh without any added scheduling.
- The healthcheck runs `python -m agentic_control_plane.heartbeat`, which exits
  non-zero when the file is missing or older than `HEARTBEAT_MAX_AGE_SECONDS`.
- **Unconfigured is a no-op.** `HEARTBEAT_PATH` is set in Compose and nowhere else, so
  the unit suite neither writes the file nor needs a writable location for it. A
  liveness signal that has to be stubbed out to run the tests is one that gets
  deleted.
- **`touch()` never raises.** The file is a report about the loop, not a dependency of
  it; a heartbeat that could take the worker down would be worse than no heartbeat.

## Consequences

A worker thread that dies now takes the container unhealthy within
`HEARTBEAT_MAX_AGE_SECONDS`, instead of leaving a live process that consumes nothing.

**A long single work item also reads as unhealthy, and that is the intended reading
rather than a false positive.** Runs execute serially on one worker, so a run holding
it for five minutes is blocking every other run behind it. That condition is worth
surfacing whether the cause is a wedged thread or a genuinely slow one — the operator
response is the same, and the alternative is a threshold so generous it detects
nothing.

The default of 300s is a judgement, not a measurement, and it is the number to revisit
first if legitimate runs start tripping it. Raising it weakens detection
proportionally; the better fix at that point is bounding run duration, not widening
the window.

This does not detect a worker that is turning but making no progress — a loop that
picks work up, fails it, and forgets it would heartbeat perfectly while achieving
nothing. That is a throughput question, and the outcome events already answer it.
Liveness and progress are separate claims, and this file only makes the first one.
