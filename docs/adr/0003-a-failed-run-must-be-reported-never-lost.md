# 0003 - A failed run must be reported, never lost

## Context

A run whose node raised an exception mid-execution disappeared without trace. Reproduced against a
live broker and a live checkpointer:

1. A gate decision arrived and a node raised while applying it.
2. `handle_decision` caught `KeyError` and `ValueError` around the whole `resume_run` call, in
   order to treat two benign preconditions - the run is unknown, the run already finished - as
   non-events. Those are the same builtin types a Pydantic validation error inside a node raises.
   The real failure was therefore logged as *"already reached a terminal state"* and discarded.
3. The checkpoint was left with no pending work but a `run_status` of `running`. It was neither
   resumable, so nothing would ever pick it up again, nor terminal, so nothing published an
   outcome.
4. The next startup's workspace reconciliation, correctly seeing a run that could not continue,
   deleted its workspace.

The net result: a run that was triggered by a real drift signal, cloned a repo, parked at a human
gate, received a human decision, and then failed - and no record of any of it existed anywhere
except one misleading log line. For a system whose purpose is governed, auditable change, losing a
run silently is the worst available outcome. Failing loudly is fine. Vanishing is not.

## Decision

**Precondition failures get their own exception types.** `runner` raises `UnknownRunError` and
`RunAlreadyTerminalError`. They still subclass `KeyError` and `ValueError` so existing callers are
unaffected, but the consumer catches the specific types. A builtin raised from inside graph
execution no longer matches.

**Graph-execution failures produce a terminal outcome.** Both the trigger path and the decision
path wrap their runner call. Any unexpected exception publishes a `run-outcome` event with
`terminal_state: "failed"` and the real exception text in `detail`, then cleans up the workspace.
The run ends visibly, in the same channel every other terminal state is reported on.

The exception is logged with its traceback as well as being reported, because the outcome event
carries a summary and an operator debugging the failure needs the stack.

## Consequences

Every run now ends in exactly one of the reported terminal states - `completed`, `failed`,
`safe_stop`, `clone_failed`, or `stale`. There is no path that ends a run without an event.

Broad exception handling remains, deliberately: the worker must survive one bad run. What changed
is that it now reports what it caught instead of relabelling it as something benign. The failure
mode was not "caught too much" but "caught the wrong thing and lied about it".

Catching a supertype to handle a specific expected case is the general trap here. `ValueError` is
too common a type to use as a signal; if a precondition is worth distinguishing, it is worth its
own type.
