# 0004 - The poll loop tolerates transient client errors

## Context

During an end-to-end run the entire control plane exited. The cause was a `ValueError: Invalid file
descriptor: -1`, raised inside the Kafka client's own event loop when its selector tried to
unregister a socket that had already been closed. It propagated out of `consumer.poll()`, out of
the poll loop, out of `main()`, and terminated the process.

The service had started correctly, joined both consumer groups, and was idle. It stopped roughly
one second later. The shutdown path ran cleanly and logged "Shutdown complete", which made the log
read like an orderly stop; the traceback appeared afterwards, below the shutdown message.

The specific error is a client-library and platform artifact. The problem it exposed is not: there
was no error handling at all around the poll call, so *any* client-side error - a transient
connection reset, a metadata refresh failure, a socket closed under the selector - would stop the
whole service. A control plane that other services depend on should not be stoppable by one bad
file descriptor.

## Decision

The poll call is wrapped. A failure is logged with its traceback, counted, and retried after a
short backoff. Ten consecutive failures re-raise.

The bound matters as much as the retry. Retrying forever would convert a loud crash into a silent
hang - a consumer that can never poll would sit in a quiet loop looking alive while consuming
nothing, which is a worse failure than exiting. The counter resets on any successful poll, so
occasional unrelated blips never accumulate toward the limit.

The retry does not attempt to rebuild the consumer. If ten consecutive polls fail, the problem is
not transient and restarting the process under the container's restart policy is a better recovery
than reconstructing client state in place.

## Consequences

A transient client error costs a couple of seconds of consumption rather than the service.

A genuinely broken consumer still fails, just after about twenty seconds instead of instantly. That
delay is worth the resilience, and the log makes the distinction obvious: each failure is numbered
against the limit.

This is only about the *poll* call. Message handling already had its own error boundary, and the
worker loop has one too. This closed the last unguarded call in the chain between the broker and a
run.

Noted for the platform generally: this was found only because a real end-to-end run was left
running long enough to hit it. A test suite exercises message handling, never the client's internal
socket lifecycle.
