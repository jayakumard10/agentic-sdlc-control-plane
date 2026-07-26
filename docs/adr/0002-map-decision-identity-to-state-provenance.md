# 0002 - Map a decision's identity to state provenance at the boundary

## Context

The event contract and the graph state both have a field called `decided_by`, and they mean
different things by it.

On the wire, `decided_by` identifies *who* made the decision. The contract's own worked example for
a gate-decision event carries a GitHub username.

In `GraphState`, `GateRecord.decided_by` records *how* the gate was resolved, and is constrained to
`Literal["human", "replayed"]` - a real person answered it, or a recorded fixture supplied the
answer. That distinction exists so an audit trail can tell a genuine approval apart from a replayed
one.

Passing the wire value straight into `GateRecord` therefore fails validation for any username that
is not the literal string `"human"`, which is every username. Every gate decision arriving from
Kafka was rejected.

This was not caught by any unit test. The tests constructed decisions themselves and naturally used
the vocabulary the model expects, so they only ever exercised values that were already valid. It
took a real event, produced by a separate process against a live broker, to put a real username
through the path.

## Decision

The mapping happens once, at the consumer boundary, where wire vocabulary is translated into state
vocabulary:

- `decided_by` becomes provenance: `replayed` when the event says so, `human` otherwise.
- The identity is preserved as `decided_by_identity` and travels with the rest of the decision into
  `GateRecord.decision_payload`.

`GraphState` is not changed. Widening `decided_by` to a free string would have made the field
accept the event's value at the cost of destroying the distinction it exists to draw, and an audit
record that cannot distinguish a human approval from a replayed one is worse than one that rejects
bad input.

A test now validates the parser's output against `GateRecord` itself, rather than against what the
parser happens to emit. That is the assertion that would have caught this without a live broker.

## Consequences

Who approved a gate is still recorded - in `decision_payload`, alongside the comment and the
override flag - so nothing an audit needs is lost.

Two fields in two schemas keep a shared name and different meanings, which is a standing hazard.
The mapping is in one place and commented at the point of translation, so the next person to touch
either schema meets the explanation rather than the bug.

The general lesson is about the shape of the test, not this field: a test that builds its own input
using the vocabulary of the thing it is testing cannot discover a mismatch with an external
producer's vocabulary. Where a boundary translates between two schemas, at least one test should
assert against the destination model.
