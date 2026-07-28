# 0008 - The audit trail must be checkable, not merely appended

## Context

An external architecture review put it precisely: audit records were "append-only by convention",
and JSONL on a mounted writable volume "is not immutable or centrally retained".

Both halves of that are right, and the phrase *append-only by convention* is the whole problem.
Appending was a description of how this code used the file, not a property the file had. Anything
with write access to the volume - the container itself, anything sharing the mount, anyone on the
host - could edit a record or remove one, and nothing anywhere would know. A gate decision could
have its approver changed, or its rejection turned into an approval, after the fact.

That matters more here than it would in most systems. This platform's entire claim is *governed,
auditable change*: an AI agent writes code, and what makes that defensible is the record of why the
run started, who approved it, and what happened. Every other property in the design document is
downstream of that record being trustworthy. An audit trail nobody can check is the wrong artefact
to take on trust, and "we only ever append to it" is not a control.

The record was also confined to one host. A copy that lives only on the machine whose behaviour it
describes is not independent evidence of anything.

## Decision

Two mechanisms, because they fail differently and neither is sufficient alone.

**The file is hash-chained.** Each line carries the SHA-256 of the line before it, computed over a
canonical serialisation so the digest depends on content rather than formatting. `verify_chain`
walks the log and reports the first record that does not hold, checking three things per record:
that the sequence number follows its predecessor, that `prev_hash` matches the previous record's
digest, and that the digest recomputed from the record's own contents matches the one stored on it.
Between them those cover alteration, removal, and reordering.

A new sink over an existing file continues the chain rather than starting a new one. Restarts are
ordinary here - the container is restartable by design and a parked run can be resumed by a
different process - so beginning again at genesis on every start would produce a log of
disconnected segments and report breaks for something that never happened.

**The same events are published to `control-plane.audit.v1`**, keyed by `run_id`, in the platform's
own envelope with the record as `payload`. This is the anchor outside the file: whoever can write
to the volume cannot reach the broker's copy, so the two can be compared. It is best-effort, like
every other publish in this service - a broker outage degrades the guarantee to one copy rather
than failing a run that really did happen.

## Consequences

Tampering with the local file is now detectable, and there is a second copy in a place the local
writer does not control. `verify_chain` is the operator-facing part: it names the line and the
reason, rather than reporting a boolean.

**What this does not do**, stated because a control whose limits are unstated gets trusted past
them:

- **Tail truncation is not detected by the chain.** Each record proves only that it follows its
  predecessor; nothing in the file asserts how long the file should be, so dropping records off the
  end leaves a shorter chain that still verifies. There is a test asserting exactly this, so it
  stays a known limitation rather than becoming a surprise. Comparing against the topic is what
  catches it.
- **A wholesale rewrite defeats the chain.** Anyone who can rewrite every line can rebuild a
  consistent one. Detecting an edit early in the file relies on the records after it - which is
  also why editing one record and fixing its own digest still fails, at the record after.
- **The topic's retention is the broker's default.** Making this a real retention guarantee needs
  the explicit topic provisioning already on the roadmap - deliberate retention, replication, and
  ACLs - rather than relying on auto-creation.
- **Neither mechanism is WORM.** Regulatory-grade retention means shipping these records to storage
  with an object lock, or to a SIEM, where the platform cannot delete its own history. Both sinks
  here are inside the trust boundary the platform runs in; that is a smaller claim than
  immutability and it is the one being made.

The general point is about the difference between a habit and a property. The old sink was written
carefully, only ever appended, and was described accurately in the design document - and none of
that survived contact with the question *"what stops this being edited?"* A control is something
that fails visibly when violated. If the answer to how a property is maintained is "because the
code only does it one way", the property is a convention, and conventions do not appear in evidence.
