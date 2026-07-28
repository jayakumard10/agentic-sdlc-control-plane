# 0009 - The audit trail does not live in the workspaces root

## Context

`AUDIT_LOG_PATH` defaulted to `/workspaces/.audit/runs.jsonl`. `WORKSPACES_ROOT` is `/workspaces`.
Startup reconciliation enumerates every directory under that root, asks the checkpointer whether
each is a resumable run, and deletes the ones that are not.

`.audit` is a directory under that root. It is not a run, so it is never resumable, so it was
deleted - on every single startup. The container's own log said so plainly:

```
WARNING  Reconciling orphaned workspace for run .audit
INFO     Removed workspace for run .audit
WARNING  Reconciled 1 orphaned workspace(s): .audit
```

Observed directly: an audit file holding 17 records before a restart held 2 after it, the 2 being
records from the run that happened next.

The audit trail had therefore never survived a restart, in a service whose restartability is a
stated design property - `restart: unless-stopped` in compose, and a parked run explicitly able to
be resumed by a different process. The one artefact that exists to prove what the system did was
being destroyed by the routine that cleans up after the system doing it.

This was found while verifying ADR 0008 against a real container. Every test of reconciliation
passed throughout, because they all construct a workspaces root containing only workspaces. Nothing
in the suite ever put a *non-workspace* in that directory, so nothing ever asked what happens to
one - and the production default put one there on every deployment.

## Decision

**The audit trail moves out of the workspaces root**, to `/var/audit/runs.jsonl` on its own Docker
volume. Two concerns were sharing a directory with different lifecycles: run workspaces are
ephemeral by design and deleted aggressively, the audit trail is the opposite of ephemeral. Nothing
made them siblings except a default path.

**Reconciliation ignores dot-directories.** This is the second half rather than the whole fix: the
first change means nothing is in that root to protect today, and this one means the next thing to
appear there is not silently swept. `existing_run_ids` now says what it returns - run workspaces,
not "every directory".

## Consequences

The audit trail survives restarts.

An existing deployment carrying the old default keeps it, because `AUDIT_LOG_PATH` is still
honoured; it also keeps the behaviour, since a path inside the workspaces root is still inside it.
The compose file supplies the new location, and the dot-directory filter means even the old default
now survives. Anyone running with a custom `AUDIT_LOG_PATH` under the workspaces root and no
leading dot should move it.

The general point is about what a test fixture quietly asserts. Every reconciliation test built a
root containing only run workspaces, so the suite encoded an assumption - *this directory contains
nothing else* - that the shipped configuration contradicted. The tests were not wrong about what
they tested. They were wrong about what the directory would contain, and no test can catch an
assumption it shares with the code.

It also lands in the same place as ADR 0006, from a different direction: behaviour that depends on
what is in an environment cannot be verified in an environment someone constructed to contain only
what the test needed.
