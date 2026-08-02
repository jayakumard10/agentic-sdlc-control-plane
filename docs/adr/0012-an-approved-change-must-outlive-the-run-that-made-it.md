# 0012 - An approved change must outlive the run that made it

## Context

A completed run cloned a repository, generated code, ran a real pytest against it, scanned it for
guardrail violations, took it through a human `merge_release_approval` gate, and committed it. Then
the worker called `workspace.cleanup(run_id)` and deleted the clone.

The commit existed for the length of one run. Nothing pushed it, and the outcome event reported
`git_target.commit_sha = commit_sha_before` — the revision the run *started* from. The SHA the run
produced was never recorded anywhere except an audit line reading `committed 9637e763`, in a trail
whose own defect (ADR 0011) meant it was sometimes not written at all.

So the platform governed a change and then destroyed it. Every property this system claims —
guardrail scanning, human approval, an attributed audit trail — described work that reached no
branch, no pull request and no repository. A reviewer could reasonably ask what the platform is
*for*, and the honest answer was "deciding whether a change should be released, and then discarding
it either way".

This was not a known limitation. It was not in the roadmap, and no ADR recorded it as a trade.

## Decision

**An approved change is delivered to the repository it came from, and the run reports what it
produced.**

**`commit_sha_after` is recorded in `GraphState` and published on the outcome event.** The release
gate sets it; the worker reads it from the run's final state. It rides in the envelope's free-form
`payload`, not in `git_target.commit_sha`, which keeps its contract meaning — the revision the run
began at — for every consumer already reading it. Reporting what a run produced does not version
the shared contract every repository installs.

**Delivery is the worker's job, not the graph's.** The node decides whether to release; the worker
knows the remote, and holds `repo_url` and `branch` on `_RunTarget` already. Putting a push inside a
graph node would give a domain-agnostic node an I/O dependency on deployment configuration. The
worker delivers between the terminal state and `workspace.cleanup`, which is the only window in
which the commit still exists on disk.

**Publishing is opt-in through `PUBLISH_MODE`, defaulting to `none`.** Three modes:

| Mode | Behaviour |
|---|---|
| `none` (default) | Commit and report. The prior behaviour, and still correct for an evaluation deployment |
| `branch` | Push `agentic-patch/{run_id}`. Host-agnostic |
| `pull_request` | Push the branch, then open a request against the branch the run cloned. GitHub only |

An unrecognised value is treated as `none` and logged. A typo in a deployment variable must not
push, and must not fail a run that a human already approved.

**A delivery failure is reported, not fatal.** The change was generated, tested and approved; those
facts hold whether or not the push landed. The run still reaches `completed`, and the outcome event
carries `published: false` with the reason. It must be loud, because a failed push means the change
really was discarded — which is the thing this ADR exists to prevent — but converting it into a
`failed` run would misreport what the gate decided.

## Consequences

**The credential posture changes when, and only when, publishing is enabled.** Everything else here
runs on a read-only PAT: it clones, and that is all the token can do. Pushing needs write access to
the tenant's repository, widening what a compromised control plane can reach from *read the
tenant's code* to *write branches in the tenant's repository*. That is why the default is `none` —
a deployment that wants governance without delivery keeps the smaller credential, and the larger one
is a decision someone makes deliberately rather than inherits. §9 of the design document records
both postures.

**The run never writes to the branch it cloned.** The push names `HEAD:refs/heads/agentic-patch/{run_id}`
explicitly. Nothing in this path can update the tenant's integration branch, and `pull_request` mode
opens a request against the branch the run cloned rather than a hardcoded `main` — the platform does
not get to choose a tenant's integration branch.

**`pull_request` is GitHub-specific and degrades rather than fails.** Opening a request is an API
call with no cross-host equivalent. On a non-GitHub remote the branch is still pushed and the reason
is reported, so the work survives and a human can open the request by hand. That is a partial
success and is reported as one: `published: true` with an `publish_error` alongside it.

**What this does not do.** There is no retry of a failed push — the next redelivery of the same
trigger is a no-op, because the run is already in the checkpointer, so a failed delivery is
recovered by a human reading the outcome event rather than by the platform. There is no cleanup of
`agentic-patch/*` branches; they accumulate until someone prunes them. Neither is worth building
before a second tenant exists to show which one actually hurts.

**Verified against a real origin rather than a mock.** The tests push to an ordinary git repository
on disk and then ask *origin* which refs it has, because the claim is that the commit exists
somewhere the workspace's deletion cannot reach — and only the remote can confirm that. One test
pushes, deletes the workspace, and asserts the branch is still there.
