# 0006 - Configure the committer identity on a cloned workspace

## Context

No run could reach `completed` in the shipped container image. The release gate, on approval,
commits the generated code and documentation into the run's workspace, and that commit failed:

```
git commit -m "[brownfield] ..." failed in /workspaces/ctr-run-0004: Author identity unknown
```

The git helpers set a committer identity inside `git_init_if_needed`, on the branch that actually
runs `git init`. That was correct for the model they were written for, where a workspace was
created by copying files into an empty directory and initialising a repository in it. Under
clone-per-run the workspace arrives from `git clone` with `.git` already present, so
`git_init_if_needed` correctly does nothing - and nothing else ever configures an identity.

It went unnoticed through every earlier test because they all ran on a developer machine, where a
global git identity exists and git silently falls back to it. The container has no global identity,
which is the environment that actually matters. The unit suite could not have caught it either: it
ran in the same machine's environment.

The failure did surface cleanly, which is worth noting - the run was reported as `failed` with the
real error text and its workspace cleaned up, rather than vanishing, because of the change in
`docs/adr/0003`.

## Decision

`workspace.clone_for_run` configures `user.name` and `user.email` on the clone immediately after
acquiring it, overridable via `GIT_COMMITTER_NAME` and `GIT_COMMITTER_EMAIL`.

**In `workspace.py`, not `tools.py`.** Acquiring a workspace is what this module does, and a
workspace that cannot be committed to is not fully acquired. Putting it here also preserves a
property worth keeping: `tools.py` was carried over from the source system unchanged, which is a
requirement of the workspace design rather than a nicety, and this repo can still say that
truthfully.

**Per-repository, not global.** The workspace is what needs the identity. A global setting would
apply to every run in the container and to anything else git touches, for no benefit.

The test asserts that a commit into a cloned workspace actually succeeds, rather than that two
config keys are present. The former is what the release gate needs; the latter is a proxy that
could pass while the real operation still failed.

## Consequences

Commits made by a run are attributed to the control plane rather than to a person, which is
correct - a run is not a person, and the human who approved the gate is recorded separately in the
audit trail via `decided_by_identity`.

The default email uses a `.local` suffix and does not resolve to a real address. That is
deliberate: an identity that looks like a real mailbox but is not is worse than one that is
obviously synthetic. Deployments wanting attribution to a service account can set both variables.

The broader point is about where behaviour was verified, not about git. Every test of the commit
path had run in an environment that supplied a missing piece for free. Anything that reads ambient
machine configuration - git identity, credential helpers, locale, `PATH` - needs at least one
exercise in the environment that has none of it, and for this platform that means the container,
not the developer machine.
