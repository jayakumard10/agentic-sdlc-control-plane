# 0001 - Keep the control plane domain-agnostic

## Context

This repo is platform infrastructure. It orchestrates work against whichever tenant service
triggered a run, and nothing in it may encode what any particular tenant service does - otherwise
"any future microservice can plug in" stops being true the first time a second tenant appears.

The code it was ported from ran against exactly one application, and had absorbed that
application's vocabulary in four places:

- The Coder node's LLM prompt opened by stating the target was a specific kind of service, and
  named a specific web framework.
- The Codebase Reasoner's re-planning conflict check was hardcoded to look for one module name
  belonging to that application, and reported the conflict in that application's terms.
- The Architecture/Design node's greenfield summary asserted the target had a particular
  framework's application object and a particular data-access layer.
- Three captured fixture transcripts consisted entirely of that application's source code.

None of these were visible as coupling while there was only one target. All of them are coupling.

## Decision

The package contains no tenant concepts. Specifically:

**Prompts describe the workspace, not the domain.** The Coder node now tells the model to read
what is already in the working directory and infer framework, layout and conventions from it,
rather than asserting them. The model has filesystem access to the clone, so this is strictly
better grounded than a hardcoded claim that may be wrong.

**The re-planning conflict check becomes configuration.** Which module names count as
"functionality that already exists" is a property of the deployment, not of the platform.
`REPLANNING_CONFLICT_MARKERS` supplies them as a comma-separated list. With none set - the
default - the check is inert and the ambiguous path proceeds without re-planning. An
unconfigured deployment must not invent a conflict it has no basis to detect.

**The platform ships no fixtures.** Replay-mode fixtures are recordings of work done on one
specific service and cannot be anything else. `FIXTURES_DIR` is a mount point, empty unless an
operator supplies fixtures for the service they are targeting. This is the one decision here with
a visible cost, addressed below.

**Test data is generic too.** The test suite uses a synthetic stand-in service. Fixtures modelled
on a real tenant would put that tenant's concepts in this repo just as effectively as production
code would, and the graph mechanics under test - routing, gates, retry, rollback - do not depend
on what the generated code is for.

## Consequences

A run in the shipped image with no fixtures mounted reaches the Coder node and safe-stops with a
stated reason, rather than completing. This is a real reduction in out-of-the-box capability
against the ported behaviour, and it is the correct trade: the alternative is shipping one
tenant's source code inside the platform that is supposed to serve all of them. The safe-stop
path already existed for missing fixtures, so this is a defined terminal state with outcome
telemetry emitted, not a crash.

Live mode has the same shape of limitation for an unrelated reason - the image installs no
`claude` CLI - so both unavailable paths now fail with an explanation of what would be needed
rather than an errno.

The re-planning conflict check does nothing until configured. A deployment that wants the
ambiguous-scenario re-planning behaviour must opt in by naming the modules that constitute a
conflict for its own target.

Enforcement is a grep for tenant vocabulary across the repo, run as part of the release-readiness
check rather than trusted to review.
