# Documentation

| Document | Purpose | Audience |
|---|---|---|
| [executive-summary.md](executive-summary.md) | Two-page orientation: what the platform does, what was delivered, what is proven, what it does not do. **Start here.** | Anyone, technical or not |
| [system-design.md](system-design.md) | Authoritative design for the whole platform: context, components, event contract, control-plane internals, reliability, security, operations, verification. | Anyone needing to understand how the system works |
| [adr/](adr/) | One record per significant decision, in Context / Decision / Consequences form. | Anyone asking why something is the way it is |
| [ai-engineering-practice.md](ai-engineering-practice.md) | How this platform was built with AI assistance, and the evidence for what caught the mistakes. | Anyone assessing the engineering process rather than the system |
| [../README.md](../README.md) | How to run and use this service. | Operators and contributors |

## The split, and why it holds

The README says **how to run this**. The design document says **what the system is**. The ADRs say
**why it is that way**.

Keeping "why" out of the README is deliberate. Rationale written inline tends to accumulate as a
narrative of bugs found, which makes the operational instructions harder to use and buries the
reasoning where nobody looks for it. An ADR is a stable address for a decision.

## Architecture decision records

| ADR | Subject |
|---|---|
| [0001](adr/0001-keep-the-control-plane-domain-agnostic.md) | Keeping the control plane domain-agnostic |
| [0002](adr/0002-map-decision-identity-to-state-provenance.md) | Mapping decision identity to state provenance |
| [0003](adr/0003-a-failed-run-must-be-reported-never-lost.md) | A failed run must be reported, never lost |
| [0004](adr/0004-the-poll-loop-tolerates-transient-client-errors.md) | Poll-loop tolerance of transient client errors |
| [0005](adr/0005-single-threaded-worker-and-when-offsets-commit.md) | Single-threaded worker; offset commit timing |
| [0006](adr/0006-configure-the-committer-identity-on-a-cloned-workspace.md) | Committer identity on a cloned workspace |
| [0007](adr/0007-a-full-work-queue-is-backpressure-not-loss.md) | A full work queue is backpressure, not loss |

New ADRs are numbered sequentially and never edited once merged. A decision that is later reversed
gets a new ADR that supersedes the old one, so the reasoning at the time stays legible.
