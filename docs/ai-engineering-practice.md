# AI-Assisted Engineering Practice

**Status:** Current · **Last verified:** 2026-07-28 · **Scope:** all four platform repositories

This platform was built with AI assistance throughout. That is worth stating plainly, and it
raises an obvious question: *what stopped AI-generated mistakes from reaching the final
implementation?*

This document answers it with evidence rather than assertion. The short version: **a passing test
suite was never treated as proof.** Every repository was exercised against running infrastructure
before any claim about it was written down, and that practice — not the test suite — is what caught
the defects.

---

## 1. How AI assistance was applied

Three roles, applied per repository. Each is a committed agent definition, not a description of
one — the standing instructions are in [`CLAUDE.md`](../CLAUDE.md) and the roles that act on them
are in [`.claude/agents/`](../.claude/agents/), so the process that produced this repository can
be re-run by anyone who clones it:

| Role | Definition | What it produced | Where that lives |
|---|---|---|---|
| **Design** | [`design.md`](../.claude/agents/design.md) | Architecture, boundaries, event contract, diagrams | [`system-design.md`](system-design.md), [`adr/`](adr/) |
| **Development** | [`development.md`](../.claude/agents/development.md) | Implementation, error handling, logging, audit sink, commit history | Source, plus 71 commits across four repositories |
| **QA** | [`qa.md`](../.claude/agents/qa.md) | Unit tests with coverage, and functional verification for anything a unit test cannot reach | Test suites, plus [§11](system-design.md#11-verification) of the design doc |

The rules in those three files are not aspirational. Each one is traceable to an ADR in §3 below:
the agent encodes the rule, and the ADR records the defect that produced it.

The division that matters is the third one: **unit tests and functional verification are treated as
answering different questions, and neither is allowed to stand in for the other.**

---

## 2. The verification standard

Two rules, applied to every repository:

> **A doc claim not backed by a command actually run against real containers or code is a bug, not
> documentation.**

> **A workflow file is not evidence; a passing run is.**

Both are recorded in the repositories' contributor guidance and were applied retroactively — CI was
at one point reported as delivered on two repositories where it had never once succeeded, and that
correction is itself recorded in the design document rather than quietly fixed.

The practical consequence is that every table of results in this platform's documentation was
produced by running the thing, not by reasoning about it.

---

## 3. The measured result

**Fifteen of the platform's nineteen ADRs document defects found only by running real
infrastructure, or by reading the code path an external reviewer pointed at.** The other four
record design decisions. Every one of the fifteen shares a property: a green unit suite was
passing at the time, and no reasonable addition to it would have caught the defect.

| ADR | Repository | Defect | Found by |
|---|---|---|---|
| [0002](adr/0002-map-decision-identity-to-state-provenance.md) | control-plane | Every gate decision from Kafka was rejected — `decided_by` means an identity on the wire and a provenance in state | A real event produced by a separate process against a live broker |
| [0003](adr/0003-a-failed-run-must-be-reported-never-lost.md) | control-plane | A run that failed mid-execution vanished: neither resumable nor terminal, no outcome published, workspace reclaimed | End-to-end run against a live broker and checkpointer |
| [0004](adr/0004-the-poll-loop-tolerates-transient-client-errors.md) | control-plane | One bad file descriptor inside the Kafka client's selector terminated the whole service | A real run left running long enough to hit it |
| [0006](adr/0006-configure-the-committer-identity-on-a-cloned-workspace.md) | control-plane | No run could reach `completed` in the container — `Author identity unknown` at the release gate | Running in the container, which has no ambient git identity |
| [0007](adr/0007-a-full-work-queue-is-backpressure-not-loss.md) | control-plane | A full work queue dropped the trigger *and* committed past it — no run, no redelivery, no outcome event | An external reviewer pointing at the line; the loss path was worse than the one already documented |
| [0008](adr/0008-the-audit-trail-must-be-checkable-not-merely-appended.md) | control-plane | The audit trail was "append-only by convention" — editable by anything holding the volume, with no way to tell | An external reviewer asking what stopped it being edited |
| [0009](adr/0009-the-audit-trail-does-not-live-in-the-workspaces-root.md) | control-plane | Startup reconciliation deleted the audit trail on **every restart** — it defaulted to a path inside the workspaces root it sweeps | Restarting the real container while verifying ADR 0008 |
| `0001` | mlops | DuckDB rejects a second connection to the same file from the same process, contradicting the design assumption | The real container against a live broker |
| `0002` | mlops | Background drift thread died with a `TypeError` on every start; the container still reported healthy | Inspecting real container logs |
| `0003` | mlops | `mem_limit` was a plausible round number, not a measurement; the container crash-looped on OOM | `docker stats` at increasing ceilings |
| `0005` | mlops | MLflow answered every request from its only client with `403 Invalid Host header`; it had never once been reached | The first real tracking call ever made from the consumer |
| `0006` | mlops | Every drift detection minted a random `correlation_id`, so an unresolved regression started a new governed change every 60s — and the control plane's deduplication, which depends on that id, never applied | Building the seeded end-to-end test for the seam between the two repos |
| `0007` | mlops | Pattern subscription skipped every event published before it discovered a topic — `auto_offset_reset` was unset, so the client default positioned at the end and committed past the data | Comparing rows written against events published, after a wrong hypothesis was ruled out |
| `0001` | eventbus | Cross-container traffic silently failed — bootstrap succeeded, then the client was told to reconnect to `localhost` | Container-to-container traffic, in both directions |
| `0001` | url-shortener | `KafkaProducer` construction blocked every HTTP request indefinitely when the broker was down | Running the real stack with the broker deliberately stopped |

Two further defects were found the same way and fixed without a dedicated ADR: outcome events
carried a null `commit_sha` because the value was captured at clone time but read on a later slice,
and CI had never passed on two repositories because a required secret was unset.

### Why the test suite could not have caught these

They are not fifteen instances of one mistake. They fall into distinct classes, and the taxonomy
is the transferable part:

| Class | Example |
|---|---|
| **The test spoke the vocabulary of the thing it tested** | Tests constructed their own gate decisions, so only ever used values the model already accepted. A real username never went through the path. |
| **Ambient machine configuration supplied a missing piece for free** | Every test of the commit path ran where a global git identity existed. The container has none. |
| **The code path was never exercised at all** | Unit tests never set `KAFKA_BOOTSTRAP_SERVERS`, so producer construction was never run. |
| **Success at the boundary, failure in the traffic** | Kafka bootstrap succeeded and every subsequent send failed, because a listener advertises a reconnect address. |
| **A library's real concurrency model differed from its documented one** | DuckDB rejected a second connection the design assumed was permitted. |
| **The failure did not propagate** | A thread died on construction; threads do not raise into their caller, so the container looked healthy. |
| **A supertype was caught and relabelled** | `KeyError`/`ValueError` were caught as benign preconditions, swallowing the same builtins raised inside nodes. |
| **A resource limit was guessed** | `mem_limit` was a round number rather than a measurement. |
| **Only long-running execution reached it** | A socket lifecycle error inside the client's event loop. |
| **The contract was tested; the call site was not** | `submit()` was written, documented, and tested to report a full queue. A passing test asserted it returned `False`. Nothing tested what the caller did with that answer — it discarded it and committed anyway. |
| **The fixture shared an assumption with the code** | Every reconciliation test built a workspaces root containing only workspaces, so the suite encoded *this directory contains nothing else*. The shipped default put the audit trail there, and reconciliation deleted it on every restart. No test can catch an assumption it shares with the code. |
| **A habit mistaken for a property** | The audit sink only ever appended, was documented as append-only, and was accurately described. None of that survived the question *"what stops this being edited?"* |
| **A contract stated in prose, in the other repository** | The control plane documented that `correlation_id` was derived from the drift condition and built deduplication on it. mlops generated a random one. The shared envelope enforces that the field is a *string* and nothing more, so both repos were internally consistent and self-consistently wrong. |
| **Every external signal agreed, and none of them measured the thing** | Consumer offsets advanced, lag was zero, logs were clean — and the telemetry had been skipped, not processed. A consumer that skips messages is indistinguishable from one that processed them, from the outside. Only comparing *rows written* against *events published* could tell. |

Every one of these is invisible to a unit test *by construction*, not by oversight. That is the
argument for the practice, and it is why coverage percentage is reported alongside functional
verification rather than instead of it.

---

## 4. What this means in practice

The rules the nine defects produced, each now applied platform-wide:

1. **Where a boundary translates between two schemas, at least one test asserts against the
   destination model** — not against what the parser happens to emit (ADR 0002).
2. **Anything reading ambient machine configuration** — git identity, credential helpers, locale,
   `PATH` — **gets at least one exercise in the environment that has none of it**, which means the
   container, not the developer machine (ADR 0006).
3. **A precondition worth distinguishing gets its own exception type.** `ValueError` is too common
   a type to use as a signal (ADR 0003).
4. **Resource limits are measurements**, verified with `docker stats`, the same way dependency pins
   are verified against the registry rather than guessed (mlops ADR 0003).
5. **Cross-repository integration points are verified from the position the calling component
   actually occupies** — a repository's own tests passing proves nothing about the seam
   (eventbus ADR 0001).

6. **Where a function reports a condition its caller must act on, a test asserts what the caller
   does** — not only that the function reports it correctly (ADR 0007). A verified contract says
   nothing about a call site honouring it.

Rule 5 is the one with the widest reach, and the platform's remaining known gap sits against it:
the drift-detection path is exercised from a published event onward, but the producing side has not
yet been driven from seeded telemetry through to a published `drift-detected` event. That is stated
in the roadmap rather than left for a reviewer to discover.

---

## 5. Limits of this practice

Stated so the claim is not read as broader than it is:

- **It does not verify correctness of generated code.** It verifies that the platform behaves as
  documented. Code produced *by* a run is governed separately — guardrail scanning at the release
  gate, and a human approval that blocks by default.
- **It found defects; it does not prove absence of defects.** Nine is the count found, not the
  count that exists.
- **The methodology is manual.** Functional verification is a run and a recorded result table, not
  an automated harness. Automating it — a seeded end-to-end integration test in CI — is a roadmap
  item, and until it exists these results are reproducible by following the documented steps rather
  than by pressing a button.
- **A reviewer needs repository access to check any of this.** The evidence is the commit history,
  the ADRs, and the CI runs. A filesystem copy of the working tree shows none of them.

---

## 6. The loop

The practice reduces to one cycle, run continuously rather than as a final phase:

```
run it against real infrastructure
  → observe a defect the suite did not catch
    → fix it
      → add the test that would now catch it
        → write the ADR explaining why it happened
          → update the design document in the same change
```

Every ADR listed in §3 is an instance of that loop. The loop is still running, and its most recent
pass is the clearest illustration in this document of why the practice exists.

Asked whether an apparently unused MLflow deployment was worth its memory cost, the answer was
found by inspecting the running container rather than by reasoning about it: an async job subsystem
this platform never invokes, accounting for 2 GiB of a 2.86 GiB platform (mlops ADR 0004). Removing
it cut the platform to 1.03 GiB.

Then, verifying that tracking still worked — from the consumer container, not from the host — the
first real tracking call the platform had *ever* made returned `403 Invalid Host header`. MLflow had
been healthy and unreachable by its only client since the day it was deployed, and nothing had ever
made a request to find out (mlops ADR 0005). Had the tracking integration been written first, every
drift evaluation would have failed at runtime.

Both were found in the same pass, by running the thing and looking, against a platform whose entire
test suite was green.
