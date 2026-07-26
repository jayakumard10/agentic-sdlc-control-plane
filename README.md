# agentic-sdlc-control-plane

LangGraph-based orchestrator for the `agentic-sdlc-*` platform. It consumes drift signals, clones
the repository of whichever service triggered the run, drives a governed multi-step
build/test/document workflow against that clone, and pauses at real human-in-the-loop gates before
continuing.

> **Designing or reviewing rather than running?** Start with
> [`docs/system-design.md`](docs/system-design.md) — the authoritative design for the whole
> platform. This README covers how to run and use this service specifically.

## Tech stack

- **Orchestration**: Python 3.12, LangGraph 1.2.9
- **State/validation**: Pydantic 2.10.4 (pinned by the shared `agentic-events` contract, not chosen
  independently)
- **Checkpointing**: langgraph-checkpoint-postgres 3.0.5 against its own `postgres:16-alpine`
- **Events**: kafka-python-ng 2.2.3, `agentic-events` (shared envelope contract)
- **Testing**: pytest 8.3.4, pytest-cov 7.1.0

## Architecture

```mermaid
flowchart LR
    subgraph platform["agentic-sdlc-* platform (domain-agnostic)"]
        EB["agentic-sdlc-eventbus<br/>Kafka broker + shared event contract"]
        MLOPS["agentic-sdlc-mlops<br/>drift detection"]
        CP["agentic-sdlc-control-plane<br/>(this repo)<br/>LangGraph orchestrator + human gates"]
    end

    TENANT["Tenant service<br/>(any app plugged into the platform —<br/>e.g. url-shortener-api)"]

    TENANT -- "request-telemetry events" --> EB
    EB -- "consume telemetry" --> MLOPS
    MLOPS -- "drift-detected events" --> EB
    EB -- "consume drift (pattern subscribe)" --> CP
    CP -- "clone-per-run, git ops<br/>(HTTPS + read-only PAT)" --> TENANT
    CP -- "gate-decision / run-outcome events" --> EB

    style TENANT fill:#f5f5f5,stroke-dasharray: 5 5
    style CP fill:#e8f0ff
```

`url-shortener-api` appears only as an illustrative example of a tenant service. This repo contains
no reference to it or to any other specific service — see `docs/adr/0001`.

### Internal structure

Two Kafka consumers and one worker. Neither consumer ever executes a run:

```mermaid
flowchart LR
    T["trigger consumer<br/>pattern: *.drift-detected.v{n}"] -- "validate + enqueue" --> Q(["work queue"])
    D["decision consumer<br/>control-plane.gate-decision.v1"] -- "validate + enqueue" --> Q
    Q --> W["worker (single thread)<br/>owns the checkpointer"]
    W -- "clone / run / resume" --> G["LangGraph"]
    G -- "parks at a gate" --> PG[("Postgres<br/>checkpoints")]
    W -- "run-outcome / DLQ" --> K["Kafka"]
    S["TTL sweep"] --> W
```

A gate can park a run for as long as a human takes to answer, so a poll loop must never wait on
one — it would exceed `max.poll.interval.ms` and trigger a rebalance that takes every other
in-flight run on the partition with it. The poll loops therefore only validate and enqueue; the
worker executes. Parked state lives entirely in Postgres, so a run can be resumed by a different
process from the one that started it. See `docs/adr/0005`.

### The graph

```mermaid
flowchart TD
    START(["run starts<br/>(drift event consumed,<br/>clone into /workspaces/run_id)"]) --> RC["requirement_clarifier"]
    RC --> ROUTE{"scenario_type"}
    ROUTE -- "brownfield / ambiguous" --> CR["codebase_reasoner"]
    ROUTE -- "greenfield: skip" --> AD["architecture_design"]
    CR --> AD
    AD --> DP["decomposer_planner"]
    DP --> CODE["coder"]
    CODE --> PT["test_executor"]
    CODE --> PD["documentation"]
    PT --> SYNC["sync<br/>(parallel join barrier)"]
    PD --> SYNC
    SYNC --> GATE{{"release_gate"}}
    GATE -- "approved" --> DONE(["terminal: completed"])
    GATE -- "rejected / test failure" --> REPLAN["replanner<br/>(bounded retry, then fallback)"]
    REPLAN -- "retry within bound" --> DP
    REPLAN -- "bound exhausted" --> ROLLBACK["rollback"]
    ROLLBACK --> SAFE(["terminal: safe_stop"])
```

Nine nodes plus two helpers (`sync`, `rollback`). Greenfield runs skip `codebase_reasoner`;
`test_executor` and `documentation` fan out in parallel and rejoin at `sync`.

### Gates

Five gate types exist in `GateType`, and which of them fire depends on `scenario_type`:
`clarification_approval` and `plan_approval` on greenfield, `codebase_impact_review` on brownfield,
`replanning_approval` when a re-planning conflict is detected, and `merge_release_approval` always.
Every gate that fires blocks, using a real LangGraph `interrupt()` backed by this repo's Postgres
checkpointer. A parked run resumes when a decision arrives on
`control-plane.gate-decision.v1`, correlated by `thread_id == run_id`.

Every run ends in exactly one reported terminal state — `completed`, `failed`, `safe_stop`,
`clone_failed`, or `stale` — published to `control-plane.run-outcome.v1`.

## Quickstart

Requires a running `agentic-sdlc-eventbus` broker.

```bash
cp secrets/postgres_password.txt.example secrets/postgres_password.txt
```

```bash
cp secrets/github_pat.txt.example secrets/github_pat.txt
```

Edit both: a password of your choosing, and a fine-grained read-only GitHub PAT with access to the
repositories this service will clone and to `agentic-sdlc-eventbus` (a private dependency needed at
build time). Then:

```bash
docker compose up -d --build
```

```bash
docker logs -f agentic-sdlc-control-plane-consumer
```

Expect `Subscribed to pattern .*\.drift-detected\.v[0-9]+`. A newly created drift topic is
discovered within about 30 seconds, governed by `metadata.max.age.ms`.

## Local development

```bash
python -m venv .venv && .venv/bin/pip install -r requirements.txt
```

Run against the compose Postgres with the broker reachable on the host:

```bash
POSTGRES_HOST=localhost POSTGRES_PORT=5433 KAFKA_BOOTSTRAP_SERVERS=localhost:9092 python -m agentic_control_plane.main
```

Configuration:

| Variable | Default | Purpose |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | *(required)* | Event bus. Use port 9093 from another container, 9092 from the host. |
| `POSTGRES_HOST` / `PORT` / `DB` / `USER` | `postgres` / `5432` / `control_plane` / `control_plane` | Checkpoint store |
| `POSTGRES_PASSWORD_FILE` | — | Path to the password file. `POSTGRES_PASSWORD` also works but is visible in `docker inspect`. |
| `GIT_PAT_FILE` | — | Path to the PAT used to clone target repositories |
| `WORKSPACES_ROOT` | `/workspaces` | Where per-run clones live |
| `FIXTURES_DIR` | `/fixtures` | Replay-mode fixtures. Empty unless you mount your own — see below. |
| `ORCHESTRATOR_MODE` | `replay` | `live` requires a `claude` CLI this image does not install |
| `PARKED_RUN_TTL_HOURS` | `24` | After this, a parked run is reported `stale` and cleaned up |
| `REPLANNING_CONFLICT_MARKERS` | *(empty)* | Comma-separated module names that count as an existing-functionality conflict |
| `AUDIT_LOG_PATH` | `/workspaces/.audit/runs.jsonl` | Append-only audit trail of every node execution and gate decision |

### Code generation modes

Neither generation mode works out of the box, by design rather than omission:

- **Replay** needs fixtures, which are recordings of work on one specific service. A domain-agnostic
  platform cannot ship them, so `FIXTURES_DIR` is an empty mount point. Mount fixtures shaped as
  `{scenario_type}/transcript.json` to enable it.
- **Live** needs the `claude` CLI on `PATH` and an authenticated session, neither of which this
  image provides. It is an opt-in built on a customised image.

Without either, a run reaches the coder node and safe-stops with a stated reason, ending in a
defined terminal state with outcome telemetry. See `docs/adr/0001`.

## Testing

```bash
pytest --cov=agentic_control_plane --cov-report=term-missing
```

205 tests, 92% statement coverage. The durability tests need a reachable Postgres and skip without
one; run them against the compose Postgres:

```bash
POSTGRES_HOST=localhost POSTGRES_PORT=5433 POSTGRES_USER=control_plane POSTGRES_DB=control_plane pytest
```

Coverage gaps are concentrated in code that needs live infrastructure to exercise meaningfully:
real `KafkaProducer`/`KafkaConsumer` construction (`events.py`, `main.py`) and the live `claude`
CLI paths (`coder.py`). Those are covered by functional verification instead.

### Functional verification

Run end-to-end against a real broker and a real Postgres, from inside the container:

| Check | Result |
|---|---|
| Pattern subscription discovers a topic created after startup | PASS — run began ~31s after publish |
| Container consumes a drift event through the event bus's cross-container listener | PASS |
| Container publishes its outcome event through the same listener | PASS — consumed back off the topic, `producer.instance_id` matching the container hostname |
| Private-repo HTTPS clone using the mounted PAT | PASS — cloned at commit `335472aa` |
| A PAT lacking access fails cleanly | PASS — 403 became a `clone_failed` outcome, no crash |
| Run parks at a real `interrupt()` without blocking the poll loop | PASS |
| Kafka gate decision resumes the parked run | PASS |
| `test_executor` runs a real pytest subprocess against the clone | PASS — 1075ms |
| Full completion in-container | PASS — commit `9224abcd`, `completed`, workspace removed |
| Unmounted fixtures safe-stop with a stated reason | PASS |
| Workspace deleted on terminal state | PASS |
| Parked run resumed by a *different* process after the original was killed | PASS |
| Startup reconciliation removes orphaned workspaces | PASS |

Memory, sampled across a full run including the pytest subprocess: **69.6 MiB against the 1 GiB
limit**, flat throughout; the Postgres container sits at 36 MiB against 512 MiB.

Five defects were found by these runs and by nothing else, with a full unit suite passing
throughout all of them. They are written up in `docs/adr/0002` through `0006`.

## Deployment / CI

`.github/workflows/ci.yml` runs on every push and pull request to `main`.

The test job runs the suite against a real `postgres:16-alpine` service container rather than
skipping the durability tests — those tests are the reason a durable checkpointer was chosen over
an in-memory one, so skipping them would leave the property that matters unverified. It also
asserts that no tenant-specific vocabulary has entered the package or its tests.

The compose job builds the image and asserts the build credential did not persist into it, by
reading `/root/.gitconfig`'s size rather than trusting the layer that unsets it. It does not run
the consumer: this repo is independently clonable with no sibling checkout, so there is no broker
in CI to point it at.

Both jobs need a repository secret named `EVENTBUS_READ_PAT` — a fine-grained, read-only PAT scoped
to `agentic-sdlc-eventbus`, which hosts the private `agentic-events` dependency.
