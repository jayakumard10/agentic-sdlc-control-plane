# AGENTS.md

How to work on this repository with an AI coding agent, and how to check that the agent is
actually bound by this repository's rules rather than its own defaults.

| File | Holds |
|---|---|
| [`CLAUDE.md`](CLAUDE.md) | Standing instructions loaded into every session: commit discipline, documentation standard, this repo's place in the platform |
| [`.claude/agents/design.md`](.claude/agents/design.md) | Maintains the design record. Read-only over the source tree by design |
| [`.claude/agents/development.md`](.claude/agents/development.md) | Implements changes. Encodes the error-handling, logging, audit and secret-handling rules |
| [`.claude/agents/qa.md`](.claude/agents/qa.md) | Tests, coverage, and functional verification for what a unit test cannot reach |

Every rule in those three files is traceable to an ADR in [`docs/adr/`](docs/adr/). The agent
encodes the rule; the ADR records the defect that produced it. They are not aspirational style
guides — each one exists because its absence broke something that is written down.

## Provenance — read this before trusting the rest

**These definitions were written on 2026-07-31, after most of the implementation they describe.**
`git log -- .claude/` will show you that, and it should. The platform was built through
interactive sessions against the standing instructions in `CLAUDE.md`; there was never a stored
prompt chain, and reconstructing one after the fact would be manufacturing an audit trail that
did not exist. That is the specific failure
[`docs/ai-engineering-practice.md`](docs/ai-engineering-practice.md) is written against.

So the honest claim is narrow and worth stating plainly:

- **What is reproducible:** the rules, and the workflow below. Clone the repo and an agent picks
  up the same constraints the work was done under.
- **What is not:** the original sessions. They are gone, and no file here pretends otherwise.

The commit history is the evidence of the practice; these files are the evidence of the rules.

## Invoking an agent

Claude Code resolves `.claude/agents/` from the repository root, so run from the repo:

```bash
claude --agent qa -p "your task here"
```

Interactively, `claude` then selecting the agent works the same way. Other agent tools that read
`AGENTS.md` or `CLAUDE.md` will pick up the standing instructions but not the three role
definitions, which are Claude Code's format.

### Check the binding actually worked

Before trusting an agent with a change, confirm it is running under this repo's rules and not
generic defaults. Ask it for its governing rule:

```bash
claude --agent qa -p "In one sentence: what is the governing rule you operate under? Do not read any files."
```

Verified 2026-08-01, this returns the rule from `qa.md` rather than a generic answer about testing:

> A passing test suite is never treated as proof — unit tests measure whether code does what it
> says in isolation (reported as coverage), while functional verification measures whether the
> deployed thing behaves as documented (reported as a results table with the command that produced
> it), and the two are never substituted for each other.

If you get a generic answer about writing good tests, the agent did not load and nothing below
applies.

## Worked example — deciding whether a change needs an ADR

The `design` agent's rule is that an ADR is warranted when a decision carries a cost someone could
reasonably dispute — not for every change, because an ADR nobody would have argued with is noise.
Asking it to apply that rule to a real decision:

```bash
claude --agent design -p "Applying only this repo's ADR rule, answer in under 60 words with a yes/no first: does 'pin the agentic-events dependency to an immutable commit SHA instead of the mutable v0.1.0 tag' warrant an ADR?"
```

Verified 2026-08-01, output:

> **Yes.** This is a decision with a disputable cost: SHA pinning trades away automatic tag-based
> updates and readable version references for reproducibility/supply-chain safety against a mutable
> tag. Someone could reasonably prefer the tag; the tradeoff and rationale belong in a numbered ADR,
> linked from system-design.md.

That is the repo's rule applied, not a generic opinion: it names the cost, identifies who could
disagree, and points at where the ADR must be linked from — all of which are `design.md`'s
instructions rather than the model's defaults.

## Which agent for which task

| Task | Agent | Why |
|---|---|---|
| Add a node to the LangGraph workflow | `development`, then `qa` | The node is implementation; the routing edge it adds is the thing node-level tests cannot check, so `qa` drives it through the compiled graph |
| Change a boundary, contract or failure mode | `design` first | It decides whether an ADR is owed and updates `system-design.md` in the same change |
| Change error handling or the audit sink | `development` | Carries the rules from ADRs 0003, 0007, 0008 and 0011 |
| Add tests after a defect | `qa` | Its rules are the five that came from real defects, including testing what the *caller* does |
| Change resource limits or dependency pins | `design` | Both are measurements in this repo, not judgement calls |

The order matters for anything that changes behaviour: `design` decides whether the design record
moves, `development` implements, `qa` establishes what the suite can and cannot prove. Skipping
`design` is how a change lands with a stale design document, which this repo treats as a bug
rather than a follow-up.

## What these agents do not do

- **They do not replace the gates.** Code generated *by* a platform run is governed separately —
  guardrail scanning at the release gate, and a human approval that blocks by default.
- **They do not verify their own output.** A green suite is the start of verification here, not the
  end. Anything touching live infrastructure gets exercised against it and recorded in a results
  table with the command that produced it.
- **They carry no credentials.** The platform's PAT is read-only and reaches git through a
  credential helper at request time; no agent needs or receives it.
