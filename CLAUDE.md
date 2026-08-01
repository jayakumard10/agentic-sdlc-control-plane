# CLAUDE.md

Standing instructions for AI agents (and human contributors) working in this repository. Claude
Code auto-loads this file every session; it is committed so the process that produced this
repository is reproducible by anyone who clones it, not just on the machine that ran it.

The three roles this file refers to are defined as executable agents in
[`.claude/agents/`](.claude/agents/).

## Commit discipline

- Build one small piece at a time: write it, test it against something real (not just "looks
  right"), and only once it passes, commit it. Never batch a large, untested pile of files into
  one commit at the end — that's exactly the failure mode this rule exists to prevent.
- Each commit should be small enough to describe honestly in its own message — if the message
  needs "and" three times, it's probably more than one commit.
- Author: Jayakumar Devaraj <jayakumar.d10@gmail.com>. Never add Co-Authored-By or "Generated
  with" trailers/footers of any kind.
- Fresh `git init` per repo, no monolith history preserved.
- **Push after every commit.** Decided 2026-07-26, after 14 commits sat unpushed for a whole
  build and GitHub showed none of the work. These repos are reviewed via GitHub only, so an
  unpushed commit is invisible. Do not wait to be asked, and do not accumulate a backlog because
  a brief said "ask before pushing" — ask once, early, then keep the remote current.

## Self-check before continuing

Periodically ask: **"Would someone looking only at GitHub right now see what I have actually
done?"** If not — uncommitted work, or committed-but-unpushed work — stop and fix that before
writing anything new. Committing locally is necessary but not sufficient; the audit trail is the
remote one. This applies to every repo in this platform, checked regularly, not just at the end
of a session.

## Keep documentation in sync

Whenever a plan or implementation changes — a design decision gets revised, a bug fix changes
behavior, a dependency pin changes — update the relevant documentation (README, this file, the
platform's planning document) in the same change, not as a follow-up. Stale docs are a bug, not
a TODO.

## Documentation standard

- README.md section order, fixed: Tech stack -> Architecture -> Quickstart -> Local development
  -> Testing -> Deployment/CI. Nothing else. README explains how to run/use this repo, never why
  a decision was made (that's ADRs, `docs/adr/`) or how this repo relates to the other repos in
  this platform split (tracked in a private planning document, not committed anywhere).
- Never reference local machine paths (`C:\Users\...`, `C:\srcCode\...`) in committed files.
- Every significant design decision gets a lightweight ADR: `docs/adr/NNNN-title.md` (Context /
  Decision / Consequences).
- A doc claim not backed by a command actually run against real containers/code is a bug, not
  documentation — verify before writing, not after.

## AI-assisted engineering practice

This platform demonstrates AI-assisted engineering across three roles per repo, scoped to what
actually applies:

1. **Design**: a design document and architecture diagram for this repo (README + this file).
2. **Development**: error handling and logging, auditing capabilities (this repo's own concern —
   it carries the platform's audit-sink module), meaningful Git commit history (see above).
3. **QA**: unit tests + coverage report, and/or a functional verification report for anything a
   unit test can't reach (e.g. real gate interrupt/resume across a container restart).

## This repo's place in the platform

LangGraph-based orchestrator — the "main" repo in a governance sense: it consumes drift-detected
events, runs a governed multi-step workflow against a cloned copy of whichever tenant service's
repo triggered it, and pauses at real human-in-the-loop gates. Must stay domain-agnostic — no
reference to any tenant service's implementation details. A tenant service may be named as an
illustrative example (e.g. in a diagram); its internals are never this repo's concern.
