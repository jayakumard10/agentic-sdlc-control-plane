# 0014 — Stage then unstage, rather than exclude in one `git add`

## Context

[ADR 0012](0012-an-approved-change-must-outlive-the-run-that-made-it.md) closed the
output loop, and the first real delivery pushed four `.pyc` files into a tenant's
repository. `test_executor` runs pytest inside the run's workspace, so by the time the
release gate commits, `__pycache__` and `.pytest_cache` exist and `git add -A` staged
them.

The fix at the time added exclude pathspecs to the `git add`:

```
git add -A -- . :(exclude)__pycache__/** :(exclude)**/__pycache__/** ...
```

That is wrong in a way its own test could not see. **An exclude pathspec makes git treat
the ignored directories as explicitly named**, and `git add` then refuses the entire
invocation rather than skipping them:

```
The following paths are ignored by one of your .gitignore files:
.pytest_cache
__pycache__
hint: Use -f if you really want to add them.
```

Exit 1. No commit. The release gate raises `GitOperationError` and the run reports
`failed` — after cloning, generating, testing, scanning and passing a human gate.

This fires for **any tenant whose `.gitignore` already covers bytecode**, which is
nearly every Python repository, including all four in this platform. The condition for
the bug is the condition the original comment assumed made the excludes harmless: *"The
tenant's own .gitignore, where it has one, already covers these."* It does — and that is
precisely what breaks it.

The regression test written alongside the original fix builds its workspace with
`write_code_files` and no `.gitignore`, so it only ever exercised a repository that does
*not* ignore bytecode: the one shape where an exclude pathspec works. The fixture shared
an assumption with the code, which is the same class as
[ADR 0009](0009-the-audit-trail-does-not-live-in-the-workspaces-root.md).

It was found by running the end-to-end demo against a real repository, not by any test.

## Decision

Stage everything, then unstage bytecode with **positive** pathspecs:

```
git add -A -- .
git reset -q -- __pycache__ **/__pycache__ .pytest_cache **/.pytest_cache *.pyc **/*.pyc *.pyo **/*.pyo
```

- **`git add -A -- .` alone exits 0 on both tenant shapes.** Where a `.gitignore` covers
  bytecode, git skips it silently, which is the entire behaviour the excludes were
  trying to buy.
- **The `reset` is for the repositories that have no `.gitignore`**, which was the
  original stated reason for the excludes. It is a no-op when nothing matches, and it
  works on a repository with no commits yet.
- **Positive pathspecs, so no path is ever "explicitly named but ignored."** That
  condition is the whole defect.
- Still removed from the index rather than from disk: the platform is a guest in the
  workspace, and deleting files it did not create is a larger liberty than declining to
  commit them.

## Consequences

Two git invocations where there was one. That is the cost, and it is worth naming
because the single-call form looks tidier and someone will be tempted back to it. The
comment in `git_commit_all` and the test added here both exist to stop that.

`--cov-fail-under` would not have caught this and neither would any addition to the
existing test that kept its fixture. The test that catches it is the one whose workspace
has a `.gitignore`, and it is asserted to fail against the previous implementation
rather than merely pass against this one.

The wider rule this is the second instance of: **a fixture built by the same code that
is under test inherits its blind spots.** Where a tenant's own configuration determines
which branch runs, at least one test supplies that configuration.
