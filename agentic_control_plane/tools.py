"""Sandboxed execution, policy guardrails, and git helpers shared across graph nodes."""

from __future__ import annotations

import logging
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Literal

from pydantic import BaseModel

logger = logging.getLogger(__name__)


class SandboxedTestRun(BaseModel):
    """Outcome of one `pytest` invocation via a sandboxed subprocess."""

    passed: bool
    returncode: int
    stdout: str
    stderr: str
    timed_out: bool
    duration_seconds: float


def run_pytest_sandboxed(
    workdir: Path, test_path: str = ".", timeout_seconds: int = 60
) -> SandboxedTestRun:
    """Run pytest against `workdir` in a subprocess with an explicit timeout.

    This is the inner sandboxing layer: Docker Compose is the outer isolation
    boundary, this subprocess call (scoped cwd, bounded timeout, no shell) is
    the inner one guarding the Test Executor node against a runaway or hung
    Coder-generated test suite.
    """
    if not workdir.is_dir():
        raise FileNotFoundError(f"Test workdir does not exist: {workdir}")

    logger.debug("Running pytest in %s (timeout=%ss)", workdir, timeout_seconds)
    start = time.monotonic()
    try:
        proc = subprocess.run(
            [sys.executable, "-m", "pytest", test_path, "-q"],
            cwd=workdir,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )
        duration = time.monotonic() - start
        if proc.returncode == 0:
            logger.debug("pytest passed in %s (%.1fs)", workdir, duration)
        else:
            logger.warning(
                "pytest failed in %s: returncode=%s (%.1fs)", workdir, proc.returncode, duration
            )
        return SandboxedTestRun(
            passed=proc.returncode == 0,
            returncode=proc.returncode,
            stdout=proc.stdout,
            stderr=proc.stderr,
            timed_out=False,
            duration_seconds=duration,
        )
    except subprocess.TimeoutExpired as exc:
        logger.warning("pytest timed out in %s after %ss", workdir, timeout_seconds)
        return SandboxedTestRun(
            passed=False,
            returncode=-1,
            stdout=exc.stdout if isinstance(exc.stdout, str) else "",
            stderr=exc.stderr if isinstance(exc.stderr, str) else "",
            timed_out=True,
            duration_seconds=time.monotonic() - start,
        )


class GuardrailFinding(BaseModel):
    """A single policy guardrail hit in Coder-generated code."""

    rule: Literal["unsafe_call", "ddl_change", "secret_detected"]
    file: str
    line: int
    snippet: str


_UNSAFE_CALL_PATTERNS: list[re.Pattern[str]] = [
    re.compile(r"\beval\s*\("),
    re.compile(r"\bexec\s*\("),
    re.compile(r"\bos\.system\s*\("),
    re.compile(r"subprocess\.\w+\([^)]*shell\s*=\s*True"),
]

_DDL_PATTERN = re.compile(
    r"\b(CREATE|ALTER|DROP)\s+(TABLE|INDEX|COLUMN|DATABASE|SCHEMA)\b", re.IGNORECASE
)

_SECRET_PATTERNS: list[re.Pattern[str]] = [
    re.compile(r"AKIA[0-9A-Z]{16}"),
    re.compile(
        r"(?i)(api[_-]?key|secret|password|token)\s*[:=]\s*['\"][A-Za-z0-9_\-/+=]{8,}['\"]"
    ),
]


def evaluate_guardrails(code_files: dict[str, str]) -> list[GuardrailFinding]:
    """Scan generated code for the three concrete policy guardrails.

    (1) unsafe calls (eval/exec/os.system/shell=True) — human-override-only;
    (2) DDL/schema-altering statements — always routes to a human gate;
    (3) secret-shaped strings — blocks merge eligibility until resolved.
    """
    findings: list[GuardrailFinding] = []
    for filename, content in code_files.items():
        for line_no, line in enumerate(content.splitlines(), start=1):
            for pattern in _UNSAFE_CALL_PATTERNS:
                if pattern.search(line):
                    findings.append(
                        GuardrailFinding(
                            rule="unsafe_call", file=filename, line=line_no, snippet=line.strip()
                        )
                    )
            if _DDL_PATTERN.search(line):
                findings.append(
                    GuardrailFinding(
                        rule="ddl_change", file=filename, line=line_no, snippet=line.strip()
                    )
                )
            for pattern in _SECRET_PATTERNS:
                if pattern.search(line):
                    findings.append(
                        GuardrailFinding(
                            rule="secret_detected",
                            file=filename,
                            line=line_no,
                            snippet=line.strip(),
                        )
                    )
    for finding in findings:
        logger.warning(
            "Guardrail hit: rule=%s file=%s line=%s", finding.rule, finding.file, finding.line
        )
    return findings


def write_code_files(workspace: Path, code_files: dict[str, str]) -> list[Path]:
    """Write Coder output into the target-app workspace, refusing path traversal."""
    workspace = workspace.resolve()
    workspace.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    for relative_path, content in code_files.items():
        target = (workspace / relative_path).resolve()
        if target != workspace and workspace not in target.parents:
            raise ValueError(f"Refusing to write outside workspace: {relative_path}")
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")
        written.append(target)
    return written


class GitOperationError(RuntimeError):
    """Raised when a git subprocess call against the target-app workspace fails."""


# Bound every git call the same way run_pytest_sandboxed bounds pytest: the
# rollback/safe-stop machinery runs entirely through _run_git, and git can hang
# (index lock, an unexpected credential prompt, a slow filesystem). A hung git
# call would otherwise stall the whole orchestrator with no timeout to break it.
_GIT_TIMEOUT_SECONDS = 30


def _run_git(workspace: Path, *args: str) -> str:
    try:
        proc = subprocess.run(
            ["git", *args],
            cwd=workspace,
            capture_output=True,
            text=True,
            timeout=_GIT_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired as exc:
        logger.error(
            "git %s timed out in %s after %ss", " ".join(args), workspace, _GIT_TIMEOUT_SECONDS
        )
        raise GitOperationError(
            f"git {' '.join(args)} timed out in {workspace} after {_GIT_TIMEOUT_SECONDS}s"
        ) from exc
    if proc.returncode != 0:
        logger.error("git %s failed in %s: %s", " ".join(args), workspace, proc.stderr.strip())
        raise GitOperationError(f"git {' '.join(args)} failed in {workspace}: {proc.stderr.strip()}")
    return proc.stdout.strip()


def git_init_if_needed(workspace: Path) -> None:
    """Initialize the target app's own nested git history, distinct from the orchestrator submission repo's history."""
    workspace.mkdir(parents=True, exist_ok=True)
    if not (workspace / ".git").is_dir():
        _run_git(workspace, "init")
        _run_git(workspace, "config", "user.email", "orchestrator@local")
        _run_git(workspace, "config", "user.name", "SDLC Orchestrator")


def git_current_commit(workspace: Path) -> str | None:
    if not (workspace / ".git").is_dir():
        return None
    try:
        return _run_git(workspace, "rev-parse", "HEAD")
    except GitOperationError:
        return None


def git_commit_all(workspace: Path, message: str) -> str:
    """Stage and commit all changes in the target-app workspace.

    No-op-safe: if nothing changed, returns the existing HEAD instead of raising
    on an empty commit.
    """
    git_init_if_needed(workspace)
    _run_git(workspace, "add", "-A")
    status = _run_git(workspace, "status", "--porcelain")
    if not status:
        logger.debug("git_commit_all: no changes in %s, skipping commit", workspace)
        return git_current_commit(workspace) or ""
    _run_git(workspace, "commit", "-m", message)
    sha = git_current_commit(workspace) or ""
    logger.info("Committed %s in %s: %s", sha[:8], workspace, message)
    return sha


def git_revert_to(workspace: Path, commit_sha: str) -> None:
    """Rollback: hard-reset the target-app workspace to a prior known-good commit.

    `git reset --hard` alone only rewinds tracked files - it leaves untracked files
    (like a Coder attempt that was written but never committed, which is exactly the
    rollback scenario) sitting on disk. `git clean -fd` after the reset is required
    to actually discard them.
    """
    logger.warning("Rolling back %s to %s", workspace, commit_sha[:8])
    _run_git(workspace, "reset", "--hard", commit_sha)
    _run_git(workspace, "clean", "-fd")
