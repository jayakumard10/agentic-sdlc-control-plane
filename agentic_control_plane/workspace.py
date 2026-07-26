"""Clone-per-run workspace acquisition, cleanup, and crash reconciliation.

Every run gets its own ephemeral clone at `{WORKSPACES_ROOT}/{run_id}`, created from
the repo URL and branch carried on the triggering event and deleted when the run
reaches a terminal state. Runs never share a working tree, so two concurrent runs
against the same target cannot see each other's uncommitted changes.

Credentials never appear in a URL. The token is read from a file and handed to git
through a credential helper invoked at request time, so it is absent from the remote
URL, from `git remote -v`, from the process argument list, and from any error message
git prints containing the URL it was working on. A token embedded in the clone URL
would be persisted into `.git/config` by the clone itself, which is the specific
outcome this avoids.
"""

from __future__ import annotations

import logging
import os
import shutil
import subprocess
from collections.abc import Callable, Iterable
from pathlib import Path

from agentic_control_plane import tools

logger = logging.getLogger(__name__)

_CLONE_TIMEOUT_SECONDS = 120
# Full history is never needed: a run reads the current tree, writes to it, and the
# only revision it ever reverts to is a commit this run made itself.
_CLONE_DEPTH = 1


class CloneError(RuntimeError):
    """Raised when acquiring a run's workspace fails (bad URL, branch, or auth)."""


def workspaces_root() -> Path:
    return Path(os.environ.get("WORKSPACES_ROOT", "/workspaces"))


def workspace_for(run_id: str) -> Path:
    return workspaces_root() / run_id


def _git_pat() -> str | None:
    """Read the PAT from GIT_PAT_FILE, or GIT_PAT directly.

    The file form is what the compose stack uses: an environment variable is visible
    in `docker inspect` output, a mounted secret file is not.
    """
    file_path = os.environ.get("GIT_PAT_FILE")
    if file_path and Path(file_path).is_file():
        token = Path(file_path).read_text(encoding="utf-8").strip()
        if token:
            return token
    token = os.environ.get("GIT_PAT", "").strip()
    return token or None


def _credential_helper_args() -> list[str]:
    """Build `-c credential.helper=...` arguments, or none if no PAT is configured.

    The helper script reads the token from the file at exec time rather than
    receiving it as an argument, so the secret is not in this process's argv either.
    Returns no arguments when unconfigured, which is correct for a public repo and
    for tests cloning from a local path.
    """
    file_path = os.environ.get("GIT_PAT_FILE")
    if not (file_path and Path(file_path).is_file()):
        return []
    helper = (
        f'!f() {{ echo "username=x-access-token"; '
        f'echo "password=$(cat {file_path})"; }}; f'
    )
    return ["-c", f"credential.helper={helper}"]


def _redact(text: str) -> str:
    """Strip the PAT from text before it reaches a log line.

    Defence in depth. Nothing here is expected to put the token into git's output,
    but a log line is the wrong place to discover that assumption was wrong.
    """
    token = _git_pat()
    if token and token in text:
        return text.replace(token, "***")
    return text


def clone_for_run(run_id: str, repo_url: str, branch: str) -> tuple[Path, str | None]:
    """Clone `branch` of `repo_url` into this run's workspace.

    Returns the workspace path and the commit SHA at clone time - the
    `commit_sha_before` a rollback later reverts to. Raises CloneError on any
    failure, which the caller turns into a terminal `clone_failed` outcome.
    """
    destination = workspace_for(run_id)
    if destination.exists():
        # A redelivered event whose prior attempt left a partial clone behind.
        # Starting from a known-empty directory is cheaper than reasoning about
        # what state the previous attempt got to.
        logger.warning("Workspace %s already exists, removing before re-clone", destination)
        cleanup(run_id)
    destination.parent.mkdir(parents=True, exist_ok=True)

    command = [
        "git",
        *_credential_helper_args(),
        "clone",
        "--depth",
        str(_CLONE_DEPTH),
        "--branch",
        branch,
        "--single-branch",
        repo_url,
        str(destination),
    ]
    logger.info("Cloning %s (branch %s) for run %s", repo_url, branch, run_id)
    try:
        proc = subprocess.run(
            command, capture_output=True, text=True, timeout=_CLONE_TIMEOUT_SECONDS
        )
    except subprocess.TimeoutExpired as exc:
        cleanup(run_id)
        raise CloneError(
            f"git clone of {repo_url} timed out after {_CLONE_TIMEOUT_SECONDS}s"
        ) from exc

    if proc.returncode != 0:
        cleanup(run_id)
        raise CloneError(
            f"git clone of {repo_url} (branch {branch}) failed: "
            f"{_redact(proc.stderr.strip())}"
        )

    _configure_commit_identity(destination)
    commit_sha_before = tools.git_current_commit(destination)
    logger.info(
        "Run %s workspace ready at %s (commit %s)",
        run_id,
        destination,
        (commit_sha_before or "unknown")[:8],
    )
    return destination, commit_sha_before


def _configure_commit_identity(destination: Path) -> None:
    """Give the clone a committer identity, because nothing else will.

    `tools.git_init_if_needed` sets one, but only on the path where it actually runs
    `git init`. A clone arrives with `.git` already present, so that call is a no-op
    and the identity is never configured - the release gate then fails at
    `git commit` with "Author identity unknown". It went unnoticed on a developer
    machine, where a global git identity exists and silently supplies one; the
    container has none, which is the environment that matters.

    Set per-repository rather than globally: the workspace is what needs the
    identity, and a global setting would leak across runs.
    """
    name = os.environ.get("GIT_COMMITTER_NAME", "agentic-sdlc-control-plane")
    email = os.environ.get("GIT_COMMITTER_EMAIL", "control-plane@agentic-sdlc.local")
    for key, value in (("user.name", name), ("user.email", email)):
        subprocess.run(
            ["git", "config", key, value],
            cwd=destination,
            capture_output=True,
            text=True,
            timeout=_CLONE_TIMEOUT_SECONDS,
            check=True,
        )


def cleanup(run_id: str) -> None:
    """Delete a run's workspace. Safe to call when it does not exist."""
    destination = workspace_for(run_id)
    if not destination.exists():
        return
    # Cloned git objects are read-only on Windows, which makes rmtree fail on the
    # .git directory unless the permission is cleared first. `onexc` rather than the
    # older `onerror`, which is deprecated as of the Python version this pins.
    def _on_exc(func, path, _exc):
        os.chmod(path, 0o700)
        func(path)

    shutil.rmtree(destination, onexc=_on_exc)
    logger.info("Removed workspace for run %s", run_id)


def existing_run_ids() -> list[str]:
    root = workspaces_root()
    if not root.is_dir():
        return []
    return sorted(entry.name for entry in root.iterdir() if entry.is_dir())


def reconcile(is_resumable: Callable[[str], bool]) -> list[str]:
    """Delete workspaces left behind by a crash. Returns the run_ids removed.

    Called once at startup. `is_resumable(run_id)` answers whether the checkpointer
    still holds a run that could legitimately continue - a run in progress, or one
    parked at a gate waiting for a human. Anything else is orphaned:

    - the run reached a terminal state but the process died before cleanup, or
    - the process died between cloning and the first checkpoint write, so the
      checkpointer has never heard of it.

    Both are safe to delete. A run that is still resumable is never touched, which
    is why the question asked is "is this resumable" rather than "is this finished" -
    the unknown case has to fall on the delete side, and phrasing it this way makes
    that the default rather than an omission.
    """
    removed: list[str] = []
    for run_id in existing_run_ids():
        try:
            resumable = is_resumable(run_id)
        except Exception:
            logger.exception(
                "Could not determine whether run %s is resumable; leaving its "
                "workspace in place",
                run_id,
            )
            continue
        if resumable:
            logger.info("Run %s is still resumable, keeping its workspace", run_id)
            continue
        logger.warning("Reconciling orphaned workspace for run %s", run_id)
        cleanup(run_id)
        removed.append(run_id)
    return removed


def total_size_bytes(run_ids: Iterable[str] | None = None) -> int:
    """Sum the on-disk size of the given runs' workspaces, or all of them.

    Workspaces are the one unbounded thing this service writes; surfacing the number
    lets an operator see growth before the volume fills rather than after.
    """
    targets = list(run_ids) if run_ids is not None else existing_run_ids()
    total = 0
    for run_id in targets:
        for path in workspace_for(run_id).rglob("*"):
            if path.is_file():
                total += path.stat().st_size
    return total
