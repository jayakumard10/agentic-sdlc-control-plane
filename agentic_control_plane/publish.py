"""Delivering an approved change, or deliberately declining to.

Until this module existed, a run cloned a repository, generated code, tested it,
scanned it for guardrail violations, put it through a human gate, committed it - and
then deleted the workspace. The commit existed for the length of one run and was
reclaimed with it, and the outcome event reported `commit_sha_before`, the revision
the run *started* from. The platform governed a change it then discarded.

**Publishing is off by default, and that default is a security position rather than
caution.** Everything else in this service runs on a read-only PAT: it clones, and
that is all the token can do. Pushing a branch requires write access to the tenant's
repository, which widens what a compromised control plane can do from "read the
tenant's code" to "write to the tenant's default branch's neighbourhood". A
deployment that wants delivery opts into that trade explicitly; one that wants
governance-only keeps the smaller credential. See docs/adr/0012.

Three modes:

- `none` (default) - commit and report, publish nothing. The pre-existing behaviour,
  and still the right one for an evaluation deployment.
- `branch` - push `agentic-patch/{run_id}`. Host-agnostic: any git remote accepts it.
- `pull_request` - push the branch, then open a pull request against the branch the
  run cloned. GitHub-specific, because opening a PR is an API call and there is no
  cross-host equivalent. On a non-GitHub remote this degrades to `branch` with a
  logged reason rather than failing the run.

**A publish failure does not fail the run.** The change was generated, tested and
approved; those facts are true whether or not the push succeeded. But it must not be
quiet either, because a failed push means the work *is* discarded - the thing this
module exists to prevent. So the outcome event carries `published: false` and the
reason, and the run still reaches `completed`. Loud in the event stream, not fatal
to the run.
"""

from __future__ import annotations

import json
import logging
import os
import re
import subprocess
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

# Reaching into workspace for these deliberately. It is the one module that knows how
# the PAT reaches git - via a credential helper invoked at request time, never in argv
# or a remote URL - and a second copy of that logic is a second place to get it wrong.
from agentic_control_plane.workspace import _credential_helper_args, _git_pat, _redact

logger = logging.getLogger(__name__)

PublishMode = Literal["none", "branch", "pull_request"]

BRANCH_PREFIX = "agentic-patch"

# Shorter than the clone timeout: a push sends one small commit to a host the run has
# already talked to, so a slow one means something is wrong rather than something is big.
_PUSH_TIMEOUT_SECONDS = 60
_API_TIMEOUT_SECONDS = 30

_GITHUB_REMOTE = re.compile(r"^https?://github\.com/(?P<owner>[^/]+)/(?P<repo>[^/]+?)(?:\.git)?/?$")


class PublishError(RuntimeError):
    """Raised internally when a push or API call fails; never escapes publish_change."""


@dataclass
class PublishResult:
    """What happened to an approved change after it was committed."""

    mode: str
    published: bool
    branch: str | None = None
    pull_request_url: str | None = None
    error: str | None = None

    def as_payload(self) -> dict:
        """The fields the run-outcome event carries.

        Goes in the envelope's free-form `payload`, not `git_target`, so adding
        delivery reporting does not version the shared contract every other
        repository installs.
        """
        payload: dict = {"publish_mode": self.mode, "published": self.published}
        if self.branch:
            payload["branch"] = self.branch
        if self.pull_request_url:
            payload["pull_request_url"] = self.pull_request_url
        if self.error:
            payload["publish_error"] = self.error
        return payload


def publish_mode() -> PublishMode:
    """Read the configured mode, defaulting to publishing nothing.

    An unrecognised value is treated as `none` and logged, rather than raising. A
    typo in a deployment variable must not turn into a failed run for a change that
    was approved - and silently pushing on an unrecognised value would be worse.
    """
    raw = os.environ.get("PUBLISH_MODE", "none").strip().lower()
    if raw in ("none", "branch", "pull_request"):
        return raw  # type: ignore[return-value]
    logger.error("PUBLISH_MODE=%r is not recognised; publishing nothing this run", raw)
    return "none"


def branch_name(run_id: str) -> str:
    return f"{BRANCH_PREFIX}/{run_id}"


def _push_branch(workspace: Path, run_id: str) -> str:
    """Push the run's commit to its own branch on the origin it was cloned from.

    Pushes `HEAD:refs/heads/<branch>` explicitly rather than checking a branch out
    first: the workspace is a detached single-branch clone, and naming the destination
    ref leaves no doubt about what is being written. Nothing here can update the
    branch the run cloned.
    """
    branch = branch_name(run_id)
    command = [
        "git",
        *_credential_helper_args(),
        "push",
        "origin",
        f"HEAD:refs/heads/{branch}",
    ]
    try:
        proc = subprocess.run(
            command,
            cwd=workspace,
            capture_output=True,
            text=True,
            timeout=_PUSH_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired as exc:
        raise PublishError(f"git push timed out after {_PUSH_TIMEOUT_SECONDS}s") from exc
    if proc.returncode != 0:
        raise PublishError(f"git push failed: {_redact(proc.stderr.strip())}")
    logger.info("Run %s pushed to %s", run_id, branch)
    return branch


def _open_pull_request(repo_url: str, head: str, base: str, title: str, body: str) -> str:
    """Open a pull request on GitHub and return its URL.

    Raises PublishError with the reason on any failure, including a non-GitHub
    remote, which the caller reports rather than retries.
    """
    match = _GITHUB_REMOTE.match(repo_url)
    if not match:
        raise PublishError(
            f"pull_request mode supports GitHub remotes only; {repo_url} is not one"
        )
    token = _git_pat()
    if not token:
        raise PublishError("pull_request mode needs a PAT; none is configured")

    url = f"https://api.github.com/repos/{match['owner']}/{match['repo']}/pulls"
    request = urllib.request.Request(
        url,
        data=json.dumps({"title": title, "head": head, "base": base, "body": body}).encode(),
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "Content-Type": "application/json",
            "User-Agent": "agentic-sdlc-control-plane",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=_API_TIMEOUT_SECONDS) as response:
            return json.load(response).get("html_url", "")
    except urllib.error.HTTPError as exc:
        # Redacted for the same reason the push path is: this string does not stop at
        # a log line. It becomes PublishResult.error, which the run-outcome event
        # carries onto Kafka, where it is durable and readable by every consumer. A
        # response body or a URL echoed back by an intermediary is not somewhere the
        # token is expected to appear, which is exactly why it is worth stripping.
        detail = _redact(exc.read().decode("utf-8", "replace"))[:300]
        raise PublishError(f"GitHub returned {exc.code} opening the pull request: {detail}") from exc
    except Exception as exc:  # noqa: BLE001 - reported, never raised onward
        raise PublishError(
            f"could not reach GitHub to open the pull request: {_redact(str(exc))}"
        ) from exc


def publish_change(
    *,
    workspace: Path,
    run_id: str,
    repo_url: str,
    base_branch: str,
    requirement: str,
) -> PublishResult:
    """Deliver an approved change according to PUBLISH_MODE, reporting what happened.

    Never raises. Every failure becomes a PublishResult the outcome event carries,
    because the run is already approved and a delivery problem is not grounds to
    report it as failed.
    """
    mode = publish_mode()
    if mode == "none":
        return PublishResult(mode=mode, published=False)

    try:
        branch = _push_branch(workspace, run_id)
    except PublishError as exc:
        logger.error("Run %s could not publish its change: %s", run_id, exc)
        return PublishResult(mode=mode, published=False, error=str(exc))

    if mode == "branch":
        return PublishResult(mode=mode, published=True, branch=branch)

    try:
        pull_request_url = _open_pull_request(
            repo_url=repo_url,
            head=branch,
            base=base_branch,
            title=f"[agentic-sdlc] {requirement[:60]}" if requirement else f"[agentic-sdlc] {run_id}",
            body=(
                f"Opened by `agentic-sdlc-control-plane` for run `{run_id}`.\n\n"
                f"This change passed the platform's guardrail scan and a human "
                f"`merge_release_approval` gate before this branch was pushed. The run's "
                f"audit trail is the record of who approved it.\n\n"
                f"**Requirement:** {requirement or '(none recorded)'}"
            ),
        )
    except PublishError as exc:
        # The branch is pushed; only the PR failed. That is a partial success worth
        # distinguishing, because the work is not lost - someone can open the PR by hand.
        logger.error("Run %s pushed %s but could not open a pull request: %s", run_id, branch, exc)
        return PublishResult(mode=mode, published=True, branch=branch, error=str(exc))

    logger.info("Run %s opened %s", run_id, pull_request_url)
    return PublishResult(
        mode=mode, published=True, branch=branch, pull_request_url=pull_request_url
    )
