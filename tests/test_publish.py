"""Delivery of an approved change.

These push to a real git origin rather than mocking the push. The origin fixture is
an ordinary repository on disk, and the run's branch is never the branch that origin
has checked out, so the push is a real one and its result is observable by asking
origin what refs it now has.
"""

from __future__ import annotations

import io
import json
import subprocess
from pathlib import Path

import pytest

from agentic_control_plane import publish, tools, workspace


@pytest.fixture()
def origin(tmp_path: Path) -> Path:
    repo = tmp_path / "origin"
    tools.write_code_files(repo, {"svc/main.py": "def handle():\n    return 1\n"})
    tools.git_commit_all(repo, "initial")
    subprocess.run(["git", "branch", "-M", "main"], cwd=repo, check=True, capture_output=True)
    return repo


@pytest.fixture()
def run_workspace(tmp_path: Path, origin: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A cloned workspace with one unpublished commit, as a release gate leaves it."""
    monkeypatch.setenv("WORKSPACES_ROOT", str(tmp_path / "workspaces"))
    path, _ = workspace.clone_for_run("run-1", str(origin), "main")
    tools.write_code_files(path, {"svc/added.py": "def added():\n    return 2\n"})
    tools.git_commit_all(path, "the approved change")
    return path


def _origin_branches(origin: Path) -> list[str]:
    out = subprocess.run(
        ["git", "branch", "--list", "--format=%(refname:short)"],
        cwd=origin,
        capture_output=True,
        text=True,
        check=True,
    )
    return [line.strip() for line in out.stdout.splitlines() if line.strip()]


def test_publish_mode_defaults_to_none(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("PUBLISH_MODE", raising=False)
    assert publish.publish_mode() == "none"


def test_an_unrecognised_mode_publishes_nothing(monkeypatch: pytest.MonkeyPatch):
    """A typo must not push, and must not fail a run that was already approved."""
    monkeypatch.setenv("PUBLISH_MODE", "pullrequest")
    assert publish.publish_mode() == "none"


def test_the_default_delivers_nothing(
    run_workspace: Path, origin: Path, monkeypatch: pytest.MonkeyPatch
):
    """Publishing is opt-in, so an unconfigured deployment keeps its read-only PAT."""
    monkeypatch.delenv("PUBLISH_MODE", raising=False)

    result = publish.publish_change(
        workspace=run_workspace,
        run_id="run-1",
        repo_url=str(origin),
        base_branch="main",
        requirement="add a thing",
    )

    assert result.published is False
    assert result.mode == "none"
    assert _origin_branches(origin) == ["main"], "nothing was pushed"
    assert result.as_payload() == {"publish_mode": "none", "published": False}


def test_branch_mode_pushes_the_run_to_its_own_branch(
    run_workspace: Path, origin: Path, monkeypatch: pytest.MonkeyPatch
):
    """The change survives the run, which is the whole point of this module.

    Asserts against origin rather than the return value: the claim is that the commit
    exists somewhere the workspace's deletion cannot reach, and only origin can
    confirm that.
    """
    monkeypatch.setenv("PUBLISH_MODE", "branch")

    result = publish.publish_change(
        workspace=run_workspace,
        run_id="run-1",
        repo_url=str(origin),
        base_branch="main",
        requirement="add a thing",
    )

    assert result.published is True
    assert result.branch == "agentic-patch/run-1"
    assert "agentic-patch/run-1" in _origin_branches(origin)
    assert result.as_payload()["published"] is True

    # And the branch the run cloned is untouched - delivery never writes to it.
    head_main = subprocess.run(
        ["git", "rev-parse", "main"], cwd=origin, capture_output=True, text=True, check=True
    ).stdout.strip()
    head_branch = subprocess.run(
        ["git", "rev-parse", "agentic-patch/run-1"],
        cwd=origin,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    assert head_main != head_branch, "the run's commit is on its own branch, not on main"


def test_the_workspace_may_be_deleted_once_the_branch_is_pushed(
    run_workspace: Path, origin: Path, monkeypatch: pytest.MonkeyPatch
):
    """The regression this module exists to prevent, stated as a test.

    Before delivery existed, the commit lived only in the workspace and the worker
    deleted it immediately afterwards. Pushing first is what makes cleanup safe.
    """
    monkeypatch.setenv("PUBLISH_MODE", "branch")
    publish.publish_change(
        workspace=run_workspace,
        run_id="run-1",
        repo_url=str(origin),
        base_branch="main",
        requirement="add a thing",
    )
    workspace.cleanup("run-1")

    assert not run_workspace.exists()
    assert "agentic-patch/run-1" in _origin_branches(origin), (
        "the change outlived the workspace that produced it"
    )


def test_a_push_failure_is_reported_rather_than_raised(
    run_workspace: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """The run was approved; a delivery problem is news, not a failure of the run.

    But it must be loud: `published` is false and the reason travels on the outcome
    event, because a failed push means the change really was discarded.
    """
    monkeypatch.setenv("PUBLISH_MODE", "branch")
    subprocess.run(
        ["git", "remote", "set-url", "origin", str(tmp_path / "does-not-exist")],
        cwd=run_workspace,
        check=True,
        capture_output=True,
    )

    result = publish.publish_change(
        workspace=run_workspace,
        run_id="run-1",
        repo_url=str(tmp_path / "does-not-exist"),
        base_branch="main",
        requirement="add a thing",
    )

    assert result.published is False
    assert result.error, "the reason must reach the outcome event"
    assert result.as_payload()["published"] is False
    assert "publish_error" in result.as_payload()


class _FakeResponse:
    def __init__(self, body: dict):
        self._body = json.dumps(body).encode()

    def read(self):
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def test_a_pull_request_is_opened_against_the_branch_the_run_cloned(
    monkeypatch: pytest.MonkeyPatch,
):
    """The base is the run's own branch, not a hardcoded default.

    A run triggered against `release/2026-08` must not open its request against
    `main`; the platform does not get to choose the tenant's integration branch.
    """
    monkeypatch.setenv("GIT_PAT", "t0ken")
    captured: dict = {}

    def fake_urlopen(request, timeout=None):
        captured["url"] = request.full_url
        captured["body"] = json.loads(request.data.decode())
        captured["auth"] = request.headers.get("Authorization")
        return _FakeResponse({"html_url": "https://github.com/o/r/pull/7"})

    monkeypatch.setattr(publish.urllib.request, "urlopen", fake_urlopen)

    url = publish._open_pull_request(
        repo_url="https://github.com/o/r.git",
        head="agentic-patch/run-9",
        base="release/2026-08",
        title="t",
        body="b",
    )

    assert url == "https://github.com/o/r/pull/7"
    assert captured["url"] == "https://api.github.com/repos/o/r/pulls"
    assert captured["body"]["base"] == "release/2026-08"
    assert captured["body"]["head"] == "agentic-patch/run-9"
    assert captured["auth"] == "Bearer t0ken"


@pytest.mark.parametrize(
    "repo_url",
    [
        "https://gitlab.com/o/r.git",
        "https://github.example.com/o/r.git",
        "git@github.com:o/r.git",
    ],
)
def test_only_a_real_github_remote_gets_a_pull_request(
    repo_url: str, monkeypatch: pytest.MonkeyPatch
):
    """Including hosts whose name merely contains github, which a loose match accepts."""
    monkeypatch.setenv("GIT_PAT", "t0ken")
    with pytest.raises(publish.PublishError, match="GitHub"):
        publish._open_pull_request(
            repo_url=repo_url, head="h", base="main", title="t", body="b"
        )


def test_a_missing_token_is_reported_before_the_call_is_made(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("GIT_PAT", raising=False)
    monkeypatch.delenv("GIT_PAT_FILE", raising=False)
    with pytest.raises(publish.PublishError, match="PAT"):
        publish._open_pull_request(
            repo_url="https://github.com/o/r.git", head="h", base="main", title="t", body="b"
        )


def test_a_github_error_response_carries_its_reason(monkeypatch: pytest.MonkeyPatch):
    """A 422 here usually means the branch already has a request open, which the
    operator needs told rather than a bare failure."""
    monkeypatch.setenv("GIT_PAT", "t0ken")

    def fake_urlopen(request, timeout=None):
        raise publish.urllib.error.HTTPError(
            request.full_url, 422, "Unprocessable", {}, io.BytesIO(b'{"message":"already exists"}')
        )

    monkeypatch.setattr(publish.urllib.request, "urlopen", fake_urlopen)

    with pytest.raises(publish.PublishError, match="422"):
        publish._open_pull_request(
            repo_url="https://github.com/o/r.git", head="h", base="main", title="t", body="b"
        )


def test_a_github_error_body_cannot_carry_the_token_onto_the_event(
    monkeypatch: pytest.MonkeyPatch,
):
    """This reason does not stop at a log line.

    It becomes PublishResult.error, which the run-outcome event carries onto Kafka -
    durable, and readable by every consumer of the topic. The push path has always
    been redacted; this one was not, and it is the path that quotes bytes chosen by
    whatever answered the request.
    """
    monkeypatch.setenv("GIT_PAT", "ghp_supersecret")

    def fake_urlopen(request, timeout=None):
        raise publish.urllib.error.HTTPError(
            request.full_url,
            401,
            "Unauthorized",
            {},
            io.BytesIO(b'{"message":"bad credentials: ghp_supersecret"}'),
        )

    monkeypatch.setattr(publish.urllib.request, "urlopen", fake_urlopen)

    with pytest.raises(publish.PublishError) as caught:
        publish._open_pull_request(
            repo_url="https://github.com/o/r.git", head="h", base="main", title="t", body="b"
        )

    assert "ghp_supersecret" not in str(caught.value)
    assert "***" in str(caught.value)


def test_a_transport_failure_reaching_github_is_redacted_too(monkeypatch: pytest.MonkeyPatch):
    """The catch-all path quotes the exception, and a URL is a place a token lands."""
    monkeypatch.setenv("GIT_PAT", "ghp_supersecret")

    def fake_urlopen(request, timeout=None):
        raise OSError("tunnel failed for https://ghp_supersecret@api.github.com")

    monkeypatch.setattr(publish.urllib.request, "urlopen", fake_urlopen)

    with pytest.raises(publish.PublishError) as caught:
        publish._open_pull_request(
            repo_url="https://github.com/o/r.git", head="h", base="main", title="t", body="b"
        )

    assert "ghp_supersecret" not in str(caught.value)
    assert "***" in str(caught.value)


def test_a_push_that_hangs_is_bounded_and_reported(
    run_workspace: Path, monkeypatch: pytest.MonkeyPatch
):
    """A push that never returns must not hold the worker.

    The worker is single-threaded, so a git push with no timeout parks every other
    run behind it for as long as the remote stays silent. The timeout is the only
    thing that ends that, and a run whose delivery timed out is still a completed
    run that reports what happened.
    """
    monkeypatch.setenv("PUBLISH_MODE", "branch")

    def fake_run(command, **kwargs):
        raise subprocess.TimeoutExpired(command, publish._PUSH_TIMEOUT_SECONDS)

    monkeypatch.setattr(publish.subprocess, "run", fake_run)

    result = publish.publish_change(
        workspace=run_workspace,
        run_id="run-1",
        repo_url="https://github.com/o/r.git",
        base_branch="main",
        requirement="add a thing",
    )

    assert result.published is False
    assert "timed out" in result.as_payload()["publish_error"]


def test_pull_request_mode_on_a_non_github_remote_still_delivers_the_branch(
    run_workspace: Path, origin: Path, monkeypatch: pytest.MonkeyPatch
):
    """Partial success is distinguished from failure.

    Opening a pull request is a GitHub API call with no cross-host equivalent, so a
    non-GitHub remote cannot have one. The branch is still pushed, so the work is not
    lost and a human can open the request by hand - `published` stays true and the
    reason is reported alongside it.
    """
    monkeypatch.setenv("PUBLISH_MODE", "pull_request")

    result = publish.publish_change(
        workspace=run_workspace,
        run_id="run-1",
        repo_url=str(origin),
        base_branch="main",
        requirement="add a thing",
    )

    assert result.published is True, "the branch landed even though the PR could not"
    assert "agentic-patch/run-1" in _origin_branches(origin)
    assert result.pull_request_url is None
    assert "GitHub" in (result.error or "")
