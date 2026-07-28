"""Tests for clone-per-run workspace acquisition, cleanup and reconciliation.

Clones run against a real local git repository rather than a mock. The properties
under test - that a clone produces a working tree with a resolvable HEAD, that a bad
branch fails rather than silently producing an empty directory, that cleanup removes
read-only git objects - are properties of git, so a fake subprocess would only
confirm the test's own assumptions.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from agentic_control_plane import tools, workspace


@pytest.fixture()
def origin_repo(tmp_path: Path) -> Path:
    """A real local repository, usable as a clone source over a filesystem path."""
    origin = tmp_path / "origin"
    tools.write_code_files(origin, {"svc/main.py": "x = 1\n"})
    tools.git_commit_all(origin, "initial commit")
    # Clone --single-branch --branch needs the branch to exist by name; the default
    # branch name varies by git version, so pin it explicitly.
    subprocess.run(["git", "branch", "-M", "main"], cwd=origin, check=True, capture_output=True)
    return origin


@pytest.fixture()
def workspaces(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    root = tmp_path / "workspaces"
    monkeypatch.setenv("WORKSPACES_ROOT", str(root))
    monkeypatch.delenv("GIT_PAT_FILE", raising=False)
    monkeypatch.delenv("GIT_PAT", raising=False)
    return root


def test_clone_produces_a_working_tree_and_a_commit_sha(origin_repo: Path, workspaces: Path):
    destination, commit_sha = workspace.clone_for_run("run-1", str(origin_repo), "main")

    assert destination == workspaces / "run-1"
    assert (destination / "svc" / "main.py").read_text().strip() == "x = 1"
    assert commit_sha is not None
    assert len(commit_sha) == 40


def test_clone_captures_the_commit_the_rollback_path_reverts_to(
    origin_repo: Path, workspaces: Path
):
    """commit_sha_before is the whole point of capturing a SHA at clone time - it is

    what the rollback node reverts to. Confirm it matches the origin's HEAD, and that
    reverting to it actually restores the tree.
    """
    origin_head = tools.git_current_commit(origin_repo)
    destination, commit_sha = workspace.clone_for_run("run-2", str(origin_repo), "main")

    assert commit_sha == origin_head

    tools.write_code_files(destination, {"svc/main.py": "x = 999\n"})
    tools.git_revert_to(destination, commit_sha)

    assert (destination / "svc" / "main.py").read_text().strip() == "x = 1"


def test_clone_configures_a_committer_identity_on_the_workspace(
    origin_repo: Path, workspaces: Path
):
    """Regression, found by running in a container. tools.git_init_if_needed sets an

    identity only on the path where it runs `git init`; a clone arrives with .git
    already present, so it is a no-op and nothing configures one. The release gate
    then fails at `git commit` with "Author identity unknown". Invisible on a
    developer machine, where a global git identity silently supplies one.
    """
    destination, _ = workspace.clone_for_run("run-identity", str(origin_repo), "main")

    name = subprocess.run(
        ["git", "config", "user.name"], cwd=destination, capture_output=True, text=True
    ).stdout.strip()
    email = subprocess.run(
        ["git", "config", "user.email"], cwd=destination, capture_output=True, text=True
    ).stdout.strip()

    assert name
    assert email


def test_a_cloned_workspace_can_actually_commit(origin_repo: Path, workspaces: Path):
    """The assertion that matters: the release gate commits into this workspace, so

    proving `git commit` succeeds is worth more than proving two config keys exist.
    """
    destination, _ = workspace.clone_for_run("run-can-commit", str(origin_repo), "main")
    tools.write_code_files(destination, {"svc/new.py": "y = 2\n"})

    sha = tools.git_commit_all(destination, "a change made by a run")

    assert sha
    assert tools.git_current_commit(destination) == sha


def test_committer_identity_is_configurable(
    origin_repo: Path, workspaces: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setenv("GIT_COMMITTER_NAME", "Custom Name")
    monkeypatch.setenv("GIT_COMMITTER_EMAIL", "custom@example.invalid")

    destination, _ = workspace.clone_for_run("run-custom-identity", str(origin_repo), "main")

    name = subprocess.run(
        ["git", "config", "user.name"], cwd=destination, capture_output=True, text=True
    ).stdout.strip()
    assert name == "Custom Name"


def test_clone_failure_raises_and_leaves_no_partial_workspace(
    origin_repo: Path, workspaces: Path
):
    with pytest.raises(workspace.CloneError):
        workspace.clone_for_run("run-3", str(origin_repo), "no-such-branch")

    assert not (workspaces / "run-3").exists()


def test_clone_failure_on_a_bad_url_raises_rather_than_creating_an_empty_dir(
    workspaces: Path, tmp_path: Path
):
    with pytest.raises(workspace.CloneError):
        workspace.clone_for_run("run-4", str(tmp_path / "does-not-exist"), "main")

    assert not (workspaces / "run-4").exists()


def test_clone_over_an_existing_workspace_starts_clean(origin_repo: Path, workspaces: Path):
    """A redelivered event whose prior attempt left a partial clone behind must not

    fail on "destination exists" - it re-clones from scratch.
    """
    stale = workspaces / "run-5"
    stale.mkdir(parents=True)
    (stale / "leftover.txt").write_text("from a previous attempt", encoding="utf-8")

    destination, _ = workspace.clone_for_run("run-5", str(origin_repo), "main")

    assert not (destination / "leftover.txt").exists()
    assert (destination / "svc" / "main.py").exists()


def test_cleanup_removes_the_workspace_including_read_only_git_objects(
    origin_repo: Path, workspaces: Path
):
    workspace.clone_for_run("run-6", str(origin_repo), "main")
    assert (workspaces / "run-6" / ".git").is_dir()

    workspace.cleanup("run-6")

    assert not (workspaces / "run-6").exists()


def test_cleanup_is_safe_when_the_workspace_does_not_exist(workspaces: Path):
    workspace.cleanup("never-existed")


def test_credential_helper_omitted_when_no_pat_is_configured(workspaces: Path):
    assert workspace._credential_helper_args() == []


def test_credential_helper_references_the_file_and_never_inlines_the_token(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """Locked requirement: the token must not land in a URL, in git config, or in

    this process's argument list. The helper passed to git names the file; the token
    itself is read by the helper at exec time.
    """
    pat_file = tmp_path / "pat.txt"
    pat_file.write_text("ghp_supersecrettoken\n", encoding="utf-8")
    monkeypatch.setenv("GIT_PAT_FILE", str(pat_file))

    args = workspace._credential_helper_args()

    assert args[0] == "-c"
    assert str(pat_file) in args[1]
    assert "ghp_supersecrettoken" not in args[1]


def test_redact_strips_the_token_from_text_bound_for_a_log(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    pat_file = tmp_path / "pat.txt"
    pat_file.write_text("ghp_supersecrettoken\n", encoding="utf-8")
    monkeypatch.setenv("GIT_PAT_FILE", str(pat_file))

    redacted = workspace._redact("fatal: could not read from ghp_supersecrettoken@github.com")

    assert "ghp_supersecrettoken" not in redacted
    assert "***" in redacted


def test_reconcile_removes_orphans_and_keeps_resumable_runs(
    origin_repo: Path, workspaces: Path
):
    workspace.clone_for_run("parked-run", str(origin_repo), "main")
    workspace.clone_for_run("finished-run", str(origin_repo), "main")
    workspace.clone_for_run("never-checkpointed-run", str(origin_repo), "main")

    resumable = {"parked-run"}
    removed = workspace.reconcile(lambda run_id: run_id in resumable)

    assert sorted(removed) == ["finished-run", "never-checkpointed-run"]
    assert (workspaces / "parked-run").exists()
    assert not (workspaces / "finished-run").exists()
    assert not (workspaces / "never-checkpointed-run").exists()


def test_reconcile_keeps_a_workspace_it_cannot_make_a_decision_about(
    origin_repo: Path, workspaces: Path
):
    """If the checkpointer cannot be queried, deleting would risk destroying a run

    that is legitimately parked. Keeping an orphan costs disk; deleting a live run's
    workspace loses work.
    """
    workspace.clone_for_run("unknown-run", str(origin_repo), "main")

    def _explode(_run_id: str) -> bool:
        raise RuntimeError("checkpointer unreachable")

    removed = workspace.reconcile(_explode)

    assert removed == []
    assert (workspaces / "unknown-run").exists()


def test_reconcile_does_not_delete_an_audit_trail_sharing_the_root(
    origin_repo: Path, workspaces: Path
):
    """Regression, found by restarting the real container.

    `AUDIT_LOG_PATH` defaulted to `/workspaces/.audit/runs.jsonl` - inside the root
    reconciliation sweeps. On every startup `.audit` was enumerated as a run,
    `is_resumable(".audit")` correctly said no, and the audit trail was deleted. The
    log read `Reconciling orphaned workspace for run .audit`, which is the system
    describing exactly what it was doing to the record of what it had done.

    The audit log has since moved to its own volume outside this root; this asserts
    the second half of the fix, so anything else that ends up alongside a workspace is
    not swept the same way.
    """
    workspace.clone_for_run("real-run", str(origin_repo), "main")
    audit_dir = workspaces / ".audit"
    audit_dir.mkdir()
    (audit_dir / "runs.jsonl").write_text('{"seq":0}\n', encoding="utf-8")

    removed = workspace.reconcile(lambda _run_id: False)

    assert removed == ["real-run"], "an orphaned run workspace is still reclaimed"
    assert (audit_dir / "runs.jsonl").exists(), "the audit trail is not a workspace"


def test_reconcile_is_a_noop_when_the_root_does_not_exist(workspaces: Path):
    assert workspace.existing_run_ids() == []
    assert workspace.reconcile(lambda _run_id: False) == []


def test_total_size_reports_bytes_actually_on_disk(origin_repo: Path, workspaces: Path):
    assert workspace.total_size_bytes() == 0

    workspace.clone_for_run("sized-run", str(origin_repo), "main")

    assert workspace.total_size_bytes() > 0
    assert workspace.total_size_bytes(["no-such-run"]) == 0
