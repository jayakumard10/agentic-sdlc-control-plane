"""Tests for tools.py: sandboxed pytest runner, guardrail checks, path-traversal

guard, and git commit/rollback (including the untracked-file cleanup bug found
during integration testing).
"""

from __future__ import annotations

import logging
import subprocess
from pathlib import Path

import pytest

from agentic_control_plane import tools


def test_run_pytest_sandboxed_reports_pass(tmp_path: Path):
    tools.write_code_files(tmp_path, {"tests/test_ok.py": "def test_ok():\n    assert True\n"})
    result = tools.run_pytest_sandboxed(tmp_path, timeout_seconds=30)
    assert result.passed is True
    assert result.returncode == 0
    assert result.timed_out is False


def test_run_pytest_sandboxed_reports_failure(tmp_path: Path):
    tools.write_code_files(
        tmp_path, {"tests/test_bad.py": "def test_bad():\n    assert False\n"}
    )
    result = tools.run_pytest_sandboxed(tmp_path, timeout_seconds=30)
    assert result.passed is False
    assert result.returncode != 0
    assert result.timed_out is False


def test_run_pytest_sandboxed_reports_timeout(tmp_path: Path):
    tools.write_code_files(
        tmp_path,
        {"tests/test_hang.py": "import time\ndef test_hang():\n    time.sleep(10)\n"},
    )
    result = tools.run_pytest_sandboxed(tmp_path, timeout_seconds=1)
    assert result.passed is False
    assert result.timed_out is True


def test_run_pytest_sandboxed_missing_workdir_raises(tmp_path: Path):
    with pytest.raises(FileNotFoundError):
        tools.run_pytest_sandboxed(tmp_path / "does_not_exist", timeout_seconds=5)


def test_write_code_files_blocks_path_traversal(tmp_path: Path):
    with pytest.raises(ValueError):
        tools.write_code_files(tmp_path, {"../escape.py": "malicious"})
    assert not (tmp_path.parent / "escape.py").exists()


def test_write_code_files_creates_nested_directories(tmp_path: Path):
    tools.write_code_files(tmp_path, {"a/b/c/deep.py": "x = 1\n"})
    assert (tmp_path / "a" / "b" / "c" / "deep.py").read_text() == "x = 1\n"


@pytest.mark.parametrize(
    "content,expected_rule",
    [
        ("eval(user_input)\n", "unsafe_call"),
        ("os.system(cmd)\n", "unsafe_call"),
        ("subprocess.run(cmd, shell=True)\n", "unsafe_call"),
        ('cursor.execute("ALTER TABLE users ADD COLUMN x INT")\n', "ddl_change"),
        ('API_KEY = "sk_live_ABCDEFGH12345678"\n', "secret_detected"),
    ],
)
def test_evaluate_guardrails_detects_each_rule(content: str, expected_rule: str):
    findings = tools.evaluate_guardrails({"bad.py": content})
    assert any(finding.rule == expected_rule for finding in findings)


def test_evaluate_guardrails_clean_code_has_no_findings():
    findings = tools.evaluate_guardrails({"ok.py": "def add(a, b):\n    return a + b\n"})
    assert findings == []


def test_evaluate_guardrails_logs_a_warning_per_finding(caplog: pytest.LogCaptureFixture):
    with caplog.at_level(logging.WARNING, logger="agentic_control_plane.tools"):
        tools.evaluate_guardrails({"bad.py": "eval(user_input)\n"})
    assert any("Guardrail hit" in record.message for record in caplog.records)


def test_git_commit_all_then_rollback_restores_tracked_file(tmp_path: Path):
    tools.write_code_files(tmp_path, {"app/ok.py": "x = 1\n"})
    sha1 = tools.git_commit_all(tmp_path, "initial commit")

    tools.write_code_files(tmp_path, {"app/ok.py": "x = 2\n"})
    sha2 = tools.git_commit_all(tmp_path, "second commit")
    assert sha1 != sha2

    tools.git_revert_to(tmp_path, sha1)
    assert (tmp_path / "app" / "ok.py").read_text().strip() == "x = 1"
    assert tools.git_current_commit(tmp_path) == sha1


def test_git_revert_to_removes_untracked_files(tmp_path: Path):
    """Regression test: git reset --hard alone leaves untracked files on disk -

    exactly what a Coder attempt that was written but never committed is. Found via
    integration testing of the rollback-with-a-prior-commit path.
    """
    tools.write_code_files(tmp_path, {"app/good.py": "x = 1\n"})
    good_sha = tools.git_commit_all(tmp_path, "known-good commit")

    tools.write_code_files(tmp_path, {"app/untracked_bad.py": "eval(x)\n"})
    assert (tmp_path / "app" / "untracked_bad.py").exists()

    tools.git_revert_to(tmp_path, good_sha)
    assert not (tmp_path / "app" / "untracked_bad.py").exists()


def test_git_commit_all_is_noop_safe_when_nothing_changed(tmp_path: Path):
    tools.write_code_files(tmp_path, {"app/ok.py": "x = 1\n"})
    sha1 = tools.git_commit_all(tmp_path, "first")
    sha2 = tools.git_commit_all(tmp_path, "no changes this time")
    assert sha1 == sha2


def test_git_current_commit_returns_none_before_any_commit(tmp_path: Path):
    assert tools.git_current_commit(tmp_path) is None


def test_git_operation_error_raised_on_invalid_revert_target(tmp_path: Path):
    tools.write_code_files(tmp_path, {"app/ok.py": "x = 1\n"})
    tools.git_commit_all(tmp_path, "initial")
    with pytest.raises(tools.GitOperationError):
        tools.git_revert_to(tmp_path, "0000000000000000000000000000000000dead")


def test_git_operation_error_raised_on_timeout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    def _raise_timeout(*_args, **_kwargs):
        raise subprocess.TimeoutExpired(cmd="git", timeout=tools._GIT_TIMEOUT_SECONDS)

    monkeypatch.setattr(subprocess, "run", _raise_timeout)
    with pytest.raises(tools.GitOperationError, match="timed out"):
        tools.git_revert_to(tmp_path, "deadbeef")


def test_git_operation_error_logs_before_raising(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
):
    tools.write_code_files(tmp_path, {"app/ok.py": "x = 1\n"})
    tools.git_commit_all(tmp_path, "initial")
    with caplog.at_level(logging.ERROR, logger="agentic_control_plane.tools"):
        with pytest.raises(tools.GitOperationError):
            tools.git_revert_to(tmp_path, "0000000000000000000000000000000000dead")
    assert any("git" in record.message and "failed" in record.message for record in caplog.records)


def test_git_commit_all_logs_info_on_real_commit(tmp_path: Path, caplog: pytest.LogCaptureFixture):
    with caplog.at_level(logging.INFO, logger="agentic_control_plane.tools"):
        tools.write_code_files(tmp_path, {"app/ok.py": "x = 1\n"})
        tools.git_commit_all(tmp_path, "initial commit")
    assert any("Committed" in record.message for record in caplog.records)


def test_a_commit_does_not_carry_the_test_runs_bytecode(tmp_path: Path):
    """Regression, found by delivering a change instead of discarding it.

    test_executor runs pytest inside the workspace, so by the time the release gate
    commits, __pycache__ and .pytest_cache exist. `git add -A` staged them. That was
    invisible while the commit died with the workspace; once ADR 0012 pushed the
    commit to the tenant's repository it meant shipping compiled bytecode into
    someone else's history.
    """
    workspace = tmp_path / "ws"
    tools.write_code_files(workspace, {"svc/main.py": "x = 1\n"})
    (workspace / "svc" / "__pycache__").mkdir(parents=True)
    (workspace / "svc" / "__pycache__" / "main.cpython-312.pyc").write_bytes(b"\x00compiled")
    (workspace / ".pytest_cache").mkdir()
    (workspace / ".pytest_cache" / "CACHEDIR.TAG").write_text("x", encoding="utf-8")

    tools.git_commit_all(workspace, "the approved change")

    tracked = subprocess.run(
        ["git", "ls-files"], cwd=workspace, capture_output=True, text=True, check=True
    ).stdout.split()

    assert "svc/main.py" in tracked, "the actual change is committed"
    assert not [f for f in tracked if "__pycache__" in f or f.endswith(".pyc")], (
        f"bytecode reached the commit: {tracked}"
    )
    assert not [f for f in tracked if ".pytest_cache" in f], (
        f"pytest cache reached the commit: {tracked}"
    )
