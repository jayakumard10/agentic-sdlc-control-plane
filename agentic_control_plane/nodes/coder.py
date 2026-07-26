"""Coder node: generates code for the target service, live via the `claude` CLI or
replayed from a fixture.

The only node in the graph that calls an LLM. Live mode shells out to the standalone
`claude` CLI (authenticated via the operator's own subscription login - no API key
anywhere in this system) and parses its `--output-format json` response. Replay mode
reads a previously captured fixture instead. Both paths converge on the same output
shape (`CoderOutput`), so downstream nodes never need to know which one ran.

Neither mode is available by default in the shipped container image, and that is
deliberate rather than an omission:

- Live mode needs the `claude` CLI on PATH plus an authenticated session. The image
  installs neither, so live mode is an opt-in built on a customized image, not a
  config toggle. `_require_claude_cli` fails with that explanation rather than
  letting a bare "No such file or directory" surface from subprocess.
- Replay mode needs fixtures, which are inherently specific to the service being
  worked on. A domain-agnostic platform cannot ship them, so FIXTURES_DIR is a mount
  point that is empty unless an operator supplies one.

Both unavailable cases route to the same safe-stop this node already had for a
missing fixture: the run ends in a defined terminal state with a stated reason, and
outcome telemetry is still emitted.
"""

from __future__ import annotations

import json
import logging
import shutil
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

from agentic_control_plane import tools
from agentic_control_plane.state import AuditEvent, CoderOutput, GraphState

logger = logging.getLogger(__name__)

CLAUDE_MODEL = "claude-sonnet-5"
# 300s was too tight for a retry prompt covering several files - observed a genuine
# timeout (not a hang) during brownfield fixture capture.
CLAUDE_CLI_TIMEOUT_SECONDS = 480


class FixtureNotFoundError(RuntimeError):
    """Raised in replay mode when no recorded fixture matches the current run."""


def _available_dependencies_note(workspace: Path) -> str | None:
    """Ground the prompt in what's actually installed, so Coder doesn't reach for a

    third-party library that isn't in requirements.txt - there is no dynamic
    dependency-installation step in this architecture, so an unlisted import is a
    guaranteed test failure regardless of how correct the rest of the code is.
    """
    requirements_path = workspace / "requirements.txt"
    if not requirements_path.is_file():
        return None
    packages = [
        line.strip()
        for line in requirements_path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]
    if not packages:
        return None
    return (
        "Only these third-party packages are installed (plus the Python standard "
        "library) - do not import anything outside this list: " + ", ".join(packages)
    )


def _task_plan_note(state: GraphState) -> str | None:
    """Surface the Decomposer/Planner's task list, including any re-planning revision.

    Without this, the re-planning mechanism (Re-planner + Decomposer/Planner's revised
    T0 reconciliation task) only ever affects orchestrator state - the actual
    instruction handed to Coder would still be the raw clarified requirement with no
    scope guidance, defeating the point of re-planning. Concretely: on a re-planned
    run, T0 says to reconcile with the conflicting module that already exists rather
    than duplicate it - Coder needs to see that, not just know it happened.
    """
    if not state.tasks:
        return None
    lines = [f"- [{task.id}] {task.description}" for task in state.tasks]
    return "Planned tasks (respect scope - do not touch files outside this plan unless a task requires it):\n" + "\n".join(lines)


def _build_prompt(state: GraphState, workspace: Path) -> str:
    dependency_note = _available_dependencies_note(workspace)
    task_note = _task_plan_note(state)

    if state.fallback_triggered:
        lines = [
            "You are generating production-quality Python code for the service "
            "checked out in this working directory. Read what is already there to "
            "infer its framework, layout and conventions - do not assume them.",
            f"Previous attempts to satisfy this requirement failed after "
            f"{state.retry_limit} retries: {state.requirement_clarified}",
            "Fall back to the SIMPLEST possible correct implementation - prefer a "
            "minimal, obviously-correct approach over an elegant one.",
        ]
    else:
        lines = [
            "You are generating production-quality Python code for the service "
            "checked out in this working directory. Read what is already there to "
            "infer its framework, layout and conventions - do not assume them.",
            f"Requirement: {state.requirement_clarified}",
            f"Architecture notes: {state.architecture_design.summary}",
        ]
        if state.test.failures:
            lines.append("The previous attempt failed these tests - fix the root cause:")
            lines.extend(f"- {failure}" for failure in state.test.failures)
    if task_note:
        lines.append(task_note)
    if dependency_note:
        lines.append(dependency_note)
    lines.append(
        "Prefer the smallest change that satisfies the requirement. If you change a "
        "shared module (e.g. models.py) in a way that breaks its existing importers, "
        "you MUST also return updated versions of every one of those importer files "
        "in this same response - a partial update that leaves the rest of the "
        "codebase inconsistent is not acceptable."
    )
    lines.append(
        "Include pytest test file(s) under tests/ covering the new/changed behavior, "
        "not just the implementation."
    )
    lines.append(
        "Return ONLY a JSON object mapping relative file paths to full file contents, "
        "no prose, no markdown fences, no explanation outside the JSON."
    )
    return "\n".join(lines)


_CLI_CALL_ATTEMPTS = 2
_CLI_RETRY_DELAY_SECONDS = 2.0


def _extract_json_object(text: str) -> dict:
    """Parse `text` as JSON, falling back to extracting the outermost {...} substring.

    `claude -p` runs the full agentic CLI (with file-system tool access), not a bare
    completion - on a heavier generation it can preface its final answer with a short
    narration ("All content verified. Here is the final JSON output.") despite explicit
    "no prose" instructions. Rather than relying on prompt wording alone to prevent
    this, parse defensively: strict JSON first, then the largest brace-delimited
    substring if that fails.
    """
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}")
        if start == -1 or end == -1 or end <= start:
            raise
        return json.loads(text[start : end + 1])


class ClaudeCliUnavailableError(RuntimeError):
    """Raised in live mode when the `claude` CLI is not installed on PATH."""


def _require_claude_cli() -> None:
    """Fail with an actionable message instead of a bare FileNotFoundError.

    The shipped image deliberately does not install the CLI (see module docstring),
    so this is the expected outcome of enabling live mode without customizing it -
    worth an explanation rather than an errno.
    """
    if shutil.which("claude") is None:
        raise ClaudeCliUnavailableError(
            "ORCHESTRATOR_MODE=live requires the `claude` CLI on PATH and an "
            "authenticated session. The shipped image installs neither; live mode "
            "needs a customized image that provides both."
        )


def _invoke_claude_cli_once(prompt: str, workspace: Path) -> dict[str, str]:
    proc = subprocess.run(
        ["claude", "-p", prompt, "--model", CLAUDE_MODEL, "--output-format", "json"],
        cwd=workspace,
        capture_output=True,
        text=True,
        timeout=CLAUDE_CLI_TIMEOUT_SECONDS,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"claude CLI exited {proc.returncode}: {proc.stderr.strip()}")
    payload = json.loads(proc.stdout)
    if payload.get("is_error") or payload.get("subtype") != "success":
        raise RuntimeError(f"claude CLI reported an error: {payload}")
    code_files = _extract_json_object(payload["result"])
    if not isinstance(code_files, dict) or not all(
        isinstance(k, str) and isinstance(v, str) for k, v in code_files.items()
    ):
        raise ValueError("claude CLI result did not decode to a flat {path: content} object")
    return code_files


def _invoke_claude_cli(prompt: str, workspace: Path) -> dict[str, str]:
    """Wraps the raw CLI call with a small bounded retry.

    Observed during development: the `claude` CLI occasionally returns an empty or
    malformed stdout on an otherwise healthy call (a transient hiccup, not a code
    generation problem) - distinct from the outer retry_count mechanism, which
    retries when the *generated code* fails tests. This retries the call itself
    before that outer mechanism ever sees a failure.

    `workspace` is passed as the subprocess cwd: `claude -p` runs the full agentic
    CLI with file-system tool access, not a bare completion, so without this it
    orients itself on whatever directory the *orchestrator* happens to be running
    in (the whole monorepo) instead of the actual target-app workspace it's meant
    to generate code for - observed taking 46 tool-use turns and 455 seconds
    exploring the wrong tree before this fix.
    """
    _require_claude_cli()
    last_error: Exception | None = None
    for attempt in range(1, _CLI_CALL_ATTEMPTS + 1):
        try:
            logger.info("Invoking claude CLI in %s (attempt %d/%d)", workspace, attempt, _CLI_CALL_ATTEMPTS)
            return _invoke_claude_cli_once(prompt, workspace)
        except (RuntimeError, ValueError, json.JSONDecodeError) as exc:
            last_error = exc
            logger.warning("claude CLI call failed (attempt %d/%d): %s", attempt, _CLI_CALL_ATTEMPTS, exc)
            if attempt < _CLI_CALL_ATTEMPTS:
                time.sleep(_CLI_RETRY_DELAY_SECONDS)
    assert last_error is not None
    raise last_error


def _load_fixture(fixtures_dir: Path, scenario_type: str) -> dict:
    fixture_path = fixtures_dir / scenario_type / "transcript.json"
    if not fixture_path.is_file():
        raise FixtureNotFoundError(
            f"no recorded fixture for scenario '{scenario_type}' (expected "
            f"{fixture_path}). Replay mode needs fixtures supplied for the service "
            "being worked on; this platform ships none. Mount them at FIXTURES_DIR, "
            "or run live mode against a customized image providing the `claude` CLI."
        )
    return json.loads(fixture_path.read_text(encoding="utf-8"))


def _select_fixture_attempt(
    attempts: list[dict], attempt_number: int, fallback_triggered: bool
) -> dict:
    if fallback_triggered:
        fallback_entries = [entry for entry in attempts if entry.get("fallback")]
        if fallback_entries:
            return fallback_entries[0]
        raise FixtureNotFoundError(
            "fallback path was triggered but this scenario's fixture has no recorded "
            "fallback attempt"
        )
    matching = [entry for entry in attempts if entry["attempt_number"] == attempt_number]
    if matching:
        return matching[0]
    raise FixtureNotFoundError(
        f"no recorded fixture attempt #{attempt_number} for this scenario "
        f"(fixture has {len(attempts)} attempt(s))"
    )


_FIXTURE_FAILURE_TYPES = (
    FixtureNotFoundError,
    RuntimeError,
    ValueError,
    json.JSONDecodeError,
    subprocess.TimeoutExpired,
    KeyError,
    OSError,
)


def coder(state: GraphState, workspace: Path, fixtures_dir: Path) -> dict:
    start = time.monotonic()
    attempt_number = state.retry_count + 1

    try:
        if state.mode == "live":
            code_files = _invoke_claude_cli(_build_prompt(state, workspace), workspace)
            fixture_source = None
            rationale = f"Live generation, attempt {attempt_number}."
        else:
            fixture = _load_fixture(fixtures_dir, state.scenario_type)
            entry = _select_fixture_attempt(
                fixture.get("attempts", []), attempt_number, state.fallback_triggered
            )
            code_files = entry["code_files"]
            fixture_source = str(fixtures_dir / state.scenario_type / "transcript.json")
            rationale = entry.get("rationale", "")
    except _FIXTURE_FAILURE_TYPES as exc:
        logger.error("Coder safe-stop on attempt %d: %s", attempt_number, exc)
        return {
            "safe_stop": True,
            "run_status": "failed",
            "finished_at": datetime.now(timezone.utc),
            "coder": CoderOutput(attempt_number=attempt_number, rationale=str(exc)),
            "events": [AuditEvent(node="coder", event_type="safe_stop", detail=str(exc))],
        }

    commit_sha_before = tools.git_current_commit(workspace)
    tools.write_code_files(workspace, code_files)

    coder_output = CoderOutput(
        code_files=code_files,
        attempt_number=attempt_number,
        fixture_source=fixture_source,
        commit_sha_before=commit_sha_before,
        rationale=rationale,
    )

    events = [
        AuditEvent(
            node="coder",
            event_type="node_end",
            detail=(
                f"attempt {attempt_number}: generated {len(code_files)} file(s) "
                f"({state.mode} mode)"
            ),
            latency_ms=(time.monotonic() - start) * 1000,
        )
    ]

    return {"coder": coder_output, "coder_attempts": [coder_output], "events": events}
