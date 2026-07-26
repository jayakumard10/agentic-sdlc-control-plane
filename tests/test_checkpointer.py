"""Tests for checkpointer.py: the serde allowlist, connection-string construction,

and the durability property the whole gate design rests on - a run paused at a gate
must be resumable from a completely fresh checkpointer connection.

The Postgres reachability probe uses psycopg (already a dependency of
langgraph-checkpoint-postgres) rather than SQLAlchemy, which belongs to the tenant
application's stack and has no place in this repo's dependency tree.
"""

from __future__ import annotations

import os

import psycopg
import pytest
from langgraph.graph import END, START, StateGraph

from agentic_control_plane.checkpointer import (
    _discover_state_model_allowlist,
    _postgres_conn_string,
    _read_secret,
    build_memory_checkpointer,
    build_postgres_checkpointer,
)
from agentic_control_plane.state import GraphState, RunMetrics


def _postgres_reachable() -> bool:
    if not os.environ.get("POSTGRES_USER"):
        return False
    try:
        with psycopg.connect(_postgres_conn_string(), connect_timeout=3):
            return True
    except psycopg.OperationalError:
        return False


def test_serde_allowlist_discovers_every_state_submodel():
    allowlist = _discover_state_model_allowlist()
    names = {name for _, name in allowlist}
    assert {
        "GraphState",
        "AuditEvent",
        "GateRecord",
        "CodebaseImpact",
        "ArchitectureDesign",
        "Task",
        "CoderOutput",
        "TestResult",
        "GuardrailViolation",
        "DocumentationOutput",
        "RunMetrics",
    } <= names


def test_memory_checkpointer_round_trips_nested_submodels():
    """Without the allowlist, LangGraph's default serde falls back to a path it

    warns will be blocked in a future version for any custom Pydantic type nested in
    state. This confirms the round-trip works with the configured serde in place.
    """
    graph = StateGraph(GraphState)
    graph.add_node("noop", lambda state: {})
    graph.add_edge(START, "noop")
    graph.add_edge("noop", END)
    compiled = graph.compile(checkpointer=build_memory_checkpointer())

    config = {"configurable": {"thread_id": "state-roundtrip-test"}}
    result = compiled.invoke(
        GraphState(
            scenario_type="ambiguous",
            requirement_raw="x",
            metrics=RunMetrics(success_rate=1.0),
        ),
        config=config,
    )
    assert result["metrics"].success_rate == 1.0


def test_read_secret_prefers_direct_env_var(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("SOME_SECRET", "direct-value")
    assert _read_secret("SOME_SECRET") == "direct-value"


def test_read_secret_falls_back_to_file(monkeypatch: pytest.MonkeyPatch, tmp_path):
    secret_file = tmp_path / "secret.txt"
    secret_file.write_text("file-value\n", encoding="utf-8")
    monkeypatch.delenv("SOME_SECRET", raising=False)
    monkeypatch.setenv("SOME_SECRET_FILE", str(secret_file))
    assert _read_secret("SOME_SECRET") == "file-value"


def test_read_secret_returns_default_when_neither_is_set(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("SOME_SECRET", raising=False)
    monkeypatch.delenv("SOME_SECRET_FILE", raising=False)
    assert _read_secret("SOME_SECRET", "fallback") == "fallback"


def test_conn_string_percent_encodes_credentials(monkeypatch: pytest.MonkeyPatch):
    """A password containing '@' or '/' - both legal - would otherwise produce a

    URI that silently parses into the wrong host or database instead of failing.
    """
    monkeypatch.setenv("POSTGRES_USER", "control_plane")
    monkeypatch.setenv("POSTGRES_PASSWORD", "p@ss/word")
    monkeypatch.setenv("POSTGRES_HOST", "postgres")
    monkeypatch.setenv("POSTGRES_PORT", "5432")
    monkeypatch.setenv("POSTGRES_DB", "control_plane")

    conn = _postgres_conn_string()

    assert conn == "postgresql://control_plane:p%40ss%2Fword@postgres:5432/control_plane"


def test_conn_string_defaults_carry_no_monolith_leftovers(monkeypatch: pytest.MonkeyPatch):
    """The source system defaulted user/database to 'orchestrator'. This repo's own

    Postgres is named control_plane; a stale default would connect somewhere real
    but wrong on a partially-configured environment.
    """
    for var in ("POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_PASSWORD_FILE", "POSTGRES_DB"):
        monkeypatch.delenv(var, raising=False)

    conn = _postgres_conn_string()

    assert "orchestrator" not in conn
    assert conn.endswith("/control_plane")


@pytest.mark.skipif(
    not _postgres_reachable(), reason="requires PostgreSQL reachable via POSTGRES_* env vars"
)
def test_postgres_checkpointer_resumes_a_thread_from_a_fresh_connection():
    """The whole reason PostgresSaver was chosen over MemorySaver: a pending gate

    must survive something equivalent to a container restart. Simulates that by
    entering a *second*, independent build_postgres_checkpointer() context and
    confirming it can read back a thread a prior context wrote.
    """
    graph = StateGraph(GraphState)
    graph.add_node("noop", lambda state: {"requirement_clarified": "seen"})
    graph.add_edge(START, "noop")
    graph.add_edge("noop", END)

    config = {"configurable": {"thread_id": "postgres-durability-test"}}

    with build_postgres_checkpointer() as checkpointer:
        compiled = graph.compile(checkpointer=checkpointer)
        compiled.invoke(GraphState(scenario_type="brownfield", requirement_raw="x"), config=config)

    # fresh checkpointer instance/connection - simulates resuming after a restart
    with build_postgres_checkpointer() as checkpointer2:
        compiled2 = graph.compile(checkpointer=checkpointer2)
        state = compiled2.get_state(config)
        assert state.values["requirement_clarified"] == "seen"
