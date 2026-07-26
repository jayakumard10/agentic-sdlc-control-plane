"""Checkpointer construction: a single source of truth for the msgpack serde config.

`GraphState` nests several custom Pydantic submodels (CodebaseImpact, CoderOutput,
etc.). Without an explicit allowlist, LangGraph's checkpoint serializer falls back to
an "unregistered type" path it warns will be blocked in a future version. Building
the allowlist here - by discovering every BaseModel defined in state.py - keeps it
self-maintaining as state.py grows, and keeps MemorySaver (tests) and PostgresSaver
(production) configured identically instead of drifting apart.

The checkpointer is what makes a parked human-approval gate durable: a run
interrupted at a gate lives entirely in Postgres, so any process can resume it
later by thread_id, and no in-memory consumer state is load-bearing.
"""

from __future__ import annotations

import inspect
import logging
import os
from collections.abc import Iterator
from contextlib import contextmanager
from urllib.parse import quote

from langgraph.checkpoint.memory import MemorySaver
from langgraph.checkpoint.postgres import PostgresSaver
from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer
from pydantic import BaseModel

from agentic_control_plane import state as state_module

logger = logging.getLogger(__name__)


def _discover_state_model_allowlist() -> list[tuple[str, str]]:
    allowlist: list[tuple[str, str]] = []
    for name, obj in inspect.getmembers(state_module, inspect.isclass):
        if issubclass(obj, BaseModel) and obj.__module__ == state_module.__name__:
            allowlist.append((state_module.__name__, name))
    return allowlist


def build_serde() -> JsonPlusSerializer:
    return JsonPlusSerializer(allowed_msgpack_modules=_discover_state_model_allowlist())


def build_memory_checkpointer() -> MemorySaver:
    """In-memory checkpointer for unit/smoke tests only.

    The running control plane always uses PostgresSaver (build_postgres_checkpointer
    below) so a pending approval gate survives a container restart.
    """
    logger.info("Using MemorySaver checkpointer (in-memory, not durable)")
    return MemorySaver(serde=build_serde())


def _read_secret(env_var: str, default: str = "") -> str:
    """Read env_var directly, or from the file at env_var + '_FILE' if set.

    The same Docker-secrets convention the official postgres image uses for its own
    POSTGRES_PASSWORD_FILE. The password must never be a plaintext compose
    environment value: anything in `environment:` is readable via `docker inspect`
    and `docker compose config`, which is exactly what file-based secrets avoid.
    """
    direct = os.environ.get(env_var)
    if direct is not None:
        return direct
    file_path = os.environ.get(f"{env_var}_FILE")
    if file_path:
        with open(file_path, encoding="utf-8") as f:
            return f.read().strip()
    return default


def _postgres_conn_string() -> str:
    """Build the connection URI, percent-encoding the credential components.

    A password containing '@', '/', ':' or '#' - all legal in a generated
    password - would otherwise silently produce a URI that parses into the wrong
    host or database rather than failing cleanly.
    """
    user = os.environ.get("POSTGRES_USER", "control_plane")
    password = _read_secret("POSTGRES_PASSWORD", "control_plane")
    host = os.environ.get("POSTGRES_HOST", "postgres")
    port = os.environ.get("POSTGRES_PORT", "5432")
    database = os.environ.get("POSTGRES_DB", "control_plane")
    return (
        f"postgresql://{quote(user, safe='')}:{quote(password, safe='')}"
        f"@{host}:{port}/{database}"
    )


@contextmanager
def build_postgres_checkpointer() -> Iterator[PostgresSaver]:
    """Durable checkpointer for the running control plane.

    A pending approval gate must survive a container restart; MemorySaver would
    silently lose it. `PostgresSaver.from_conn_string` constructs its instance
    internally without a serde argument, so the shared allowlisted serde is assigned
    right after entering the context, before `.setup()` (idempotent - safe to call on
    every startup) creates the checkpoint tables if they don't already exist.
    """
    host = os.environ.get("POSTGRES_HOST", "postgres")
    port = os.environ.get("POSTGRES_PORT", "5432")
    database = os.environ.get("POSTGRES_DB", "control_plane")
    logger.info("Using PostgresSaver checkpointer at %s:%s/%s", host, port, database)
    with PostgresSaver.from_conn_string(_postgres_conn_string()) as checkpointer:
        checkpointer.serde = build_serde()
        checkpointer.setup()
        yield checkpointer
