"""Assembles the 9 nodes into the real LangGraph StateGraph: conditional routing,

parallel fan-out/join, the bounded retry -> fallback -> rollback -> safe-stop
governance chain, and the checkpointer that makes human-approval gates durable.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING

from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.graph import END, START, StateGraph

from agentic_control_plane import tools
from agentic_control_plane.nodes.architecture_design import architecture_design
from agentic_control_plane.nodes.codebase_reasoner import codebase_reasoner
from agentic_control_plane.nodes.coder import coder
from agentic_control_plane.nodes.decomposer_planner import decomposer_planner
from agentic_control_plane.nodes.documentation import documentation
from agentic_control_plane.nodes.release_gate import release_gate
from agentic_control_plane.nodes.replanner import replanner
from agentic_control_plane.nodes.requirement_clarifier import requirement_clarifier
from agentic_control_plane.nodes.test_executor import test_executor
from agentic_control_plane.state import AuditEvent, GraphState

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from langgraph.graph.state import CompiledStateGraph


def _sync(state: GraphState) -> dict:
    """No-op join barrier: Test Executor and Documentation both edge into this node,

    so LangGraph waits for both parallel branches before routing onward from here.
    """
    return {}


def _rollback(state: GraphState, workspace: Path) -> dict:
    """Rollback: revert the workspace to the last commit that existed before this

    run's Coder wrote anything. Safe-stops if there is no such commit to revert to,
    or if the git revert itself fails - matching the governance model's definition
    that safe-stop is specifically "rollback itself fails or state is unrecoverable".
    """
    start = time.monotonic()
    target_commit = state.coder.commit_sha_before
    updates: dict = {"rollback_count": state.rollback_count + 1}

    if not target_commit:
        logger.error("Rollback safe-stop: no known-good commit recorded for %s", workspace)
        updates["safe_stop"] = True
        updates["run_status"] = "failed"
        updates["finished_at"] = datetime.now(timezone.utc)
        updates["events"] = [
            AuditEvent(
                node="rollback",
                event_type="safe_stop",
                detail="no known-good commit recorded to roll back to",
                latency_ms=(time.monotonic() - start) * 1000,
            )
        ]
        return updates

    try:
        tools.git_revert_to(workspace, target_commit)
        logger.info("Rolled back %s to %s", workspace, target_commit[:8])
        updates["run_status"] = "failed"
        updates["events"] = [
            AuditEvent(
                node="rollback",
                event_type="rollback",
                detail=f"rolled back to {target_commit[:8]}",
                latency_ms=(time.monotonic() - start) * 1000,
            )
        ]
    except tools.GitOperationError as exc:
        logger.error("Rollback safe-stop: revert to %s failed for %s: %s", target_commit[:8], workspace, exc)
        updates["safe_stop"] = True
        updates["run_status"] = "failed"
        updates["events"] = [
            AuditEvent(
                node="rollback",
                event_type="safe_stop",
                detail=f"rollback failed: {exc}",
                latency_ms=(time.monotonic() - start) * 1000,
            )
        ]

    updates["finished_at"] = datetime.now(timezone.utc)
    return updates


def _route_after_clarifier(state: GraphState) -> str:
    if state.scenario_type == "greenfield":
        return "architecture_design"
    return "codebase_reasoner"


def _route_after_planner(state: GraphState) -> str:
    if (
        state.scenario_type == "ambiguous"
        and state.replanning_triggered
        and "replanning_approval" not in state.gates
    ):
        return "replanner"
    return "coder"


def _route_after_coder(state: GraphState) -> list[str]:
    if state.safe_stop:
        return ["end"]
    return ["test_executor", "documentation"]


def _route_after_sync(state: GraphState) -> str:
    if state.route_hint in ("retry", "fallback_attempt"):
        return "coder"
    if state.route_hint == "rollback":
        return "rollback"
    return "release_gate"


def _route_after_release_gate(state: GraphState) -> str:
    gate = state.gates.get("merge_release_approval")
    if gate is not None and gate.status == "approved":
        return "end"
    return "rollback"


def build_graph(
    workspace: Path, fixtures_dir: Path, checkpointer: BaseCheckpointSaver
) -> "CompiledStateGraph":
    graph = StateGraph(GraphState)

    graph.add_node("requirement_clarifier", requirement_clarifier)
    graph.add_node("codebase_reasoner", partial(codebase_reasoner, workspace=workspace))
    graph.add_node("architecture_design", architecture_design)
    graph.add_node("decomposer_planner", decomposer_planner)
    graph.add_node("replanner", replanner)
    graph.add_node("coder", partial(coder, workspace=workspace, fixtures_dir=fixtures_dir))
    graph.add_node("test_executor", partial(test_executor, workspace=workspace))
    graph.add_node("documentation", partial(documentation, workspace=workspace))
    graph.add_node("sync", _sync)
    graph.add_node("release_gate", partial(release_gate, workspace=workspace))
    graph.add_node("rollback", partial(_rollback, workspace=workspace))

    graph.add_edge(START, "requirement_clarifier")

    graph.add_conditional_edges(
        "requirement_clarifier",
        _route_after_clarifier,
        {"architecture_design": "architecture_design", "codebase_reasoner": "codebase_reasoner"},
    )
    graph.add_edge("codebase_reasoner", "architecture_design")
    graph.add_edge("architecture_design", "decomposer_planner")

    graph.add_conditional_edges(
        "decomposer_planner",
        _route_after_planner,
        {"replanner": "replanner", "coder": "coder"},
    )
    graph.add_edge("replanner", "decomposer_planner")

    graph.add_conditional_edges(
        "coder",
        _route_after_coder,
        {"test_executor": "test_executor", "documentation": "documentation", "end": END},
    )
    graph.add_edge("test_executor", "sync")
    graph.add_edge("documentation", "sync")

    graph.add_conditional_edges(
        "sync",
        _route_after_sync,
        {"coder": "coder", "rollback": "rollback", "release_gate": "release_gate"},
    )

    graph.add_conditional_edges(
        "release_gate",
        _route_after_release_gate,
        {"end": END, "rollback": "rollback"},
    )
    graph.add_edge("rollback", END)

    return graph.compile(checkpointer=checkpointer)
