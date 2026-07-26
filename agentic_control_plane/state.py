"""Shared LangGraph state schema for the agentic SDLC orchestrator."""

from __future__ import annotations

import operator
import uuid
from datetime import datetime, timezone
from typing import Annotated, Literal

from pydantic import BaseModel, Field

ScenarioType = Literal["greenfield", "brownfield", "ambiguous"]
OrchestratorMode = Literal["live", "replay"]
RunStatus = Literal["running", "completed", "failed"]
GateType = Literal[
    "clarification_approval",
    "codebase_impact_review",
    "plan_approval",
    "replanning_approval",
    "merge_release_approval",
]
GateDecisionStatus = Literal["pending", "approved", "rejected", "edited"]
DecidedBy = Literal["human", "replayed"]
RouteHint = Literal["", "proceed", "retry", "fallback_attempt", "rollback"]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class AuditEvent(BaseModel):
    """One append-only record of a node execution, gate decision, retry, or rollback."""

    timestamp: datetime = Field(default_factory=_utc_now)
    node: str
    event_type: Literal[
        "node_start",
        "node_end",
        "gate_decision",
        "retry",
        "fallback",
        "rollback",
        "safe_stop",
        "guardrail_violation",
    ]
    detail: str = ""
    decision: str | None = None
    latency_ms: float | None = None


class GateRecord(BaseModel):
    """A single human-approval checkpoint and how it was resolved."""

    gate_type: GateType
    status: GateDecisionStatus = "pending"
    decision_payload: str | None = None
    decided_by: DecidedBy | None = None
    replayed_from_fixture: bool = False
    timestamp: datetime | None = None


class CodebaseImpact(BaseModel):
    """Brownfield impact analysis output. Left at defaults (skipped=True) on greenfield."""

    skipped: bool = True
    impacted_modules: list[str] = Field(default_factory=list)
    impacted_apis: list[str] = Field(default_factory=list)
    summary: str = ""


class ArchitectureDesign(BaseModel):
    """The *how*: component shape, API/schema design, data model changes."""

    summary: str = ""
    api_schema_changes: list[str] = Field(default_factory=list)


class Task(BaseModel):
    """A single unit of work in the decomposed plan."""

    id: str
    description: str
    depends_on: list[str] = Field(default_factory=list)
    status: Literal["pending", "in_progress", "done"] = "pending"


class CoderOutput(BaseModel):
    """Result of one Coder node invocation (live subprocess or replayed fixture)."""

    code_files: dict[str, str] = Field(default_factory=dict)
    attempt_number: int = 0
    fixture_source: str | None = None
    commit_sha_before: str | None = None
    rationale: str = ""


class TestResult(BaseModel):
    """Result of one Test Executor invocation against the current Coder output."""

    passed: bool = False
    failures: list[str] = Field(default_factory=list)
    attempt_number: int = 0
    logs: str = ""


class GuardrailViolation(BaseModel):
    """A policy guardrail hit found in generated code, awaiting or resolved by override."""

    rule: Literal["unsafe_call", "ddl_change", "secret_detected"]
    location: str
    overridden_by_human: bool = False


class DocumentationOutput(BaseModel):
    """Documentation node output for the target app (README/docstrings/API docs)."""

    doc_files: dict[str, str] = Field(default_factory=dict)


class RunMetrics(BaseModel):
    """Reliability metrics computed from the audit trail at run end."""

    success_rate: float | None = None
    retry_frequency: float | None = None
    rollback_frequency: float | None = None
    mttr_seconds: float | None = None
    e2e_latency_seconds: float | None = None


class GraphState(BaseModel):
    """The full shared state threaded through every LangGraph node.

    ``events`` uses an additive reducer (``operator.add``) because the Test Executor
    and Documentation nodes run in parallel and both append audit events to it; every
    other field is written by exactly one node per run, so no reducer is needed there.
    """

    run_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    scenario_type: ScenarioType
    mode: OrchestratorMode = "replay"
    fixture_id: str | None = None
    run_status: RunStatus = "running"

    requirement_raw: str = ""
    requirement_clarified: str = ""
    ambiguities: list[str] = Field(default_factory=list)

    gates: dict[GateType, GateRecord] = Field(default_factory=dict)

    codebase_impact: CodebaseImpact = Field(default_factory=CodebaseImpact)
    architecture_design: ArchitectureDesign = Field(default_factory=ArchitectureDesign)
    tasks: list[Task] = Field(default_factory=list)

    coder: CoderOutput = Field(default_factory=CoderOutput)
    # Every Coder invocation appends here too (operator.add), so a retry loop's earlier
    # (failed) attempts stay inspectable in the audit trail instead of being silently
    # overwritten by `coder` once a later attempt supersedes them - this is exactly the
    # data fixture capture and any "why did it retry" review needs.
    coder_attempts: Annotated[list[CoderOutput], operator.add] = Field(default_factory=list)
    test: TestResult = Field(default_factory=TestResult)
    documentation: DocumentationOutput = Field(default_factory=DocumentationOutput)

    retry_count: int = 0
    retry_limit: int = 3
    fallback_triggered: bool = False
    rollback_count: int = 0
    safe_stop: bool = False
    # Set explicitly by Test Executor rather than re-derived from counters downstream:
    # "fallback_triggered=True and test failed" is ambiguous on its own between "about
    # to try the fallback attempt" and "the fallback attempt just failed too" - the
    # node that observes the transition records which one happened, so graph.py's
    # routing (Phase 3) never has to guess.
    route_hint: RouteHint = ""

    guardrail_violations: list[GuardrailViolation] = Field(default_factory=list)

    replanning_triggered: bool = False
    replanning_reason: str = ""

    events: Annotated[list[AuditEvent], operator.add] = Field(default_factory=list)
    metrics: RunMetrics = Field(default_factory=RunMetrics)

    started_at: datetime = Field(default_factory=_utc_now)
    finished_at: datetime | None = None
