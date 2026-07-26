"""Central stdlib logging configuration for the orchestrator.

Distinct from telemetry.py's AuditEvent/JSONL system, which is a structured,
domain-specific audit trail (one record per node execution/gate/retry/rollback)
feeding the reliability metrics computed in metrics.py - this is standard, leveled
application logging (DEBUG/INFO/WARNING/ERROR) for operational diagnostics: what a
subprocess call actually returned, whether a git operation succeeded, why a
checkpointer was selected. The two systems serve different audiences - an audit
reviewer wants AuditEvents tied to GraphState; an engineer debugging a stuck run
wants log levels, timestamps, and a stack trace - and are kept separate rather than
conflated into one.
"""

from __future__ import annotations

import logging
import os
import sys

_configured = False


def configure_logging(level: str | None = None) -> None:
    """Configure the root logger once. Safe to call from every entry point -

    subsequent calls are no-ops rather than duplicating handlers.
    """
    global _configured
    if _configured:
        return

    resolved_level = (level or os.environ.get("LOG_LEVEL", "INFO")).upper()
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)-8s %(name)s: %(message)s", "%H:%M:%S")
    )
    logging.basicConfig(level=resolved_level, handlers=[handler], force=True)
    _configured = True
