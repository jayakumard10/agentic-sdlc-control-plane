"""Tests for logging_config.py: format and idempotent handler registration."""

from __future__ import annotations

import logging

from agentic_control_plane import logging_config


def test_configure_logging_is_idempotent():
    logging_config._configured = False
    root = logging.getLogger()
    original_handlers = list(root.handlers)
    try:
        logging_config.configure_logging()
        first_count = len(root.handlers)
        logging_config.configure_logging()
        assert len(root.handlers) == first_count
    finally:
        root.handlers = original_handlers
        logging_config._configured = False


def test_configure_logging_respects_explicit_level():
    logging_config._configured = False
    root = logging.getLogger()
    original_handlers = list(root.handlers)
    original_level = root.level
    try:
        logging_config.configure_logging(level="WARNING")
        assert root.level == logging.WARNING
    finally:
        root.handlers = original_handlers
        root.setLevel(original_level)
        logging_config._configured = False
