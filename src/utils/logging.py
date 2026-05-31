"""
Structured logging.

We use structlog so every log line is a dict that can carry context
(batch_id, layer, table, run_id) without manual string formatting.
In production / Airflow we emit JSON so log aggregators can parse it.
In local development a coloured console renderer is much easier to read.

Usage:

    from src.utils.logging import configure_logging, get_logger, bind_batch_context

    configure_logging()                          # once per process
    log = get_logger(__name__)
    log.info("ingestion_started", table="players", rows=125000)

    with bind_batch_context(batch_id="2026-05-29T15", layer="bronze"):
        log.info("partition_written")            # batch_id + layer auto-included
"""

from __future__ import annotations

import logging
import sys
from contextlib import contextmanager
from typing import Any

import structlog

from src.utils.config import get_config


_CONFIGURED = False


def configure_logging(level: str | None = None, as_json: bool | None = None) -> None:
    """
    Idempotent setup. Safe to call from every module entry point.

    Args:
        level: Override the level (otherwise from config.yaml).
        as_json: Override JSON vs console (otherwise from config.yaml).
    """
    global _CONFIGURED
    if _CONFIGURED:
        return

    cfg = get_config()
    log_level = (level or cfg.logging.level).upper()
    json_output = cfg.logging.as_json if as_json is None else as_json

    # Route stdlib logging through structlog so libraries (airflow, pyspark)
    # get the same formatting.
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=log_level,
    )

    shared_processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    if json_output:
        renderer: Any = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.dev.ConsoleRenderer(colors=True)

    structlog.configure(
        processors=[*shared_processors, renderer],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, log_level, logging.INFO)
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )
    _CONFIGURED = True


def get_logger(name: str | None = None) -> Any:
    """Get a logger; auto-configures on first call."""
    if not _CONFIGURED:
        configure_logging()
    return structlog.get_logger(name)


@contextmanager
def bind_batch_context(**kwargs: Any):
    """
    Bind keys for the duration of a `with` block. All log lines emitted
    inside the block will include the bound keys automatically.

    Typical use: bind batch_id and layer at the top of an Airflow task.
    """
    tokens = structlog.contextvars.bind_contextvars(**kwargs)
    try:
        yield
    finally:
        structlog.contextvars.reset_contextvars(**tokens)
