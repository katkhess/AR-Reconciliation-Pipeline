from __future__ import annotations

import logging
import sys
from typing import Any

import structlog


def configure_logger(service_name: str, log_level: str = "INFO") -> structlog.BoundLogger:
    """Configure structlog with JSON rendering and return a bound logger.

    Args:
        service_name: A short identifier embedded in every log record.
        log_level: Standard Python log level string (DEBUG, INFO, WARNING, …).

    Returns:
        A :class:`structlog.BoundLogger` pre-bound with ``service_name``.
    """
    log_level_int = getattr(logging, log_level.upper(), logging.INFO)

    # Configure the standard library root logger so that structlog output
    # is routed through it.
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=log_level_int,
    )

    shared_processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    structlog.configure(
        processors=[
            *shared_processors,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(log_level_int),
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    return structlog.get_logger(service_name).bind(service=service_name)
