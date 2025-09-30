import logging
import sys
import uuid
from typing import Optional

import structlog

_CONFIGURED = False


def configure_logging() -> None:
    global _CONFIGURED
    if _CONFIGURED:
        return
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.add_log_level,
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
        cache_logger_on_first_use=True,
    )
    _CONFIGURED = True


def get_logger(**bindings):
    configure_logging()
    return structlog.get_logger(**bindings)


def bind_request(logger, correlation_id: Optional[str], route: str):
    if not correlation_id:
        correlation_id = str(uuid.uuid4())
    return logger.bind(correlation_id=correlation_id, route=route)
