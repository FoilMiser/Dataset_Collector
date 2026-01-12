from __future__ import annotations

import json
import logging
import time
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any


def utc_now() -> str:
    """Return current UTC time in ISO 8601 format."""
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def log_event(logger: logging.Logger, message: str, **fields: Any) -> None:
    """Log a structured message with JSON fields."""
    if fields:
        payload = json.dumps(fields, sort_keys=True, ensure_ascii=False)
        logger.info("%s | %s", message, payload)
    else:
        logger.info("%s", message)


@contextmanager
def log_context(logger: logging.Logger, **fields: Any) -> Iterator[logging.LoggerAdapter]:
    """Bind extra fields to a logger for a block."""
    adapter = logging.LoggerAdapter(logger, extra=fields)
    yield adapter
