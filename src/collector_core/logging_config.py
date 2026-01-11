from __future__ import annotations

import contextvars
import json
import logging
import time
from typing import Any

from collector_core.secrets import redact_string, redact_structure

_CONFIGURED = False

# Context variables for structured logging
_log_context: contextvars.ContextVar[dict[str, Any] | None] = contextvars.ContextVar(
    "log_context", default=None
)


def get_log_context() -> dict[str, Any]:
    """Get the current logging context."""
    ctx = _log_context.get()
    return ctx.copy() if ctx else {}


def set_log_context(**kwargs: Any) -> None:
    """Set the current logging context (replaces existing)."""
    _log_context.set(dict(kwargs))


def update_log_context(**kwargs: Any) -> None:
    """Update the current logging context (merges with existing)."""
    ctx = _log_context.get()
    current = ctx.copy() if ctx else {}
    current.update(kwargs)
    _log_context.set(current)


def clear_log_context() -> None:
    """Clear the current logging context."""
    _log_context.set({})


class LogContext:
    """Context manager for temporary log context."""

    def __init__(self, **kwargs: Any):
        self.kwargs = kwargs
        self.token: contextvars.Token | None = None

    def __enter__(self) -> LogContext:
        current = _log_context.get().copy()
        current.update(self.kwargs)
        self.token = _log_context.set(current)
        return self

    def __exit__(self, *args: Any) -> None:
        if self.token is not None:
            _log_context.reset(self.token)


class TextFormatter(logging.Formatter):
    def __init__(self) -> None:
        super().__init__("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
        self.converter = time.gmtime

    def format(self, record: logging.LogRecord) -> str:
        original_msg = record.msg
        original_args = record.args
        record.msg = redact_structure(record.msg)
        record.args = redact_structure(record.args)
        try:
            formatted = super().format(record)
        finally:
            record.msg = original_msg
            record.args = original_args
        return redact_string(formatted)


class JsonFormatter(logging.Formatter):
    def __init__(self) -> None:
        super().__init__()
        self.converter = time.gmtime

    def format(self, record: logging.LogRecord) -> str:
        message = self._format_message(record)
        payload: dict[str, Any] = {
            "timestamp": self.formatTime(record, "%Y-%m-%dT%H:%M:%SZ"),
            "level": record.levelname,
            "logger": record.name,
            "message": message,
        }

        # Include structured context
        context = get_log_context()
        if context:
            payload["context"] = redact_structure(context)

        if record.exc_info:
            payload["exc_info"] = redact_string(self.formatException(record.exc_info))
        return json.dumps(payload, ensure_ascii=False)

    @staticmethod
    def _format_message(record: logging.LogRecord) -> str:
        msg = redact_structure(record.msg)
        args = redact_structure(record.args)
        if args:
            try:
                return redact_string(str(msg) % args)
            except Exception:
                return redact_string(str(msg))
        return redact_string(str(msg))


def _resolve_level(level: str | int | None) -> int:
    if isinstance(level, int):
        return level
    if level is None:
        return logging.INFO
    return logging._nameToLevel.get(str(level).upper(), logging.INFO)


def configure_logging(*, level: str | int | None = None, fmt: str = "text") -> None:
    global _CONFIGURED
    if _CONFIGURED:
        return

    root = logging.getLogger()
    root.setLevel(_resolve_level(level))

    if not root.handlers:
        handler = logging.StreamHandler()
        if fmt.lower() == "json":
            handler.setFormatter(JsonFormatter())
        else:
            handler.setFormatter(TextFormatter())
        root.addHandler(handler)

    _CONFIGURED = True


def add_logging_args(parser: Any) -> None:
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level (default: INFO)",
    )
    parser.add_argument(
        "--log-format",
        default="text",
        choices=["text", "json"],
        help="Logging format (default: text)",
    )
