"""
collector_core/result.py

Standardized result types for operation outcomes.

Error Handling Convention:
--------------------------
1. **Exceptions** are raised for programmer/config errors:
   - ConfigValidationError: Invalid configuration (missing fields, schema violations)
   - RuntimeError: Internal errors that shouldn't happen in normal operation
   - ValueError: Invalid arguments to functions

2. **Result types** (this module) are returned for recoverable per-target runtime issues:
   - Network failures, timeouts, retries exhausted
   - File download/verification failures
   - External service errors (API rate limits, unavailable resources)
   - Missing optional resources

3. At boundaries (CLI, API responses, JSONL output), Result objects are serialized
   to dicts with a "status" field:
   - {"status": "ok", ...} for success
   - {"status": "error", "error": "error_code", "message": "...", ...} for failures
   - {"status": "noop", "reason": "..."} for skipped operations

Usage:
------
    from collector_core.result import Result, Ok, Err

    def download_file(url: str) -> Result[Path]:
        try:
            # ... download logic ...
            return Ok(path, sha256=hash_value)
        except requests.Timeout:
            return Err("timeout", "Download timed out after 60 seconds")

    result = download_file(url)
    if result.is_ok:
        print(f"Downloaded to {result.value}")
    else:
        print(f"Failed: {result.error}")

    # Serialize for output
    output_dict = result.to_dict()
"""

from __future__ import annotations
from pathlib import Path

from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar

T = TypeVar("T")


@dataclass
class Result(Generic[T]):
    """
    A result type that represents either success (Ok) or failure (Err).

    Attributes:
        status: "ok" for success, "error" for failure, "noop" for skipped
        value: The success value (only meaningful when status="ok")
        error: Error code (only meaningful when status="error")
        message: Human-readable error message
        extras: Additional context (path, sha256, reason, etc.)
    """

    status: str
    value: T | None = None
    error: str | None = None
    message: str | None = None
    extras: dict[str, Any] = field(default_factory=dict)

    @property
    def is_ok(self) -> bool:
        return self.status == "ok"

    @property
    def is_err(self) -> bool:
        return self.status == "error"

    @property
    def is_noop(self) -> bool:
        return self.status == "noop"

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dict for JSON output."""
        d: dict[str, Any] = {"status": self.status}
        if self.status == "ok":
            if self.value is not None:
                d["value"] = self.value
        elif self.status == "error":
            if self.error:
                d["error"] = self.error
            if self.message:
                d["message"] = self.message
        elif self.status == "noop":
            if self.message:
                d["reason"] = self.message
        d.update(self.extras)
        return d

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> Result[Any]:
        """Reconstruct from dict."""
        status = d.get("status", "error")
        error = d.get("error")
        message = d.get("message") or d.get("reason")
        extras = {k: v for k, v in d.items() if k not in ("status", "error", "message", "reason", "value")}
        return cls(
            status=status,
            value=d.get("value"),
            error=error,
            message=message,
            extras=extras,
        )


def Ok(value: T = None, **extras: Any) -> Result[T]:  # noqa: N802 - intentional PascalCase
    """Create a successful result."""
    return Result(status="ok", value=value, extras=extras)


def Err(error: str, message: str | None = None, **extras: Any) -> Result[Any]:  # noqa: N802
    """Create a failure result."""
    return Result(status="error", error=error, message=message, extras=extras)


def Noop(reason: str, **extras: Any) -> Result[Any]:  # noqa: N802
    """Create a no-operation result (skipped)."""
    return Result(status="noop", message=reason, extras=extras)


# Type aliases for common result patterns
DownloadResult = Result[str]  # value is path
FetchResult = Result[bytes]  # value is content
