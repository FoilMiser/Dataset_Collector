"""Base classes and utilities for content checks."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class CheckResult:
    """Result of a content check.
    
    Attributes:
        passed: Whether the check passed
        action: Action to take (keep, filter, flag, reject)
        reason: Human-readable reason
        details: Additional check-specific details
        confidence: Confidence in the result (0.0-1.0)
    """
    passed: bool
    action: str  # keep | filter | flag | reject
    reason: str | None = None
    details: dict[str, Any] = field(default_factory=dict)
    confidence: float = 1.0


__all__ = ["CheckResult"]
