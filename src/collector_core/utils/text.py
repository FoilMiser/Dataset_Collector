from __future__ import annotations

import logging
import re
from typing import Any

logger = logging.getLogger("collector_core.utils")


def normalize_whitespace(text: str) -> str:
    """Collapse all whitespace to single spaces and strip."""
    return re.sub(r"\s+", " ", (text or "")).strip()


def lower(text: str) -> str:
    """Lowercase string, handling None."""
    return (text or "").lower()


def safe_text(value: Any) -> str:
    """Convert value to string, handling None."""
    return "" if value is None else str(value)


def contains_any(haystack: str, needles: list[str]) -> list[str]:
    """Return list of needles found in haystack (case-insensitive)."""
    h = lower(haystack)
    return [n for n in needles if n and lower(n) in h]


def coerce_int(val: Any, default: int | None = None) -> int | None:
    """Safely convert value to int, returning default on failure."""
    try:
        return int(val)
    except Exception:
        logger.debug("Failed to coerce value %r to int, using default %r", val, default)
        return default
