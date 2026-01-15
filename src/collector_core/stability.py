from __future__ import annotations

from typing import TypeVar

T = TypeVar("T")


def stable_api(obj: T) -> T:
    """Annotate a public API object as stable for tooling and documentation."""
    try:
        obj.__stability__ = "stable"
    except (AttributeError, TypeError):
        # P1.1A: Some objects (e.g., built-ins) don't support setattr
        pass
    return obj
