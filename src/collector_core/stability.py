from __future__ import annotations

from typing import TypeVar

T = TypeVar("T")


def stable_api(obj: T) -> T:
    """Annotate a public API object as stable for tooling and documentation."""
    try:
        setattr(obj, "__stability__", "stable")
    except Exception:
        pass
    return obj
