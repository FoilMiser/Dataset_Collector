from __future__ import annotations

import importlib
from typing import Any


def _try_import(module: str, attr: str | None = None) -> Any | None:
    try:
        imported = importlib.import_module(module)
    except ImportError:
        return None
    if attr:
        return getattr(imported, attr, None)
    return imported


def requires(name: str, dependency: Any, *, install: str | None = None) -> str | None:
    if dependency is not None:
        return None
    hint = f" (install: {install})" if install else ""
    return f"missing dependency: {name}{hint}"
