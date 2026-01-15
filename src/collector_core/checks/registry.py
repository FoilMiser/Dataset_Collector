from __future__ import annotations

from collector_core.checks.base import BaseCheck

_CHECK_REGISTRY: dict[str, type[BaseCheck]] = {}


def register_check(check_cls: type[BaseCheck]) -> type[BaseCheck]:
    name = check_cls.check_name().strip()
    if not name:
        raise ValueError("Check classes must define a non-empty name.")
    _CHECK_REGISTRY[name] = check_cls
    check_cls.name = name
    return check_cls


def get_check(name: str) -> type[BaseCheck] | None:
    return _CHECK_REGISTRY.get(name)


def list_checks() -> list[str]:
    return sorted(_CHECK_REGISTRY.keys())
