from __future__ import annotations

from typing import Type

from collector_core.checks.base import BaseCheck

_CHECK_REGISTRY: dict[str, Type[BaseCheck]] = {}


def register_check(check_cls: Type[BaseCheck]) -> Type[BaseCheck]:
    name = check_cls.check_name().strip()
    if not name:
        raise ValueError("Check classes must define a non-empty name.")
    _CHECK_REGISTRY[name] = check_cls
    check_cls.name = name
    return check_cls


def get_check(name: str) -> Type[BaseCheck] | None:
    return _CHECK_REGISTRY.get(name)


def list_checks() -> list[str]:
    return sorted(_CHECK_REGISTRY.keys())
