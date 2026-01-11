from __future__ import annotations

import abc
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class CheckContext:
    run_id: str
    target_id: str
    stage: str
    content_checks: list[str]
    target: dict[str, Any] | None = None
    row: dict[str, Any] | None = None
    extra: dict[str, Any] | None = None


class BaseCheck(abc.ABC):
    name: str = ""
    description: str = ""

    @classmethod
    def check_name(cls) -> str:
        return cls.name or cls.__name__

    @abc.abstractmethod
    def run(self, ctx: CheckContext) -> dict[str, Any]:
        raise NotImplementedError
