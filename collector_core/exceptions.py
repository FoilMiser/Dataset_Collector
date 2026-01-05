from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping


@dataclass
class CollectorError(Exception):
    message: str
    code: str = "collector_error"
    context: dict[str, Any] = field(default_factory=dict)

    def __init__(self, message: str, *, code: str | None = None, context: Mapping[str, Any] | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.code = code or self.code
        self.context = dict(context or {})

    def as_log_fields(self) -> dict[str, Any]:
        return {
            "error_code": self.code,
            "error_message": self.message,
            "error_context": self.context,
        }


class DependencyMissingError(CollectorError):
    code = "missing_dependency"

    def __init__(self, message: str, *, dependency: str, install: str | None = None) -> None:
        context = {"dependency": dependency}
        if install:
            context["install"] = install
        super().__init__(message, context=context)


class ConfigValidationError(CollectorError):
    code = "config_validation_error"


class YamlParseError(CollectorError):
    code = "yaml_parse_error"


class OutputPathsBuilderError(CollectorError):
    code = "output_paths_builder_required"
