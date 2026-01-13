from __future__ import annotations

import json
import re
from importlib import resources
from functools import cache
from pathlib import Path
from typing import Any

try:
    import yaml
except ModuleNotFoundError:  # pragma: no cover - fallback for minimal environments
    from collector_core import yaml_lite as yaml

from collector_core.exceptions import ConfigValidationError, YamlParseError
from collector_core.schema_version import validate_schema_version

try:
    from jsonschema import Draft7Validator, FormatChecker
except ImportError:  # pragma: no cover - optional in some environments
    Draft7Validator = None
    FormatChecker = None

_FALLBACK_SCHEMA_DIR = Path(__file__).resolve().parent / "schemas"


def _load_schema_from_package(schema_name: str) -> dict[str, Any] | None:
    try:
        schema_path = resources.files("collector_core").joinpath(
            "schemas",
            f"{schema_name}.schema.json",
        )
        return json.loads(schema_path.read_text(encoding="utf-8"))
    except (FileNotFoundError, ModuleNotFoundError, AttributeError):
        return None


@cache
def load_schema(schema_name: str) -> dict[str, Any]:
    schema = _load_schema_from_package(schema_name)
    if schema is not None:
        return schema
    schema_path = _FALLBACK_SCHEMA_DIR / f"{schema_name}.schema.json"
    if not schema_path.exists():
        raise FileNotFoundError(f"Schema not found: {schema_path}")
    return json.loads(schema_path.read_text(encoding="utf-8"))


def validate_config(config: Any, schema_name: str, *, config_path: Path | None = None) -> None:
    if Draft7Validator is None or FormatChecker is None:
        return
    schema = load_schema(schema_name)
    validator = Draft7Validator(schema, format_checker=FormatChecker())
    errors = sorted(validator.iter_errors(config), key=lambda exc: list(exc.path))
    if not errors:
        return
    location = str(config_path) if config_path else "<config>"
    lines = [f"Schema validation failed for {location} ({schema_name})."]
    error_details: list[dict[str, str]] = []
    for error in errors[:10]:
        path = ".".join(str(p) for p in error.path) if error.path else "<root>"
        lines.append(f"- {path}: {error.message}")
        error_details.append({"path": path, "message": error.message})
    if len(errors) > 10:
        lines.append(f"... and {len(errors) - 10} more errors.")
    raise ConfigValidationError(
        "\n".join(lines),
        context={
            "path": location,
            "schema": schema_name,
            "errors": error_details,
            "truncated": len(errors) > 10,
        },
    )


def read_yaml(path: Path, schema_name: str | None = None) -> Any:
    text = path.read_text(encoding="utf-8")
    text = _expand_includes(text, path.parent)
    try:
        data = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise YamlParseError(
            f"YAML parse error in {path}: {exc}",
            context={"path": str(path), "error": str(exc)},
        ) from exc
    if data is None:
        data = {}
    if schema_name:
        validate_config(data, schema_name, config_path=path)
        validate_schema_version(schema_name, data)
    return data


_INCLUDE_PATTERN = re.compile(r"^(?P<indent>\s*)(?P<key>[^:#]+:\s*)!include\s+(?P<path>.+)$")


def _expand_includes(text: str, base_dir: Path) -> str:
    lines: list[str] = []
    trailing_newline = text.endswith("\n")
    for raw_line in text.splitlines():
        match = _INCLUDE_PATTERN.match(raw_line)
        if not match:
            lines.append(raw_line)
            continue
        indent = match.group("indent")
        key = match.group("key").rstrip()
        raw_path = match.group("path").split("#", 1)[0].strip()
        raw_path = raw_path.strip("'\"")
        include_path = Path(raw_path)
        if not include_path.is_absolute():
            include_path = (base_dir / include_path).resolve()
        include_text = include_path.read_text(encoding="utf-8")
        expanded = _expand_includes(include_text, include_path.parent)
        indented_lines = [
            f"{indent}  {line}" if line else f"{indent}  {line}"
            for line in expanded.splitlines()
        ]
        lines.append(f"{indent}{key}")
        lines.extend(indented_lines)
    result = "\n".join(lines)
    if trailing_newline:
        result += "\n"
    return result
