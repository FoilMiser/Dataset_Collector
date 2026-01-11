from __future__ import annotations

import json
from functools import cache
from pathlib import Path
from typing import Any

try:
    import yaml
except ModuleNotFoundError:  # pragma: no cover - fallback for minimal environments
    from collector_core import yaml_lite as yaml

from collector_core.exceptions import ConfigValidationError, YamlParseError

try:
    from jsonschema import Draft7Validator, FormatChecker
except ImportError:  # pragma: no cover - optional in some environments
    Draft7Validator = None
    FormatChecker = None

_DEFAULT_SCHEMA_DIR = Path(__file__).resolve().parents[1] / "schemas"
_FALLBACK_SCHEMA_DIR = Path(__file__).resolve().parents[2] / "schemas"
SCHEMA_DIR = _DEFAULT_SCHEMA_DIR if _DEFAULT_SCHEMA_DIR.exists() else _FALLBACK_SCHEMA_DIR


@cache
def load_schema(schema_name: str) -> dict[str, Any]:
    schema_path = SCHEMA_DIR / f"{schema_name}.schema.json"
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
    return data
