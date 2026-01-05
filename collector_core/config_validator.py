from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from jsonschema import Draft7Validator, FormatChecker

SCHEMA_DIR = Path(__file__).resolve().parents[1] / "schemas"


class ConfigValidationError(ValueError):
    pass


@lru_cache(maxsize=None)
def load_schema(schema_name: str) -> dict[str, Any]:
    schema_path = SCHEMA_DIR / f"{schema_name}.schema.json"
    if not schema_path.exists():
        raise FileNotFoundError(f"Schema not found: {schema_path}")
    return json.loads(schema_path.read_text(encoding="utf-8"))


def validate_config(config: Any, schema_name: str, *, config_path: Path | None = None) -> None:
    schema = load_schema(schema_name)
    validator = Draft7Validator(schema, format_checker=FormatChecker())
    errors = sorted(validator.iter_errors(config), key=lambda exc: list(exc.path))
    if not errors:
        return
    location = str(config_path) if config_path else "<config>"
    lines = [f"Schema validation failed for {location} ({schema_name})."]
    for error in errors[:10]:
        path = ".".join(str(p) for p in error.path) if error.path else "<root>"
        lines.append(f"- {path}: {error.message}")
    if len(errors) > 10:
        lines.append(f"... and {len(errors) - 10} more errors.")
    raise ConfigValidationError("\n".join(lines))


def read_yaml(path: Path, schema_name: str | None = None) -> Any:
    text = path.read_text(encoding="utf-8")
    try:
        data = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise RuntimeError(f"YAML parse error in {path}: {exc}") from exc
    if data is None:
        data = {}
    if schema_name:
        validate_config(data, schema_name, config_path=path)
    return data
