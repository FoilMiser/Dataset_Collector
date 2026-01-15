from __future__ import annotations

import json
import re
from functools import cache
from importlib import resources
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


def _find_repo_root(start_path: Path) -> Path:
    """Find repository root by walking up to find .git directory.

    Falls back to start_path if no .git found (for testing/non-git scenarios).
    """
    current = start_path.resolve()
    while current != current.parent:
        if (current / ".git").exists():
            return current
        current = current.parent
    # Fallback: return the original path if no .git found
    return start_path.resolve()


def _expand_includes(text: str, base_dir: Path, repo_root: Path | None = None) -> str:
    lines: list[str] = []
    trailing_newline = text.endswith("\n")

    # Determine repository root for security boundary check
    if repo_root is None:
        repo_root = _find_repo_root(base_dir)

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
            include_path = base_dir / include_path

        # P0.6: Symlinks not allowed in includes (prevent symlink attacks)
        # Check BEFORE resolving, so we can detect symlinks
        if include_path.is_symlink():
            raise ValueError(f"Symlinks not allowed in includes: {include_path}")

        # Resolve the path after symlink check
        include_path = include_path.resolve()

        # P0.6B: Verify resolved path is within repository root (prevent path traversal)
        # Use repo_root instead of base_dir to allow legitimate cross-directory includes
        try:
            if not include_path.is_relative_to(repo_root):
                raise ValueError(
                    f"Include path escapes repository: {include_path} "
                    f"(repo root: {repo_root})"
                )
        except ValueError:
            raise ValueError(
                f"Include path escapes repository: {include_path} "
                f"(repo root: {repo_root})"
            ) from None

        include_text = include_path.read_text(encoding="utf-8")
        # Pass repo_root to recursive calls to maintain security boundary
        expanded = _expand_includes(include_text, include_path.parent, repo_root=repo_root)
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
