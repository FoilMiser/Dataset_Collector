"""
Path Templates - Variable expansion for paths in targets YAML.

Issue 5.2 (v3.0): Targets YAML uses templates instead of fixed absolute paths.
Works on Windows + Linux (path handling is robust).

Supported templates:
- ${DATASET_ROOT} - Dataset root directory
- ${REPO_ROOT} - Repository root directory
- ${HOME} - User home directory
- ${PIPELINE} - Pipeline domain name
- ${DOMAIN} - Alias for ${PIPELINE}
"""

from __future__ import annotations

import os
import re
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any

from collector_core.dataset_root import resolve_dataset_root
from collector_core.stability import stable_api


# Template pattern: ${VAR_NAME} or $VAR_NAME
TEMPLATE_PATTERN = re.compile(r"\$\{([A-Z_][A-Z0-9_]*)\}|\$([A-Z_][A-Z0-9_]*)")


@stable_api
class PathTemplateContext:
    """Context for path template expansion.

    Provides all variables available for template substitution.
    """

    def __init__(
        self,
        *,
        dataset_root: Path | str | None = None,
        repo_root: Path | str | None = None,
        pipeline: str | None = None,
        extra_vars: dict[str, str] | None = None,
    ):
        self._vars: dict[str, str] = {}

        # DATASET_ROOT from explicit value, env var, or None
        if dataset_root:
            self._vars["DATASET_ROOT"] = str(Path(dataset_root).expanduser().resolve())
        else:
            resolved = resolve_dataset_root()
            if resolved:
                self._vars["DATASET_ROOT"] = str(resolved)

        # REPO_ROOT
        if repo_root:
            self._vars["REPO_ROOT"] = str(Path(repo_root).expanduser().resolve())

        # HOME directory
        self._vars["HOME"] = str(Path.home())

        # Pipeline/domain
        if pipeline:
            self._vars["PIPELINE"] = pipeline
            self._vars["DOMAIN"] = pipeline

        # Environment variables as fallback
        for key in ["DATASET_ROOT", "DATASET_COLLECTOR_ROOT", "REPO_ROOT"]:
            if key not in self._vars and os.getenv(key):
                self._vars[key] = os.path.expanduser(os.getenv(key, ""))

        # Extra custom variables
        if extra_vars:
            self._vars.update(extra_vars)

    def get(self, name: str) -> str | None:
        """Get a variable value."""
        return self._vars.get(name)

    def set(self, name: str, value: str) -> None:
        """Set a variable value."""
        self._vars[name] = value

    def has(self, name: str) -> bool:
        """Check if a variable is defined."""
        return name in self._vars

    @property
    def variables(self) -> dict[str, str]:
        """Get all defined variables."""
        return dict(self._vars)


@stable_api
def expand_path_template(
    template: str,
    ctx: PathTemplateContext,
    *,
    strict: bool = False,
) -> str:
    """
    Expand template variables in a path string.

    Args:
        template: Path string with ${VAR} or $VAR placeholders
        ctx: Template context with variable values
        strict: If True, raise ValueError for undefined variables

    Returns:
        Expanded path string

    Raises:
        ValueError: If strict=True and a variable is undefined
    """
    def replace(match: re.Match[str]) -> str:
        var_name = match.group(1) or match.group(2)
        value = ctx.get(var_name)
        if value is None:
            if strict:
                raise ValueError(f"Undefined template variable: ${{{var_name}}}")
            # Keep original if not strict
            return match.group(0)
        return value

    return TEMPLATE_PATTERN.sub(replace, template)


@stable_api
def expand_path(
    template: str | Path,
    ctx: PathTemplateContext,
    *,
    strict: bool = False,
) -> Path:
    """
    Expand template variables and return a Path object.

    Handles platform-specific path separators correctly.
    """
    expanded = expand_path_template(str(template), ctx, strict=strict)
    return Path(expanded).expanduser()


@stable_api
def normalize_path_for_platform(path_str: str) -> str:
    """
    Normalize a path string for the current platform.

    Converts forward slashes to backslashes on Windows,
    and vice versa on Unix.
    """
    if os.name == "nt":
        # Windows: convert forward slashes to backslashes
        # but preserve network paths (\\server\share)
        if path_str.startswith("//") or path_str.startswith("\\\\"):
            return path_str.replace("/", "\\")
        return path_str.replace("/", "\\")
    else:
        # Unix: convert backslashes to forward slashes
        return path_str.replace("\\", "/")


@stable_api
def expand_paths_in_config(
    config: dict[str, Any],
    ctx: PathTemplateContext,
    *,
    path_keys: set[str] | None = None,
) -> dict[str, Any]:
    """
    Recursively expand path templates in a configuration dictionary.

    Args:
        config: Configuration dictionary
        ctx: Template context
        path_keys: Keys to treat as paths (default: common path keys)

    Returns:
        New dictionary with expanded paths
    """
    if path_keys is None:
        path_keys = {
            "path",
            "root",
            "dir",
            "directory",
            "output",
            "input",
            "raw_root",
            "screened_root",
            "combined_root",
            "manifests_root",
            "queues_root",
            "catalogs_root",
            "ledger_root",
            "pitches_root",
            "logs_root",
            "dataset_root",
            "targets_path",
            "output_path",
            "permissive",
            "copyleft",
            "quarantine",
        }

    def expand_value(key: str, value: Any) -> Any:
        if isinstance(value, str):
            # Check if this looks like a path (contains template or path sep)
            is_path_key = any(pk in key.lower() for pk in path_keys)
            has_template = "$" in value
            has_path_sep = "/" in value or "\\" in value

            if is_path_key or has_template:
                expanded = expand_path_template(value, ctx, strict=False)
                if has_path_sep or has_template:
                    expanded = normalize_path_for_platform(expanded)
                return expanded
            return value
        elif isinstance(value, dict):
            return {k: expand_value(k, v) for k, v in value.items()}
        elif isinstance(value, list):
            return [expand_value(key, item) for item in value]
        return value

    return {k: expand_value(k, v) for k, v in config.items()}


@stable_api
def find_templates_in_config(config: dict[str, Any]) -> list[tuple[str, str]]:
    """
    Find all template variables used in a configuration.

    Returns:
        List of (path, variable_name) tuples
    """
    templates: list[tuple[str, str]] = []

    def search(obj: Any, path: str = "") -> None:
        if isinstance(obj, str):
            for match in TEMPLATE_PATTERN.finditer(obj):
                var_name = match.group(1) or match.group(2)
                templates.append((path, var_name))
        elif isinstance(obj, dict):
            for key, value in obj.items():
                search(value, f"{path}.{key}" if path else key)
        elif isinstance(obj, list):
            for i, item in enumerate(obj):
                search(item, f"{path}[{i}]")

    search(config)
    return templates


@stable_api
def validate_template_context(
    config: dict[str, Any],
    ctx: PathTemplateContext,
) -> list[str]:
    """
    Validate that all templates in config have defined values in context.

    Returns:
        List of error messages for undefined variables
    """
    errors: list[str] = []
    for path, var_name in find_templates_in_config(config):
        if not ctx.has(var_name):
            errors.append(f"Undefined template ${{{var_name}}} at {path}")
    return errors


__all__ = [
    "PathTemplateContext",
    "expand_path",
    "expand_path_template",
    "expand_paths_in_config",
    "find_templates_in_config",
    "normalize_path_for_platform",
    "validate_template_context",
]
