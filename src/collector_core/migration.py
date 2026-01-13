"""Migration helpers for deprecated imports.

This module provides utilities for managing deprecated code during migration
periods. It helps users transition from old APIs to new ones with clear
guidance and appropriate warnings.

Functions:
    deprecated_import_error: Raise ImportError with migration guidance
    emit_deprecation_warning: Emit a deprecation warning with guidance
    check_migration_status: Check if deprecated code is being used

Example:
    # In a deprecated module:
    from collector_core.migration import deprecated_import_error
    deprecated_import_error(
        "old_module.py",
        "dc run --pipeline <name> --stage acquire"
    )
"""

from __future__ import annotations

import warnings
from typing import NoReturn


def deprecated_import_error(module_name: str, replacement: str) -> NoReturn:
    """Raise ImportError with migration guidance.

    This function is called when deprecated modules are imported directly.
    It provides clear guidance on how to migrate to the new approach.

    Args:
        module_name: The deprecated module being imported
        replacement: The replacement command or import path

    Raises:
        ImportError: Always, with migration guidance

    Example:
        >>> deprecated_import_error(
        ...     "acquire_worker.py",
        ...     "dc run --pipeline math --stage acquire"
        ... )
        Traceback (most recent call last):
            ...
        ImportError: The module 'acquire_worker.py' has been removed in v3.0.
        Use instead: dc run --pipeline math --stage acquire
        See docs/migration_guide.md for details.
    """
    raise ImportError(
        f"The module '{module_name}' has been removed in v3.0.\n"
        f"Use instead: {replacement}\n"
        f"See docs/migration_guide.md for details."
    )


def emit_deprecation_warning(
    old_usage: str,
    new_usage: str,
    removal_version: str = "4.0",
    stacklevel: int = 3,
) -> None:
    """Emit a deprecation warning with migration guidance.

    This function emits a standard DeprecationWarning with helpful
    information about how to migrate to the new API.

    Args:
        old_usage: Description of the deprecated usage
        new_usage: Description of the replacement
        removal_version: Version when old usage will be removed
        stacklevel: How many stack frames to skip for the warning location

    Example:
        >>> emit_deprecation_warning(
        ...     "acquire_worker.py",
        ...     "dc run --pipeline <name> --stage acquire",
        ...     removal_version="4.0"
        ... )
        # UserWarning: acquire_worker.py is deprecated and will be removed
        # in v4.0. Use dc run --pipeline <name> --stage acquire instead.
    """
    warnings.warn(
        f"{old_usage} is deprecated and will be removed in v{removal_version}. "
        f"Use {new_usage} instead.",
        DeprecationWarning,
        stacklevel=stacklevel,
    )


def emit_future_warning(
    old_usage: str,
    new_usage: str,
    change_version: str = "4.0",
    stacklevel: int = 3,
) -> None:
    """Emit a FutureWarning for behavior changes.

    Use this for cases where behavior will change in a future version,
    rather than being removed entirely.

    Args:
        old_usage: Description of the current behavior
        new_usage: Description of the future behavior
        change_version: Version when behavior will change
        stacklevel: How many stack frames to skip for the warning location
    """
    warnings.warn(
        f"{old_usage} behavior will change in v{change_version}. "
        f"Future behavior: {new_usage}.",
        FutureWarning,
        stacklevel=stacklevel,
    )


def check_migration_status(component: str) -> dict[str, str]:
    """Check migration status for a component.

    Returns information about the migration status of a component,
    including deprecation status and recommended replacements.

    Args:
        component: Name of the component to check

    Returns:
        Dictionary with migration information:
        - status: 'current', 'deprecated', or 'removed'
        - replacement: Replacement if deprecated/removed
        - removal_version: Version when it will be removed
        - notes: Additional migration notes
    """
    # Migration registry for deprecated components
    migrations = {
        "acquire_worker.py": {
            "status": "removed",
            "replacement": "dc run --pipeline <name> --stage acquire",
            "removal_version": "3.0",
            "notes": "Per-pipeline wrapper scripts removed. Use unified CLI.",
        },
        "merge_worker.py": {
            "status": "removed",
            "replacement": "dc run --pipeline <name> --stage merge",
            "removal_version": "3.0",
            "notes": "Per-pipeline wrapper scripts removed. Use unified CLI.",
        },
        "yellow_screen_worker.py": {
            "status": "removed",
            "replacement": "dc run --pipeline <name> --stage yellow_screen",
            "removal_version": "3.0",
            "notes": "Per-pipeline wrapper scripts removed. Use unified CLI.",
        },
        "pipeline_driver.py": {
            "status": "removed",
            "replacement": "dc pipeline <name>",
            "removal_version": "3.0",
            "notes": "Per-pipeline driver scripts removed. Use unified CLI.",
        },
        "catalog_builder.py": {
            "status": "removed",
            "replacement": "dc catalog build",
            "removal_version": "3.0",
            "notes": "Per-pipeline catalog builders removed. Use unified CLI.",
        },
        "review_queue.py": {
            "status": "removed",
            "replacement": "dc review",
            "removal_version": "3.0",
            "notes": "Per-pipeline review queues removed. Use unified CLI.",
        },
        "pmc_worker.py": {
            "status": "removed",
            "replacement": "dc run --pipeline <name> --stage pmc",
            "removal_version": "3.0",
            "notes": "Per-pipeline PMC workers removed. Use unified CLI.",
        },
        "legacy/": {
            "status": "removed",
            "replacement": "dc run or dc pipeline commands",
            "removal_version": "3.0",
            "notes": "Legacy shell scripts removed. Use unified CLI.",
        },
    }

    if component in migrations:
        return migrations[component]

    return {
        "status": "current",
        "replacement": None,
        "removal_version": None,
        "notes": "Component is current and not deprecated.",
    }


def list_deprecated_components() -> list[dict[str, str]]:
    """List all deprecated components and their replacements.

    Returns:
        List of dictionaries with component migration information
    """
    return [
        {
            "component": "acquire_worker.py",
            "replacement": "dc run --pipeline <name> --stage acquire",
            "status": "removed",
        },
        {
            "component": "merge_worker.py",
            "replacement": "dc run --pipeline <name> --stage merge",
            "status": "removed",
        },
        {
            "component": "yellow_screen_worker.py",
            "replacement": "dc run --pipeline <name> --stage yellow_screen",
            "status": "removed",
        },
        {
            "component": "pipeline_driver.py",
            "replacement": "dc pipeline <name>",
            "status": "removed",
        },
        {
            "component": "catalog_builder.py",
            "replacement": "dc catalog build",
            "status": "removed",
        },
        {
            "component": "review_queue.py",
            "replacement": "dc review",
            "status": "removed",
        },
        {
            "component": "pmc_worker.py",
            "replacement": "dc run --pipeline <name> --stage pmc",
            "status": "removed",
        },
        {
            "component": "legacy/",
            "replacement": "dc run or dc pipeline commands",
            "status": "removed",
        },
    ]


__all__ = [
    "deprecated_import_error",
    "emit_deprecation_warning",
    "emit_future_warning",
    "check_migration_status",
    "list_deprecated_components",
]
