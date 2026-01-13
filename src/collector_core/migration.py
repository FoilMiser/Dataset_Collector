"""Migration helpers for deprecated imports."""

from __future__ import annotations

import warnings
from typing import NoReturn


def deprecated_import_error(module_name: str, replacement: str) -> NoReturn:
    """Raise ImportError with migration guidance.

    This function is called when deprecated modules are imported directly.

    Args:
        module_name: The deprecated module being imported
        replacement: The replacement command or import path

    Raises:
        ImportError: Always, with migration guidance
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
) -> None:
    """Emit a deprecation warning with migration guidance.

    Args:
        old_usage: Description of the deprecated usage
        new_usage: Description of the replacement
        removal_version: Version when old usage will be removed
    """
    warnings.warn(
        f"{old_usage} is deprecated and will be removed in v{removal_version}. "
        f"Use {new_usage} instead.",
        DeprecationWarning,
        stacklevel=3,
    )
