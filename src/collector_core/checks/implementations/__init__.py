"""Content check implementations.

This package contains individual content check implementations that can be
loaded and configured via the check registry.

Each check module exports:
- check_name: str - The name used in targets YAML
- check(record: dict, config: dict) -> CheckResult
"""

from collector_core.checks.implementations.base import CheckResult

__all__ = ["CheckResult"]
