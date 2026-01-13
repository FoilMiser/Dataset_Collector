from __future__ import annotations

from collections.abc import Callable
from typing import Any

from collector_core.checks.implementations import (
    distribution_statement,
    language_detect,
    license_validate,
    pii_detect,
    schema_validate,
    toxicity_scan,
)

ContentCheck = Callable[[dict[str, Any], dict[str, Any]], Any]

CHECK_IMPLEMENTATIONS: dict[str, ContentCheck] = {
    distribution_statement.check_name: distribution_statement.check,
    language_detect.check_name: language_detect.check,
    license_validate.check_name: license_validate.check,
    pii_detect.check_name: pii_detect.check,
    schema_validate.check_name: schema_validate.check,
    toxicity_scan.check_name: toxicity_scan.check,
}


def load_check_implementations() -> dict[str, ContentCheck]:
    return dict(CHECK_IMPLEMENTATIONS)


__all__ = ["CHECK_IMPLEMENTATIONS", "ContentCheck", "load_check_implementations"]
