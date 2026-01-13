from __future__ import annotations

from collector_core.pipeline_driver_base import (
    SUPPORTED_CONTENT_CHECKS,
    load_content_check_registry,
)


def test_content_check_registry_filters_supported_checks() -> None:
    registry = load_content_check_registry()
    assert set(registry).issubset(SUPPORTED_CONTENT_CHECKS)
    for name in (
        "distribution_statement",
        "language_detect",
        "license_validate",
        "schema_validate",
        "toxicity_scan",
    ):
        assert name in registry
