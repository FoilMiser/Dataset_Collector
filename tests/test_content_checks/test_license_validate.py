from __future__ import annotations

from collector_core.checks.implementations import license_validate


def test_license_validate_accepts_allowed_license() -> None:
    result = license_validate.check(
        {"license": "MIT"},
        {"allowed_licenses": ["MIT", "Apache-2.0"]},
    )
    assert result.check == license_validate.check_name
    assert result.status == "ok"
    assert result.details["licenses"] == ["MIT"]


def test_license_validate_fails_disallowed_license() -> None:
    result = license_validate.check(
        {"license": "Proprietary"},
        {"allowed_licenses": ["MIT"]},
    )
    assert result.status == "fail"
    assert result.details["allowed_licenses"] == ["MIT"]
