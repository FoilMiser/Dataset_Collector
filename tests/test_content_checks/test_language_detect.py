from __future__ import annotations

from collector_core.checks.implementations import language_detect


def test_language_detect_allows_configured_language() -> None:
    result = language_detect.check(
        {"language": "en"},
        {"allowed_languages": ["en", "es"]},
    )
    assert result.check == language_detect.check_name
    assert result.status == "ok"
    assert result.details["languages"] == ["en"]


def test_language_detect_warns_on_missing_language() -> None:
    result = language_detect.check({}, {"allowed_languages": ["en"]})
    assert result.status == "warn"
    assert result.details["reason"] == "missing_language"
