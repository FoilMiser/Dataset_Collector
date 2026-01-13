"""Tests for language detection content check."""

from __future__ import annotations

import pytest
from collector_core.checks.implementations.language_detect import (
    check, detect_language, check_name,
)
from collector_core.checks.implementations.base import CheckResult


class TestDetectLanguage:
    """Tests for detect_language function."""

    def test_detect_english(self) -> None:
        """English text is detected."""
        text = "The quick brown fox jumps over the lazy dog."
        lang, confidence = detect_language(text)
        assert lang == "en"
        assert confidence > 0.3

    def test_detect_spanish(self) -> None:
        """Spanish text is detected."""
        text = "El rápido zorro marrón salta sobre el perro perezoso."
        lang, confidence = detect_language(text)
        assert lang == "es"
        assert confidence > 0.3

    def test_detect_french(self) -> None:
        """French text is detected."""
        text = "Le renard brun rapide saute par-dessus le chien paresseux."
        lang, confidence = detect_language(text)
        assert lang == "fr"
        assert confidence > 0.2

    def test_detect_german(self) -> None:
        """German text is detected."""
        text = "Der schnelle braune Fuchs springt über den faulen Hund."
        lang, confidence = detect_language(text)
        assert lang == "de"
        assert confidence > 0.2

    def test_unknown_language(self) -> None:
        """Unknown language returns low confidence."""
        text = "xyz abc 123"
        lang, confidence = detect_language(text)
        # May match something but with low confidence
        assert confidence < 0.5


class TestLanguageCheck:
    """Tests for check function."""

    def test_passes_with_matching_language(self) -> None:
        """Record with matching language passes."""
        record = {"text": "This is an English text with many words."}
        config = {"required_languages": ["en"], "min_confidence": 0.3}

        result = check(record, config)

        assert result.passed is True
        assert result.action == "keep"

    def test_fails_with_wrong_language(self) -> None:
        """Record with wrong language fails."""
        record = {"text": "El texto está en español."}
        config = {"required_languages": ["en"], "min_confidence": 0.3}

        result = check(record, config)

        assert result.passed is False
        assert "language_not_allowed" in (result.reason or "")

    def test_no_text_fails(self) -> None:
        """Record with no text fails."""
        record = {"id": "123"}
        config = {}

        result = check(record, config)

        assert result.passed is False
        assert result.reason == "no_text_content"

    def test_low_confidence_flags(self) -> None:
        """Low confidence detection flags the record."""
        record = {"text": "xyz abc"}
        config = {"min_confidence": 0.5}

        result = check(record, config)

        # Should flag due to low confidence
        assert result.action == "flag"
        assert "low_language_confidence" in (result.reason or "")

    def test_any_language_allowed(self) -> None:
        """No required_languages allows any language."""
        record = {"text": "日本語のテキスト。これは日本語です。"}
        config = {}

        result = check(record, config)

        assert result.passed is True

    def test_custom_action_on_fail(self) -> None:
        """Custom action_on_fail is used."""
        record = {"text": "El texto está en español con muchas palabras."}
        config = {"required_languages": ["en"], "action_on_fail": "flag"}

        result = check(record, config)

        assert result.passed is False
        assert result.action == "flag"

    def test_details_include_detection(self) -> None:
        """Details include detection information."""
        record = {"text": "This is a test document with some English words."}
        config = {}

        result = check(record, config)

        assert "detected_language" in result.details
        assert "confidence" in result.details


class TestCheckName:
    """Test check name constant."""

    def test_check_name(self) -> None:
        """Check name is set correctly."""
        assert check_name == "language_detect"
