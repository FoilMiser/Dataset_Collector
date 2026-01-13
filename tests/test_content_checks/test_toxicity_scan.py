"""Tests for toxicity scan content check."""

from __future__ import annotations

import pytest
from collector_core.checks.implementations.toxicity_scan import (
    check, scan_content, check_name,
)
from collector_core.checks.implementations.base import CheckResult


class TestScanContent:
    """Tests for scan_content function."""

    def test_clean_content(self) -> None:
        """Clean content has no flags."""
        text = "This is a normal document about programming."
        result = scan_content(text)
        assert result["total_flags"] == 0

    def test_detects_hate_speech(self) -> None:
        """Hate speech patterns are detected."""
        text = "We need to eliminate all enemies."
        result = scan_content(text)
        assert len(result["hate_speech"]) > 0

    def test_detects_violence(self) -> None:
        """Violence patterns are detected."""
        text = "How to make a bomb at home."
        result = scan_content(text)
        assert len(result["violence"]) > 0

    def test_detects_self_harm(self) -> None:
        """Self-harm patterns are detected."""
        text = "Information about self-harm methods."
        result = scan_content(text)
        assert len(result["self_harm"]) > 0

    def test_limited_matches(self) -> None:
        """Matches are limited per category."""
        # Even with many matches, should limit to 3 per category
        text = "kill all kill all kill all kill all kill all"
        result = scan_content(text)
        assert len(result["hate_speech"]) <= 3


class TestToxicityCheck:
    """Tests for check function."""

    def test_clean_content_passes(self) -> None:
        """Clean content passes."""
        record = {"text": "A normal document about science and technology."}
        config = {}

        result = check(record, config)

        assert result.passed is True
        assert result.action == "keep"

    def test_hate_speech_rejects(self) -> None:
        """Hate speech rejects by default."""
        record = {"text": "We must destroy all enemies of the state."}
        config = {}

        result = check(record, config)

        assert result.passed is False

    def test_csam_always_rejects(self) -> None:
        """CSAM is always rejected regardless of config."""
        record = {"text": "Content about child sexual material."}
        config = {"categories": []}  # Even with empty categories

        result = check(record, config)

        assert result.passed is False
        assert result.action == "reject"
        assert "csam" in (result.reason or "")

    def test_custom_action(self) -> None:
        """Custom action_on_detect is used."""
        record = {"text": "Detailed violence and torture descriptions."}
        config = {"action_on_detect": "flag"}

        result = check(record, config)

        assert result.passed is False
        assert result.action == "flag"

    def test_flag_threshold(self) -> None:
        """Flag threshold controls detection."""
        record = {"text": "Some potentially violent content."}
        config = {"flag_threshold": 10}  # Very high threshold

        result = check(record, config)

        # May pass due to high threshold
        assert result.details["relevant_flags"] < 10

    def test_specific_categories(self) -> None:
        """Specific categories can be checked."""
        record = {"text": "Content with self-harm references."}
        config = {"categories": ["violence"]}  # Only check violence

        result = check(record, config)

        # Self-harm not in categories, so may pass
        details = result.details
        assert "scan_results" in details

    def test_no_content_passes(self) -> None:
        """Empty content passes."""
        record = {"text": ""}
        config = {}

        result = check(record, config)

        assert result.passed is True
        assert result.reason == "no_content"

    def test_details_include_scan_results(self) -> None:
        """Details include scan results."""
        record = {"text": "A normal document."}
        config = {}

        result = check(record, config)

        assert "scan_results" in result.details
        assert "relevant_flags" in result.details


class TestCheckName:
    """Test check name constant."""

    def test_check_name(self) -> None:
        """Check name is set correctly."""
        assert check_name == "toxicity_scan"
