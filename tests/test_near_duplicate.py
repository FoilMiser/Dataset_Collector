"""
Tests for near-duplicate detection module.

Issue 2.4 (v3.0): Tests for MinHash LSH near-duplicate detection.
"""

from __future__ import annotations

import pytest
from collector_core.checks.near_duplicate import (
    NearDuplicateDetector,
    DuplicateResult,
    DetectorStats,
    create_detector,
    normalize_text,
    get_shingles,
)


class TestNormalizeText:
    """Tests for text normalization."""

    def test_basic_normalization(self) -> None:
        """Text is lowercased and whitespace collapsed."""
        result = normalize_text("  Hello   World  ")
        assert result == "hello world"

    def test_unicode_normalization(self) -> None:
        """Unicode is normalized."""
        # Composed vs decomposed forms
        result = normalize_text("café")
        assert result == "café"

    def test_empty_string(self) -> None:
        """Empty string returns empty."""
        result = normalize_text("")
        assert result == ""

    def test_only_whitespace(self) -> None:
        """Whitespace-only returns empty."""
        result = normalize_text("   \t\n   ")
        assert result == ""


class TestGetShingles:
    """Tests for shingle extraction."""

    def test_basic_shingles(self) -> None:
        """Extracts correct shingles."""
        shingles = get_shingles("hello world test", k=2)
        assert "hello world" in shingles
        assert "world test" in shingles
        assert len(shingles) == 2

    def test_short_text(self) -> None:
        """Short text returns single shingle."""
        shingles = get_shingles("hello", k=3)
        assert len(shingles) == 1
        assert "hello" in shingles

    def test_empty_text(self) -> None:
        """Empty text returns empty set."""
        shingles = get_shingles("", k=3)
        assert len(shingles) == 0


class TestNearDuplicateDetector:
    """Tests for NearDuplicateDetector class."""

    def test_exact_duplicate_detection(self) -> None:
        """Exact duplicates are detected."""
        detector = create_detector(threshold=0.8)
        detector.add("doc1", "This is a sample document for testing.")

        result = detector.query("This is a sample document for testing.")
        assert result.is_duplicate is True
        assert result.similarity >= 0.99

    def test_near_duplicate_detection(self) -> None:
        """Near-duplicates are detected."""
        detector = create_detector(threshold=0.5)
        detector.add("doc1", "Machine learning is a subset of artificial intelligence.")

        result = detector.query("Machine learning is part of artificial intelligence.")
        assert result.is_duplicate is True
        assert 0.5 <= result.similarity <= 1.0

    def test_different_documents(self) -> None:
        """Different documents are not flagged as duplicates."""
        detector = create_detector(threshold=0.8)
        detector.add("doc1", "The quick brown fox jumps over the lazy dog.")

        result = detector.query("Python is a programming language for data science.")
        assert result.is_duplicate is False
        assert result.similarity < 0.8

    def test_add_and_check(self) -> None:
        """add_and_check returns result and adds to index."""
        detector = create_detector(threshold=0.8)

        # First add should not be duplicate
        result1 = detector.add_and_check("doc1", "First document content here.")
        assert result1.is_duplicate is False

        # Same content should be duplicate
        result2 = detector.add_and_check("doc2", "First document content here.")
        assert result2.is_duplicate is True

    def test_stats(self) -> None:
        """Stats are tracked correctly."""
        detector = create_detector(threshold=0.8)
        detector.add("doc1", "Document one.")
        detector.add("doc2", "Document two.")
        detector.add("doc3", "Document three.")

        stats = detector.get_stats()
        assert stats.total_documents == 3
        assert stats.threshold == 0.8

    def test_clear(self) -> None:
        """Clear removes all documents."""
        detector = create_detector(threshold=0.8)
        detector.add("doc1", "Document one.")
        detector.add("doc2", "Document two.")

        detector.clear()
        stats = detector.get_stats()
        assert stats.total_documents == 0

    def test_contains(self) -> None:
        """Contains check works."""
        detector = create_detector(threshold=0.8)
        detector.add("doc1", "Document one.")

        assert detector.contains("doc1") is True
        assert detector.contains("doc2") is False


class TestDuplicateResult:
    """Tests for DuplicateResult dataclass."""

    def test_duplicate_result_creation(self) -> None:
        """DuplicateResult can be created."""
        result = DuplicateResult(
            is_duplicate=True,
            similarity=0.95,
            matched_id="doc1",
        )
        assert result.is_duplicate is True
        assert result.similarity == 0.95
        assert result.matched_id == "doc1"

    def test_non_duplicate_result(self) -> None:
        """Non-duplicate result has no matched_id."""
        result = DuplicateResult(
            is_duplicate=False,
            similarity=0.2,
            matched_id=None,
        )
        assert result.is_duplicate is False
        assert result.matched_id is None


class TestDetectorStats:
    """Tests for DetectorStats dataclass."""

    def test_stats_creation(self) -> None:
        """DetectorStats can be created."""
        stats = DetectorStats(
            total_documents=100,
            threshold=0.8,
            num_perm=128,
        )
        assert stats.total_documents == 100
        assert stats.threshold == 0.8
        assert stats.num_perm == 128


class TestCreateDetector:
    """Tests for create_detector factory function."""

    def test_default_detector(self) -> None:
        """Creates detector with default settings."""
        detector = create_detector()
        assert detector is not None
        stats = detector.get_stats()
        assert stats.threshold == 0.8

    def test_custom_threshold(self) -> None:
        """Creates detector with custom threshold."""
        detector = create_detector(threshold=0.5)
        stats = detector.get_stats()
        assert stats.threshold == 0.5

    def test_custom_num_perm(self) -> None:
        """Creates detector with custom num_perm."""
        detector = create_detector(num_perm=64)
        stats = detector.get_stats()
        assert stats.num_perm == 64


class TestEdgeCases:
    """Edge case tests."""

    def test_very_short_documents(self) -> None:
        """Very short documents are handled."""
        detector = create_detector(threshold=0.8)
        detector.add("doc1", "Hi")

        result = detector.query("Hi")
        # Should still work, though accuracy may vary
        assert result is not None

    def test_unicode_documents(self) -> None:
        """Unicode documents are handled."""
        detector = create_detector(threshold=0.8)
        detector.add("doc1", "日本語のテキスト")

        result = detector.query("日本語のテキスト")
        assert result.is_duplicate is True

    def test_special_characters(self) -> None:
        """Documents with special characters are handled."""
        detector = create_detector(threshold=0.8)
        detector.add("doc1", "Code: def foo(): return 'bar'")

        result = detector.query("Code: def foo(): return 'bar'")
        assert result.is_duplicate is True

    def test_empty_document(self) -> None:
        """Empty documents are handled gracefully."""
        detector = create_detector(threshold=0.8)
        detector.add("doc1", "")

        result = detector.query("")
        # Empty docs should match
        assert result is not None
