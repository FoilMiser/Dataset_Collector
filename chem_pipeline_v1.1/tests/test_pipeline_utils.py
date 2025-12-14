"""
test_pipeline_utils.py

Unit tests for pipeline utility functions.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest


class TestWhitespaceNormalization:
    """Tests for whitespace normalization utilities."""

    def test_normalize_whitespace(self):
        """Test whitespace normalization."""
        from pipeline_driver import normalize_whitespace

        assert normalize_whitespace("  hello   world  ") == "hello world"
        assert normalize_whitespace("hello\n\nworld") == "hello world"
        assert normalize_whitespace("hello\t\tworld") == "hello world"
        assert normalize_whitespace("") == ""
        assert normalize_whitespace(None) == ""

    def test_lower_function(self):
        """Test lowercase conversion."""
        from pipeline_driver import lower

        assert lower("HELLO") == "hello"
        assert lower("Hello World") == "hello world"
        assert lower("") == ""
        assert lower(None) == ""


class TestContainsAny:
    """Tests for substring matching utilities."""

    def test_contains_any_basic(self):
        """Test basic substring matching."""
        from pipeline_driver import contains_any

        haystack = "This is a test with NO AI training allowed"
        needles = ["no ai", "no ml", "no llm"]

        hits = contains_any(haystack, needles)
        assert "no ai" in hits
        assert "no ml" not in hits

    def test_contains_any_case_insensitive(self):
        """Test case-insensitive matching."""
        from pipeline_driver import contains_any

        haystack = "NO COMMERCIAL USE ALLOWED"
        needles = ["no commercial use", "for academic use only"]

        hits = contains_any(haystack, needles)
        assert "no commercial use" in hits

    def test_contains_any_empty(self):
        """Test with empty inputs."""
        from pipeline_driver import contains_any

        assert contains_any("", ["test"]) == []
        assert contains_any("test", []) == []
        assert contains_any("test", [None, ""]) == []


class TestSHA256:
    """Tests for SHA256 hashing utilities."""

    def test_sha256_bytes(self):
        """Test SHA256 hashing of bytes."""
        from pipeline_driver import sha256_bytes

        # Known hash for "hello"
        result = sha256_bytes(b"hello")
        assert len(result) == 64
        assert result == "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"

    def test_sha256_bytes_empty(self):
        """Test SHA256 of empty bytes."""
        from pipeline_driver import sha256_bytes

        result = sha256_bytes(b"")
        assert len(result) == 64
        # SHA256 of empty string
        assert result == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"


class TestMergeGates:
    """Tests for gate merging functionality."""

    def test_merge_gates_add(self):
        """Test adding gates."""
        from pipeline_driver import merge_gates

        default = ["gate1", "gate2"]
        override = {"add": ["gate3"]}

        result = merge_gates(default, override)
        assert "gate1" in result
        assert "gate2" in result
        assert "gate3" in result

    def test_merge_gates_remove(self):
        """Test removing gates."""
        from pipeline_driver import merge_gates

        default = ["gate1", "gate2", "gate3"]
        override = {"remove": ["gate2"]}

        result = merge_gates(default, override)
        assert "gate1" in result
        assert "gate2" not in result
        assert "gate3" in result

    def test_merge_gates_empty_override(self):
        """Test with empty override."""
        from pipeline_driver import merge_gates

        default = ["gate1", "gate2"]

        result = merge_gates(default, {})
        assert result == default

        result = merge_gates(default, None)
        assert result == default


class TestUtcNow:
    """Tests for UTC timestamp generation."""

    def test_utc_now_format(self):
        """Test UTC timestamp format."""
        from pipeline_driver import utc_now

        result = utc_now()
        # Should match ISO 8601 format
        assert len(result) == 20
        assert result.endswith("Z")
        assert "T" in result


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
