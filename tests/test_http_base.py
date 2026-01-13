"""
Tests for HTTP base utilities module.

Issue 1.3 (v3.0): Tests for shared HTTP download utilities.
"""

from __future__ import annotations

import pytest
from pathlib import Path
from collector_core.acquire.strategies.http_base import (
    DownloadResult,
    UrlValidationResult,
    validate_url,
    compute_file_hash,
    parse_content_disposition,
    sanitize_filename,
    get_extension_from_url,
    get_extension_from_content_type,
    DEFAULT_TIMEOUT,
    DEFAULT_USER_AGENT,
)


class TestValidateUrl:
    """Tests for URL validation."""

    def test_valid_https_url(self) -> None:
        """Valid HTTPS URL passes."""
        result = validate_url("https://example.com/data.zip")
        assert result.is_valid is True
        assert result.error is None

    def test_valid_http_url(self) -> None:
        """Valid HTTP URL passes."""
        result = validate_url("http://example.com/data.zip")
        assert result.is_valid is True

    def test_invalid_scheme(self) -> None:
        """Invalid scheme fails."""
        result = validate_url("ftp://example.com/data.zip")
        assert result.is_valid is False
        assert "scheme" in (result.error or "").lower()

    def test_missing_host(self) -> None:
        """Missing host fails."""
        result = validate_url("https:///path")
        assert result.is_valid is False

    def test_empty_url(self) -> None:
        """Empty URL fails."""
        result = validate_url("")
        assert result.is_valid is False

    def test_relative_url(self) -> None:
        """Relative URL fails."""
        result = validate_url("/path/to/file.zip")
        assert result.is_valid is False


class TestComputeFileHash:
    """Tests for file hash computation."""

    def test_hash_file(self, tmp_path: Path) -> None:
        """File hash is computed correctly."""
        test_file = tmp_path / "test.txt"
        test_file.write_bytes(b"hello world")

        hash_value = compute_file_hash(test_file)
        # SHA256 of "hello world"
        expected = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        assert hash_value == expected

    def test_hash_empty_file(self, tmp_path: Path) -> None:
        """Empty file hash is computed."""
        test_file = tmp_path / "empty.txt"
        test_file.write_bytes(b"")

        hash_value = compute_file_hash(test_file)
        # SHA256 of empty string
        expected = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        assert hash_value == expected

    def test_hash_binary_file(self, tmp_path: Path) -> None:
        """Binary file hash is computed."""
        test_file = tmp_path / "binary.bin"
        test_file.write_bytes(bytes(range(256)))

        hash_value = compute_file_hash(test_file)
        assert len(hash_value) == 64  # SHA256 hex length


class TestParseContentDisposition:
    """Tests for Content-Disposition header parsing."""

    def test_simple_filename(self) -> None:
        """Simple filename is extracted."""
        result = parse_content_disposition('attachment; filename="data.zip"')
        assert result == "data.zip"

    def test_filename_without_quotes(self) -> None:
        """Filename without quotes is extracted."""
        result = parse_content_disposition("attachment; filename=data.zip")
        assert result == "data.zip"

    def test_filename_star_encoding(self) -> None:
        """RFC 5987 filename* is extracted."""
        result = parse_content_disposition("attachment; filename*=UTF-8''data%20file.zip")
        assert result == "data file.zip"

    def test_no_filename(self) -> None:
        """Missing filename returns None."""
        result = parse_content_disposition("attachment")
        assert result is None

    def test_empty_header(self) -> None:
        """Empty header returns None."""
        result = parse_content_disposition("")
        assert result is None

    def test_none_header(self) -> None:
        """None header returns None."""
        result = parse_content_disposition(None)
        assert result is None


class TestSanitizeFilename:
    """Tests for filename sanitization."""

    def test_basic_filename(self) -> None:
        """Basic filename is unchanged."""
        result = sanitize_filename("data.zip")
        assert result == "data.zip"

    def test_path_traversal(self) -> None:
        """Path traversal is removed."""
        result = sanitize_filename("../../../etc/passwd")
        assert ".." not in result
        assert "/" not in result

    def test_special_characters(self) -> None:
        """Special characters are removed."""
        result = sanitize_filename("file<>:\"|?*name.txt")
        assert "<" not in result
        assert ">" not in result
        assert ":" not in result
        assert "|" not in result

    def test_null_bytes(self) -> None:
        """Null bytes are removed."""
        result = sanitize_filename("file\x00name.txt")
        assert "\x00" not in result

    def test_max_length(self) -> None:
        """Long filenames are truncated."""
        long_name = "a" * 300 + ".zip"
        result = sanitize_filename(long_name, max_length=100)
        assert len(result) <= 100

    def test_empty_after_sanitization(self) -> None:
        """Empty result after sanitization returns default."""
        result = sanitize_filename("...")
        assert result != ""


class TestGetExtensionFromUrl:
    """Tests for extension extraction from URL."""

    def test_simple_extension(self) -> None:
        """Simple extension is extracted."""
        result = get_extension_from_url("https://example.com/data.zip")
        assert result == ".zip"

    def test_no_extension(self) -> None:
        """No extension returns empty string."""
        result = get_extension_from_url("https://example.com/data")
        assert result == ""

    def test_query_string(self) -> None:
        """Query string is ignored."""
        result = get_extension_from_url("https://example.com/data.zip?key=value")
        assert result == ".zip"

    def test_multiple_dots(self) -> None:
        """Multiple dots returns last extension."""
        result = get_extension_from_url("https://example.com/data.tar.gz")
        assert result == ".gz"


class TestGetExtensionFromContentType:
    """Tests for extension extraction from Content-Type."""

    def test_common_types(self) -> None:
        """Common content types return correct extension."""
        assert get_extension_from_content_type("application/zip") == ".zip"
        assert get_extension_from_content_type("application/json") == ".json"
        assert get_extension_from_content_type("text/plain") == ".txt"
        assert get_extension_from_content_type("text/html") == ".html"

    def test_with_charset(self) -> None:
        """Charset parameter is ignored."""
        result = get_extension_from_content_type("text/plain; charset=utf-8")
        assert result == ".txt"

    def test_unknown_type(self) -> None:
        """Unknown type returns empty string."""
        result = get_extension_from_content_type("application/x-unknown")
        assert result == ""

    def test_empty_type(self) -> None:
        """Empty type returns empty string."""
        result = get_extension_from_content_type("")
        assert result == ""


class TestDownloadResult:
    """Tests for DownloadResult dataclass."""

    def test_success_result(self, tmp_path: Path) -> None:
        """Success result has path and hash."""
        result = DownloadResult(
            success=True,
            path=tmp_path / "file.zip",
            content_hash="abc123",
            size_bytes=1024,
            content_type="application/zip",
        )
        assert result.success is True
        assert result.path is not None
        assert result.error is None

    def test_failure_result(self) -> None:
        """Failure result has error."""
        result = DownloadResult(
            success=False,
            error="Connection refused",
        )
        assert result.success is False
        assert result.error == "Connection refused"
        assert result.path is None


class TestUrlValidationResult:
    """Tests for UrlValidationResult dataclass."""

    def test_valid_result(self) -> None:
        """Valid result."""
        result = UrlValidationResult(is_valid=True)
        assert result.is_valid is True
        assert result.error is None

    def test_invalid_result(self) -> None:
        """Invalid result with error."""
        result = UrlValidationResult(is_valid=False, error="Invalid scheme")
        assert result.is_valid is False
        assert result.error == "Invalid scheme"


class TestConstants:
    """Tests for module constants."""

    def test_default_timeout(self) -> None:
        """Default timeout is reasonable."""
        assert DEFAULT_TIMEOUT > 0
        assert DEFAULT_TIMEOUT <= 300  # Max 5 minutes

    def test_default_user_agent(self) -> None:
        """Default user agent is set."""
        assert "DatasetCollector" in DEFAULT_USER_AGENT or len(DEFAULT_USER_AGENT) > 0
