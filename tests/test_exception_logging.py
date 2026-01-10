"""
test_exception_logging.py

Tests verifying that exception paths emit appropriate log warnings.
Ensures best-effort behavior while making failures observable.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest

from collector_core.pipeline_driver_base import read_review_signoff
from collector_core.review_queue import load_existing_signoff, load_license_evidence_meta
from collector_core.utils import coerce_int, sha256_file


class TestUtilsExceptionLogging:
    """Tests for exception logging in utils module."""

    def test_sha256_file_logs_warning_on_error(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """sha256_file should log warning when file cannot be read."""
        nonexistent = tmp_path / "nonexistent.txt"

        with caplog.at_level(logging.WARNING, logger="collector_core.utils"):
            result = sha256_file(nonexistent)

        assert result is None
        assert "Failed to compute SHA-256 hash" in caplog.text

    def test_sha256_file_logs_on_permission_error(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """sha256_file should log warning on permission errors."""
        test_file = tmp_path / "test.txt"
        test_file.write_text("test")

        # Simulate permission error
        def raise_permission_error(*args, **kwargs):
            raise PermissionError("Access denied")

        monkeypatch.setattr(Path, "open", raise_permission_error)

        with caplog.at_level(logging.WARNING, logger="collector_core.utils"):
            result = sha256_file(test_file)

        assert result is None
        assert "Failed to compute SHA-256 hash" in caplog.text

    def test_coerce_int_logs_debug_on_failure(self, caplog: pytest.LogCaptureFixture) -> None:
        """coerce_int should log debug when conversion fails."""
        with caplog.at_level(logging.DEBUG, logger="collector_core.utils"):
            result = coerce_int("not_a_number", default=42)

        assert result == 42
        assert "Failed to coerce value" in caplog.text


class TestReviewQueueExceptionLogging:
    """Tests for exception logging in review_queue module."""

    def test_load_existing_signoff_logs_warning_on_corrupt_json(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """load_existing_signoff should log warning on corrupt JSON."""
        manifest_dir = tmp_path / "manifest"
        manifest_dir.mkdir()
        signoff_path = manifest_dir / "review_signoff.json"
        signoff_path.write_text("{ invalid json }", encoding="utf-8")

        with caplog.at_level(logging.WARNING, logger="collector_core.review_queue"):
            result = load_existing_signoff(manifest_dir)

        assert result == {}
        assert "Failed to load review signoff" in caplog.text

    def test_load_license_evidence_meta_logs_warning_on_corrupt_json(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """load_license_evidence_meta should log warning on corrupt JSON."""
        manifest_dir = tmp_path / "manifest"
        manifest_dir.mkdir()
        meta_path = manifest_dir / "license_evidence_meta.json"
        meta_path.write_text("not valid json", encoding="utf-8")

        with caplog.at_level(logging.WARNING, logger="collector_core.review_queue"):
            result = load_license_evidence_meta(manifest_dir)

        assert result == {}
        assert "Failed to load license evidence meta" in caplog.text


class TestPipelineDriverBaseExceptionLogging:
    """Tests for exception logging in pipeline_driver_base module."""

    def test_read_review_signoff_logs_warning_on_corrupt_json(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """read_review_signoff should log warning on corrupt JSON."""
        manifest_dir = tmp_path / "manifest"
        manifest_dir.mkdir()
        signoff_path = manifest_dir / "review_signoff.json"
        signoff_path.write_text("{ invalid: json }", encoding="utf-8")

        with caplog.at_level(logging.WARNING, logger="collector_core.pipeline_driver_base"):
            result = read_review_signoff(manifest_dir)

        assert result == {}
        assert "Failed to read review signoff" in caplog.text


class TestBestEffortBehaviorPreserved:
    """Tests ensuring best-effort behavior is preserved."""

    def test_sha256_file_returns_none_on_error(self, tmp_path: Path) -> None:
        """sha256_file should still return None on error (best-effort)."""
        result = sha256_file(tmp_path / "missing.txt")
        assert result is None

    def test_coerce_int_returns_default_on_error(self) -> None:
        """coerce_int should still return default on error (best-effort)."""
        assert coerce_int("bad", default=10) == 10
        assert coerce_int(None, default=5) == 5
        assert coerce_int([], default=0) == 0

    def test_load_signoff_returns_empty_dict_on_error(self, tmp_path: Path) -> None:
        """load functions should still return {} on error (best-effort)."""
        manifest_dir = tmp_path / "manifest"
        manifest_dir.mkdir()

        # Create corrupt JSON files
        (manifest_dir / "review_signoff.json").write_text("corrupt", encoding="utf-8")
        (manifest_dir / "license_evidence_meta.json").write_text("corrupt", encoding="utf-8")

        assert load_existing_signoff(manifest_dir) == {}
        assert load_license_evidence_meta(manifest_dir) == {}
        assert read_review_signoff(manifest_dir) == {}

    def test_valid_files_still_work(self, tmp_path: Path) -> None:
        """Valid files should still be processed correctly."""
        # Test sha256_file
        test_file = tmp_path / "test.txt"
        test_file.write_text("test content")
        result = sha256_file(test_file)
        assert result is not None
        assert len(result) == 64  # SHA-256 hex digest length

        # Test coerce_int
        assert coerce_int("42") == 42
        assert coerce_int(3.14) == 3

        # Test JSON loading
        manifest_dir = tmp_path / "manifest"
        manifest_dir.mkdir()

        signoff_data = {"status": "approved", "reviewer": "test"}
        (manifest_dir / "review_signoff.json").write_text(
            json.dumps(signoff_data), encoding="utf-8"
        )

        assert load_existing_signoff(manifest_dir) == signoff_data
        assert read_review_signoff(manifest_dir) == signoff_data
