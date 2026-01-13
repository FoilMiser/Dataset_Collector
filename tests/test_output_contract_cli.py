"""Tests for output contract CLI tool."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from tools.output_contract import main, validate_output_files


class TestValidateOutputFiles:
    """Tests for validate_output_files function."""

    def test_empty_directory_returns_zero_files(self, tmp_path: Path) -> None:
        """Empty directory should return zero files checked."""
        files_checked, records_checked, errors = validate_output_files(tmp_path)
        assert files_checked == 0
        assert records_checked == 0
        assert errors == []

    def test_valid_records_pass_validation(self, tmp_path: Path) -> None:
        """Valid records should pass validation."""
        screened_dir = tmp_path / "screened" / "permissive" / "shards"
        screened_dir.mkdir(parents=True)

        valid_record = {
            "dataset_id": "test_dataset",
            "split": "train",
            "config": "default",
            "row_id": "row_001",
            "license_spdx": "MIT",
            "license_profile": "permissive",
            "source_urls": ["https://example.com/data"],
            "reviewer_notes": "",
            "content_sha256": "abc123",
            "normalized_sha256": "abc123",
            "pool": "permissive",
            "pipeline": "test_pipeline",
            "target_name": "test_target",
            "timestamp_created": "2024-01-01T00:00:00Z",
            "timestamp_updated": "2024-01-01T00:00:00Z",
        }

        jsonl_file = screened_dir / "shard_0000.jsonl"
        jsonl_file.write_text(json.dumps(valid_record) + "\n")

        files_checked, records_checked, errors = validate_output_files(tmp_path)
        assert files_checked == 1
        assert records_checked == 1
        assert errors == []

    def test_invalid_records_fail_validation(self, tmp_path: Path) -> None:
        """Records missing required fields should fail validation."""
        screened_dir = tmp_path / "screened" / "permissive" / "shards"
        screened_dir.mkdir(parents=True)

        invalid_record = {
            "dataset_id": "test_dataset",
            # Missing many required fields
        }

        jsonl_file = screened_dir / "shard_0000.jsonl"
        jsonl_file.write_text(json.dumps(invalid_record) + "\n")

        files_checked, records_checked, errors = validate_output_files(tmp_path)
        assert files_checked == 1
        assert records_checked == 1
        assert len(errors) == 1
        assert "missing required fields" in errors[0]


class TestMain:
    """Tests for main CLI function."""

    def test_returns_zero_for_empty_directory(self, tmp_path: Path) -> None:
        """Empty directory should return 0 (no files is ok by default)."""
        exit_code = main(["--root", str(tmp_path)])
        assert exit_code == 0

    def test_returns_one_for_require_files_with_no_files(self, tmp_path: Path) -> None:
        """--require-files with no output files should return 1."""
        exit_code = main(["--root", str(tmp_path), "--require-files"])
        assert exit_code == 1

    def test_returns_zero_for_valid_files(self, tmp_path: Path) -> None:
        """Valid output files should return 0."""
        screened_dir = tmp_path / "screened" / "permissive" / "shards"
        screened_dir.mkdir(parents=True)

        valid_record = {
            "dataset_id": "test_dataset",
            "split": "train",
            "config": "default",
            "row_id": "row_001",
            "license_spdx": "MIT",
            "license_profile": "permissive",
            "source_urls": ["https://example.com/data"],
            "reviewer_notes": "",
            "content_sha256": "abc123",
            "normalized_sha256": "abc123",
            "pool": "permissive",
            "pipeline": "test_pipeline",
            "target_name": "test_target",
            "timestamp_created": "2024-01-01T00:00:00Z",
            "timestamp_updated": "2024-01-01T00:00:00Z",
        }

        jsonl_file = screened_dir / "shard_0000.jsonl"
        jsonl_file.write_text(json.dumps(valid_record) + "\n")

        exit_code = main(["--root", str(tmp_path)])
        assert exit_code == 0

    def test_returns_one_for_invalid_files(self, tmp_path: Path) -> None:
        """Invalid output files should return 1."""
        screened_dir = tmp_path / "screened" / "permissive" / "shards"
        screened_dir.mkdir(parents=True)

        invalid_record = {"only_one_field": "not_enough"}

        jsonl_file = screened_dir / "shard_0000.jsonl"
        jsonl_file.write_text(json.dumps(invalid_record) + "\n")

        exit_code = main(["--root", str(tmp_path)])
        assert exit_code == 1

    def test_returns_one_for_nonexistent_directory(self) -> None:
        """Non-existent directory should return 1."""
        exit_code = main(["--root", "/nonexistent/path/that/does/not/exist"])
        assert exit_code == 1

    def test_verbose_flag_accepted(self, tmp_path: Path) -> None:
        """--verbose flag should be accepted."""
        exit_code = main(["--root", str(tmp_path), "--verbose"])
        assert exit_code == 0
