"""Tests for catalog_builder module contract verification.

These tests verify that the catalog builder produces output that conforms
to the expected structure and contains required fields.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from collector_core.catalog_builder import (
    build_catalog,
    collect_queue_stats,
    collect_raw_stats,
    collect_shard_stage,
    collect_strategy_counts,
    count_lines,
    file_stats,
    pipeline_slug_from_id,
)


class TestCatalogContract:
    """Tests for catalog output contract."""

    def test_build_catalog_returns_dict(self, tmp_path: Path) -> None:
        """build_catalog should return a dictionary."""
        # Create minimal config
        cfg: dict[str, Any] = {
            "globals": {
                "raw_root": str(tmp_path / "raw"),
                "screened_yellow_root": str(tmp_path / "screened"),
                "combined_root": str(tmp_path / "combined"),
                "ledger_root": str(tmp_path / "ledger"),
                "queues_root": str(tmp_path / "queues"),
            },
            "targets": [],
        }

        # Create directories
        (tmp_path / "raw").mkdir()
        (tmp_path / "screened").mkdir()
        (tmp_path / "combined").mkdir()
        (tmp_path / "ledger").mkdir()
        (tmp_path / "queues").mkdir()

        catalog = build_catalog(cfg, pipeline_slug="test")
        assert isinstance(catalog, dict)

    def test_build_catalog_has_required_fields(self, tmp_path: Path) -> None:
        """build_catalog output should have all required fields."""
        cfg: dict[str, Any] = {
            "globals": {
                "raw_root": str(tmp_path / "raw"),
                "screened_yellow_root": str(tmp_path / "screened"),
                "combined_root": str(tmp_path / "combined"),
                "ledger_root": str(tmp_path / "ledger"),
                "queues_root": str(tmp_path / "queues"),
            },
            "targets": [],
        }

        (tmp_path / "raw").mkdir()
        (tmp_path / "screened").mkdir()
        (tmp_path / "combined").mkdir()
        (tmp_path / "ledger").mkdir()
        (tmp_path / "queues").mkdir()

        catalog = build_catalog(cfg, pipeline_slug="test")

        # Check required top-level fields
        assert "generated_at_utc" in catalog
        assert "version" in catalog
        assert "raw" in catalog
        assert "screened_yellow" in catalog
        assert "combined" in catalog
        assert "ledgers" in catalog
        assert "queues" in catalog
        assert "license_pools" in catalog
        assert "strategy_counts" in catalog
        assert "top_licenses" in catalog
        assert "top_targets_by_bytes" in catalog

    def test_build_catalog_timestamps_are_valid_iso_format(self, tmp_path: Path) -> None:
        """Timestamps should be valid ISO format."""
        cfg: dict[str, Any] = {
            "globals": {
                "raw_root": str(tmp_path / "raw"),
                "screened_yellow_root": str(tmp_path / "screened"),
                "combined_root": str(tmp_path / "combined"),
                "ledger_root": str(tmp_path / "ledger"),
                "queues_root": str(tmp_path / "queues"),
            },
            "targets": [],
        }

        (tmp_path / "raw").mkdir()
        (tmp_path / "screened").mkdir()
        (tmp_path / "combined").mkdir()
        (tmp_path / "ledger").mkdir()
        (tmp_path / "queues").mkdir()

        catalog = build_catalog(cfg, pipeline_slug="test")

        # Check timestamp format (should end with Z for UTC)
        assert catalog["generated_at_utc"].endswith("Z")
        assert "T" in catalog["generated_at_utc"]

    def test_build_catalog_is_json_serializable(self, tmp_path: Path) -> None:
        """Catalog should be JSON serializable."""
        cfg: dict[str, Any] = {
            "globals": {
                "raw_root": str(tmp_path / "raw"),
                "screened_yellow_root": str(tmp_path / "screened"),
                "combined_root": str(tmp_path / "combined"),
                "ledger_root": str(tmp_path / "ledger"),
                "queues_root": str(tmp_path / "queues"),
            },
            "targets": [],
        }

        (tmp_path / "raw").mkdir()
        (tmp_path / "screened").mkdir()
        (tmp_path / "combined").mkdir()
        (tmp_path / "ledger").mkdir()
        (tmp_path / "queues").mkdir()

        catalog = build_catalog(cfg, pipeline_slug="test")

        # Should not raise
        json_str = json.dumps(catalog)
        assert isinstance(json_str, str)

        # Should be able to round-trip
        parsed = json.loads(json_str)
        assert parsed == catalog


class TestCatalogHelpers:
    """Tests for catalog helper functions."""

    def test_count_lines_returns_correct_count(self, tmp_path: Path) -> None:
        """count_lines should return correct line count."""
        test_file = tmp_path / "test.txt"
        test_file.write_text("line1\nline2\nline3\n")

        count = count_lines(test_file)
        assert count == 3

    def test_count_lines_with_max_lines(self, tmp_path: Path) -> None:
        """count_lines should respect max_lines limit."""
        test_file = tmp_path / "test.txt"
        test_file.write_text("line1\nline2\nline3\nline4\nline5\n")

        count = count_lines(test_file, max_lines=2)
        assert count == 2

    def test_file_stats_returns_dict(self, tmp_path: Path) -> None:
        """file_stats should return a dict with required keys."""
        test_file = tmp_path / "test.jsonl"
        test_file.write_text('{"key": "value"}\n')

        stats = file_stats(test_file)
        assert "name" in stats
        assert "bytes" in stats
        assert "lines_estimate" in stats
        assert stats["name"] == "test.jsonl"

    def test_pipeline_slug_from_id_strips_suffix(self) -> None:
        """pipeline_slug_from_id should strip _pipeline_v2 suffix."""
        assert pipeline_slug_from_id("physics_pipeline_v2") == "physics"
        assert pipeline_slug_from_id("chem_pipeline_v2") == "chem"
        assert pipeline_slug_from_id("custom") == "custom"
        assert pipeline_slug_from_id(None) is None

    def test_collect_strategy_counts(self) -> None:
        """collect_strategy_counts should count strategies correctly."""
        cfg = {
            "targets": [
                {"id": "t1", "download": {"strategy": "wget"}},
                {"id": "t2", "download": {"strategy": "wget"}},
                {"id": "t3", "download": {"strategy": "github"}},
                {"id": "t4", "download": {}},
            ]
        }

        counts = collect_strategy_counts(cfg)
        assert counts["wget"] == 2
        assert counts["github"] == 1
        assert counts["unknown"] == 1


class TestCollectRawStats:
    """Tests for collect_raw_stats function."""

    def test_empty_directory_returns_empty_stats(self, tmp_path: Path) -> None:
        """Empty directory should return empty bucket stats."""
        stats = collect_raw_stats(tmp_path, top_n=10)

        assert "buckets" in stats
        assert "green" in stats["buckets"]
        assert "yellow" in stats["buckets"]
        assert stats["buckets"]["green"]["targets"] == 0
        assert stats["buckets"]["yellow"]["targets"] == 0

    def test_counts_targets_correctly(self, tmp_path: Path) -> None:
        """Should count targets correctly in each bucket."""
        # Create structure: raw/yellow/permissive/target_1/file.jsonl
        (tmp_path / "yellow" / "permissive" / "target_1").mkdir(parents=True)
        (tmp_path / "yellow" / "permissive" / "target_2").mkdir(parents=True)
        (tmp_path / "yellow" / "permissive" / "target_1" / "data.jsonl").write_text("{}\n")
        (tmp_path / "yellow" / "permissive" / "target_2" / "data.jsonl").write_text("{}\n")

        stats = collect_raw_stats(tmp_path, top_n=10)

        assert stats["buckets"]["yellow"]["targets"] == 2
        assert stats["buckets"]["yellow"]["pools"]["permissive"]["targets"] == 2


class TestCollectQueueStats:
    """Tests for collect_queue_stats function."""

    def test_missing_queues_returns_empty_stats(self, tmp_path: Path) -> None:
        """Missing queue files should return empty stats."""
        stats = collect_queue_stats(tmp_path)

        assert "buckets" in stats
        assert stats["buckets"]["green"]["targets"] == 0
        assert stats["buckets"]["yellow"]["targets"] == 0
        assert stats["buckets"]["red"]["targets"] == 0

    def test_counts_queue_entries(self, tmp_path: Path) -> None:
        """Should count entries in queue files."""
        (tmp_path / "yellow_pipeline.jsonl").write_text(
            '{"id": "t1", "license_profile": "permissive"}\n'
            '{"id": "t2", "license_profile": "copyleft"}\n'
        )

        stats = collect_queue_stats(tmp_path)
        assert stats["buckets"]["yellow"]["targets"] == 2


class TestCollectShardStage:
    """Tests for collect_shard_stage function."""

    def test_missing_directory_returns_empty_stats(self, tmp_path: Path) -> None:
        """Missing directory should return empty stats."""
        stats = collect_shard_stage(tmp_path / "nonexistent")
        assert stats["pools"] == {}

    def test_counts_shards_correctly(self, tmp_path: Path) -> None:
        """Should count shard files correctly."""
        (tmp_path / "permissive" / "shards").mkdir(parents=True)
        (tmp_path / "permissive" / "shards" / "shard_0000.jsonl").write_text("{}\n")
        (tmp_path / "permissive" / "shards" / "shard_0001.jsonl").write_text("{}\n")

        stats = collect_shard_stage(tmp_path)
        assert stats["pools"]["permissive"]["files"] == 2
