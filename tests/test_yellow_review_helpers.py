"""Tests for collector_core.yellow_review_helpers module."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from collector_core.yellow_review_helpers import (
    QueueEntry,
    load_queue,
    print_summary,
    summarize,
    write_plan,
)


@pytest.fixture
def sample_queue_data() -> list[dict[str, Any]]:
    """Sample queue data for testing."""
    return [
        {
            "id": "test-001",
            "name": "Test Dataset One",
            "effective_bucket": "yellow",
            "license_profile": "unknown",
            "resolved_spdx": "UNKNOWN",
            "restriction_hits": ["research-only", "no-commercial"],
            "require_yellow_signoff": True,
            "review_required": True,
            "denylist_hits": [],
            "priority": 1,
            "manifest_dir": "/data/test/_manifests/test-001",
        },
        {
            "id": "test-002",
            "name": "Test Dataset Two",
            "effective_bucket": "yellow",
            "license_profile": "permissive",
            "resolved_spdx": "MIT",
            "restriction_hits": [],
            "require_yellow_signoff": False,
            "review_required": False,
            "denylist_hits": [],
            "priority": 2,
            "manifest_dir": "/data/test/_manifests/test-002",
        },
        {
            "id": "test-003",
            "name": "Test Dataset Three",
            "effective_bucket": "yellow",
            "license_profile": "copyleft",
            "resolved_spdx": "GPL-3.0",
            "restriction_hits": ["copyleft"],
            "require_yellow_signoff": True,
            "review_required": False,
            "denylist_hits": [],
            "priority": 3,
            "manifest_dir": "/data/test/_manifests/test-003",
        },
    ]


@pytest.fixture
def sample_queue_file(tmp_path: Path, sample_queue_data: list[dict[str, Any]]) -> Path:
    """Create a sample queue file."""
    queue_path = tmp_path / "yellow_pipeline.jsonl"
    with queue_path.open("w", encoding="utf-8") as f:
        for record in sample_queue_data:
            f.write(json.dumps(record) + "\n")
    return queue_path


class TestQueueEntry:
    def test_from_raw_creates_entry(self, sample_queue_data: list[dict[str, Any]]):
        entry = QueueEntry.from_raw(sample_queue_data[0])
        assert entry.id == "test-001"
        assert entry.name == "Test Dataset One"
        assert entry.license_profile == "unknown"
        assert entry.restriction_hits == ["research-only", "no-commercial"]
        assert entry.require_yellow_signoff is True

    def test_from_raw_handles_missing_fields(self):
        entry = QueueEntry.from_raw({})
        assert entry.id == ""
        assert entry.name == ""
        assert entry.resolved_spdx == "UNKNOWN"
        assert entry.restriction_hits == []
        assert entry.require_yellow_signoff is False


class TestLoadQueue:
    def test_load_queue_returns_entries(
        self, sample_queue_file: Path, sample_queue_data: list[dict[str, Any]]
    ):
        entries = load_queue(sample_queue_file)
        assert len(entries) == len(sample_queue_data)
        assert all(isinstance(e, QueueEntry) for e in entries)

    def test_load_queue_respects_limit(self, sample_queue_file: Path):
        entries = load_queue(sample_queue_file, limit=2)
        assert len(entries) == 2

    def test_load_queue_with_limit_greater_than_entries(self, sample_queue_file: Path):
        entries = load_queue(sample_queue_file, limit=100)
        assert len(entries) == 3


class TestSummarize:
    def test_summarize_counts_total(self, sample_queue_file: Path):
        entries = load_queue(sample_queue_file)
        summary = summarize(entries)
        assert summary["total"] == 3

    def test_summarize_counts_by_profile(self, sample_queue_file: Path):
        entries = load_queue(sample_queue_file)
        summary = summarize(entries)
        assert summary["by_profile"]["unknown"] == 1
        assert summary["by_profile"]["permissive"] == 1
        assert summary["by_profile"]["copyleft"] == 1

    def test_summarize_counts_by_spdx(self, sample_queue_file: Path):
        entries = load_queue(sample_queue_file)
        summary = summarize(entries)
        assert "MIT" in summary["by_spdx"]
        assert "UNKNOWN" in summary["by_spdx"]

    def test_summarize_counts_restriction_hits(self, sample_queue_file: Path):
        entries = load_queue(sample_queue_file)
        summary = summarize(entries)
        assert "research-only" in summary["restriction_hits"]
        assert "copyleft" in summary["restriction_hits"]

    def test_summarize_counts_review_required(self, sample_queue_file: Path):
        entries = load_queue(sample_queue_file)
        summary = summarize(entries)
        assert summary["review_required"] == 2  # test-001 and test-003


class TestWritePlan:
    def test_write_plan_creates_file(self, tmp_path: Path, sample_queue_file: Path):
        entries = load_queue(sample_queue_file)
        summary = summarize(entries)
        output_path = tmp_path / "review_plan.json"

        write_plan(output_path, entries, summary, sample_queue_file)

        assert output_path.exists()
        plan = json.loads(output_path.read_text())
        assert plan["total_entries"] == 3
        assert "generated_utc" in plan
        assert "summary" in plan
        assert "entries" in plan

    def test_write_plan_entries_serializable(self, tmp_path: Path, sample_queue_file: Path):
        entries = load_queue(sample_queue_file)
        summary = summarize(entries)
        output_path = tmp_path / "review_plan.json"

        write_plan(output_path, entries, summary, sample_queue_file)

        plan = json.loads(output_path.read_text())
        assert len(plan["entries"]) == 3
        assert plan["entries"][0]["id"] == "test-001"


class TestPrintSummary:
    def test_print_summary_runs_without_error(self, sample_queue_file: Path, capsys):
        entries = load_queue(sample_queue_file)
        summary = summarize(entries)

        print_summary("Test", summary)

        captured = capsys.readouterr()
        assert "Test YELLOW queue summary" in captured.out
        assert "Total entries: 3" in captured.out
