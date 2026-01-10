"""Test fixtures for Dataset Collector integration tests."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def create_minimal_targets_yaml(path: Path, domain: str = "test") -> None:
    """Create a minimal valid targets YAML for testing."""
    content = f"""# Test targets for {domain} pipeline
globals:
  raw_root: "{path.parent}/raw"
  screened_yellow_root: "{path.parent}/screened_yellow"
  combined_root: "{path.parent}/combined"
  manifests_root: "{path.parent}/_manifests"
  queues_root: "{path.parent}/_queues"
  ledger_root: "{path.parent}/_ledger"
  pitches_root: "{path.parent}/_pitches"
  logs_root: "{path.parent}/_logs"
  screening:
    text_field_candidates: [text, content, body]
    min_chars: 50
    max_chars: 50000

targets:
  - id: test-dataset-001
    name: Test Dataset One
    enabled: true
    license_profile: permissive
    resolved_spdx: MIT
    download:
      strategy: none
"""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)


def create_sample_jsonl(path: Path, records: list[dict[str, Any]]) -> None:
    """Create a JSONL file with sample records."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")


def create_sample_yellow_queue(path: Path) -> None:
    """Create a sample yellow_pipeline.jsonl for testing."""
    records = [
        {
            "id": "test-001",
            "name": "Test Dataset",
            "effective_bucket": "yellow",
            "license_profile": "unknown",
            "resolved_spdx": "UNKNOWN",
            "restriction_hits": ["research-only"],
            "require_yellow_signoff": True,
            "review_required": True,
            "denylist_hits": [],
            "priority": 1,
            "manifest_dir": "/data/test/_manifests/test-001",
        },
        {
            "id": "test-002",
            "name": "Permissive Dataset",
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
    ]
    create_sample_jsonl(path, records)


def create_sample_merged_records(path: Path) -> None:
    """Create sample merged records for output contract testing."""
    records = [
        {
            "text": "This is sample text content for testing purposes.",
            "content_sha256": "abc123",
            "source_urls": ["https://example.com/data"],
            "source": {
                "pipeline_id": "test_pipeline",
                "target_id": "test-001",
            },
        },
        {
            "text": "Another sample record with different content.",
            "content_sha256": "def456",
            "source_urls": ["https://example.com/data2"],
            "source": {
                "pipeline_id": "test_pipeline",
                "target_id": "test-002",
            },
        },
    ]
    create_sample_jsonl(path, records)
