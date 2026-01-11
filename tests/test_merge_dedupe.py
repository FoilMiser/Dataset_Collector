from __future__ import annotations

import pytest

from collector_core.merge.dedupe import (
    DedupeIndex,
    PartitionedDedupeIndex,
    build_dedupe_update,
    merge_distinct_urls,
    merge_provenance_update,
    merge_update_payload,
)


def test_merge_distinct_urls_dedupes_and_limits() -> None:
    merged = merge_distinct_urls(["a", "b"], ["b", "", "c", "d"], limit=3)
    assert merged == ["a", "b", "c"]


def test_dedupe_index_add_if_new(tmp_path) -> None:
    index = DedupeIndex(tmp_path / "dedupe.sqlite")
    assert index.add_if_new("hash1") is True
    assert index.add_if_new("hash1") is False
    index.close()


def test_partitioned_dedupe_index_partitions(tmp_path) -> None:
    index = PartitionedDedupeIndex(tmp_path / "dedupe.sqlite", partitions=2)
    assert index.add_if_new("00" * 32) is True
    assert index.add_if_new("00" * 32) is False
    index.close()

    with pytest.raises(ValueError, match="at least 2 partitions"):
        PartitionedDedupeIndex(tmp_path / "other.sqlite", partitions=1)


def test_merge_update_payload_and_provenance() -> None:
    base = {"source_urls": ["a"], "duplicates": []}
    update = build_dedupe_update(
        {"content_sha256": "hash", "source_urls": ["b"], "source": {}},
        source_kind="jsonl",
        source_path=None,
    )
    merged = merge_update_payload(base, update, max_source_urls=5, max_duplicates=10)
    assert merged["source_urls"] == ["a", "b"]
    assert len(merged["duplicates"]) == 1

    record = {"source_urls": ["a"], "provenance": {"duplicates": []}}
    merge_provenance_update(record, update, max_source_urls=5, max_duplicates=10)
    assert record["source_urls"] == ["a", "b"]
    assert record["provenance"]["duplicates"]
    assert "timestamp_updated" in record
