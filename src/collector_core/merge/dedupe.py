from __future__ import annotations

import sqlite3
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from collector_core.stability import stable_api
from collector_core.utils.logging import utc_now
from collector_core.utils.paths import ensure_dir


@stable_api
def merge_distinct_urls(
    existing: Iterable[str],
    incoming: Iterable[str],
    limit: int,
) -> list[str]:
    merged: list[str] = []
    seen: set[str] = set()
    for url in list(existing) + list(incoming):
        if not url:
            continue
        if url in seen:
            continue
        merged.append(url)
        seen.add(url)
        if len(merged) >= limit:
            break
    return merged


@stable_api
def build_dedupe_update(
    record: dict[str, Any],
    *,
    source_kind: str,
    source_path: Path | None,
) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "content_sha256": record.get("content_sha256"),
        "source_urls": record.get("source_urls", []),
        "source": record.get("source", {}),
        "source_kind": source_kind,
        "seen_at_utc": utc_now(),
    }
    if source_path is not None:
        entry["source_path"] = str(source_path)
    return {
        "source_urls": record.get("source_urls", []),
        "duplicates": [entry],
    }


@stable_api
def merge_update_payload(
    base: dict[str, Any],
    update: dict[str, Any],
    *,
    max_source_urls: int,
    max_duplicates: int,
) -> dict[str, Any]:
    merged_urls = merge_distinct_urls(
        base.get("source_urls", []),
        update.get("source_urls", []),
        max_source_urls,
    )
    duplicates = list(base.get("duplicates", []))
    for entry in update.get("duplicates", []):
        if entry not in duplicates:
            duplicates.append(entry)
    if len(duplicates) > max_duplicates:
        duplicates = duplicates[-max_duplicates:]
    return {
        "source_urls": merged_urls,
        "duplicates": duplicates,
    }


@stable_api
def merge_provenance_update(
    record: dict[str, Any],
    update: dict[str, Any],
    *,
    max_source_urls: int,
    max_duplicates: int,
) -> None:
    record["source_urls"] = merge_distinct_urls(
        record.get("source_urls", []),
        update.get("source_urls", []),
        max_source_urls,
    )
    provenance = record.get("provenance") or {}
    duplicates = list(provenance.get("duplicates", []))
    for entry in update.get("duplicates", []):
        if entry not in duplicates:
            duplicates.append(entry)
    if len(duplicates) > max_duplicates:
        duplicates = duplicates[-max_duplicates:]
    if duplicates:
        provenance["duplicates"] = duplicates
        record["provenance"] = provenance
    record["timestamp_updated"] = utc_now()


@stable_api
class DedupeIndex:
    def __init__(self, path: Path) -> None:
        self.path = path
        ensure_dir(path.parent)
        if path.exists():
            path.unlink()
        self.conn = sqlite3.connect(str(path))
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA synchronous=OFF;")
        self.conn.execute("CREATE TABLE IF NOT EXISTS seen (content_sha256 TEXT PRIMARY KEY)")

    def add_if_new(self, content_hash: str) -> bool:
        cursor = self.conn.execute(
            "INSERT OR IGNORE INTO seen (content_sha256) VALUES (?)",
            (content_hash,),
        )
        return cursor.rowcount == 1

    def close(self) -> None:
        self.conn.commit()
        self.conn.close()


@stable_api
class PartitionedDedupeIndex:
    def __init__(self, path: Path, partitions: int) -> None:
        if partitions < 2:
            raise ValueError("PartitionedDedupeIndex requires at least 2 partitions.")
        self.partitions = partitions
        self.paths = [self._partition_path(path, idx) for idx in range(partitions)]
        self.indices = [DedupeIndex(part_path) for part_path in self.paths]

    @staticmethod
    def _partition_path(path: Path, idx: int) -> Path:
        suffix = path.suffix or ".sqlite"
        stem = path.stem
        return path.with_name(f"{stem}_part{idx:03d}{suffix}")

    def _partition_index(self, content_hash: str) -> int:
        if not content_hash:
            return 0
        return int(content_hash[:8], 16) % self.partitions

    def add_if_new(self, content_hash: str) -> bool:
        idx = self._partition_index(content_hash)
        return self.indices[idx].add_if_new(content_hash)

    def close(self) -> None:
        for index in self.indices:
            index.close()


@stable_api
def build_dedupe_index(
    roots: Roots,
    partitions: int,
) -> DedupeIndex | PartitionedDedupeIndex:

    base_path = roots.ledger_root / "combined_dedupe.sqlite"
    if partitions > 1:
        return PartitionedDedupeIndex(base_path, partitions)
    return DedupeIndex(base_path)
