from __future__ import annotations

import dataclasses
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collector_core.merge.dedupe import DedupeIndex, PartitionedDedupeIndex
    from collector_core.merge.shard import Sharder


@dataclasses.dataclass(frozen=True)
class RootDefaults:
    raw_root: str
    screened_root: str
    combined_root: str
    ledger_root: str


@dataclasses.dataclass
class Roots:
    raw_root: Path
    screened_root: Path
    combined_root: Path
    ledger_root: Path


@dataclasses.dataclass
class ShardingConfig:
    max_records_per_shard: int
    compression: str
    prefix: str


@dataclasses.dataclass
class GreenInput:
    raw: dict[str, Any]
    target_id: str
    pool: str
    source_path: Path
    source_kind: str


@dataclasses.dataclass
class GreenSkip:
    target_id: str
    pool: str
    source_path: Path
    source_kind: str
    reason: str
    detail: dict[str, Any] | None = None


@dataclasses.dataclass
class MergeState:
    summary: dict[str, Any]
    dedupe: DedupeIndex | PartitionedDedupeIndex
    shard_cfg: ShardingConfig
    pool_sharders: dict[str, Sharder]
    target_meta: dict[str, dict[str, Any]]
    pipeline_id: str
    execute: bool
    progress: bool
    progress_interval: int
    inflight_records: dict[str, dict[str, Any]]
    shard_index: dict[str, str]
    pending_updates: dict[str, dict[str, Any]]
    max_source_urls: int
    max_duplicates: int


@dataclasses.dataclass
class MergeRuntimeConfig:
    progress: bool = False
    progress_interval: int = 10000
    trace_memory: bool = False
    profile: bool = False
    profile_path: Path | None = None
    profile_sort: str = "tottime"
    dedupe_partitions: int = 1
