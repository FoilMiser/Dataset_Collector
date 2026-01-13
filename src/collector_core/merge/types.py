from __future__ import annotations

import dataclasses
from pathlib import Path
from typing import TYPE_CHECKING, Any

from collector_core.stability import stable_api

if TYPE_CHECKING:
    from collector_core.checks.near_duplicate import NearDuplicateDetector
    from collector_core.merge.dedupe import DedupeIndex, PartitionedDedupeIndex
    from collector_core.merge.shard import Sharder


@stable_api
@dataclasses.dataclass(frozen=True)
class RootDefaults:
    raw_root: str
    screened_root: str
    combined_root: str
    ledger_root: str


@stable_api
@dataclasses.dataclass
class Roots:
    raw_root: Path
    screened_root: Path
    combined_root: Path
    ledger_root: Path


@stable_api
@dataclasses.dataclass
class ShardingConfig:
    max_records_per_shard: int
    compression: str
    prefix: str


@stable_api
@dataclasses.dataclass
class GreenInput:
    raw: dict[str, Any]
    target_id: str
    pool: str
    source_path: Path
    source_kind: str


@stable_api
@dataclasses.dataclass
class GreenSkip:
    target_id: str
    pool: str
    source_path: Path
    source_kind: str
    reason: str
    detail: dict[str, Any] | None = None


@stable_api
@dataclasses.dataclass
class MergeState:
    summary: dict[str, Any]
    dedupe: DedupeIndex | PartitionedDedupeIndex
    near_dedup: NearDuplicateDetector | None
    near_dedup_text_field: str
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


@stable_api
@dataclasses.dataclass
class MergeRuntimeConfig:
    progress: bool = False
    progress_interval: int = 10000
    trace_memory: bool = False
    profile: bool = False
    profile_path: Path | None = None
    profile_sort: str = "tottime"
    dedupe_partitions: int = 1
    near_dedup: bool = False
    near_dedup_text_field: str = "text"
    near_dedup_threshold: float = 0.85
    near_dedup_backend: str | None = None
    near_dedup_num_perm: int = 128
    near_dedup_shingle_size: int = 3
    near_dedup_max_tokens: int = 2000
    near_dedup_max_candidates: int = 50
