from __future__ import annotations

from pathlib import Path
from typing import Any

from collector_core.merge.types import ShardingConfig
from collector_core.stability import stable_api
from collector_core.utils import append_jsonl, ensure_dir


@stable_api
class Sharder:
    def __init__(self, base_dir: Path, cfg: ShardingConfig) -> None:
        self.base_dir = base_dir
        self.cfg = cfg
        self.current: list[dict[str, Any]] = []
        self.shard_idx = 0

    def _path(self) -> Path:
        suffix = "jsonl.gz" if self.cfg.compression == "gzip" else "jsonl"
        return self.base_dir / f"{self.cfg.prefix}_{self.shard_idx:05d}.{suffix}"

    def add(self, row: dict[str, Any]) -> tuple[Path | None, list[dict[str, Any]]]:
        self.current.append(row)
        if len(self.current) >= self.cfg.max_records_per_shard:
            path, flushed = self.flush()
            self.shard_idx += 1
            return path, flushed
        return None, []

    def flush(self) -> tuple[Path | None, list[dict[str, Any]]]:
        if not self.current:
            return None, []
        path = self._path()
        records = self.current
        append_jsonl(path, records)
        self.current = []
        return path, records


@stable_api
def sharding_cfg(cfg: dict[str, Any]) -> ShardingConfig:
    g = cfg.get("globals", {}).get("sharding", {}) or {}
    return ShardingConfig(
        max_records_per_shard=int(g.get("max_records_per_shard", 50000)),
        compression=str(g.get("compression", "gzip")),
        prefix="combined",
    )


@stable_api
def ensure_shard_dir(sharder: Sharder) -> None:
    ensure_dir(sharder.base_dir)
