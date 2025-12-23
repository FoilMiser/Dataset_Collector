#!/usr/bin/env python3
"""
merge_worker.py (v2.0)

Merges canonical GREEN records with screened YELLOW shards into combined shards
with lightweight deduplication on content_sha256.

Outputs:
  - combined/{license_pool}/shards/combined_00000.jsonl.gz
  - _ledger/combined_index.jsonl (content_sha256 -> shard mapping)
"""

from __future__ import annotations

import argparse
import dataclasses
import gzip
import json
import time
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Set

import yaml

VERSION = "2.0"


def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def read_jsonl(path: Path) -> Iterator[Dict[str, Any]]:
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    continue


def append_jsonl(path: Path, rows: Iterable[Dict[str, Any]]) -> None:
    ensure_dir(path.parent)
    opener = gzip.open if path.suffix == ".gz" else open
    mode = "ab" if path.suffix == ".gz" else "at"
    if path.suffix == ".gz":
        with opener(path, mode) as f:  # type: ignore
            for row in rows:
                f.write((json.dumps(row, ensure_ascii=False) + "\n").encode("utf-8"))
    else:
        with opener(path, mode, encoding="utf-8") as f:  # type: ignore
            for row in rows:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")


def write_json(path: Path, obj: Dict[str, Any]) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


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


class Sharder:
    def __init__(self, base_dir: Path, cfg: ShardingConfig):
        self.base_dir = base_dir
        self.cfg = cfg
        self.current: List[Dict[str, Any]] = []
        self.shard_idx = 0

    def _path(self) -> Path:
        suffix = "jsonl.gz" if self.cfg.compression == "gzip" else "jsonl"
        return self.base_dir / f"{self.cfg.prefix}_{self.shard_idx:05d}.{suffix}"

    def add(self, row: Dict[str, Any]) -> Optional[Path]:
        self.current.append(row)
        if len(self.current) >= self.cfg.max_records_per_shard:
            path = self.flush()
            self.shard_idx += 1
            return path
        return None

    def flush(self) -> Optional[Path]:
        if not self.current:
            return None
        path = self._path()
        append_jsonl(path, self.current)
        self.current = []
        return path


def resolve_roots(cfg: Dict[str, Any]) -> Roots:
    g = (cfg.get("globals", {}) or {})
    return Roots(
        raw_root=Path(g.get("raw_root", "/data/engineering/raw")),
        screened_root=Path(g.get("screened_yellow_root", "/data/engineering/screened_yellow")),
        combined_root=Path(g.get("combined_root", "/data/engineering/combined")),
        ledger_root=Path(g.get("ledger_root", "/data/engineering/_ledger")),
    )


def sharding_cfg(cfg: Dict[str, Any]) -> ShardingConfig:
    g = (cfg.get("globals", {}).get("sharding", {}) or {})
    return ShardingConfig(
        max_records_per_shard=int(g.get("max_records_per_shard", 50000)),
        compression=str(g.get("compression", "gzip")),
        prefix="combined",
    )


def iter_green_records(roots: Roots) -> Iterator[Dict[str, Any]]:
    base = roots.raw_root / "green"
    for pool_dir in base.iterdir() if base.exists() else []:
        if not pool_dir.is_dir():
            continue
        for target_dir in pool_dir.iterdir():
            if not target_dir.is_dir():
                continue
            for fp in target_dir.glob("**/*.jsonl*"):
                yield from read_jsonl(fp)


def iter_screened_yellow(roots: Roots) -> Iterator[Dict[str, Any]]:
    base = roots.screened_root
    for pool_dir in base.iterdir() if base.exists() else []:
        shards_dir = pool_dir / "shards"
        if not shards_dir.exists():
            continue
        for fp in shards_dir.glob("*.jsonl*"):
            yield from read_jsonl(fp)


def route_pool(record: Dict[str, Any]) -> str:
    src = record.get("source", {}) or {}
    lp = src.get("license_profile") or record.get("license_profile")
    lp = str(lp or "quarantine").lower()
    if lp not in {"permissive", "copyleft", "quarantine"}:
        lp = "quarantine"
    return lp


def merge_records(cfg: Dict[str, Any], roots: Roots, execute: bool) -> Dict[str, Any]:
    shard_cfg = sharding_cfg(cfg)
    dedupe: Set[str] = set()
    summary = {"written": 0, "deduped": 0, "shards": []}

    pool_sharders: Dict[str, Sharder] = {}

    def get_sharder(pool: str) -> Sharder:
        if pool not in pool_sharders:
            sharder = Sharder(roots.combined_root / pool / "shards", shard_cfg)
            pool_sharders[pool] = sharder
            ensure_dir(sharder.base_dir)
        return pool_sharders[pool]

    def handle_record(rec: Dict[str, Any]) -> None:
        content_hash = ((rec.get("hash") or {}).get("content_sha256") or rec.get("content_sha256"))
        if not content_hash:
            return
        if content_hash in dedupe:
            summary["deduped"] += 1
            return
        dedupe.add(content_hash)
        pool = route_pool(rec)
        sharder = get_sharder(pool)
        shard_path = str(sharder._path())
        if execute:
            path = sharder.add(rec)
            if path:
                shard_path = str(path)
                summary["shards"].append(shard_path)
            append_jsonl(roots.ledger_root / "combined_index.jsonl", [{
                "content_sha256": content_hash,
                "license_pool": pool,
                "output_shard": shard_path,
                "source": rec.get("source", {}),
                "seen_at_utc": utc_now(),
            }])
        summary["written"] += 1

    for rec in iter_green_records(roots):
        handle_record(rec)
    for rec in iter_screened_yellow(roots):
        handle_record(rec)

    if execute:
        for sharder in pool_sharders.values():
            flushed = sharder.flush()
            if flushed:
                summary["shards"].append(str(flushed))

    summary["finished_at_utc"] = utc_now()
    return summary


def main() -> None:
    ap = argparse.ArgumentParser(description=f"Merge Worker v{VERSION}")
    ap.add_argument("--targets", required=True, help="targets_engineering.yaml")
    ap.add_argument("--execute", action="store_true", help="Write combined shards")
    args = ap.parse_args()

    cfg = yaml.safe_load(Path(args.targets).read_text(encoding="utf-8")) or {}
    roots = resolve_roots(cfg)
    summary = merge_records(cfg, roots, args.execute)
    write_json(roots.ledger_root / "merge_summary.json", summary)


if __name__ == "__main__":
    main()
