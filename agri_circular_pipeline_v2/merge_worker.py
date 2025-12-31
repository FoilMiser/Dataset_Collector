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
import hashlib
import json
import re
import time
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any

import yaml

VERSION = "2.0"


def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def read_jsonl(path: Path) -> Iterator[dict[str, Any]]:
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    continue


def append_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> None:
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


def write_json(path: Path, obj: dict[str, Any]) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def sha256_text(text: str) -> str:
    norm = re.sub(r"\\s+", " ", (text or "").strip())
    return hashlib.sha256(norm.encode("utf-8")).hexdigest()


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
        self.current: list[dict[str, Any]] = []
        self.shard_idx = 0

    def _path(self) -> Path:
        suffix = "jsonl.gz" if self.cfg.compression == "gzip" else "jsonl"
        return self.base_dir / f"{self.cfg.prefix}_{self.shard_idx:05d}.{suffix}"

    def add(self, row: dict[str, Any]) -> Path | None:
        self.current.append(row)
        if len(self.current) >= self.cfg.max_records_per_shard:
            path = self.flush()
            self.shard_idx += 1
            return path
        return None

    def flush(self) -> Path | None:
        if not self.current:
            return None
        path = self._path()
        append_jsonl(path, self.current)
        self.current = []
        return path


def resolve_roots(cfg: dict[str, Any]) -> Roots:
    g = (cfg.get("globals", {}) or {})
    return Roots(
        raw_root=Path(g.get("raw_root", "/data/agri_circular/raw")),
        screened_root=Path(g.get("screened_yellow_root", "/data/agri_circular/screened_yellow")),
        combined_root=Path(g.get("combined_root", "/data/agri_circular/combined")),
        ledger_root=Path(g.get("ledger_root", "/data/agri_circular/_ledger")),
    )


def sharding_cfg(cfg: dict[str, Any]) -> ShardingConfig:
    g = (cfg.get("globals", {}).get("sharding", {}) or {})
    return ShardingConfig(
        max_records_per_shard=int(g.get("max_records_per_shard", 50000)),
        compression=str(g.get("compression", "gzip")),
        prefix="combined",
    )


def iter_data_files(base: Path) -> Iterator[Path]:
    patterns = [
        "*.jsonl",
        "*.jsonl.gz",
        "*.csv",
        "*.csv.gz",
        "*.tsv",
        "*.tsv.gz",
        "*.txt",
        "*.md",
        "*.html",
        "*.htm",
    ]
    seen: set[Path] = set()
    for pattern in patterns:
        for fp in base.glob(pattern):
            if fp.is_file() and fp not in seen:
                seen.add(fp)
                yield fp


def iter_records_from_path(path: Path) -> Iterator[dict[str, Any]]:
    name = path.name.lower()
    if name.endswith(".jsonl") or name.endswith(".jsonl.gz"):
        yield from read_jsonl(path)
        return
    opener = gzip.open if name.endswith(".gz") else open
    if name.endswith(".csv") or name.endswith(".csv.gz"):
        import csv

        with opener(path, newline="", encoding="utf-8", errors="ignore") as f:  # type: ignore[arg-type]
            reader = csv.DictReader(f)
            for idx, row in enumerate(reader):
                row = dict(row)
                row.setdefault("id", f"{path.stem}-{idx}")
                yield row
        return
    if name.endswith(".tsv") or name.endswith(".tsv.gz"):
        import csv

        with opener(path, newline="", encoding="utf-8", errors="ignore") as f:  # type: ignore[arg-type]
            reader = csv.DictReader(f, delimiter="\\t")
            for idx, row in enumerate(reader):
                row = dict(row)
                row.setdefault("id", f"{path.stem}-{idx}")
                yield row
        return
    if any(name.endswith(ext) for ext in (".txt", ".md", ".html", ".htm")):
        try:
            text = opener(path, "rt", encoding="utf-8", errors="ignore").read()  # type: ignore[arg-type]
        except Exception:
            return
        if text:
            yield {"id": path.stem, "text": text, "source": {"origin": str(path)}}


def canonicalize_record(raw: dict[str, Any], pool: str, target_id: str | None) -> dict[str, Any]:
    rec = dict(raw)
    rec.setdefault("license_profile", pool)
    source = rec.get("source", {}) or {}
    source.setdefault("license_profile", pool)
    if target_id:
        source.setdefault("target_id", target_id)
    rec["source"] = source
    text = rec.get("text")
    content_hash = ((rec.get("hash") or {}).get("content_sha256") or rec.get("content_sha256"))
    if not content_hash and text:
        content_hash = sha256_text(str(text))
        rec.setdefault("hash", {})["content_sha256"] = content_hash
    return rec


def iter_green_records(roots: Roots) -> Iterator[dict[str, Any]]:
    base = roots.raw_root / "green"
    for pool_dir in base.iterdir() if base.exists() else []:
        if not pool_dir.is_dir():
            continue
        for target_dir in pool_dir.iterdir():
            if not target_dir.is_dir():
                continue
            for fp in iter_data_files(target_dir):
                for raw in iter_records_from_path(fp):
                    yield canonicalize_record(raw, pool_dir.name, target_dir.name)


def iter_screened_yellow(roots: Roots) -> Iterator[dict[str, Any]]:
    base = roots.screened_root
    for pool_dir in base.iterdir() if base.exists() else []:
        shards_dir = pool_dir / "shards"
        if not shards_dir.exists():
            continue
        for fp in shards_dir.glob("*.jsonl*"):
            yield from read_jsonl(fp)


LICENSE_POOL_MAP = {
    "permissive": "permissive",
    "public_domain": "permissive",
    "record_level": "permissive",
    "copyleft": "copyleft",
    "unknown": "quarantine",
    "quarantine": "quarantine",
    "deny": "quarantine",
}


def route_pool(record: dict[str, Any]) -> str:
    src = record.get("source", {}) or {}
    lp = src.get("license_profile") or record.get("license_profile")
    lp = str(lp or "quarantine").lower()
    return LICENSE_POOL_MAP.get(lp, "quarantine")


def merge_records(cfg: dict[str, Any], roots: Roots, execute: bool) -> dict[str, Any]:
    shard_cfg = sharding_cfg(cfg)
    dedupe: set[str] = set()
    summary = {"written": 0, "deduped": 0, "shards": []}

    pool_sharders: dict[str, Sharder] = {}

    def get_sharder(pool: str) -> Sharder:
        if pool not in pool_sharders:
            sharder = Sharder(roots.combined_root / pool / "shards", shard_cfg)
            pool_sharders[pool] = sharder
            ensure_dir(sharder.base_dir)
        return pool_sharders[pool]

    def handle_record(rec: dict[str, Any]) -> None:
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
    ap.add_argument("--targets", required=True, help="targets_agri_circular.yaml")
    ap.add_argument("--execute", action="store_true", help="Write combined shards")
    args = ap.parse_args()

    cfg = yaml.safe_load(Path(args.targets).read_text(encoding="utf-8")) or {}
    roots = resolve_roots(cfg)
    summary = merge_records(cfg, roots, args.execute)
    write_json(roots.ledger_root / "merge_summary.json", summary)


if __name__ == "__main__":
    main()
