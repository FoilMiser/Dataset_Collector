#!/usr/bin/env python3
"""
catalog_builder.py (v2.0)

Builds a lightweight catalog for the v2 code pipeline layout. Summaries are
organized by stage (raw, screened_yellow, combined) and by license pool
when applicable. Adds lightweight language/source coverage stats from combined
shards.
"""

from __future__ import annotations

import argparse
import gzip
import json
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import yaml

VERSION = "2.0"


def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def count_lines(path: Path, max_lines: int = 0) -> int:
    opener = gzip.open if path.suffix == ".gz" else open
    count = 0
    with opener(path, "rt", encoding="utf-8", errors="ignore") as f:
        for count, _ in enumerate(f, start=1):
            if max_lines and count >= max_lines:
                break
    return count


def file_stats(path: Path) -> dict[str, Any]:
    return {"name": path.name, "bytes": path.stat().st_size, "lines_estimate": count_lines(path, max_lines=1000)}


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


def collect_raw_stats(root: Path) -> dict[str, Any]:
    stats: dict[str, Any] = {"path": str(root), "buckets": {}}
    for bucket in ["green", "yellow"]:
        bucket_dir = root / bucket
        bucket_stats = {"targets": 0, "bytes": 0, "files": 0}
        if bucket_dir.exists():
            for pool_dir in bucket_dir.iterdir():
                if not pool_dir.is_dir():
                    continue
                for target_dir in pool_dir.iterdir():
                    if not target_dir.is_dir():
                        continue
                    bucket_stats["targets"] += 1
                    for fp in target_dir.glob("**/*"):
                        if fp.is_file():
                            bucket_stats["files"] += 1
                            bucket_stats["bytes"] += fp.stat().st_size
        stats["buckets"][bucket] = bucket_stats
    return stats


def collect_shard_stage(root: Path) -> dict[str, Any]:
    stage = {"path": str(root), "pools": {}}
    if not root.exists():
        return stage
    for pool_dir in root.iterdir():
        if not pool_dir.is_dir():
            continue
        shard_dir = pool_dir / "shards"
        pool_stats = {"files": 0, "bytes": 0, "examples": []}
        if shard_dir.exists():
            for fp in shard_dir.glob("*.jsonl*"):
                pool_stats["files"] += 1
                pool_stats["bytes"] += fp.stat().st_size
                if len(pool_stats["examples"]) < 3:
                    pool_stats["examples"].append(file_stats(fp))
        stage["pools"][pool_dir.name] = pool_stats
    return stage


def collect_code_stats(combined_root: Path, sample_limit: int = 2000) -> dict[str, Any]:
    stats: dict[str, Any] = {"languages": {}, "avg_loc": 0.0, "samples": 0, "top_sources": {}}
    loc_total = 0
    shards = list(combined_root.rglob("*.jsonl*")) if combined_root.exists() else []
    for fp in shards:
        for row in read_jsonl(fp):
            payload = row.get("text") or row.get("code") or ""
            loc = len(str(payload).splitlines())
            loc_total += loc
            stats["samples"] += 1
            lang = (row.get("code_metadata") or {}).get("language") or (row.get("source") or {}).get("language") or "unknown"
            stats["languages"][lang] = stats["languages"].get(lang, 0) + 1
            source_url = (row.get("source") or {}).get("source_url") or (row.get("source") or {}).get("repo_url")
            if source_url:
                stats["top_sources"][source_url] = stats["top_sources"].get(source_url, 0) + 1
            if stats["samples"] >= sample_limit:
                break
        if stats["samples"] >= sample_limit:
            break
    if stats["samples"]:
        stats["avg_loc"] = loc_total / stats["samples"]
    return stats


def build_catalog(cfg: dict[str, Any]) -> dict[str, Any]:
    g = (cfg.get("globals", {}) or {})
    raw_root = Path(g.get("raw_root", "/data/code/raw"))
    screened_root = Path(g.get("screened_yellow_root", "/data/code/screened_yellow"))
    combined_root = Path(g.get("combined_root", "/data/code/combined"))
    ledger_root = Path(g.get("ledger_root", "/data/code/_ledger"))

    catalog = {
        "generated_at_utc": utc_now(),
        "version": VERSION,
        "raw": collect_raw_stats(raw_root),
        "screened_yellow": collect_shard_stage(screened_root),
        "combined": collect_shard_stage(combined_root),
        "ledgers": {},
        "code": collect_code_stats(combined_root),
    }

    for ledger_name in ["yellow_passed.jsonl", "yellow_pitched.jsonl", "combined_index.jsonl"]:
        lp = ledger_root / ledger_name
        catalog["ledgers"][ledger_name] = {"exists": lp.exists(), "path": str(lp)}

    return catalog


def main() -> None:
    ap = argparse.ArgumentParser(description=f"Catalog Builder v{VERSION}")
    ap.add_argument("--targets", required=True, help="targets_code.yaml")
    ap.add_argument("--output", required=True, help="Output JSON path")
    args = ap.parse_args()

    cfg = yaml.safe_load(Path(args.targets).read_text(encoding="utf-8")) or {}
    catalog = build_catalog(cfg)
    out_path = Path(args.output).expanduser().resolve()
    ensure_dir(out_path.parent)
    out_path.write_text(json.dumps(catalog, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
