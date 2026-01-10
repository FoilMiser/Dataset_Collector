#!/usr/bin/env python3
"""
catalog_builder.py (v2.0)

Builds a lightweight catalog for the v2 pipeline layout. Summaries are
organized by stage (raw, screened_yellow, combined) and by license pool
when applicable.
"""

from __future__ import annotations

import argparse
import gzip
import json
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from collector_core.artifact_metadata import build_artifact_metadata
from collector_core.config_validator import read_yaml
from collector_core.__version__ import __version__ as PIPELINE_VERSION


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


def collect_raw_stats(root: Path, top_n: int) -> dict[str, Any]:
    stats: dict[str, Any] = {"path": str(root), "buckets": {}, "top_targets_by_bytes": []}
    top_targets: list[dict[str, Any]] = []
    for bucket in ["green", "yellow"]:
        bucket_dir = root / bucket
        bucket_stats = {"targets": 0, "bytes": 0, "files": 0, "pools": {}}
        if bucket_dir.exists():
            for pool_dir in bucket_dir.iterdir():
                if not pool_dir.is_dir():
                    continue
                pool_stats = {"targets": 0, "bytes": 0, "files": 0}
                for target_dir in pool_dir.iterdir():
                    if not target_dir.is_dir():
                        continue
                    pool_stats["targets"] += 1
                    target_bytes = 0
                    target_files = 0
                    for fp in target_dir.glob("**/*"):
                        if fp.is_file():
                            size = fp.stat().st_size
                            target_files += 1
                            target_bytes += size
                            pool_stats["files"] += 1
                            pool_stats["bytes"] += size
                    top_targets.append(
                        {
                            "target_id": target_dir.name,
                            "bucket": bucket,
                            "pool": pool_dir.name,
                            "bytes": target_bytes,
                            "files": target_files,
                        }
                    )
                bucket_stats["targets"] += pool_stats["targets"]
                bucket_stats["bytes"] += pool_stats["bytes"]
                bucket_stats["files"] += pool_stats["files"]
                bucket_stats["pools"][pool_dir.name] = pool_stats
        stats["buckets"][bucket] = bucket_stats
    stats["top_targets_by_bytes"] = sorted(top_targets, key=lambda item: item["bytes"], reverse=True)[:top_n]
    return stats


def iter_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return rows


def collect_queue_stats(root: Path) -> dict[str, Any]:
    queue_files = {
        "green": root / "green_download.jsonl",
        "yellow": root / "yellow_pipeline.jsonl",
        "red": root / "red_rejected.jsonl",
    }
    stats: dict[str, Any] = {"path": str(root), "buckets": {}, "license_counts": {}}
    license_counts: Counter[str] = Counter()
    for bucket, path in queue_files.items():
        pool_counts: Counter[str] = Counter()
        rows = iter_jsonl(path)
        for row in rows:
            pool = row.get("output_pool") or row.get("license_profile") or "unknown"
            pool_counts[str(pool)] += 1
            license_name = row.get("resolved_spdx") or row.get("spdx_hint") or "unknown"
            license_counts[str(license_name)] += 1
        stats["buckets"][bucket] = {"targets": len(rows), "pools": dict(pool_counts)}
    stats["license_counts"] = dict(license_counts)
    return stats


def collect_strategy_counts(cfg: dict[str, Any]) -> dict[str, int]:
    counter: Counter[str] = Counter()
    for target in cfg.get("targets", []) or []:
        download = target.get("download", {}) or {}
        strategy = download.get("strategy") or "unknown"
        counter[str(strategy)] += 1
    return dict(counter)


def build_license_pool_summary(raw_stats: dict[str, Any], queue_stats: dict[str, Any]) -> dict[str, Any]:
    summary: dict[str, Any] = {"green": {}, "yellow": {}, "red": {}}
    for bucket in ["green", "yellow"]:
        for pool, pool_stats in (raw_stats.get("buckets", {}).get(bucket, {}).get("pools", {}) or {}).items():
            summary[bucket][pool] = {
                "targets": pool_stats.get("targets", 0),
                "bytes": pool_stats.get("bytes", 0),
            }
    red_pools = queue_stats.get("buckets", {}).get("red", {}).get("pools", {}) or {}
    for pool, count in red_pools.items():
        summary["red"][pool] = {"targets": count, "bytes": 0}
    return summary


def top_n_counts(counter: Counter[str], top_n: int) -> list[dict[str, Any]]:
    return [
        {"license": value, "count": count}
        for value, count in sorted(counter.items(), key=lambda item: (-item[1], item[0]))[:top_n]
    ]


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


def pipeline_slug_from_id(pipeline_id: str | None) -> str | None:
    if not pipeline_id:
        return None
    if pipeline_id.endswith("_pipeline_v2"):
        return pipeline_id[: -len("_pipeline_v2")]
    return pipeline_id


def derive_pipeline_slug_from_cfg(cfg: dict[str, Any]) -> str | None:
    g = cfg.get("globals", {}) or {}
    raw_root = g.get("raw_root")
    if raw_root:
        parts = Path(raw_root).parts
        if len(parts) >= 3 and parts[1] == "data":
            return parts[2]
    return None


def default_root(pipeline_slug: str | None, suffix: str) -> Path:
    if pipeline_slug:
        return Path(f"/data/{pipeline_slug}/{suffix}")
    return Path(f"/data/{suffix}")


def build_catalog(cfg: dict[str, Any], pipeline_slug: str | None = None) -> dict[str, Any]:
    g = (cfg.get("globals", {}) or {})
    raw_root = Path(g.get("raw_root", default_root(pipeline_slug, "raw")))
    screened_root = Path(g.get("screened_yellow_root", default_root(pipeline_slug, "screened_yellow")))
    combined_root = Path(g.get("combined_root", default_root(pipeline_slug, "combined")))
    ledger_root = Path(g.get("ledger_root", default_root(pipeline_slug, "_ledger")))
    queues_root = Path(g.get("queues_root", default_root(pipeline_slug, "_queues")))
    top_n = int(g.get("catalog_top_n", 10))

    generated_at = utc_now()
    raw_stats = collect_raw_stats(raw_root, top_n)
    queue_stats = collect_queue_stats(queues_root)
    license_pool_summary = build_license_pool_summary(raw_stats, queue_stats)
    license_counts = Counter(queue_stats.get("license_counts", {}))
    catalog = {
        "generated_at_utc": generated_at,
        "version": PIPELINE_VERSION,
        "raw": raw_stats,
        "screened_yellow": collect_shard_stage(screened_root),
        "combined": collect_shard_stage(combined_root),
        "ledgers": {},
        "queues": queue_stats,
        "license_pools": license_pool_summary,
        "strategy_counts": collect_strategy_counts(cfg),
        "top_licenses": top_n_counts(license_counts, top_n),
    }
    catalog.update(build_artifact_metadata(written_at_utc=generated_at))
    catalog["top_targets_by_bytes"] = raw_stats.get("top_targets_by_bytes", [])

    for ledger_name in ["yellow_passed.jsonl", "yellow_pitched.jsonl", "combined_index.jsonl"]:
        lp = ledger_root / ledger_name
        catalog["ledgers"][ledger_name] = {"exists": lp.exists(), "path": str(lp)}

    return catalog


def main(*, pipeline_id: str | None = None) -> None:
    ap = argparse.ArgumentParser(description=f"Catalog Builder v{PIPELINE_VERSION}")
    ap.add_argument("--targets", required=True, help="targets YAML")
    ap.add_argument("--output", required=True, help="Output JSON path")
    args = ap.parse_args()

    cfg = read_yaml(Path(args.targets), schema_name="targets") or {}
    pipeline_slug = pipeline_slug_from_id(pipeline_id) or derive_pipeline_slug_from_cfg(cfg)
    catalog = build_catalog(cfg, pipeline_slug=pipeline_slug)
    out_path = Path(args.output).expanduser().resolve()
    ensure_dir(out_path.parent)
    tmp_path = Path(f"{out_path}.tmp")
    tmp_path.write_text(json.dumps(catalog, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    tmp_path.replace(out_path)


if __name__ == "__main__":
    main()
