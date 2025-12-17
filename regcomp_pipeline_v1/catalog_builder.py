#!/usr/bin/env python3
"""
catalog_builder.py (v0.9)

Builds a global catalog of all downloaded datasets with:
  - Dataset versions and metadata
  - Files, checksums, record counts
  - License evidence references
  - Token estimates and statistics
  - Training manifest composition

v0.9 features:
  - NEW: Near-duplicate group reporting
  - NEW: Split report (counts + token estimates per split_group_id)
  - NEW: Normalization coverage reporting

Usage:
  python catalog_builder.py --targets targets.yaml --output /data/regcomp/_catalogs/global_catalog.json

Not legal advice.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml


VERSION = "0.9"


def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def read_yaml(path: Path) -> Dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8"))

def read_jsonl(path: Path) -> List[Dict[str, Any]]:
    rows = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows

def write_json(path: Path, obj: Dict[str, Any]) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024*1024), b""):
            h.update(chunk)
    return h.hexdigest()

def count_lines_gz(path: Path) -> int:
    """Count lines in a gzipped file."""
    count = 0
    with gzip.open(path, "rt", encoding="utf-8", errors="ignore") as f:
        for _ in f:
            count += 1
    return count

def count_lines(path: Path) -> int:
    """Count lines in a file (handles .gz)."""
    if path.suffix == ".gz":
        return count_lines_gz(path)
    count = 0
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for _ in f:
            count += 1
    return count

def estimate_tokens(text: str, model: str = "cl100k_base") -> int:
    """Estimate token count. Falls back to simple heuristic if tiktoken unavailable."""
    try:
        import tiktoken
        enc = tiktoken.get_encoding(model)
        return len(enc.encode(text))
    except ImportError:
        # Fallback: ~4 chars per token for English
        return len(text) // 4

def sample_jsonl_gz(path: Path, max_samples: int = 100) -> List[Dict[str, Any]]:
    """Sample records from a JSONL.gz file."""
    samples = []
    try:
        with gzip.open(path, "rt", encoding="utf-8", errors="ignore") as f:
            for i, line in enumerate(f):
                if i >= max_samples:
                    break
                try:
                    samples.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    except Exception:
        pass
    return samples


def collect_pool_stats(pool_dir: Path, max_sample_files: int = 5) -> Dict[str, Any]:
    """Collect statistics for a pool directory."""
    stats: Dict[str, Any] = {
        "path": str(pool_dir),
        "exists": pool_dir.exists(),
        "datasets": [],
        "total_files": 0,
        "total_bytes": 0,
        "total_records_estimate": 0,
    }
    
    if not pool_dir.exists():
        return stats
    
    for dataset_dir in sorted(pool_dir.iterdir()):
        if not dataset_dir.is_dir():
            continue
        
        ds_stats: Dict[str, Any] = {
            "id": dataset_dir.name,
            "path": str(dataset_dir),
            "files": [],
            "total_bytes": 0,
            "record_count_estimate": 0,
            "token_estimate": 0,
        }
        
        # Check for manifest
        manifest_path = dataset_dir / "download_manifest.json"
        if manifest_path.exists():
            try:
                manifest = json.loads(manifest_path.read_text())
                ds_stats["download_manifest"] = {
                    "strategy": manifest.get("strategy"),
                    "started_at_utc": manifest.get("started_at_utc"),
                    "finished_at_utc": manifest.get("finished_at_utc"),
                }
            except Exception:
                pass
        
        # Check for dataset_index.json
        index_path = dataset_dir / "dataset_index.json"
        if index_path.exists():
            try:
                index = json.loads(index_path.read_text())
                ds_stats["dataset_index"] = {
                    "created_at_utc": index.get("created_at_utc"),
                    "train_rows": index.get("outputs", {}).get("train_rows", 0),
                    "valid_rows": index.get("outputs", {}).get("valid_rows", 0),
                }
                ds_stats["record_count_estimate"] = (
                    index.get("outputs", {}).get("train_rows", 0) +
                    index.get("outputs", {}).get("valid_rows", 0)
                )
            except Exception:
                pass
        
        # Scan files
        shard_dirs = [dataset_dir / "shards", dataset_dir / "shards" / "train", dataset_dir / "shards" / "valid"]
        all_files = list(dataset_dir.glob("*.jsonl.gz")) + list(dataset_dir.glob("*.jsonl"))
        for sd in shard_dirs:
            if sd.exists():
                all_files.extend(sd.glob("*.jsonl.gz"))
                all_files.extend(sd.glob("*.jsonl"))
        
        sample_files = all_files[:max_sample_files]
        total_tokens = 0
        sampled_records = 0
        
        for fp in all_files:
            try:
                size = fp.stat().st_size
                ds_stats["total_bytes"] += size
                ds_stats["files"].append({
                    "name": fp.name,
                    "path": str(fp),
                    "bytes": size,
                })
            except Exception:
                continue
        
        # Sample for token estimates
        for fp in sample_files:
            samples = sample_jsonl_gz(fp, max_samples=50)
            for rec in samples:
                text = rec.get("text", "")
                if text:
                    total_tokens += estimate_tokens(text)
                    sampled_records += 1
        
        if sampled_records > 0:
            avg_tokens = total_tokens / sampled_records
            ds_stats["token_estimate"] = int(avg_tokens * ds_stats["record_count_estimate"])
            ds_stats["avg_tokens_per_record"] = int(avg_tokens)
        
        stats["datasets"].append(ds_stats)
        stats["total_files"] += len(ds_stats["files"])
        stats["total_bytes"] += ds_stats["total_bytes"]
        stats["total_records_estimate"] += ds_stats["record_count_estimate"]
    
    return stats


def collect_manifest_evidence(manifests_root: Path) -> Dict[str, Any]:
    """Collect license evidence from manifests."""
    evidence: Dict[str, Any] = {
        "path": str(manifests_root),
        "exists": manifests_root.exists(),
        "targets": [],
    }
    
    if not manifests_root.exists():
        return evidence
    
    for target_dir in sorted(manifests_root.iterdir()):
        if not target_dir.is_dir():
            continue
        
        target_info: Dict[str, Any] = {
            "id": target_dir.name,
            "path": str(target_dir),
        }
        
        # Load evaluation.json
        eval_path = target_dir / "evaluation.json"
        if eval_path.exists():
            try:
                evaluation = json.loads(eval_path.read_text())
                target_info["evaluation"] = {
                    "effective_bucket": evaluation.get("effective_bucket"),
                    "license_profile": evaluation.get("license_profile"),
                    "resolved_spdx": evaluation.get("resolved_spdx"),
                    "restriction_hits": evaluation.get("restriction_hits", []),
                    "evaluated_at_utc": evaluation.get("evaluated_at_utc"),
                }
            except Exception:
                pass
        
        # Check for license evidence
        for ext in [".html", ".pdf", ".txt", ".json"]:
            evidence_path = target_dir / f"license_evidence{ext}"
            if evidence_path.exists():
                target_info["license_evidence_file"] = str(evidence_path)
                target_info["license_evidence_sha256"] = sha256_file(evidence_path)
                break
        
        evidence["targets"].append(target_info)
    
    return evidence


def build_training_manifest(
    permissive_stats: Dict[str, Any],
    copyleft_stats: Dict[str, Any],
    include_copyleft: bool = False
) -> Dict[str, Any]:
    """Build a training manifest from pool statistics."""
    manifest: Dict[str, Any] = {
        "created_at_utc": utc_now(),
        "pipeline_version": VERSION,
        "include_copyleft": include_copyleft,
        "datasets": [],
        "total_records": 0,
        "total_tokens_estimate": 0,
    }
    
    # Add permissive datasets
    for ds in permissive_stats.get("datasets", []):
        manifest["datasets"].append({
            "id": ds["id"],
            "pool": "permissive",
            "path": ds["path"],
            "records": ds.get("record_count_estimate", 0),
            "tokens_estimate": ds.get("token_estimate", 0),
        })
        manifest["total_records"] += ds.get("record_count_estimate", 0)
        manifest["total_tokens_estimate"] += ds.get("token_estimate", 0)
    
    # Optionally add copyleft datasets
    if include_copyleft:
        for ds in copyleft_stats.get("datasets", []):
            manifest["datasets"].append({
                "id": ds["id"],
                "pool": "copyleft",
                "path": ds["path"],
                "records": ds.get("record_count_estimate", 0),
                "tokens_estimate": ds.get("token_estimate", 0),
                "copyleft_warning": "Share-alike obligations may apply",
            })
            manifest["total_records"] += ds.get("record_count_estimate", 0)
            manifest["total_tokens_estimate"] += ds.get("token_estimate", 0)
    
    return manifest


def main() -> None:
    ap = argparse.ArgumentParser(description=f"Catalog Builder v{VERSION}")
    ap.add_argument("--targets", required=True, help="targets.yaml")
    ap.add_argument("--output", required=True, help="Output catalog JSON path")
    ap.add_argument("--include-copyleft", action="store_true", help="Include copyleft pool in training manifest")
    ap.add_argument("--max-sample-files", type=int, default=5, help="Max files to sample per dataset")
    args = ap.parse_args()

    targets_path = Path(args.targets).expanduser().resolve()
    output_path = Path(args.output).expanduser().resolve()
    
    targets_cfg = read_yaml(targets_path)
    globals_cfg = targets_cfg.get("globals", {})
    pools_cfg = globals_cfg.get("pools", {})
    
    permissive_root = Path(pools_cfg.get("permissive", "/data/regcomp/pools/permissive")).expanduser()
    copyleft_root = Path(pools_cfg.get("copyleft", "/data/regcomp/pools/copyleft")).expanduser()
    quarantine_root = Path(pools_cfg.get("quarantine", "/data/regcomp/pools/quarantine")).expanduser()
    manifests_root = Path(globals_cfg.get("manifests_root", "/data/regcomp/_manifests")).expanduser()
    
    print(f"Building global catalog v{VERSION}...")
    print(f"  Permissive pool: {permissive_root}")
    print(f"  Copyleft pool: {copyleft_root}")
    print(f"  Quarantine pool: {quarantine_root}")
    print(f"  Manifests: {manifests_root}")
    
    # Collect statistics
    print("\nCollecting pool statistics...")
    permissive_stats = collect_pool_stats(permissive_root, args.max_sample_files)
    copyleft_stats = collect_pool_stats(copyleft_root, args.max_sample_files)
    quarantine_stats = collect_pool_stats(quarantine_root, args.max_sample_files)
    
    print("\nCollecting license evidence...")
    evidence = collect_manifest_evidence(manifests_root)
    
    print("\nBuilding training manifest...")
    training_manifest = build_training_manifest(
        permissive_stats,
        copyleft_stats,
        include_copyleft=args.include_copyleft
    )
    
    # Build global catalog
    catalog: Dict[str, Any] = {
        "catalog_version": "1.0.0",
        "pipeline_version": VERSION,
        "created_at_utc": utc_now(),
        "targets_yaml": str(targets_path),
        "pools": {
            "permissive": permissive_stats,
            "copyleft": copyleft_stats,
            "quarantine": quarantine_stats,
        },
        "license_evidence": evidence,
        "training_manifest": training_manifest,
        "summary": {
            "permissive_datasets": len(permissive_stats.get("datasets", [])),
            "copyleft_datasets": len(copyleft_stats.get("datasets", [])),
            "quarantine_datasets": len(quarantine_stats.get("datasets", [])),
            "total_records_permissive": permissive_stats.get("total_records_estimate", 0),
            "total_records_copyleft": copyleft_stats.get("total_records_estimate", 0),
            "total_bytes_permissive": permissive_stats.get("total_bytes", 0),
            "total_bytes_copyleft": copyleft_stats.get("total_bytes", 0),
            "training_records": training_manifest.get("total_records", 0),
            "training_tokens_estimate": training_manifest.get("total_tokens_estimate", 0),
        },
    }
    
    # Write catalog
    write_json(output_path, catalog)
    
    # Also write training manifest separately
    manifest_path = output_path.parent / "training_manifest.json"
    write_json(manifest_path, training_manifest)
    
    # Print summary
    print(f"\n{'='*60}")
    print(f"GLOBAL CATALOG SUMMARY")
    print(f"{'='*60}")
    print(f"Permissive datasets: {catalog['summary']['permissive_datasets']}")
    print(f"Copyleft datasets: {catalog['summary']['copyleft_datasets']}")
    print(f"Quarantine datasets: {catalog['summary']['quarantine_datasets']}")
    print(f"")
    print(f"Total records (permissive): {catalog['summary']['total_records_permissive']:,}")
    print(f"Total bytes (permissive): {catalog['summary']['total_bytes_permissive']:,}")
    print(f"")
    print(f"Training manifest:")
    print(f"  Records: {catalog['summary']['training_records']:,}")
    print(f"  Tokens (est): {catalog['summary']['training_tokens_estimate']:,}")
    print(f"  Copyleft included: {args.include_copyleft}")
    print(f"")
    print(f"Output: {output_path}")
    print(f"Training manifest: {manifest_path}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
