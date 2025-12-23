#!/usr/bin/env python3
"""
yellow_scrubber.py (kg-nav placeholder)

Stages reserved for YELLOW bucket processing (e.g., PII scrub for ORCID,
record-level filtering for mixed-license sources). Currently logs a plan so we
can plug in real transforms later.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List

import yaml

VERSION = "0.1"


def utc_now() -> str:
    import time
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def read_yaml(path: Path) -> Dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def load_targets(path: Path) -> Dict[str, Any]:
    cfg = read_yaml(path)
    return {t.get("id"): t for t in cfg.get("targets", []) or []}


def emit_manifest(manifest_dir: Path, data: Dict[str, Any]) -> None:
    ensure_dir(manifest_dir)
    (manifest_dir / "yellow_scrub_manifest.json").write_text(
        json.dumps(data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )


def plan_yellow_tasks(targets: Dict[str, Any]) -> List[Dict[str, Any]]:
    planned: List[Dict[str, Any]] = []
    for tid, t in targets.items():
        bucket = (t.get("license_profile") or "").lower()
        if bucket in {"record_level", "copyleft"} or t.get("review_required"):
            planned.append({"id": tid, "name": t.get("name", tid), "plan": "scrub_or_filter"})
    return planned


def main() -> None:
    ap = argparse.ArgumentParser(description="YELLOW pipeline placeholder for kg_nav")
    ap.add_argument("--targets", required=True, help="Path to targets.yaml")
    ap.add_argument("--output-dir", default="/data/kg_nav/_staging/yellow_runs", help="Where to write manifests")
    ap.add_argument("--execute", action="store_true", help="Execute (no-op for now; still emits manifest)")
    args = ap.parse_args()

    targets_path = Path(args.targets).expanduser().resolve()
    targets = load_targets(targets_path)

    manifest = {
        "version": VERSION,
        "executed": bool(args.execute),
        "generated_at_utc": utc_now(),
        "targets_file": str(targets_path),
        "planned_tasks": plan_yellow_tasks(targets),
        "note": "Transforms not yet implemented; plug in KG scrubbing/extraction here.",
    }

    out_dir = Path(args.output_dir).expanduser()
    emit_manifest(out_dir, manifest)
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
