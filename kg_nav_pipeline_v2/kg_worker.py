#!/usr/bin/env python3
"""
kg_worker.py (scaffold)

Placeholder worker for normalizing raw KG dumps (OpenAlex, Wikidata, ROR,
OpenCitations) into minimal node/edge/provenance JSONL shards. Keeps identifiers
and structural fields only.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def write_manifest(out_dir: Path, manifest: Dict[str, Any]) -> None:
    ensure_dir(out_dir)
    (out_dir / "kg_worker_manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")


def main() -> None:
    ap = argparse.ArgumentParser(description="KG normalization worker (placeholder)")
    ap.add_argument("--targets", required=True, help="Path to targets.yaml for pool roots")
    ap.add_argument("--execute", action="store_true", help="Execute transforms (otherwise plan only)")
    ap.add_argument("--output-dir", default="/data/kg_nav/_staging/kg_worker_runs", help="Where to place manifests")
    args = ap.parse_args()

    out_dir = Path(args.output_dir).expanduser()
    manifest = {
        "status": "planned" if not args.execute else "noop",
        "note": "kg_worker scaffold - implement graph normalization",
        "targets_config": str(Path(args.targets).expanduser()),
    }
    write_manifest(out_dir, manifest)
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
