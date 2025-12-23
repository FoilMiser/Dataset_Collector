#!/usr/bin/env python3
"""
nav_episode_builder.py (scaffold)

Placeholder for synthesizing grounded navigation episodes using normalized graph
artifacts (OpenAlex minimal graph, OpenCitations COCI, Wikidata, ROR). Outputs
prompt/answer/evidence/metadata records aligned to nav_episode_v1.0.0 schema.
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
    (out_dir / "nav_episode_manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")


def main() -> None:
    ap = argparse.ArgumentParser(description="Navigation episode builder (placeholder)")
    ap.add_argument("--targets", required=True, help="Path to targets.yaml")
    ap.add_argument("--execute", action="store_true", help="Generate episodes (otherwise plan only)")
    ap.add_argument("--output-dir", default="/data/kg_nav/_staging/nav_episode_runs", help="Where to place manifests")
    args = ap.parse_args()

    manifest: Dict[str, Any] = {
        "status": "planned" if not args.execute else "noop",
        "note": "nav_episode_builder scaffold - implement grounded episode synthesis",
        "targets_config": str(Path(args.targets).expanduser()),
    }
    write_manifest(Path(args.output_dir).expanduser(), manifest)
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
