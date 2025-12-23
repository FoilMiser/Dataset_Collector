#!/usr/bin/env python3
"""
pii_scrub_worker.py (scaffold)

Intended to scrub person registries (e.g., ORCID Public Data File) by removing
emails, biographies, and personal URLs while keeping stable identifiers and
safe linkouts.
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
    (out_dir / "pii_scrub_manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")


def main() -> None:
    ap = argparse.ArgumentParser(description="PII scrubber for KG registries (placeholder)")
    ap.add_argument("--targets", required=True, help="Path to targets.yaml")
    ap.add_argument("--execute", action="store_true", help="Run scrubbing (otherwise plan only)")
    ap.add_argument("--output-dir", default="/data/kg_nav/_staging/pii_scrub_runs", help="Where to place manifests")
    args = ap.parse_args()

    manifest = {
        "status": "planned" if not args.execute else "noop",
        "note": "pii_scrub_worker scaffold - add ORCID/registry scrub logic",
        "targets_config": str(Path(args.targets).expanduser()),
    }
    write_manifest(Path(args.output_dir).expanduser(), manifest)
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
