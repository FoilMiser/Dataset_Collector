#!/usr/bin/env python3
"""
code_worker.py (v0.1)

Lightweight shim for the code corpus pipeline. This is not a full
extractor; it simply validates that a requested target exists in
`targets_code.yaml` and writes a stub status artifact so that pipeline
integrations can confirm the worker was invoked.

The real implementation should follow CODE_PIPELINE_ADAPTATION.md
(gates A–E, secrets scanning, vendored stripping, AST-aware chunking,
and attribution bundles). Until that lands, this file aims to:
  - Fail fast when an unknown target is requested.
  - Emit an auditable stub artifact so downstream steps notice the
    missing implementation.

Not legal advice; you remain responsible for compliance.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import yaml


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Minimal code worker shim. Validates targets and emits a stub artifact "
            "so downstream steps see the missing implementation."
        )
    )
    parser.add_argument("--targets", required=True, help="Path to targets_code.yaml")
    parser.add_argument("--target-id", required=True, help="Target slug to process")
    parser.add_argument(
        "--output",
        required=False,
        default="/tmp/code_worker_stub",
        help="Directory where the stub artifact will be written",
    )
    return parser


def load_targets(targets_path: Path) -> dict[str, Any]:
    if not targets_path.exists():
        raise FileNotFoundError(f"Targets file not found: {targets_path}")
    with targets_path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def ensure_target_exists(targets: dict[str, Any], target_id: str) -> dict[str, Any]:
    if "targets" not in targets:
        raise ValueError("Targets file is missing a top-level 'targets' entry")

    target_entries = targets["targets"]
    if isinstance(target_entries, dict):
        match = target_entries.get(target_id)
        if isinstance(match, dict):
            return match
    elif isinstance(target_entries, list):
        for entry in target_entries:
            if isinstance(entry, dict) and entry.get("id") == target_id:
                return entry

    raise KeyError(f"Target '{target_id}' not found in targets_code.yaml")


def write_stub_artifact(
    output_dir: Path, target_id: str, target_entry: dict[str, Any], targets_path: Path
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    artifact = {
        "target_id": target_id,
        "status": "not_implemented",
        "message": (
            "code_worker.py is a shim; implement AST-aware extraction, secrets scanning, "
            "and license-safe emission per CODE_PIPELINE_ADAPTATION.md"
        ),
        "target_name": target_entry.get("name"),
        "license_profile": target_entry.get("license_profile"),
        "targets_file": str(targets_path),
    }
    artifact_path = output_dir / f"{target_id}_stub.json"
    artifact_path.write_text(json.dumps(artifact, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return artifact_path


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    targets_path = Path(args.targets)
    output_dir = Path(args.output)
    try:
        targets = load_targets(targets_path)
        target_entry = ensure_target_exists(targets, args.target_id)
        artifact_path = write_stub_artifact(output_dir, args.target_id, target_entry, targets_path)
    except Exception as exc:  # noqa: BLE001 - escalate any failure clearly
        sys.stderr.write(f"[code_worker] error: {exc}\n")
        return 1

    sys.stderr.write(
        "[code_worker] Stub artifact written to "
        f"{artifact_path}. Replace this shim with the real implementation.\n"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
