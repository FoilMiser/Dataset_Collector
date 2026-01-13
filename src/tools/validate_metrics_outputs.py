#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from collector_core.metrics.dashboard import collect_run_metrics, write_dashboard

REQUIRED_TOP_LEVEL = {"run_id", "pipeline_id", "counts", "timings_ms", "bytes"}


def validate_payload(payload: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    missing = REQUIRED_TOP_LEVEL - set(payload.keys())
    if missing:
        errors.append(f"Missing required keys: {', '.join(sorted(missing))}")
    counts = payload.get("counts")
    if not isinstance(counts, dict):
        errors.append("counts must be a dict")
    timings = payload.get("timings_ms")
    if not isinstance(timings, dict):
        errors.append("timings_ms must be a dict")
    bytes_blob = payload.get("bytes")
    if not isinstance(bytes_blob, dict):
        errors.append("bytes must be a dict")
    return errors


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate pipeline metrics outputs.")
    parser.add_argument("--ledger-root", required=True, help="Ledger root containing run outputs.")
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Optional output directory for dashboard reports.",
    )
    args = parser.parse_args(argv)

    ledger_root = Path(args.ledger_root).expanduser().resolve()
    metrics = collect_run_metrics(ledger_root)
    if not metrics:
        print(f"No metrics.json files found under {ledger_root}")
        return 1

    errors: list[str] = []
    for payload in metrics:
        payload_errors = validate_payload(payload)
        if payload_errors:
            run_id = payload.get("run_id", "<unknown>")
            errors.extend([f"{run_id}: {err}" for err in payload_errors])

    if errors:
        print("Metrics validation failed:")
        for err in errors:
            print(f"- {err}")
        return 1

    if args.output_dir:
        output_dir = Path(args.output_dir).expanduser().resolve()
        write_dashboard(ledger_root, output_dir)

    print("Metrics validation succeeded.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
