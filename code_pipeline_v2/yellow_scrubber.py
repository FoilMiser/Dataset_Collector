"""
Helper script for YELLOW bucket planning and manual review prep.

This script does **not** perform code-specific transforms. Instead it:

1) Reads the YELLOW queue emitted by pipeline_driver.py.
2) Emits a concise summary grouped by target, license profile, and restriction hits.
3) Writes a review plan JSON for humans to triage (assign reviewers, track notes).

Usage examples:
    python yellow_scrubber.py --targets targets_code.yaml
    python yellow_scrubber.py --targets targets_code.yaml --limit 20 --output /tmp/yellow_plan.json

Outputs:
- summary printed to stdout
- review plan JSON (default: <queues_root>/yellow_review_plan.json)

Not legal advice.
"""

from __future__ import annotations

import argparse
import json
import time
from collections import Counter
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import yaml
from collector_core.__version__ import __schema_version__ as VERSION

@dataclass
class QueueEntry:
    id: str
    name: str
    license_profile: str
    resolved_spdx: str
    restriction_hits: list[str]
    download_strategy: str
    routing_subject: str
    routing_domain: str
    routing_category: str
    routing_level: int
    notes: str = ""
    reviewer: str = ""
    status: str = "pending"


def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def load_queue(queue_path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with queue_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def queue_root_from_targets(targets_path: Path) -> Path:
    cfg = yaml.safe_load(targets_path.read_text(encoding="utf-8")) or {}
    queues_root = cfg.get("globals", {}).get("queues_root", "/data/code/_queues")
    return Path(queues_root)


def summary_stats(entries: Iterable[QueueEntry]) -> dict[str, Any]:
    lp = Counter(e.license_profile for e in entries)
    spdx = Counter(e.resolved_spdx for e in entries)
    strat = Counter(e.download_strategy for e in entries)
    return {"license_profiles": lp, "resolved_spdx": spdx, "download_strategies": strat}


def build_review_plan(entries: Iterable[QueueEntry]) -> dict[str, Any]:
    plan = {
        "generated_at_utc": utc_now(),
        "version": VERSION,
        "entries": [asdict(e) for e in entries],
    }
    return plan


def print_summary(entries: list[QueueEntry]) -> None:
    stats = summary_stats(entries)
    print(f"[yellow_scrubber] {len(entries)} targets")
    print(f"License profiles: {stats['license_profiles']}")
    print(f"SPDX hints: {stats['resolved_spdx']}")
    print(f"Download strategies: {stats['download_strategies']}")
    print("")
    for e in entries:
        restriction = f" restriction_hits={len(e.restriction_hits)}" if e.restriction_hits else ""
        routing = f" routing={e.routing_subject}/{e.routing_domain}/{e.routing_category}/l{e.routing_level}"
        print(f"- {e.id} ({e.name}) [{e.license_profile}] spdx={e.resolved_spdx}{restriction}{routing} strategy={e.download_strategy}")


def queue_entry_from_row(row: dict[str, Any]) -> QueueEntry:
    routing = row.get("routing") or {}
    return QueueEntry(
        id=row.get("id", ""),
        name=row.get("name", row.get("id", "")),
        license_profile=row.get("license_profile", "unknown"),
        resolved_spdx=row.get("resolved_spdx", "UNKNOWN"),
        restriction_hits=row.get("restriction_hits") or [],
        download_strategy=(row.get("download", {}) or {}).get("strategy", "unknown"),
        routing_subject=routing.get("subject", "code"),
        routing_domain=routing.get("domain", "multi"),
        routing_category=routing.get("category", "misc"),
        routing_level=int(routing.get("level", 5) or 5),
        notes=row.get("notes", ""),
        reviewer=row.get("reviewer", ""),
        status=row.get("status", "pending"),
    )


def main() -> None:
    ap = argparse.ArgumentParser(description=f"Yellow scrubber v{VERSION}")
    ap.add_argument("--targets", required=True, help="targets_code.yaml")
    ap.add_argument("--queue", default=None, help="Override queue path (defaults to globals.queues_root/yellow_pipeline.jsonl)")
    ap.add_argument("--limit", type=int, default=None, help="Limit number of entries")
    ap.add_argument("--output", default=None, help="Path to write review plan JSON")
    args = ap.parse_args()

    targets_path = Path(args.targets).expanduser().resolve()
    queue_path = Path(args.queue) if args.queue else queue_root_from_targets(targets_path) / "yellow_pipeline.jsonl"
    rows = load_queue(queue_path)
    rows = [r for r in rows if r.get("enabled", True) and r.get("id")]
    if args.limit:
        rows = rows[: args.limit]

    entries = [queue_entry_from_row(r) for r in rows]
    print_summary(entries)

    plan = build_review_plan(entries)
    out_path = Path(args.output) if args.output else queue_path.parent / "yellow_review_plan.json"
    out_path = out_path.expanduser().resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(plan, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"[yellow_scrubber] Review plan written to: {out_path}")


if __name__ == "__main__":
    main()
