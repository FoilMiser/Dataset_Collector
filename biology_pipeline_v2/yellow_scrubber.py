"""
Biology pipeline helper for YELLOW bucket planning and manual review prep.

This script does **not** perform biology-specific transforms; downstream workers
handle PMC/HF filtering and extraction. Instead it:

1) Reads the YELLOW queue emitted by pipeline_driver.py.
2) Emits a concise summary grouped by target, license profile, and restriction hits.
3) Writes a review plan JSON for humans to triage (assign reviewers, track notes).

Usage examples:
    python yellow_scrubber.py --targets targets_biology.yaml
    python yellow_scrubber.py --targets targets_biology.yaml --limit 20 --output /tmp/yellow_plan.json

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

from collector_core.__version__ import __schema_version__ as SCHEMA_VERSION
from collector_core.__version__ import __version__ as TOOL_VERSION
from collector_core.config_validator import read_yaml as read_yaml_config


@dataclass
class QueueEntry:
    id: str
    name: str
    effective_bucket: str
    license_profile: str
    resolved_spdx: str
    restriction_hits: list[str]
    require_yellow_signoff: bool
    review_required: bool
    denylist_hits: list[dict[str, Any]]
    priority: Any
    manifest_dir: str

    @classmethod
    def from_raw(cls, raw: dict[str, Any]) -> QueueEntry:
        return cls(
            id=str(raw.get("id", "")),
            name=str(raw.get("name", "")),
            effective_bucket=str(raw.get("effective_bucket", "")),
            license_profile=str(raw.get("license_profile", "")),
            resolved_spdx=str(raw.get("resolved_spdx", "UNKNOWN")),
            restriction_hits=list(raw.get("restriction_hits", []) or []),
            require_yellow_signoff=bool(raw.get("require_yellow_signoff", False)),
            review_required=bool(raw.get("review_required", False)),
            denylist_hits=list(raw.get("denylist_hits", []) or []),
            priority=raw.get("priority", None),
            manifest_dir=str(raw.get("manifest_dir", "")),
        )


def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def read_yaml(path: Path) -> dict[str, Any]:
    return read_yaml_config(path, schema_name="targets") or {}


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def load_queue(queue_path: Path, limit: int | None = None) -> list[QueueEntry]:
    raw_rows = read_jsonl(queue_path)
    entries: list[QueueEntry] = []
    for raw in raw_rows[:limit]:
        entries.append(QueueEntry.from_raw(raw))
    return entries


def summarize(entries: Iterable[QueueEntry]) -> dict[str, Any]:
    total = 0
    by_profile = Counter()
    by_spdx = Counter()
    by_restriction = Counter()
    needs_review = 0

    for e in entries:
        total += 1
        by_profile[e.license_profile] += 1
        by_spdx[e.resolved_spdx] += 1
        if e.review_required or e.require_yellow_signoff:
            needs_review += 1
        for hit in e.restriction_hits:
            by_restriction[hit] += 1

    return {
        "total": total,
        "by_profile": dict(by_profile),
        "by_spdx": dict(by_spdx.most_common()),
        "restriction_hits": dict(by_restriction.most_common()),
        "review_required": needs_review,
    }


def write_plan(output_path: Path, entries: list[QueueEntry], summary: dict[str, Any], targets_path: Path) -> None:
    ensure_dir(output_path.parent)
    plan = {
        "generated_utc": utc_now(),
        "tool_version": TOOL_VERSION,
        "schema_version": SCHEMA_VERSION,
        "targets_path": str(targets_path),
        "total_entries": len(entries),
        "summary": summary,
        "entries": [asdict(e) for e in entries],
    }
    tmp_path = Path(f"{output_path}.tmp")
    tmp_path.write_text(json.dumps(plan, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    tmp_path.replace(output_path)


def main() -> None:
    ap = argparse.ArgumentParser(description="Math pipeline YELLOW review helper")
    ap.add_argument("--targets", required=True, help="Path to targets_biology.yaml")
    ap.add_argument("--queue", default=None, help="Path to yellow_pipeline.jsonl (auto-detected if omitted)")
    ap.add_argument("--output", default=None, help="Where to write the review plan (default: queues_root/yellow_review_plan.json)")
    ap.add_argument("--limit", type=int, default=None, help="Limit number of YELLOW entries to include")
    args = ap.parse_args()

    targets_path = Path(args.targets).expanduser().resolve()
    if not targets_path.exists():
        raise SystemExit(f"Targets file not found: {targets_path}")

    cfg = read_yaml(targets_path)
    queues_root = Path(cfg.get("globals", {}).get("queues_root", "/data/bio/_queues")).expanduser()
    queue_path = Path(args.queue or (queues_root / "yellow_pipeline.jsonl")).expanduser()
    output_path = Path(args.output or (queues_root / "yellow_review_plan.json")).expanduser()

    if not queue_path.exists():
        raise SystemExit(f"YELLOW queue not found: {queue_path}. Run pipeline_driver classify stage first.")

    entries = load_queue(queue_path, limit=args.limit)
    summary = summarize(entries)

    print("Math YELLOW queue summary")
    print(f"  Total entries: {summary['total']}")
    print(f"  Requires review: {summary['review_required']}")
    if summary["by_profile"]:
        print("  By license profile:")
        for profile, count in summary["by_profile"].items():
            print(f"    - {profile}: {count}")
    if summary["by_spdx"]:
        print("  By resolved SPDX:")
        for spdx, count in summary["by_spdx"].items():
            print(f"    - {spdx}: {count}")
    if summary["restriction_hits"]:
        print("  Restriction hits:")
        for hit, count in summary["restriction_hits"].items():
            print(f"    - {hit}: {count}")

    write_plan(output_path, entries, summary, targets_path)
    print(f"\nReview plan written to: {output_path}")


if __name__ == "__main__":
    main()
