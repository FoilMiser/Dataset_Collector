"""
collector_core/yellow_review_helpers.py

Shared helper for YELLOW bucket planning and manual review prep.
Consolidates the duplicated logic from physics/cyber/nlp/etc. pipeline yellow_scrubber.py files.

This module does **not** perform domain-specific transforms. Instead it:
1) Reads the YELLOW queue emitted by pipeline_driver.py.
2) Emits a concise summary grouped by target, license profile, and restriction hits.
3) Writes a review plan JSON for humans to triage (assign reviewers, track notes).

Not legal advice.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from collector_core.__version__ import __schema_version__ as SCHEMA_VERSION
from collector_core.__version__ import __version__ as TOOL_VERSION
from collector_core.config_validator import read_yaml as read_yaml_config
from collector_core.dataset_root import ensure_data_root_allowed
from collector_core.utils import ensure_dir, read_jsonl_list, utc_now


@dataclass
class QueueEntry:
    """Represents a single entry in the YELLOW review queue."""

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
    bucket_reason: str
    signals: dict[str, Any]

    @classmethod
    def from_raw(cls, raw: dict[str, Any]) -> QueueEntry:
        """Create a QueueEntry from a raw dictionary."""
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
            bucket_reason=str(raw.get("bucket_reason", "")),
            signals=dict(raw.get("signals", {}) or {}),
        )


def read_targets_yaml(path: Path) -> dict[str, Any]:
    """Read and validate a targets YAML file."""
    return read_yaml_config(path, schema_name="targets") or {}


def load_queue(queue_path: Path, limit: int | None = None) -> list[QueueEntry]:
    """Load the YELLOW queue from a JSONL file."""
    raw_rows = read_jsonl_list(queue_path)
    entries: list[QueueEntry] = []
    for raw in raw_rows[:limit]:
        entries.append(QueueEntry.from_raw(raw))
    return entries


def summarize(entries: Iterable[QueueEntry]) -> dict[str, Any]:
    """Generate summary statistics from queue entries."""
    total = 0
    by_profile: Counter[str] = Counter()
    by_spdx: Counter[str] = Counter()
    by_restriction: Counter[str] = Counter()
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


def write_plan(
    output_path: Path,
    entries: list[QueueEntry],
    summary: dict[str, Any],
    targets_path: Path,
) -> None:
    """Write the review plan JSON file."""
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


def print_summary(domain_name: str, summary: dict[str, Any]) -> None:
    """Print a formatted summary to stdout."""
    print(f"{domain_name} YELLOW queue summary")
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


def run_yellow_review(
    *,
    domain_name: str,
    domain_prefix: str,
    targets_yaml_name: str,
    args: argparse.Namespace,
) -> None:
    """
    Run the yellow review helper for a domain.

    Args:
        domain_name: Human-readable domain name (e.g., "Physics")
        domain_prefix: Domain prefix for paths (e.g., "physics")
        targets_yaml_name: Name of targets file (e.g., "targets_physics.yaml")
        args: Parsed arguments with targets, queue, output, limit
    """
    targets_path = Path(args.targets).expanduser().resolve()
    if not targets_path.exists():
        raise SystemExit(f"Targets file not found: {targets_path}")

    cfg = read_targets_yaml(targets_path)
    queues_root = Path(
        cfg.get("globals", {}).get("queues_root", f"/data/{domain_prefix}/_queues")
    ).expanduser()
    queue_path = Path(args.queue or (queues_root / "yellow_pipeline.jsonl")).expanduser()
    output_path = Path(args.output or (queues_root / "yellow_review_plan.json")).expanduser()
    ensure_data_root_allowed(
        [queue_path, output_path], bool(getattr(args, "allow_data_root", False))
    )

    if not queue_path.exists():
        raise SystemExit(
            f"YELLOW queue not found: {queue_path}. Run pipeline_driver classify stage first."
        )

    entries = load_queue(queue_path, limit=args.limit)
    summary = summarize(entries)

    print_summary(domain_name, summary)
    write_plan(output_path, entries, summary, targets_path)
    print(f"\nReview plan written to: {output_path}")


def make_main(
    domain_name: str,
    domain_prefix: str,
    targets_yaml_name: str,
) -> None:
    """
    Create and run a main function for a domain yellow review helper.

    This is the entry point for per-pipeline yellow_scrubber.py files.
    """
    ap = argparse.ArgumentParser(description=f"{domain_name} pipeline YELLOW review helper")
    ap.add_argument("--targets", required=True, help=f"Path to {targets_yaml_name}")
    ap.add_argument(
        "--queue",
        default=None,
        help="Path to yellow_pipeline.jsonl (auto-detected if omitted)",
    )
    ap.add_argument(
        "--output",
        default=None,
        help="Where to write the review plan (default: queues_root/yellow_review_plan.json)",
    )
    ap.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of YELLOW entries to include",
    )
    ap.add_argument(
        "--allow-data-root",
        action="store_true",
        help="Allow /data defaults for queue/output paths (default: disabled).",
    )
    args = ap.parse_args()

    run_yellow_review(
        domain_name=domain_name,
        domain_prefix=domain_prefix,
        targets_yaml_name=targets_yaml_name,
        args=args,
    )
