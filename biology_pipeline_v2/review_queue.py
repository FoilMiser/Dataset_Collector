#!/usr/bin/env python3
"""
review_queue.py (v0.9)

Manual review helper for YELLOW targets.

This script is intentionally lightweight and conservative:
- It reads YELLOW queue JSONL (emitted by pipeline_driver.py)
- It shows a summary of pending items
- It can write a review_signoff.json into each target's manifest dir
- NEW in v0.9: Export reviewed targets to CSV/JSON, extended signoff schema

Signoff file schema (v0.2):
{
  "target_id": "...",
  "status": "approved" | "rejected" | "deferred",
  "reviewer": "Name",
  "reviewer_contact": "email@example.com",      # NEW in v0.9 (optional)
  "reason": "Why",
  "promote_to": "GREEN" | "" ,                  # optional
  "evidence_links_checked": ["url1", "url2"],   # NEW in v0.9 (optional)
  "constraints": "Attribution requirements...",  # NEW in v0.9 (optional)
  "notes": "Additional notes...",               # NEW in v0.9 (optional)
  "reviewed_at_utc": "YYYY-MM-DDTHH:MM:SSZ"
}
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional


VERSION = "0.9"


def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def read_jsonl(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    rows = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return rows


def write_json(path: Path, obj: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def load_existing_signoff(manifest_dir: Path) -> Dict[str, Any]:
    p = manifest_dir / "review_signoff.json"
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


def cmd_list(args: argparse.Namespace) -> int:
    yellow_rows = read_jsonl(Path(args.queue))
    if not yellow_rows:
        print("No YELLOW items found.")
        return 0

    pending = []
    for r in yellow_rows:
        mdir = Path(r.get("manifest_dir", ""))
        signoff = load_existing_signoff(mdir) if mdir else {}
        status = str(signoff.get("status", "") or "").lower()
        if status not in {"approved", "rejected"}:
            pending.append((r, status or "pending"))

    if not pending:
        print("No pending YELLOW items (all have signoffs).")
        return 0

    print("=" * 78)
    print(f"YELLOW REVIEW QUEUE (pending) — v{VERSION} — {utc_now()}")
    print("=" * 78)
    for r, status in pending[: args.limit]:
        print(f"- {r.get('id')}  [{status}]")
        print(f"  name: {r.get('name')}")
        print(f"  license_profile: {r.get('license_profile')}  resolved_spdx: {r.get('resolved_spdx')}")
        rh = r.get("restriction_hits") or []
        if rh:
            print(f"  restriction_hits: {rh[:3]}")
        dl = r.get("denylist_hits") or []
        if dl:
            print(f"  denylist_hits: {dl[:2]}")
        print(f"  license_evidence_url: {r.get('license_evidence_url')}")
        print(f"  manifest_dir: {r.get('manifest_dir')}")
        print()
    if len(pending) > args.limit:
        print(f"... and {len(pending) - args.limit} more")
    return 0


def write_signoff(
    target_id: str,
    manifest_dir: Path,
    status: str,
    reviewer: str,
    reason: str,
    promote_to: str = "",
    reviewer_contact: str = "",
    evidence_links_checked: Optional[List[str]] = None,
    constraints: str = "",
    notes: str = "",
) -> None:
    """Write signoff with extended schema (v0.2)."""
    signoff: Dict[str, Any] = {
        "target_id": target_id,
        "status": status,
        "reviewer": reviewer,
        "reason": reason,
        "promote_to": promote_to,
        "reviewed_at_utc": utc_now(),
        "signoff_schema_version": "0.2",
        "tool_version": VERSION,
    }
    # v0.9: Extended fields (optional)
    if reviewer_contact:
        signoff["reviewer_contact"] = reviewer_contact
    if evidence_links_checked:
        signoff["evidence_links_checked"] = evidence_links_checked
    if constraints:
        signoff["constraints"] = constraints
    if notes:
        signoff["notes"] = notes

    write_json(manifest_dir / "review_signoff.json", signoff)


def find_target_in_queue(queue_path: Path, target_id: str) -> Dict[str, Any]:
    for r in read_jsonl(queue_path):
        if str(r.get("id", "")).strip() == target_id:
            return r
    return {}


def cmd_set(args: argparse.Namespace, status: str) -> int:
    qpath = Path(args.queue)
    row = find_target_in_queue(qpath, args.target)
    if not row:
        print(f"Target not found in queue: {args.target}")
        return 2

    mdir = Path(row.get("manifest_dir", ""))
    if not mdir.exists():
        print(f"Manifest dir does not exist: {mdir}")
        return 2

    promote_to = ""
    if status == "approved" and hasattr(args, "promote_to") and args.promote_to:
        promote_to = str(args.promote_to).upper()

    # v0.9: Extended signoff fields
    evidence_links: Optional[List[str]] = None
    if hasattr(args, "evidence_links") and args.evidence_links:
        evidence_links = [link.strip() for link in args.evidence_links.split(",")]

    write_signoff(
        target_id=args.target,
        manifest_dir=mdir,
        status=status,
        reviewer=args.reviewer,
        reason=args.reason,
        promote_to=promote_to,
        reviewer_contact=getattr(args, "reviewer_contact", "") or "",
        evidence_links_checked=evidence_links,
        constraints=getattr(args, "constraints", "") or "",
        notes=getattr(args, "notes", "") or "",
    )
    print(f"Wrote signoff: {mdir / 'review_signoff.json'}")
    return 0


def cmd_export(args: argparse.Namespace) -> int:
    """Export reviewed targets to CSV or JSON (NEW in v0.9)."""
    qpath = Path(args.queue)
    yellow_rows = read_jsonl(qpath)

    if not yellow_rows:
        print("No YELLOW items found.")
        return 0

    reviewed: List[Dict[str, Any]] = []
    for r in yellow_rows:
        mdir = Path(r.get("manifest_dir", ""))
        signoff = load_existing_signoff(mdir) if mdir else {}
        status = str(signoff.get("status", "") or "").lower()

        if status in {"approved", "rejected", "deferred"}:
            reviewed.append({
                "target_id": r.get("id", ""),
                "name": r.get("name", ""),
                "license_profile": r.get("license_profile", ""),
                "resolved_spdx": r.get("resolved_spdx", ""),
                "license_evidence_url": r.get("license_evidence_url", ""),
                "status": status,
                "reviewer": signoff.get("reviewer", ""),
                "reviewer_contact": signoff.get("reviewer_contact", ""),
                "reason": signoff.get("reason", ""),
                "promote_to": signoff.get("promote_to", ""),
                "constraints": signoff.get("constraints", ""),
                "notes": signoff.get("notes", ""),
                "evidence_links_checked": ", ".join(signoff.get("evidence_links_checked", []) or []),
                "reviewed_at_utc": signoff.get("reviewed_at_utc", ""),
            })

    if not reviewed:
        print("No reviewed targets found.")
        return 0

    out_path = Path(args.output)
    fmt = args.format.lower()

    if fmt == "json":
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(reviewed, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    elif fmt == "csv":
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with out_path.open("w", newline="", encoding="utf-8") as f:
            if reviewed:
                writer = csv.DictWriter(f, fieldnames=reviewed[0].keys())
                writer.writeheader()
                writer.writerows(reviewed)
    else:
        print(f"Unknown format: {fmt}. Use 'json' or 'csv'.")
        return 1

    print(f"Exported {len(reviewed)} reviewed targets to: {out_path}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Manual review helper for YELLOW targets (v0.9).")
    ap.add_argument("--queue", default="/data/bio/_queues/yellow_pipeline.jsonl", help="Path to yellow queue JSONL")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p_list = sub.add_parser("list", help="List pending YELLOW items")
    p_list.add_argument("--limit", type=int, default=50)

    # v0.9: Extended signoff fields for approve/reject/defer
    def add_extended_args(parser: argparse.ArgumentParser) -> None:
        parser.add_argument("--reviewer-contact", dest="reviewer_contact", default="", help="Reviewer email/contact (optional)")
        parser.add_argument("--evidence-links", dest="evidence_links", default="", help="Comma-separated URLs of checked evidence (optional)")
        parser.add_argument("--constraints", default="", help="Attribution or usage constraints (optional)")
        parser.add_argument("--notes", default="", help="Additional notes (optional)")

    p_app = sub.add_parser("approve", help="Approve a YELLOW item (writes review_signoff.json)")
    p_app.add_argument("--target", required=True)
    p_app.add_argument("--reviewer", required=True)
    p_app.add_argument("--reason", required=True)
    p_app.add_argument("--promote-to", dest="promote_to", default="", help="Optional: set promote_to=GREEN")
    add_extended_args(p_app)

    p_rej = sub.add_parser("reject", help="Reject a YELLOW item (forces RED in next classify pass)")
    p_rej.add_argument("--target", required=True)
    p_rej.add_argument("--reviewer", required=True)
    p_rej.add_argument("--reason", required=True)
    add_extended_args(p_rej)

    p_def = sub.add_parser("defer", help="Defer a YELLOW item (marks as deferred)")
    p_def.add_argument("--target", required=True)
    p_def.add_argument("--reviewer", required=True)
    p_def.add_argument("--reason", required=True)
    add_extended_args(p_def)

    # v0.9: NEW export command
    p_export = sub.add_parser("export", help="Export reviewed targets to CSV/JSON report (NEW in v0.9)")
    p_export.add_argument("--output", required=True, help="Output file path")
    p_export.add_argument("--format", default="csv", choices=["csv", "json"], help="Output format (default: csv)")

    return ap


def main() -> int:
    ap = build_parser()
    args = ap.parse_args()
    if args.cmd == "list":
        return cmd_list(args)
    if args.cmd == "approve":
        return cmd_set(args, "approved")
    if args.cmd == "reject":
        return cmd_set(args, "rejected")
    if args.cmd == "defer":
        return cmd_set(args, "deferred")
    if args.cmd == "export":
        return cmd_export(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
