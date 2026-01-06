#!/usr/bin/env python3
"""
review_queue.py

Manual review helper for YELLOW targets.

Tool/version metadata comes from collector_core.__version__.__version__ and
collector_core.__version__.__schema_version__ (source of truth).

This script is intentionally lightweight and conservative:
- It reads YELLOW queue JSONL (emitted by pipeline_driver.py)
- It shows a summary of pending items
- It can write a review_signoff.json into each target's manifest dir
- Export reviewed targets to CSV/JSON, extended signoff schema

Signoff file schema (see collector_core.__version__.__schema_version__):
{
  "target_id": "...",
  "status": "approved" | "rejected" | "deferred",
  "reviewer": "Name",
  "reviewer_contact": "contact info",             # optional
  "reason": "Why",
  "promote_to": "GREEN" | "" ,                    # optional
  "signoff_schema_version": "...",               # tool-managed
  "tool_version": "...",                          # tool-managed
  "license_evidence_sha256": "...",               # evidence-hash binding (optional)
  "license_evidence_url": "...",                  # evidence-hash binding (optional)
  "license_evidence_fetched_at_utc": "...",       # evidence-hash binding (optional)
  "evidence_links_checked": ["url1", "url2"],     # optional
  "constraints": "Attribution requirements...",  # optional
  "notes": "Additional notes...",                 # optional
  "reviewed_at_utc": "YYYY-MM-DDTHH:MM:SSZ"
}
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import time
from pathlib import Path
from typing import Any

from collector_core.__version__ import __version__ as TOOL_VERSION
from collector_core.config_validator import read_yaml
from collector_core.logging_config import add_logging_args, configure_logging

logger = logging.getLogger(__name__)


def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def pipeline_slug_from_id(pipeline_id: str | None) -> str | None:
    if not pipeline_id:
        return None
    if pipeline_id.endswith("_pipeline_v2"):
        return pipeline_id[: -len("_pipeline_v2")]
    return pipeline_id


def derive_pipeline_slug_from_cfg(cfg: dict[str, Any]) -> str | None:
    g = cfg.get("globals", {}) or {}
    for key in (
        "raw_root",
        "queues_root",
        "catalogs_root",
        "manifests_root",
        "pitches_root",
        "ledger_root",
    ):
        value = g.get(key)
        if not value:
            continue
        parts = Path(value).parts
        if len(parts) >= 3 and parts[1] == "data":
            return parts[2]
    return None


def resolve_queue_from_cfg(cfg: dict[str, Any]) -> str | None:
    queues = (cfg.get("queues", {}) or {}).get("emit", []) or []
    for item in queues:
        queue_id = str(item.get("id", ""))
        criteria = item.get("criteria", {}) or {}
        if queue_id == "yellow_pipeline" or criteria.get("effective_bucket") == "YELLOW":
            path = item.get("path")
            if path:
                return str(path)
    g = cfg.get("globals", {}) or {}
    queues_root = g.get("queues_root")
    if queues_root:
        return str(Path(queues_root) / "yellow_pipeline.jsonl")
    return None


def resolve_default_queue(pipeline_slug: str | None, cfg: dict[str, Any]) -> str:
    cfg_queue = resolve_queue_from_cfg(cfg)
    if cfg_queue:
        return cfg_queue
    if pipeline_slug:
        return f"/data/{pipeline_slug}/_queues/yellow_pipeline.jsonl"
    return "/data/_queues/yellow_pipeline.jsonl"


def read_jsonl(path: Path) -> list[dict[str, Any]]:
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


def write_json(path: Path, obj: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def load_existing_signoff(manifest_dir: Path) -> dict[str, Any]:
    p = manifest_dir / "review_signoff.json"
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


def load_license_evidence_meta(manifest_dir: Path) -> dict[str, Any]:
    p = manifest_dir / "license_evidence_meta.json"
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


def cmd_list(args: argparse.Namespace) -> int:
    yellow_rows = read_jsonl(Path(args.queue))
    if not yellow_rows:
        logger.info("No YELLOW items found.")
        return 0

    pending = []
    for r in yellow_rows:
        mdir = Path(r.get("manifest_dir", ""))
        signoff = load_existing_signoff(mdir) if mdir else {}
        status = str(signoff.get("status", "") or "").lower()
        if status not in {"approved", "rejected"}:
            pending.append((r, status or "pending"))

    if not pending:
        logger.info("No pending YELLOW items (all have signoffs).")
        return 0

    logger.info("%s", "=" * 78)
    logger.info("YELLOW REVIEW QUEUE (pending) — v%s — %s", TOOL_VERSION, utc_now())
    logger.info("%s", "=" * 78)
    for r, status in pending[: args.limit]:
        logger.info("- %s  [%s]", r.get("id"), status)
        logger.info("  name: %s", r.get("name"))
        logger.info(
            "  license_profile: %s  resolved_spdx: %s",
            r.get("license_profile"),
            r.get("resolved_spdx"),
        )
        rh = r.get("restriction_hits") or []
        if rh:
            logger.info("  restriction_hits: %s", rh[:3])
        dl = r.get("denylist_hits") or []
        if dl:
            logger.info("  denylist_hits: %s", dl[:2])
        logger.info("  license_evidence_url: %s", r.get("license_evidence_url"))
        logger.info("  manifest_dir: %s", r.get("manifest_dir"))
        logger.info("")
    if len(pending) > args.limit:
        logger.info("... and %s more", len(pending) - args.limit)
    return 0


def write_signoff(
    target_id: str,
    manifest_dir: Path,
    status: str,
    reviewer: str,
    reason: str,
    promote_to: str = "",
    reviewer_contact: str = "",
    evidence_links_checked: list[str] | None = None,
    constraints: str = "",
    notes: str = "",
) -> None:
    """Write signoff with extended schema (v0.2)."""
    meta = load_license_evidence_meta(manifest_dir)
    signoff: dict[str, Any] = {
        "target_id": target_id,
        "status": status,
        "reviewer": reviewer,
        "reason": reason,
        "promote_to": promote_to,
        "reviewed_at_utc": utc_now(),
        "signoff_schema_version": "0.2",
        "tool_version": TOOL_VERSION,
    }
    evidence_sha = meta.get("sha256")
    if evidence_sha:
        signoff["license_evidence_sha256"] = evidence_sha
    evidence_url = meta.get("url")
    if evidence_url:
        signoff["license_evidence_url"] = evidence_url
    evidence_fetched_at = meta.get("fetched_at_utc")
    if evidence_fetched_at:
        signoff["license_evidence_fetched_at_utc"] = evidence_fetched_at
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


def find_target_in_queue(queue_path: Path, target_id: str) -> dict[str, Any]:
    for r in read_jsonl(queue_path):
        if str(r.get("id", "")).strip() == target_id:
            return r
    return {}


def cmd_set(args: argparse.Namespace, status: str) -> int:
    qpath = Path(args.queue)
    row = find_target_in_queue(qpath, args.target)
    if not row:
        logger.error("Target not found in queue: %s", args.target)
        return 2

    mdir = Path(row.get("manifest_dir", ""))
    if not mdir.exists():
        logger.error("Manifest dir does not exist: %s", mdir)
        return 2

    promote_to = ""
    if status == "approved" and hasattr(args, "promote_to") and args.promote_to:
        promote_to = str(args.promote_to).upper()

    # v0.9: Extended signoff fields
    evidence_links: list[str] | None = None
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
    logger.info("Wrote signoff: %s", mdir / "review_signoff.json")
    return 0


def cmd_export(args: argparse.Namespace) -> int:
    """Export reviewed targets to CSV or JSON (NEW in v0.9)."""
    qpath = Path(args.queue)
    yellow_rows = read_jsonl(qpath)

    if not yellow_rows:
        logger.info("No YELLOW items found.")
        return 0

    reviewed: list[dict[str, Any]] = []
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
        logger.info("No reviewed targets found.")
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
        logger.error("Unknown format: %s. Use 'json' or 'csv'.", fmt)
        return 1

    logger.info("Exported %s reviewed targets to: %s", len(reviewed), out_path)
    return 0


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Manual review helper for YELLOW targets (v0.9).")
    ap.add_argument(
        "--queue",
        default=None,
        help="Path to yellow queue JSONL (defaults from targets or pipeline_id)",
    )
    ap.add_argument(
        "--targets",
        default=None,
        help="Optional targets YAML to derive queue defaults",
    )
    add_logging_args(ap)
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


def main(*, pipeline_id: str | None = None) -> int:
    ap = build_parser()
    args = ap.parse_args()
    configure_logging(level=args.log_level, fmt=args.log_format)

    cfg: dict[str, Any] = {}
    if args.targets:
        cfg = read_yaml(Path(args.targets), schema_name="targets") or {}

    pipeline_slug = pipeline_slug_from_id(pipeline_id) or derive_pipeline_slug_from_cfg(cfg)
    args.queue = args.queue or resolve_default_queue(pipeline_slug, cfg)

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
