from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import logging
import os
import re
import time
from pathlib import Path
from typing import Any

import requests

from collector_core.__version__ import __version__ as VERSION
from collector_core.config_validator import read_yaml
from collector_core.dependencies import _try_import
from collector_core.logging_config import add_logging_args, configure_logging

logger = logging.getLogger(__name__)
PdfReader = _try_import("pypdf", "PdfReader")

def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def sha256_file(path: Path) -> str | None:
    try:
        return sha256_bytes(path.read_bytes())
    except Exception:
        return None


def resolve_dataset_root(explicit: str | None) -> Path | None:
    value = explicit or os.getenv("DATASET_ROOT") or os.getenv("DATASET_COLLECTOR_ROOT")
    if not value:
        return None
    return Path(value).expanduser().resolve()


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def write_json(path: Path, obj: dict[str, Any]) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()


def lower(text: str) -> str:
    return (text or "").lower()


def contains_any(haystack: str, needles: list[str]) -> list[str]:
    hits = []
    h = lower(haystack)
    for n in needles:
        if n and lower(n) in h:
            hits.append(n)
    return hits


def coerce_int(val: Any, default: int | None = None) -> int | None:
    try:
        return int(val)
    except Exception:
        return default


@dataclasses.dataclass
class LicenseMap:
    allow: list[str]
    conditional: list[str]
    deny_prefixes: list[str]
    normalization_rules: list[dict[str, Any]]
    restriction_phrases: list[str]
    gating: dict[str, str]
    profiles: dict[str, dict[str, Any]]


@dataclasses.dataclass(frozen=True)
class DriverConfig:
    args: argparse.Namespace
    retry_max: int
    retry_backoff: float
    headers: dict[str, str]
    targets_path: Path
    targets_cfg: dict[str, Any]
    license_map_path: Path
    license_map: LicenseMap
    denylist: dict[str, Any]
    manifests_root: Path
    queues_root: Path
    default_gates: list[Any]
    targets: list[dict[str, Any]]
    require_yellow_signoff: bool


@dataclasses.dataclass(frozen=True)
class EvidenceResult:
    snapshot: dict[str, Any]
    text: str
    license_change_detected: bool
    no_fetch_missing_evidence: bool


@dataclasses.dataclass(frozen=True)
class TargetContext:
    target: dict[str, Any]
    tid: str
    name: str
    profile: str
    evidence_url: str
    spdx_hint: str
    download_blob: str
    review_required: bool
    gates: dict[str, Any]
    target_manifest_dir: Path
    signoff: dict[str, Any]
    review_status: str
    promote_to: str
    routing: dict[str, Any]
    dl_hits: list[dict[str, Any]]
    enabled: bool
    split_group_id: str


@dataclasses.dataclass
class ClassificationResult:
    green_rows: list[dict[str, Any]]
    yellow_rows: list[dict[str, Any]]
    red_rows: list[dict[str, Any]]
    warnings: list[dict[str, Any]]


def load_license_map(path: Path) -> LicenseMap:
    m = read_yaml(path, schema_name="license_map")
    spdx = m.get("spdx", {}) or {}
    normalization = m.get("normalization", {}) or {}
    restriction_scan = m.get("restriction_scan", {}) or {}
    gating = m.get("gating", {}) or {}
    profiles = m.get("profiles", {}) or m.get("license_profiles", {}) or {}

    return LicenseMap(
        allow=spdx.get("allow", []),
        conditional=spdx.get("conditional", []),
        deny_prefixes=spdx.get("deny_prefixes", []),
        normalization_rules=normalization.get("rules", []),
        restriction_phrases=restriction_scan.get("phrases", []),
        gating=gating,
        profiles=profiles,
    )


def resolve_retry_config(args: argparse.Namespace, globals_cfg: dict[str, Any]) -> tuple[int, float]:
    retry_max_env = os.getenv("PIPELINE_RETRY_MAX")
    retry_backoff_env = os.getenv("PIPELINE_RETRY_BACKOFF")
    retry_cfg = globals_cfg.get("retry", {}) or {}
    retry_max = args.retry_max if args.retry_max is not None else args.max_retries
    if retry_max is None:
        retry_max = (
            int(retry_cfg.get("max"))
            if retry_cfg.get("max") is not None
            else (int(retry_max_env) if retry_max_env else 3)
        )
    retry_backoff = (
        args.retry_backoff
        if args.retry_backoff is not None
        else (
            float(retry_cfg.get("backoff"))
            if retry_cfg.get("backoff") is not None
            else (float(retry_backoff_env) if retry_backoff_env else 2.0)
        )
    )
    return retry_max, retry_backoff


def build_evidence_headers(raw_headers: list[str]) -> dict[str, str]:
    headers: dict[str, str] = {}
    for raw in raw_headers:
        if "=" not in raw:
            continue
        key, value = raw.split("=", 1)
        if key.strip():
            headers[key.strip()] = value.strip()
    return headers


def resolve_output_roots(args: argparse.Namespace, globals_cfg: dict[str, Any]) -> tuple[Path, Path]:
    dataset_root = resolve_dataset_root(args.dataset_root)
    manifests_override = args.manifests_root or args.out_manifests
    queues_override = args.queues_root or args.out_queues
    if manifests_override:
        manifests_root = Path(manifests_override).expanduser().resolve()
    elif dataset_root:
        manifests_root = (dataset_root / "_manifests").resolve()
    else:
        manifests_root = Path(globals_cfg.get("manifests_root", "./manifests")).expanduser().resolve()
    if queues_override:
        queues_root = Path(queues_override).expanduser().resolve()
    elif dataset_root:
        queues_root = (dataset_root / "_queues").resolve()
    else:
        queues_root = Path(globals_cfg.get("queues_root", "./queues")).expanduser().resolve()
    return manifests_root, queues_root


def load_driver_config(args: argparse.Namespace) -> DriverConfig:
    headers = build_evidence_headers(args.evidence_header)
    targets_path = Path(args.targets).resolve()
    targets_cfg = read_yaml(targets_path, schema_name="targets")
    globals_cfg = targets_cfg.get("globals", {}) or {}
    retry_max, retry_backoff = resolve_retry_config(args, globals_cfg)
    companion = targets_cfg.get("companion_files", {}) or {}
    license_map_path = Path(args.license_map or companion.get("license_map", "./license_map.yaml")).resolve()
    license_map = load_license_map(license_map_path)
    denylist_path = Path(companion.get("denylist", "./denylist.yaml")).resolve()
    denylist = load_denylist(denylist_path)
    manifests_root, queues_root = resolve_output_roots(args, globals_cfg)
    ensure_dir(manifests_root)
    ensure_dir(queues_root)
    return DriverConfig(
        args=args,
        retry_max=retry_max,
        retry_backoff=retry_backoff,
        headers=headers,
        targets_path=targets_path,
        targets_cfg=targets_cfg,
        license_map_path=license_map_path,
        license_map=license_map,
        denylist=denylist,
        manifests_root=manifests_root,
        queues_root=queues_root,
        default_gates=globals_cfg.get("default_gates", []) or [],
        targets=targets_cfg.get("targets", []) or [],
        require_yellow_signoff=bool(globals_cfg.get("require_yellow_signoff", False)),
    )


def find_existing_evidence(manifest_dir: Path) -> Path | None:
    for ext in [".html", ".pdf", ".txt", ".json"]:
        candidate = manifest_dir / f"license_evidence{ext}"
        if candidate.exists():
            return candidate
    return None


def apply_denylist_bucket(dl_hits: list[dict[str, Any]], eff_bucket: str) -> str:
    for hit in dl_hits:
        severity = hit.get("severity", "hard_red")
        if severity == "hard_red":
            return "RED"
        if severity == "force_yellow":
            eff_bucket = "YELLOW"
    return eff_bucket


def apply_review_gates(
    eff_bucket: str,
    review_required: bool,
    review_status: str,
    promote_to: str,
    restriction_hits: list[str],
) -> str:
    if review_status == "rejected":
        return "RED"
    if review_required and eff_bucket != "RED" and review_status != "approved":
        if eff_bucket == "GREEN":
            return "YELLOW"
        return eff_bucket
    if review_status == "approved" and promote_to == "GREEN" and not restriction_hits and eff_bucket != "RED":
        return "GREEN"
    return eff_bucket


def resolve_effective_bucket(
    license_map: LicenseMap,
    gates: list[str],
    evidence: EvidenceResult,
    spdx: str,
    restriction_hits: list[str],
    min_confidence: float,
    resolved_confidence: float,
    review_required: bool,
    review_status: str,
    promote_to: str,
    denylist_hits: list[dict[str, Any]],
) -> str:
    eff_bucket = compute_effective_bucket(
        license_map,
        gates,
        spdx,
        restriction_hits,
        evidence.snapshot,
        min_confidence,
        resolved_confidence,
    )
    eff_bucket = apply_denylist_bucket(denylist_hits, eff_bucket)
    if evidence.no_fetch_missing_evidence and eff_bucket == "GREEN":
        eff_bucket = "YELLOW"
    return apply_review_gates(eff_bucket, review_required, review_status, promote_to, restriction_hits)


def apply_yellow_signoff_requirement(
    eff_bucket: str,
    review_status: str,
    review_required: bool,
    require_yellow_signoff: bool,
) -> bool:
    if require_yellow_signoff and eff_bucket == "YELLOW" and review_status not in {"approved", "rejected"}:
        return True
    return review_required


def resolve_output_pool(profile: str, eff_bucket: str, target: dict[str, Any]) -> str:
    out_pool = (target.get("output", {}) or {}).get("pool")
    if out_pool:
        return out_pool
    if profile == "copyleft":
        return "copyleft"
    if eff_bucket == "GREEN":
        return "permissive"
    return "quarantine"


def sort_queue_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    def sort_key(row: dict[str, Any]) -> tuple[int, str]:
        p = row.get("priority", None)
        try:
            pi = int(p) if p is not None else -999999
        except Exception:
            pi = -999999
        return (-pi, str(row.get("id", "")))

    return sorted(rows, key=sort_key)


def build_target_identity(
    target: dict[str, Any],
    license_map: LicenseMap,
) -> tuple[str, str, str, bool, list[dict[str, Any]]]:
    enabled = bool(target.get("enabled", True))
    tid = str(target.get("id", "")).strip() or "unknown_id"
    name = str(target.get("name", tid))
    profile = str(target.get("license_profile", "unknown"))
    warnings: list[dict[str, Any]] = []
    if profile not in license_map.profiles:
        warnings.append(
            {
                "type": "unknown_license_profile",
                "target_id": tid,
                "license_profile": profile,
                "known_profiles": sorted(license_map.profiles.keys()),
                "message": f"Target {tid} uses license_profile '{profile}' not present in license_map profiles.",
            }
        )
    return tid, name, profile, enabled, warnings


def extract_evidence_fields(target: dict[str, Any]) -> tuple[str, str]:
    evidence = target.get("license_evidence", {}) or {}
    spdx_hint = str(evidence.get("spdx_hint", "UNKNOWN"))
    evidence_url = str(evidence.get("url", ""))
    return spdx_hint, evidence_url


def build_denylist_haystack(
    tid: str,
    name: str,
    evidence_url: str,
    download_blob: str,
    target: dict[str, Any],
) -> dict[str, str]:
    return {
        "id": tid,
        "name": name,
        "license_evidence_url": evidence_url,
        "download_blob": download_blob,
        "publisher": str(target.get("publisher", "") or ""),
    }


def extract_domain(url: str) -> str:
    """Extract domain from URL for domain-based denylist matching."""
    try:
        from urllib.parse import urlparse

        parsed = urlparse(url)
        return parsed.netloc.lower()
    except Exception:
        return ""


def load_denylist(path: Path) -> dict[str, Any]:
    """Load denylist.yaml if present. Returns dict with keys: patterns, domain_patterns, publisher_patterns."""
    if not path or not path.exists():
        return {"patterns": [], "domain_patterns": [], "publisher_patterns": []}
    try:
        d = read_yaml(path, schema_name="denylist") or {}
        patterns = d.get("patterns", []) or []
        domain_patterns = d.get("domain_patterns", []) or []
        publisher_patterns = d.get("publisher_patterns", []) or []

        # Normalize main patterns (v0.9: with severity and provenance)
        norm = []
        for p in patterns:
            if not isinstance(p, dict):
                continue
            kind = str(p.get("type", "substring")).lower()
            value = str(p.get("value", "") or "")
            if not value:
                continue
            fields = p.get("fields", None)
            if fields is None:
                fields = ["id", "name", "license_evidence_url", "download_blob"]
            norm.append(
                {
                    "type": kind,
                    "value": value,
                    "fields": [str(f) for f in (fields or [])],
                    "severity": str(p.get("severity", "hard_red")).lower(),  # v0.9: hard_red | force_yellow
                    "reason": str(p.get("reason", p.get("rationale", "")) or ""),
                    "link": str(p.get("link", "") or ""),  # v0.9: provenance
                    "rationale": str(p.get("rationale", "") or ""),  # v0.9: provenance
                }
            )

        # v0.9: Normalize domain patterns
        norm_domain = []
        for p in domain_patterns:
            if not isinstance(p, dict):
                continue
            domain = str(p.get("domain", "") or "").lower()
            if not domain:
                continue
            norm_domain.append(
                {
                    "domain": domain,
                    "severity": str(p.get("severity", "hard_red")).lower(),
                    "link": str(p.get("link", "") or ""),
                    "rationale": str(p.get("rationale", "") or ""),
                }
            )

        # v0.9: Normalize publisher patterns
        norm_publisher = []
        for p in publisher_patterns:
            if not isinstance(p, dict):
                continue
            publisher = str(p.get("publisher", "") or "")
            if not publisher:
                continue
            norm_publisher.append(
                {
                    "publisher": publisher,
                    "severity": str(p.get("severity", "hard_red")).lower(),
                    "link": str(p.get("link", "") or ""),
                    "rationale": str(p.get("rationale", "") or ""),
                }
            )

        return {
            "patterns": norm,
            "domain_patterns": norm_domain,
            "publisher_patterns": norm_publisher,
        }
    except Exception:
        return {"patterns": [], "domain_patterns": [], "publisher_patterns": []}


def denylist_hits(denylist: dict[str, Any], hay: dict[str, str]) -> list[dict[str, Any]]:
    """Return list of matched denylist patterns with field, reason, severity (v0.9)."""
    hits: list[dict[str, Any]] = []

    # Process standard patterns
    pats = (denylist or {}).get("patterns", []) or []
    for p in pats:
        kind = p.get("type", "substring")
        val = p.get("value", "")
        fields = p.get("fields", [])
        severity = p.get("severity", "hard_red")

        for f in fields:
            src = str(hay.get(f, "") or "")
            if not src:
                continue

            matched = False
            if kind == "regex":
                try:
                    if re.search(val, src, flags=re.IGNORECASE):
                        matched = True
                except re.error:
                    continue
            elif kind == "domain":
                # v0.9: Domain extraction matching
                src_domain = extract_domain(src)
                if src_domain and val.lower() in src_domain:
                    matched = True
            else:  # substring
                if val.lower() in src.lower():
                    matched = True

            if matched:
                hits.append(
                    {
                        "field": f,
                        "pattern": val,
                        "type": kind,
                        "severity": severity,
                        "reason": p.get("reason", ""),
                        "link": p.get("link", ""),
                        "rationale": p.get("rationale", ""),
                    }
                )
                break

    # v0.9: Process domain patterns (against URLs in hay)
    domain_pats = (denylist or {}).get("domain_patterns", []) or []
    url_fields = ["license_evidence_url", "download_blob"]
    for dp in domain_pats:
        target_domain = dp.get("domain", "").lower()
        if not target_domain:
            continue
        for f in url_fields:
            src = str(hay.get(f, "") or "")
            if not src:
                continue
            src_domain = extract_domain(src)
            if src_domain and target_domain in src_domain:
                hits.append(
                    {
                        "field": f,
                        "pattern": target_domain,
                        "type": "domain",
                        "severity": dp.get("severity", "hard_red"),
                        "reason": dp.get("rationale", ""),
                        "link": dp.get("link", ""),
                        "rationale": dp.get("rationale", ""),
                    }
                )
                break

    # v0.9: Process publisher patterns (if publisher metadata available)
    publisher_pats = (denylist or {}).get("publisher_patterns", []) or []
    publisher_val = str(hay.get("publisher", "") or "")
    if publisher_val:
        for pp in publisher_pats:
            target_pub = pp.get("publisher", "")
            if target_pub and target_pub.lower() in publisher_val.lower():
                hits.append(
                    {
                        "field": "publisher",
                        "pattern": target_pub,
                        "type": "publisher",
                        "severity": pp.get("severity", "hard_red"),
                        "reason": pp.get("rationale", ""),
                        "link": pp.get("link", ""),
                        "rationale": pp.get("rationale", ""),
                    }
                )

    return hits


def read_review_signoff(manifest_dir: Path) -> dict[str, Any]:
    """Read review_signoff.json if present."""
    p = manifest_dir / "review_signoff.json"
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


def resolve_spdx_with_confidence(
    license_map: LicenseMap, evidence_text: str, spdx_hint: str
) -> tuple[str, float, str]:
    """Resolve SPDX with a lightweight confidence score and rationale."""

    hint = normalize_whitespace(str(spdx_hint or ""))
    if hint and hint.upper() not in {"MIXED", "UNKNOWN", "DERIVED"}:
        return hint, 0.95, "explicit SPDX hint"

    blob = normalize_whitespace(f"{hint} {evidence_text}")
    blob_l = lower(blob)

    for rule in license_map.normalization_rules:
        needles = [lower(x) for x in (rule.get("match_any") or []) if x]
        if needles and any(n in blob_l for n in needles):
            confidence = min(0.9, 0.6 + 0.05 * len(needles))
            return str(rule.get("spdx", "UNKNOWN")) or "UNKNOWN", confidence, "normalized via rule match"

    if hint.upper() == "DERIVED":
        return "Derived", 0.6, "derived content flag"

    return "UNKNOWN", 0.2, "no confident match"


def spdx_bucket(license_map: LicenseMap, spdx: str) -> str:
    s = str(spdx or "").strip()
    if not s or s.upper() == "UNKNOWN":
        return license_map.gating.get("unknown_spdx_bucket", "YELLOW")

    for pref in license_map.deny_prefixes:
        if s.startswith(pref):
            return license_map.gating.get("deny_spdx_bucket", "RED")

    if s in license_map.allow:
        return "GREEN"
    if s in license_map.conditional:
        return license_map.gating.get("conditional_spdx_bucket", "YELLOW")

    return license_map.gating.get("unknown_spdx_bucket", "YELLOW")


def extract_text_for_scanning(evidence: dict[str, Any]) -> str:
    saved = str(evidence.get("saved_path") or "")
    if not saved:
        return ""
    path = Path(saved)
    if not path.exists():
        return ""
    if path.suffix.lower() == ".pdf":
        evidence["pdf_text_extraction_failed"] = False
        if PdfReader is None:
            evidence["pdf_text_extraction_failed"] = True
            return ""
        try:
            reader = PdfReader(str(path))
            pages = []
            for page in list(reader.pages)[:5]:
                text = page.extract_text() or ""
                if text:
                    pages.append(text)
            extracted = "\n\n".join(pages).strip()
            if not extracted:
                evidence["pdf_text_extraction_failed"] = True
            return extracted
        except Exception:
            evidence["pdf_text_extraction_failed"] = True
            return ""
    try:
        return path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return ""


def merge_gates(default_gates: list[str], gates_override: dict[str, Any]) -> list[str]:
    """Merge default gates with overrides from target config."""
    merged = list(default_gates)
    add = gates_override.get("add", []) or []
    remove = gates_override.get("remove", []) or []

    for g in add:
        if g not in merged:
            merged.append(g)
    for g in remove:
        if g in merged:
            merged.remove(g)
    return merged


def canonicalize_gates(gates: list[str]) -> list[str]:
    canonical_map = {
        "no_restrictions": "restriction_phrase_scan",
        "manual_review": "manual_legal_review",
    }
    canonicalized: list[str] = []
    for gate in gates:
        mapped = canonical_map.get(gate, gate)
        if mapped not in canonicalized:
            canonicalized.append(mapped)
    return canonicalized


def compute_effective_bucket(
    license_map: LicenseMap,
    gates: list[str],
    resolved_spdx: str,
    restriction_hits: list[str],
    evidence_snapshot: dict[str, Any],
    min_confidence: float,
    resolved_confidence: float,
) -> str:
    """Compute effective bucket based on gates and scan results."""
    bucket = spdx_bucket(license_map, resolved_spdx)

    # Confidence gate: if confidence is too low, force YELLOW
    if resolved_confidence < min_confidence and bucket == "GREEN":
        bucket = license_map.gating.get("low_confidence_bucket", "YELLOW")

    if "snapshot_terms" in gates and evidence_snapshot.get("status") != "ok":
        # If we require snapshot and failed, force YELLOW
        bucket = "YELLOW"

    if evidence_snapshot.get("changed_from_previous"):
        bucket = "YELLOW"

    if ("restriction_phrase_scan" in gates or "no_restrictions" in gates) and restriction_hits:
        bucket = "YELLOW"
    if ("restriction_phrase_scan" in gates or "no_restrictions" in gates) and evidence_snapshot.get(
        "pdf_text_extraction_failed"
    ):
        bucket = "YELLOW"

    if "manual_legal_review" in gates or "manual_review" in gates:
        bucket = "YELLOW"

    return bucket


def generate_dry_run_report(
    queues_root: Path,
    targets: list[dict[str, Any]],
    green_rows: list[dict[str, Any]],
    yellow_rows: list[dict[str, Any]],
    red_rows: list[dict[str, Any]],
    warnings: list[dict[str, Any]],
) -> str:
    lines = [
        "=" * 70,
        "PIPELINE DRIVER DRY-RUN SUMMARY",
        "=" * 70,
        "",
        f"Total targets: {len(targets)}",
        f"  GREEN (approved): {len(green_rows)}",
        f"  YELLOW (needs review): {len(yellow_rows)}",
        f"  RED (rejected): {len(red_rows)}",
        "",
    ]

    if warnings:
        lines.extend(
            [
                "WARNINGS",
                "-" * 40,
            ]
        )
        for w in warnings[:20]:
            lines.append(f"  ⚠ {w.get('message', str(w))}")
        if len(warnings) > 20:
            lines.append(f"  ... and {len(warnings) - 20} more")
        lines.append("")

    if green_rows:
        lines.extend(
            [
                "GREEN TARGETS (will be downloaded)",
                "-" * 40,
            ]
        )
        for r in green_rows[:20]:
            lines.append(f"  ✓ {r['id']}: {r['name'][:50]}")
            lines.append(f"      License: {r['resolved_spdx']}")
        if len(green_rows) > 20:
            lines.append(f"  ... and {len(green_rows) - 20} more")
        lines.append("")

    if yellow_rows:
        lines.extend(
            [
                "YELLOW TARGETS (need additional processing)",
                "-" * 40,
            ]
        )
        for r in yellow_rows[:20]:
            reason = "record-level filtering" if r.get("license_profile") == "record_level" else "manual review"
            if r.get("restriction_hits"):
                reason = f"restriction phrase: {r['restriction_hits'][0]}"
            lines.append(f"  ⚠ {r['id']}: {r['name'][:50]}")
            lines.append(f"      Reason: {reason}")
            lines.append(f"      Signoff SHA256: {r.get('signoff_evidence_sha256')}")
            lines.append(f"      Current SHA256: {r.get('current_evidence_sha256')}")
            lines.append(f"      Signoff stale: {r.get('signoff_is_stale')}")
        if len(yellow_rows) > 20:
            lines.append(f"  ... and {len(yellow_rows) - 20} more")
        lines.append("")

    if red_rows:
        lines.extend(
            [
                "RED TARGETS (rejected)",
                "-" * 40,
            ]
        )
        for r in red_rows[:10]:
            lines.append(f"  ✗ {r['id']}: {r['name'][:50]}")
            lines.append(f"      License: {r['resolved_spdx']}")
        if len(red_rows) > 10:
            lines.append(f"  ... and {len(red_rows) - 10} more")
        lines.append("")

    lines.extend(
        [
            "NEXT STEPS",
            "-" * 40,
            "  1. Review this summary for any unexpected classifications",
            "  2. Run acquire_worker.py with --execute to download GREEN and YELLOW targets",
            "  3. Run yellow_screen_worker.py and merge_worker.py per docs/PIPELINE_V2_REWORK_PLAN.md",
            "",
            "=" * 70,
        ]
    )

    report = "\n".join(lines)

    # Write report to file
    report_path = queues_root / "dry_run_report.txt"
    report_path.write_text(report, encoding="utf-8")

    return report


@dataclasses.dataclass(frozen=True)
class RoutingBlockSpec:
    name: str
    sources: list[str]
    mode: str = "subset"


class BasePipelineDriver:
    DOMAIN = "base"
    TARGETS_LABEL = "targets.yaml"
    USER_AGENT = "dataset-collector-pipeline"
    ROUTING_KEYS: list[str] = []
    ROUTING_CONFIDENCE_KEYS: list[str] = []
    ROUTING_BLOCKS: list[RoutingBlockSpec] = []
    DEFAULT_ROUTING: dict[str, Any] = {"granularity": "target"}
    INCLUDE_ROUTING_DICT_IN_ROW = False

    def _routing_sources(self, target: dict[str, Any]) -> list[dict[str, Any]]:
        routing = target.get("routing", {}) or {}
        return [routing] + [(target.get(key, {}) or {}) for key in self.ROUTING_KEYS]

    def _confidence_sources(self, target: dict[str, Any]) -> list[dict[str, Any]]:
        routing = target.get("routing", {}) or {}
        sources = [routing]
        for key in self.ROUTING_CONFIDENCE_KEYS:
            sources.append(target.get(key, {}) or {})
        return sources

    def _first_value(self, sources: list[dict[str, Any]], key: str) -> Any | None:
        for src in sources:
            val = src.get(key)
            if val not in (None, ""):
                return val
        return None

    def _first_level(self, sources: list[dict[str, Any]]) -> int | None:
        for src in sources:
            val = coerce_int(src.get("level"))
            if val is not None:
                return val
        return None

    def resolve_routing_fields(self, target: dict[str, Any]) -> dict[str, Any]:
        sources = self._routing_sources(target)
        confidence_sources = self._confidence_sources(target)

        subject = self._first_value(sources, "subject")
        domain = self._first_value(sources, "domain")
        category = self._first_value(sources, "category")
        level = self._first_level(sources)
        granularity = self._first_value(sources, "granularity")
        confidence = self._first_value(confidence_sources, "confidence")
        reason = self._first_value(confidence_sources, "reason")

        return {
            "subject": subject if subject is not None else self.DEFAULT_ROUTING.get("subject"),
            "domain": domain if domain is not None else self.DEFAULT_ROUTING.get("domain"),
            "category": category if category is not None else self.DEFAULT_ROUTING.get("category"),
            "level": level if level is not None else self.DEFAULT_ROUTING.get("level"),
            "granularity": granularity if granularity is not None else self.DEFAULT_ROUTING.get("granularity"),
            "confidence": confidence,
            "reason": reason,
        }

    def build_routing_block(self, target: dict[str, Any], spec: RoutingBlockSpec) -> dict[str, Any]:
        chosen: dict[str, Any] = {}
        for key in spec.sources:
            candidate = target.get(key, {}) or {}
            if candidate:
                chosen = candidate
                break
        if spec.mode == "raw":
            return chosen
        return {
            "domain": chosen.get("domain"),
            "category": chosen.get("category"),
            "level": chosen.get("level"),
            "granularity": chosen.get("granularity"),
        }

    def build_evaluation_extras(self, target: dict[str, Any], routing: dict[str, Any]) -> dict[str, Any]:
        extras: dict[str, Any] = {}
        for spec in self.ROUTING_BLOCKS:
            extras[spec.name] = self.build_routing_block(target, spec)
        return extras

    def build_row_extras(self, target: dict[str, Any], routing: dict[str, Any]) -> dict[str, Any]:
        return {}

    def fetch_url_with_retry(
        self,
        url: str,
        timeout_s: int = 30,
        max_retries: int = 3,
        backoff_base: float = 2.0,
        headers: dict[str, str] | None = None,
    ) -> tuple[bytes | None, str | None, dict[str, Any]]:
        """Fetch URL with retry and exponential backoff."""
        meta: dict[str, Any] = {"retries": 0, "errors": []}

        for attempt in range(max_retries):
            try:
                r = requests.get(
                    url,
                    timeout=timeout_s,
                    headers={"User-Agent": f"{self.USER_AGENT}/{VERSION}", **(headers or {})},
                )
                r.raise_for_status()
                ctype = r.headers.get("Content-Type", "")
                meta["final_status"] = r.status_code
                return r.content, ctype, meta
            except Exception as e:
                meta["retries"] = attempt + 1
                meta["errors"].append({"attempt": attempt + 1, "error": repr(e)})
                if attempt < max_retries - 1:
                    sleep_time = min(backoff_base**attempt, 60)
                    time.sleep(sleep_time)

        return None, f"Failed after {max_retries} attempts", meta

    def snapshot_evidence(
        self,
        manifest_dir: Path,
        url: str,
        max_retries: int = 3,
        backoff_base: float = 2.0,
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        result: dict[str, Any] = {
            "url": url,
            "fetched_at_utc": utc_now(),
            "status": "skipped",
            "headers_used": headers or {},
        }
        if not url:
            result["status"] = "no_url"
            return result

        previous_digest = None
        existing_path = None
        existing_meta: dict[str, Any] = {}
        for ext in [".html", ".pdf", ".txt", ".json"]:
            candidate = manifest_dir / f"license_evidence{ext}"
            if candidate.exists():
                existing_path = candidate
                previous_digest = sha256_file(candidate)
                break
        meta_path = manifest_dir / "license_evidence_meta.json"
        if meta_path.exists():
            try:
                existing_meta = json.loads(meta_path.read_text(encoding="utf-8"))
            except Exception:
                existing_meta = {}

        content, info, meta = self.fetch_url_with_retry(
            url,
            max_retries=max_retries,
            backoff_base=backoff_base,
            headers=headers,
        )
        result["fetch_meta"] = meta

        if content is None:
            result["status"] = "error"
            result["error"] = info
            return result

        ctype = info or ""
        digest = sha256_bytes(content)
        result.update(
            {
                "status": "ok",
                "content_type": ctype,
                "sha256": digest,
                "bytes": len(content),
                "previous_sha256": previous_digest,
                "previous_path": str(existing_path) if existing_path else None,
                "changed_from_previous": bool(previous_digest and previous_digest != digest),
            }
        )

        ext = ".html"
        if "pdf" in ctype.lower():
            ext = ".pdf"
        elif "json" in ctype.lower():
            ext = ".json"
        elif "text/plain" in ctype.lower():
            ext = ".txt"

        ensure_dir(manifest_dir)
        out_path = manifest_dir / f"license_evidence{ext}"
        history: list[dict[str, Any]] = []
        if isinstance(existing_meta.get("history"), list):
            history = list(existing_meta.get("history", []))
        previous_entry = None
        previous_renamed_path = None
        if result["changed_from_previous"] and existing_path and previous_digest:
            prev_ext = existing_path.suffix
            prev_prefix = f"license_evidence.prev_{previous_digest[:8]}"
            prev_path = manifest_dir / f"{prev_prefix}{prev_ext}"
            counter = 1
            while prev_path.exists():
                prev_path = manifest_dir / f"{prev_prefix}_{counter}{prev_ext}"
                counter += 1
            existing_path.rename(prev_path)
            previous_renamed_path = prev_path
            previous_entry = {
                "sha256": previous_digest,
                "filename": prev_path.name,
                "fetched_at_utc": existing_meta.get("fetched_at_utc"),
            }
            history.append(previous_entry)
        out_path.write_bytes(content)
        result["saved_path"] = str(out_path)
        if previous_renamed_path:
            result["previous_renamed_path"] = str(previous_renamed_path)
        result["history"] = history
        if result["changed_from_previous"]:
            result["evidence_files_verified"] = bool(
                (previous_renamed_path and previous_renamed_path.exists()) and out_path.exists()
            )
            if not result["evidence_files_verified"]:
                result["status"] = "error"
                result["error"] = "Evidence file rename/write verification failed."

        write_json(manifest_dir / "license_evidence_meta.json", result)
        return result

    def run(self, args: argparse.Namespace) -> None:
        cfg = load_driver_config(args)
        results = self.classify_targets(cfg)
        self.emit_queues(cfg.queues_root, results)
        self.emit_summary(cfg, results)
        self.emit_report(cfg, results)

    def classify_targets(self, cfg: DriverConfig) -> ClassificationResult:
        results = ClassificationResult([], [], [], [])
        for target in cfg.targets:
            ctx, warnings = self.prepare_target_context(target, cfg)
            results.warnings.extend(warnings)
            evaluation, row = self.classify_target(ctx, cfg)
            write_json(ctx.target_manifest_dir / "evaluation.json", evaluation)
            if not ctx.enabled:
                continue
            if row["effective_bucket"] == "GREEN":
                results.green_rows.append(row)
            elif row["effective_bucket"] == "YELLOW":
                results.yellow_rows.append(row)
            else:
                results.red_rows.append(row)
        return results

    def prepare_target_context(self, target: dict[str, Any], cfg: DriverConfig) -> tuple[TargetContext, list[dict[str, Any]]]:
        tid, name, profile, enabled, warnings = build_target_identity(target, cfg.license_map)
        spdx_hint, evidence_url = extract_evidence_fields(target)
        download_cfg = target.get("download", {}) or {}
        download_blob = json.dumps(download_cfg, ensure_ascii=False)
        review_required = bool(target.get("review_required", False))
        gates = canonicalize_gates(merge_gates(cfg.default_gates, target.get("gates_override", {}) or {}))
        target_manifest_dir = cfg.manifests_root / tid
        ensure_dir(target_manifest_dir)
        signoff = read_review_signoff(target_manifest_dir)
        review_status = str(signoff.get("status", "") or "").lower()
        promote_to = str(signoff.get("promote_to", "") or "").upper()
        dl_hits = denylist_hits(cfg.denylist, build_denylist_haystack(tid, name, evidence_url, download_blob, target))
        routing = self.resolve_routing_fields(target)
        split_group_id = str(target.get("split_group_id", "") or tid)
        ctx = TargetContext(
            target=target,
            tid=tid,
            name=name,
            profile=profile,
            evidence_url=evidence_url,
            spdx_hint=spdx_hint,
            download_blob=download_blob,
            review_required=review_required,
            gates=gates,
            target_manifest_dir=target_manifest_dir,
            signoff=signoff,
            review_status=review_status,
            promote_to=promote_to,
            routing=routing,
            dl_hits=dl_hits,
            enabled=enabled,
            split_group_id=split_group_id,
        )
        return ctx, warnings

    def classify_target(self, ctx: TargetContext, cfg: DriverConfig) -> tuple[dict[str, Any], dict[str, Any]]:
        evidence = self.fetch_evidence(ctx, cfg)
        review_status = ctx.review_status
        promote_to = ctx.promote_to
        review_required = ctx.review_required
        evidence_sha = evidence.snapshot.get("sha256")
        signoff_sha = ctx.signoff.get("license_evidence_sha256")
        if evidence_sha and signoff_sha and evidence_sha != signoff_sha:
            review_status = "pending"
            promote_to = ""
            review_required = True
        restriction_hits = contains_any(evidence.text, cfg.license_map.restriction_phrases)
        resolved, resolved_confidence, confidence_reason = resolve_spdx_with_confidence(
            cfg.license_map, evidence.text, ctx.spdx_hint
        )
        eff_bucket = resolve_effective_bucket(
            cfg.license_map,
            ctx.gates,
            evidence,
            resolved,
            restriction_hits,
            cfg.args.min_license_confidence,
            resolved_confidence,
            review_required,
            review_status,
            promote_to,
            ctx.dl_hits,
        )
        review_required = apply_yellow_signoff_requirement(
            eff_bucket,
            review_status,
            review_required,
            cfg.require_yellow_signoff,
        )
        out_pool = resolve_output_pool(ctx.profile, eff_bucket, ctx.target)
        evaluation = self.build_evaluation(
            ctx,
            cfg,
            evidence,
            restriction_hits,
            resolved,
            resolved_confidence,
            confidence_reason,
            eff_bucket,
            review_required,
            review_status,
            out_pool,
        )
        row = self.build_row(
            ctx,
            evidence,
            restriction_hits,
            resolved,
            resolved_confidence,
            eff_bucket,
            review_required,
            out_pool,
        )
        return evaluation, row

    def fetch_evidence(self, ctx: TargetContext, cfg: DriverConfig) -> EvidenceResult:
        evidence_snapshot = {"status": "skipped", "url": ctx.evidence_url}
        evidence_text = ""
        license_change_detected = False
        no_fetch_missing_evidence = False
        if "snapshot_terms" in ctx.gates and not cfg.args.no_fetch:
            evidence_snapshot = self.snapshot_evidence(
                ctx.target_manifest_dir,
                ctx.evidence_url,
                max_retries=cfg.retry_max,
                backoff_base=cfg.retry_backoff,
                headers=cfg.headers,
            )
            evidence_text = extract_text_for_scanning(evidence_snapshot)
            license_change_detected = bool(evidence_snapshot.get("changed_from_previous"))
        elif "snapshot_terms" in ctx.gates and cfg.args.no_fetch:
            existing_evidence_path = find_existing_evidence(ctx.target_manifest_dir)
            if existing_evidence_path:
                evidence_snapshot = {
                    "status": "ok",
                    "url": ctx.evidence_url,
                    "saved_path": str(existing_evidence_path),
                    "fetched_at_utc": utc_now(),
                    "offline_mode": True,
                }
                evidence_text = extract_text_for_scanning(evidence_snapshot)
            else:
                evidence_snapshot = {"status": "offline_missing", "url": ctx.evidence_url}
                no_fetch_missing_evidence = True
        return EvidenceResult(
            snapshot=evidence_snapshot,
            text=evidence_text,
            license_change_detected=license_change_detected,
            no_fetch_missing_evidence=no_fetch_missing_evidence,
        )

    def build_evaluation(
        self,
        ctx: TargetContext,
        cfg: DriverConfig,
        evidence: EvidenceResult,
        restriction_hits: list[str],
        resolved: str,
        resolved_confidence: float,
        confidence_reason: str,
        eff_bucket: str,
        review_required: bool,
        review_status: str,
        out_pool: str,
    ) -> dict[str, Any]:
        signoff_evidence_sha256 = (ctx.signoff or {}).get("license_evidence_sha256")
        current_evidence_sha256 = evidence.snapshot.get("sha256")
        signoff_is_stale = bool(
            signoff_evidence_sha256
            and current_evidence_sha256
            and signoff_evidence_sha256 != current_evidence_sha256
        )
        evaluation = {
            "id": ctx.tid,
            "name": ctx.name,
            "enabled": ctx.enabled,
            "evaluated_at_utc": utc_now(),
            "pipeline_version": VERSION,
            "review_required": review_required,
            "review_signoff": ctx.signoff or None,
            "review_status": review_status or "pending",
            "denylist_hits": ctx.dl_hits,
            "license_profile": ctx.profile,
            "spdx_hint": ctx.spdx_hint,
            "resolved_spdx": resolved,
            "resolved_spdx_confidence": resolved_confidence,
            "resolved_spdx_confidence_reason": confidence_reason,
            "restriction_hits": restriction_hits,
            "gates": ctx.gates,
            "effective_bucket": eff_bucket,
            "queue_bucket": eff_bucket,
            "license_evidence_url": ctx.evidence_url,
            "evidence_snapshot": evidence.snapshot,
            "evidence_headers_used": cfg.headers,
            "license_change_detected": evidence.license_change_detected,
            "signoff_evidence_sha256": signoff_evidence_sha256,
            "current_evidence_sha256": current_evidence_sha256,
            "signoff_is_stale": signoff_is_stale,
            "download": ctx.target.get("download", {}),
            "build": ctx.target.get("build", {}),
            "data_type": ctx.target.get("data_type", []),
            "priority": ctx.target.get("priority", None),
            "statistics": ctx.target.get("statistics", {}),
            "split_group_id": ctx.split_group_id,
            "no_fetch_missing_evidence": evidence.no_fetch_missing_evidence,
            "require_yellow_signoff": cfg.require_yellow_signoff,
            "output_pool": out_pool,
        }
        evaluation.update(self.build_evaluation_extras(ctx.target, ctx.routing))
        evaluation["routing"] = ctx.routing
        return evaluation

    def build_row(
        self,
        ctx: TargetContext,
        evidence: EvidenceResult,
        restriction_hits: list[str],
        resolved: str,
        resolved_confidence: float,
        eff_bucket: str,
        review_required: bool,
        out_pool: str,
    ) -> dict[str, Any]:
        signoff_evidence_sha256 = (ctx.signoff or {}).get("license_evidence_sha256")
        current_evidence_sha256 = evidence.snapshot.get("sha256")
        signoff_is_stale = bool(
            signoff_evidence_sha256
            and current_evidence_sha256
            and signoff_evidence_sha256 != current_evidence_sha256
        )
        row = {
            "id": ctx.tid,
            "name": ctx.name,
            "effective_bucket": eff_bucket,
            "queue_bucket": eff_bucket,
            "license_profile": ctx.profile,
            "resolved_spdx": resolved,
            "resolved_spdx_confidence": resolved_confidence,
            "restriction_hits": restriction_hits,
            "license_evidence_url": ctx.evidence_url,
            "manifest_dir": str(ctx.target_manifest_dir),
            "download": ctx.target.get("download", {}),
            "build": ctx.target.get("build", {}),
            "data_type": ctx.target.get("data_type", []),
            "priority": ctx.target.get("priority", None),
            "enabled": ctx.enabled,
            "statistics": ctx.target.get("statistics", {}),
            "split_group_id": ctx.split_group_id,
            "denylist_hits": ctx.dl_hits,
            "review_required": review_required,
            "license_change_detected": evidence.license_change_detected,
            "signoff_evidence_sha256": signoff_evidence_sha256,
            "current_evidence_sha256": current_evidence_sha256,
            "signoff_is_stale": signoff_is_stale,
            "output_pool": out_pool,
            "routing_subject": ctx.routing.get("subject"),
            "routing_domain": ctx.routing.get("domain"),
            "routing_category": ctx.routing.get("category"),
            "routing_level": ctx.routing.get("level"),
            "routing_granularity": ctx.routing.get("granularity"),
            "routing_confidence": ctx.routing.get("confidence"),
            "routing_reason": ctx.routing.get("reason"),
        }
        row.update(self.build_row_extras(ctx.target, ctx.routing))
        if self.INCLUDE_ROUTING_DICT_IN_ROW and "routing" not in row:
            row["routing"] = ctx.routing
        return row

    def emit_queues(self, queues_root: Path, results: ClassificationResult) -> None:
        results.green_rows = sort_queue_rows(results.green_rows)
        results.yellow_rows = sort_queue_rows(results.yellow_rows)
        results.red_rows = sort_queue_rows(results.red_rows)
        write_jsonl(queues_root / "green_download.jsonl", results.green_rows)
        write_jsonl(queues_root / "yellow_pipeline.jsonl", results.yellow_rows)
        write_jsonl(queues_root / "red_rejected.jsonl", results.red_rows)

    def emit_summary(self, cfg: DriverConfig, results: ClassificationResult) -> None:
        summary = {
            "run_at_utc": utc_now(),
            "pipeline_version": VERSION,
            "targets_total": len(cfg.targets),
            "queued_green": len(results.green_rows),
            "queued_yellow": len(results.yellow_rows),
            "queued_red": len(results.red_rows),
            "targets_path": str(cfg.targets_path),
            "license_map_path": str(cfg.license_map_path),
            "manifests_root": str(cfg.manifests_root),
            "queues_root": str(cfg.queues_root),
            "warnings": results.warnings,
        }
        write_json(cfg.queues_root / "run_summary.json", summary)

    def emit_report(self, cfg: DriverConfig, results: ClassificationResult) -> None:
        report = generate_dry_run_report(
            queues_root=cfg.queues_root,
            targets=cfg.targets,
            green_rows=results.green_rows,
            yellow_rows=results.yellow_rows,
            red_rows=results.red_rows,
            warnings=results.warnings,
        )
        if not cfg.args.quiet:
            logger.info(report)

    @classmethod
    def build_arg_parser(cls) -> argparse.ArgumentParser:
        ap = argparse.ArgumentParser(description=f"Pipeline Driver v{VERSION}")
        ap.add_argument(
            "--targets",
            required=True,
            help=f"Path to {cls.TARGETS_LABEL} (v0.8)",
        )
        ap.add_argument(
            "--license-map",
            default=None,
            help="Path to license_map.yaml (defaults to companion_files.license_map)",
        )
        ap.add_argument("--dataset-root", default=None, help="Override dataset root (sets manifests/queues defaults)")
        ap.add_argument("--manifests-root", default=None, help="Override manifests_root (alias: --out-manifests)")
        ap.add_argument("--queues-root", default=None, help="Override queues_root (alias: --out-queues)")
        ap.add_argument("--out-manifests", default=None, help=argparse.SUPPRESS)
        ap.add_argument("--out-queues", default=None, help=argparse.SUPPRESS)
        ap.add_argument(
            "--no-fetch",
            action="store_true",
            help="Do not fetch evidence URLs (offline mode - v0.9: forces YELLOW if no snapshot)",
        )
        ap.add_argument("--retry-max", type=int, default=None, help="Max retries for evidence fetching")
        ap.add_argument("--retry-backoff", type=float, default=None, help="Backoff base for evidence fetching")
        ap.add_argument("--max-retries", type=int, default=None, help=argparse.SUPPRESS)
        ap.add_argument(
            "--min-license-confidence",
            type=float,
            default=0.6,
            help="Minimum SPDX confidence required before GREEN classification",
        )
        ap.add_argument(
            "--evidence-header",
            action="append",
            default=[],
            help="Custom header for evidence fetcher (KEY=VALUE). Useful for license-gated pages",
        )
        ap.add_argument("--quiet", action="store_true", help="Suppress dry-run report output")
        add_logging_args(ap)
        return ap

    @classmethod
    def main(cls) -> None:
        args = cls.build_arg_parser().parse_args()
        configure_logging(level=args.log_level, fmt=args.log_format)
        cls().run(args)
