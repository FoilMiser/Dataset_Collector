from __future__ import annotations

import argparse
import dataclasses
import hashlib
import html
import ipaddress
import json
import logging
import os
import re
import socket
import time
import urllib.parse
from collections import Counter
from pathlib import Path
from typing import Any

import requests

from collector_core.__version__ import __version__ as VERSION
from collector_core.companion_files import read_denylist_raw, read_license_maps, resolve_companion_paths
from collector_core.config_validator import read_yaml
from collector_core.dependencies import _try_import
from collector_core.exceptions import ConfigValidationError
from collector_core.logging_config import add_logging_args, configure_logging
from collector_core.network_utils import _with_retries
from collector_core.secrets import REDACTED, SecretStr, redact_headers

logger = logging.getLogger(__name__)
PdfReader = _try_import("pypdf", "PdfReader")

SUPPORTED_GATES = {
    "manual_legal_review",
    "manual_review",
    "no_restrictions",
    "restriction_phrase_scan",
    "snapshot_terms",
}
EVIDENCE_CHANGE_POLICIES = {"raw", "normalized", "either"}
COSMETIC_CHANGE_POLICIES = {"warn_only", "treat_as_changed"}
EVIDENCE_EXTENSIONS = [".html", ".pdf", ".txt", ".json"]

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
    tmp_path = Path(f"{path}.tmp")
    tmp_path.write_text(json.dumps(obj, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    tmp_path.replace(path)


def normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()


def lower(text: str) -> str:
    return (text or "").lower()


def _is_blocked_ip(ip_value: str) -> bool:
    ip = ipaddress.ip_address(ip_value)
    return ip.is_private or ip.is_loopback or ip.is_link_local


def _resolve_host_ips(hostname: str) -> list[str]:
    ips: set[str] = set()
    try:
        addrinfo = socket.getaddrinfo(hostname, None)
    except socket.gaierror:
        return []
    for item in addrinfo:
        sockaddr = item[4]
        if sockaddr:
            ips.add(sockaddr[0])
    return sorted(ips)


def validate_evidence_url(url: str, allow_private_hosts: bool) -> tuple[bool, str | None]:
    parsed = urllib.parse.urlparse(url)
    scheme = (parsed.scheme or "").lower()
    if scheme not in {"http", "https"}:
        return False, f"unsupported_scheme:{scheme or 'missing'}"
    if not parsed.hostname:
        return False, "missing_hostname"
    if allow_private_hosts:
        return True, None
    hostname = parsed.hostname
    try:
        ip_value = ipaddress.ip_address(hostname)
    except ValueError:
        ips = _resolve_host_ips(hostname)
        if not ips:
            return False, "unresolvable_hostname"
        for ip_value in ips:
            if _is_blocked_ip(ip_value):
                return False, f"blocked_ip:{ip_value}"
        return True, None
    if _is_blocked_ip(str(ip_value)):
        return False, f"blocked_ip:{ip_value}"
    return True, None


_HTML_SCRIPT_STYLE_RE = re.compile(r"(?is)<(script|style)[^>]*>.*?</\1>")
_HTML_COMMENT_RE = re.compile(r"(?s)<!--.*?-->")
_HTML_TAG_RE = re.compile(r"(?s)<[^>]+>")
_URL_QUERYSTRING_RE = re.compile(r"(https?://[^\s?#]+)\?[^\s#]+")
_TIMESTAMP_PATTERNS = [
    re.compile(r"\b\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?\b"),
    re.compile(r"\b\d{4}-\d{2}-\d{2}\b"),
    re.compile(r"\b\d{1,2}/\d{1,2}/\d{4}\b"),
    re.compile(r"\b\d{2}:\d{2}:\d{2}\b"),
    re.compile(r"\b\d{2}:\d{2}\b"),
]


def html_to_text(text: str) -> str:
    cleaned = html.unescape(text or "")
    cleaned = _HTML_SCRIPT_STYLE_RE.sub(" ", cleaned)
    cleaned = _HTML_COMMENT_RE.sub(" ", cleaned)
    cleaned = _HTML_TAG_RE.sub(" ", cleaned)
    return cleaned


def normalize_evidence_text(text: str) -> str:
    cleaned = _URL_QUERYSTRING_RE.sub(r"\1", text or "")
    for pattern in _TIMESTAMP_PATTERNS:
        cleaned = pattern.sub(" ", cleaned)
    return normalize_whitespace(cleaned)


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


def normalize_evidence_change_policy(value: Any) -> str:
    policy = str(value or "").strip().lower()
    if policy in EVIDENCE_CHANGE_POLICIES:
        return policy
    return "normalized"


def normalize_cosmetic_change_policy(value: Any) -> str:
    policy = str(value or "").strip().lower()
    if policy in COSMETIC_CHANGE_POLICIES:
        return policy
    return "warn_only"


def resolve_evidence_change(
    raw_changed: bool,
    normalized_changed: bool,
    cosmetic_change: bool,
    evidence_policy: str,
    cosmetic_policy: str,
) -> bool:
    if evidence_policy == "raw":
        changed = raw_changed
    elif evidence_policy == "either":
        changed = raw_changed or normalized_changed
    else:
        changed = normalized_changed
    if cosmetic_change and cosmetic_policy == "treat_as_changed":
        return True
    return changed


@dataclasses.dataclass
class LicenseMap:
    allow: list[str]
    conditional: list[str]
    deny_prefixes: list[str]
    normalization_rules: list[dict[str, Any]]
    restriction_phrases: list[str]
    gating: dict[str, str]
    profiles: dict[str, dict[str, Any]]
    evidence_change_policy: str
    cosmetic_change_policy: str


@dataclasses.dataclass(frozen=True)
class DriverConfig:
    args: argparse.Namespace
    retry_max: int
    retry_backoff: float
    headers: dict[str, str]
    targets_path: Path
    targets_cfg: dict[str, Any]
    license_map_path: list[Path]
    license_map: LicenseMap
    denylist: dict[str, Any]
    manifests_root: Path
    queues_root: Path
    default_gates: list[str]
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
    gates: list[str]
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


def load_license_map(paths: Path | list[Path]) -> LicenseMap:
    path_list = paths if isinstance(paths, list) else [paths]
    m = read_license_maps(path_list)
    spdx = m.get("spdx", {}) or {}
    normalization = m.get("normalization", {}) or {}
    restriction_scan = m.get("restriction_scan", {}) or {}
    gating = m.get("gating", {}) or {}
    profiles = m.get("profiles", {}) or {}
    evidence_change_policy = normalize_evidence_change_policy(m.get("evidence_change_policy"))
    cosmetic_change_policy = normalize_cosmetic_change_policy(m.get("cosmetic_change_policy"))

    return LicenseMap(
        allow=spdx.get("allow", []),
        conditional=spdx.get("conditional", []),
        deny_prefixes=spdx.get("deny_prefixes", []),
        normalization_rules=normalization.get("rules", []),
        restriction_phrases=restriction_scan.get("phrases", []),
        gating=gating,
        profiles=profiles,
        evidence_change_policy=evidence_change_policy,
        cosmetic_change_policy=cosmetic_change_policy,
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


def _scrub_secret_values(value: Any) -> Any:
    if isinstance(value, SecretStr):
        return REDACTED
    if isinstance(value, dict):
        return {key: _scrub_secret_values(val) for key, val in value.items()}
    if isinstance(value, list):
        return [_scrub_secret_values(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_scrub_secret_values(item) for item in value)
    return value


def redact_headers_for_manifest(headers: dict[str, str] | None) -> dict[str, Any]:
    if not headers:
        return {}
    return _scrub_secret_values(redact_headers(headers))


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
    license_map_value = args.license_map if args.license_map is not None else companion.get("license_map")
    license_map_paths = resolve_companion_paths(targets_path, license_map_value, "./license_map.yaml")
    license_map = load_license_map(license_map_paths)
    denylist_paths = resolve_companion_paths(targets_path, companion.get("denylist"), "./denylist.yaml")
    denylist = load_denylist(denylist_paths)
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
        license_map_path=license_map_paths,
        license_map=license_map,
        denylist=denylist,
        manifests_root=manifests_root,
        queues_root=queues_root,
        default_gates=globals_cfg.get("default_gates", []) or [],
        targets=targets_cfg.get("targets", []) or [],
        require_yellow_signoff=bool(globals_cfg.get("require_yellow_signoff", False)),
    )


def find_existing_evidence(manifest_dir: Path) -> Path | None:
    for ext in EVIDENCE_EXTENSIONS:
        candidate = manifest_dir / f"license_evidence{ext}"
        if candidate.exists():
            return candidate
    return None


def cleanup_stale_evidence(manifest_dir: Path, keep: Path | None = None) -> list[Path]:
    removed: list[Path] = []
    keep_resolved = keep.resolve() if keep else None
    for path in manifest_dir.glob("license_evidence*"):
        if path.name == "license_evidence_meta.json":
            continue
        if path.name.startswith("license_evidence.prev_"):
            continue
        if keep_resolved and path.resolve() == keep_resolved:
            continue
        if path.is_file():
            path.unlink()
            removed.append(path)
    return removed


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


def validate_target_gates(
    gates: list[str],
    target_id: str,
    *,
    strict: bool,
) -> list[dict[str, Any]]:
    unknown_gates = sorted({gate for gate in gates if gate not in SUPPORTED_GATES})
    if not unknown_gates:
        return []
    message = f"Target {target_id} uses unsupported gates: {', '.join(unknown_gates)}."
    warning = {
        "type": "unknown_gate",
        "target_id": target_id,
        "unknown_gates": unknown_gates,
        "supported_gates": sorted(SUPPORTED_GATES),
        "message": message,
    }
    if strict:
        raise ConfigValidationError(
            message,
            context={
                "target_id": target_id,
                "unknown_gates": unknown_gates,
                "supported_gates": sorted(SUPPORTED_GATES),
            },
        )
    return [warning]


def extract_evidence_fields(target: dict[str, Any]) -> tuple[str, str]:
    evidence = target.get("license_evidence", {}) or {}
    spdx_hint = str(evidence.get("spdx_hint", "UNKNOWN"))
    evidence_url = str(evidence.get("url", ""))
    return spdx_hint, evidence_url


def _merge_download_config(download: dict[str, Any]) -> dict[str, Any]:
    download_cfg = dict(download or {})
    cfg = download_cfg.get("config")
    if isinstance(cfg, dict):
        merged = dict(cfg)
        merged.update({k: v for k, v in download_cfg.items() if k != "config"})
        return merged
    return download_cfg


def _is_probable_url(value: str) -> bool:
    if not value:
        return False
    try:
        from urllib.parse import urlparse

        parsed = urlparse(value)
        return parsed.scheme.lower() in {"http", "https"} and bool(parsed.netloc)
    except Exception:
        return False


def _collect_urls(value: Any, urls: list[str], seen: set[str]) -> None:
    if isinstance(value, str):
        if _is_probable_url(value) and value not in seen:
            seen.add(value)
            urls.append(value)
        return
    if isinstance(value, dict):
        for item in value.values():
            _collect_urls(item, urls, seen)
        return
    if isinstance(value, (list, tuple, set)):
        for item in value:
            _collect_urls(item, urls, seen)


def extract_download_urls(target: dict[str, Any]) -> list[str]:
    download_cfg = _merge_download_config(target.get("download", {}) or {})
    urls: list[str] = []
    _collect_urls(download_cfg, urls, set())
    return urls


def build_denylist_haystack(
    tid: str,
    name: str,
    evidence_url: str,
    download_urls: list[str],
    target: dict[str, Any],
) -> dict[str, Any]:
    download_blob = " ".join(download_urls)
    return {
        "id": tid,
        "name": name,
        "license_evidence_url": evidence_url,
        "download_blob": download_blob,
        "download_urls": download_urls,
        "publisher": str(target.get("publisher", "") or ""),
    }


def extract_domain(url: str) -> str:
    """Extract domain from URL for domain-based denylist matching."""
    try:
        from urllib.parse import urlparse

        parsed = urlparse(url)
        return parsed.hostname or ""
    except Exception:
        return ""


def _domain_matches(host: str, target: str) -> bool:
    if not host or not target:
        return False
    host_l = host.lower()
    target_l = target.lower()
    return host_l == target_l or host_l.endswith(f".{target_l}")


def _normalize_denylist(data: dict[str, Any]) -> dict[str, Any]:
    patterns = data.get("patterns", []) or []
    domain_patterns = data.get("domain_patterns", []) or []
    publisher_patterns = data.get("publisher_patterns", []) or []

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
            fields = ["id", "name", "license_evidence_url", "download_urls", "download_blob"]
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


def load_denylist(paths: Path | list[Path]) -> dict[str, Any]:
    """Load denylist.yaml if present. Returns dict with keys: patterns, domain_patterns, publisher_patterns."""
    path_list = paths if isinstance(paths, list) else [paths]
    raw = read_denylist_raw(path_list)
    return _normalize_denylist(raw)


def _iter_hay_values(value: Any) -> list[str]:
    if isinstance(value, (list, tuple, set)):
        return [str(item) for item in value if item]
    if value:
        return [str(value)]
    return []


def denylist_hits(denylist: dict[str, Any], hay: dict[str, Any]) -> list[dict[str, Any]]:
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
            for src in _iter_hay_values(hay.get(f, "")):
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
                    if _domain_matches(src_domain, val):
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
            else:
                continue
            break

    # v0.9: Process domain patterns (against URLs in hay)
    domain_pats = (denylist or {}).get("domain_patterns", []) or []
    url_fields = ["license_evidence_url", "download_urls"]
    for dp in domain_pats:
        target_domain = dp.get("domain", "").lower()
        if not target_domain:
            continue
        for f in url_fields:
            for src in _iter_hay_values(hay.get(f, "")):
                src_domain = extract_domain(src)
                if _domain_matches(src_domain, target_domain):
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
            else:
                continue
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

    def _find_rule_match(needle: str) -> tuple[int, int] | None:
        if not needle:
            return None
        if len(needle) <= 4 and re.fullmatch(r"[A-Za-z0-9]+", needle):
            pattern = re.compile(rf"\b{re.escape(needle)}\b", re.IGNORECASE)
            match = pattern.search(blob)
            if match:
                return match.start(), match.end()
            return None
        idx = blob_l.find(lower(needle))
        if idx == -1:
            return None
        return idx, idx + len(needle)

    def _excerpt(start: int, end: int, context: int = 40) -> str:
        before = max(0, start - context)
        after = min(len(blob), end + context)
        return blob[before:after].strip()

    for rule in license_map.normalization_rules:
        needles = [x for x in (rule.get("match_any") or []) if x]
        matched_needle = None
        match_span = None
        for needle in needles:
            match_span = _find_rule_match(str(needle))
            if match_span:
                matched_needle = str(needle)
                break
        if matched_needle and match_span:
            confidence = min(0.9, 0.6 + 0.05 * len(needles))
            spdx = str(rule.get("spdx", "UNKNOWN")) or "UNKNOWN"
            snippet = _excerpt(*match_span)
            reason = (
                "normalized via rule match: "
                f"spdx={spdx} needle='{matched_needle}' excerpt='{snippet}'"
            )
            return spdx, confidence, reason

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


def extract_text_from_path(path: Path, evidence: dict[str, Any] | None = None) -> str:
    if not path.exists():
        if evidence is not None:
            evidence["text_extraction_failed"] = True
        return ""
    if path.suffix.lower() == ".pdf":
        if evidence is not None:
            evidence["pdf_text_extraction_failed"] = False
            evidence["text_extraction_failed"] = False
        if PdfReader is None:
            if evidence is not None:
                evidence["pdf_text_extraction_failed"] = True
                evidence["text_extraction_failed"] = True
            return ""
        try:
            reader = PdfReader(str(path))
            pages = []
            for page in list(reader.pages)[:5]:
                text = page.extract_text() or ""
                if text:
                    pages.append(text)
            extracted = "\n\n".join(pages).strip()
            if not extracted and evidence is not None:
                evidence["pdf_text_extraction_failed"] = True
                evidence["text_extraction_failed"] = True
            return extracted
        except Exception:
            if evidence is not None:
                evidence["pdf_text_extraction_failed"] = True
                evidence["text_extraction_failed"] = True
            return ""
    try:
        raw = path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        if evidence is not None:
            evidence["text_extraction_failed"] = True
        return ""
    if evidence is not None:
        evidence["text_extraction_failed"] = False
    if path.suffix.lower() in {".html", ".htm"}:
        return html_to_text(raw)
    return raw


def extract_text_for_scanning(evidence: dict[str, Any]) -> str:
    saved = str(evidence.get("saved_path") or "")
    if not saved:
        return ""
    return extract_text_from_path(Path(saved), evidence)


def compute_normalized_text_hash(text: str) -> str:
    normalized = normalize_evidence_text(text)
    return sha256_bytes(normalized.encode("utf-8"))


def apply_normalized_hash_fallback(
    *,
    evidence: dict[str, Any] | None,
    raw_hash: str | None,
    extraction_failed: bool,
    normalized_hash: str | None,
) -> str | None:
    if extraction_failed and raw_hash:
        if evidence is not None:
            evidence["normalized_hash_fallback"] = "raw_bytes"
            evidence["text_extraction_failed"] = True
        return raw_hash
    return normalized_hash


def compute_file_hashes(path: Path, evidence: dict[str, Any] | None = None) -> tuple[str | None, str | None]:
    raw_hash = sha256_file(path)
    extracted = extract_text_from_path(path, evidence)
    extraction_failed = bool(
        evidence
        and (evidence.get("text_extraction_failed") or evidence.get("pdf_text_extraction_failed"))
    )
    if not extraction_failed and evidence is None and path.suffix.lower() == ".pdf" and PdfReader is None:
        extraction_failed = True
    normalized_hash = None
    if not extraction_failed:
        normalized_hash = compute_normalized_text_hash(extracted)
    normalized_hash = apply_normalized_hash_fallback(
        evidence=evidence,
        raw_hash=raw_hash,
        extraction_failed=extraction_failed,
        normalized_hash=normalized_hash,
    )
    return raw_hash, normalized_hash


def compute_signoff_mismatches(
    *,
    signoff_raw_sha: str | None,
    signoff_normalized_sha: str | None,
    current_raw_sha: str | None,
    current_normalized_sha: str | None,
    text_extraction_failed: bool,
) -> tuple[bool, bool, bool]:
    raw_mismatch = bool(signoff_raw_sha and current_raw_sha and signoff_raw_sha != current_raw_sha)
    normalized_mismatch = bool(
        signoff_normalized_sha
        and current_normalized_sha
        and signoff_normalized_sha != current_normalized_sha
    )
    if text_extraction_failed and raw_mismatch:
        normalized_mismatch = True
    cosmetic_change = bool(
        raw_mismatch
        and not normalized_mismatch
        and signoff_normalized_sha
        and current_normalized_sha
        and not text_extraction_failed
    )
    return raw_mismatch, normalized_mismatch, cosmetic_change


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
    EVIDENCE_MAX_BYTES = 20 * 1024 * 1024
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
        timeout_s: float | tuple[float, float] = (15.0, 60.0),
        max_retries: int = 3,
        backoff_base: float = 2.0,
        headers: dict[str, str] | None = None,
        max_bytes: int | None = None,
        allow_private_hosts: bool = False,
    ) -> tuple[bytes | None, str | None, dict[str, Any]]:
        """Fetch URL with retry and exponential backoff."""
        meta: dict[str, Any] = {"retries": 0, "errors": [], "timeout": timeout_s, "max_bytes": max_bytes}
        allowed, reason = validate_evidence_url(url, allow_private_hosts)
        if not allowed:
            meta["blocked_url"] = url
            meta["blocked_reason"] = reason
            return None, "blocked_url", meta

        def _record_retry(attempt: int, exc: Exception) -> None:
            meta["retries"] = attempt
            meta["errors"].append({"attempt": attempt, "error": repr(exc)})

        def _fetch_once() -> tuple[bytes, str]:
            with requests.get(
                url,
                timeout=timeout_s,
                headers={"User-Agent": f"{self.USER_AGENT}/{VERSION}", **(headers or {})},
                stream=True,
            ) as r:
                r.raise_for_status()
                ctype = r.headers.get("Content-Type", "")
                final_url = r.url
                meta["final_status"] = r.status_code
                meta["final_url"] = final_url
                meta["content_type"] = ctype
                redirect_urls: list[str] = []
                for resp in r.history:
                    location = resp.headers.get("Location")
                    if not location:
                        continue
                    redirect_urls.append(urllib.parse.urljoin(resp.url, location))
                redirect_urls.append(final_url)
                for redirect_url in redirect_urls:
                    allowed, reason = validate_evidence_url(redirect_url, allow_private_hosts)
                    if not allowed:
                        meta["blocked_url"] = redirect_url
                        meta["blocked_reason"] = reason
                        logger.warning(
                            "Evidence fetch blocked: url=%s blocked_url=%s reason=%s",
                            url,
                            redirect_url,
                            reason,
                        )
                        raise RuntimeError("blocked_url")
                content_length = r.headers.get("Content-Length")
                if max_bytes and content_length:
                    try:
                        if int(content_length) > max_bytes:
                            meta["size_exceeded"] = True
                            meta["bytes"] = int(content_length)
                            logger.warning(
                                "Evidence fetch aborted: url=%s final_url=%s content_type=%s bytes=%s limit=%s",
                                url,
                                final_url,
                                ctype,
                                content_length,
                                max_bytes,
                            )
                            raise RuntimeError("response_too_large")
                    except ValueError:
                        pass

                chunks: list[bytes] = []
                total = 0
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if not chunk:
                        continue
                    total += len(chunk)
                    if max_bytes and total > max_bytes:
                        meta["size_exceeded"] = True
                        meta["bytes"] = total
                        logger.warning(
                            "Evidence fetch aborted: url=%s final_url=%s content_type=%s bytes=%s limit=%s",
                            url,
                            final_url,
                            ctype,
                            total,
                            max_bytes,
                        )
                        raise RuntimeError("response_too_large")
                    chunks.append(chunk)
                content = b"".join(chunks)
                meta["bytes"] = total
                logger.info(
                    "Evidence fetched: url=%s final_url=%s content_type=%s bytes=%s",
                    url,
                    final_url,
                    ctype,
                    total,
                )
                return content, ctype

        try:
            content, ctype = _with_retries(
                _fetch_once,
                max_attempts=max_retries,
                backoff_base=backoff_base,
                backoff_max=60.0,
                on_retry=_record_retry,
            )
            return content, ctype, meta
        except Exception as e:
            meta["errors"].append({"attempt": meta["retries"] + 1, "error": repr(e)})
            meta["retries"] = max(meta["retries"], 1)
            if str(e) == "blocked_url":
                return None, "blocked_url", meta
            if str(e) == "response_too_large":
                return None, "response_too_large", meta

        return None, f"Failed after {meta['retries']} attempts", meta

    def snapshot_evidence(
        self,
        manifest_dir: Path,
        url: str,
        *,
        evidence_change_policy: str = "normalized",
        cosmetic_change_policy: str = "warn_only",
        max_retries: int = 3,
        backoff_base: float = 2.0,
        headers: dict[str, str] | None = None,
        allow_private_hosts: bool = False,
    ) -> dict[str, Any]:
        result: dict[str, Any] = {
            "url": url,
            "fetched_at_utc": utc_now(),
            "status": "skipped",
            "headers_used": redact_headers_for_manifest(headers),
        }
        if not url:
            result["status"] = "no_url"
            return result

        previous_digest = None
        previous_normalized_digest = None
        existing_path = None
        existing_meta: dict[str, Any] = {}
        existing_path = find_existing_evidence(manifest_dir)
        if existing_path:
            previous_digest = sha256_file(existing_path)
        meta_path = manifest_dir / "license_evidence_meta.json"
        if meta_path.exists():
            try:
                existing_meta = json.loads(meta_path.read_text(encoding="utf-8"))
            except Exception:
                existing_meta = {}
        previous_normalized_digest = existing_meta.get("sha256_normalized_text")
        if previous_normalized_digest is None and existing_path:
            _, previous_normalized_digest = compute_file_hashes(existing_path)

        content, info, meta = self.fetch_url_with_retry(
            url,
            max_retries=max_retries,
            backoff_base=backoff_base,
            headers=headers,
            max_bytes=self.EVIDENCE_MAX_BYTES,
            allow_private_hosts=allow_private_hosts,
        )
        result["fetch_meta"] = meta

        if content is None:
            if meta.get("size_exceeded"):
                result["status"] = "needs_manual_evidence"
                result["error"] = info
            else:
                result["status"] = "error"
                result["error"] = info
            return result

        ctype = info or ""
        if meta.get("final_url"):
            result["final_url"] = meta.get("final_url")
        digest = sha256_bytes(content)
        result.update(
            {
                "status": "ok",
                "content_type": ctype,
                "sha256": digest,
                "sha256_raw_bytes": digest,
                "bytes": len(content),
                "previous_sha256": previous_digest,
                "previous_sha256_raw_bytes": previous_digest,
                "previous_sha256_normalized_text": previous_normalized_digest,
                "previous_path": str(existing_path) if existing_path else None,
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
        temp_path = manifest_dir / f"license_evidence{ext}.part"
        cleanup_stale_evidence(manifest_dir, keep=existing_path)
        raw_changed_from_previous = bool(previous_digest and previous_digest != digest)
        history: list[dict[str, Any]] = []
        if isinstance(existing_meta.get("history"), list):
            history = list(existing_meta.get("history", []))
        previous_entry = None
        previous_renamed_path = None
        if raw_changed_from_previous and existing_path and previous_digest:
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
                "sha256_raw_bytes": previous_digest,
                "sha256_normalized_text": previous_normalized_digest,
                "filename": prev_path.name,
                "fetched_at_utc": existing_meta.get("fetched_at_utc"),
            }
            history.append(previous_entry)
        temp_path.write_bytes(content)
        temp_path.replace(out_path)
        saved_digest = sha256_file(out_path)
        result["saved_path"] = str(out_path)
        if not saved_digest or saved_digest != digest:
            result["status"] = "error"
            result["error"] = "Evidence file write verification failed."
        normalized_text = extract_text_for_scanning(result)
        extraction_failed = bool(
            result.get("text_extraction_failed") or result.get("pdf_text_extraction_failed")
        )
        normalized_digest = None
        if not extraction_failed:
            normalized_digest = compute_normalized_text_hash(normalized_text)
        normalized_digest = apply_normalized_hash_fallback(
            evidence=result,
            raw_hash=digest,
            extraction_failed=extraction_failed,
            normalized_hash=normalized_digest,
        )
        normalized_changed_from_previous = bool(
            previous_normalized_digest
            and normalized_digest
            and previous_normalized_digest != normalized_digest
        )
        cosmetic_change = bool(
            raw_changed_from_previous
            and not normalized_changed_from_previous
            and normalized_digest
            and previous_normalized_digest
        )
        changed_from_previous = resolve_evidence_change(
            raw_changed_from_previous,
            normalized_changed_from_previous,
            cosmetic_change,
            evidence_change_policy,
            cosmetic_change_policy,
        )
        result.update(
            {
                "sha256_normalized_text": normalized_digest,
                "raw_changed_from_previous": raw_changed_from_previous,
                "normalized_changed_from_previous": normalized_changed_from_previous,
                "cosmetic_change": cosmetic_change,
                "changed_from_previous": changed_from_previous,
            }
        )
        if previous_renamed_path:
            result["previous_renamed_path"] = str(previous_renamed_path)
        result["history"] = history
        if result["changed_from_previous"]:
            result["evidence_files_verified"] = bool(
                (previous_renamed_path and previous_renamed_path.exists())
                and out_path.exists()
                and saved_digest == digest
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
        download_urls = extract_download_urls(target)
        review_required = bool(target.get("review_required", False))
        merged_gates = merge_gates(cfg.default_gates, target.get("gates_override", {}) or {})
        warnings.extend(validate_target_gates(merged_gates, tid, strict=cfg.args.strict))
        gates = canonicalize_gates(merged_gates)
        target_manifest_dir = cfg.manifests_root / tid
        ensure_dir(target_manifest_dir)
        signoff = read_review_signoff(target_manifest_dir)
        review_status = str(signoff.get("status", "") or "").lower()
        promote_to = str(signoff.get("promote_to", "") or "").upper()
        dl_hits = denylist_hits(cfg.denylist, build_denylist_haystack(tid, name, evidence_url, download_urls, target))
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
        evidence_raw_sha = evidence.snapshot.get("sha256_raw_bytes") or evidence.snapshot.get("sha256")
        evidence_normalized_sha = evidence.snapshot.get("sha256_normalized_text")
        signoff_raw_sha = ctx.signoff.get("license_evidence_sha256_raw_bytes") or ctx.signoff.get("license_evidence_sha256")
        signoff_normalized_sha = ctx.signoff.get("license_evidence_sha256_normalized_text")
        raw_mismatch, normalized_mismatch, cosmetic_change = compute_signoff_mismatches(
            signoff_raw_sha=signoff_raw_sha,
            signoff_normalized_sha=signoff_normalized_sha,
            current_raw_sha=evidence_raw_sha,
            current_normalized_sha=evidence_normalized_sha,
            text_extraction_failed=bool(evidence.snapshot.get("text_extraction_failed")),
        )
        change_requires_review = resolve_evidence_change(
            raw_mismatch,
            normalized_mismatch,
            cosmetic_change,
            cfg.license_map.evidence_change_policy,
            cfg.license_map.cosmetic_change_policy,
        )
        if change_requires_review:
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
            cfg.license_map,
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
                evidence_change_policy=cfg.license_map.evidence_change_policy,
                cosmetic_change_policy=cfg.license_map.cosmetic_change_policy,
                max_retries=cfg.retry_max,
                backoff_base=cfg.retry_backoff,
                headers=cfg.headers,
                allow_private_hosts=cfg.args.allow_private_evidence_hosts,
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
                raw_hash, normalized_hash = compute_file_hashes(existing_evidence_path, evidence_snapshot)
                evidence_snapshot.update(
                    {
                        "sha256": raw_hash,
                        "sha256_raw_bytes": raw_hash,
                        "sha256_normalized_text": normalized_hash,
                    }
                )
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
        signoff_evidence_sha256_raw = (ctx.signoff or {}).get("license_evidence_sha256_raw_bytes") or signoff_evidence_sha256
        signoff_evidence_sha256_normalized = (ctx.signoff or {}).get("license_evidence_sha256_normalized_text")
        current_evidence_sha256 = evidence.snapshot.get("sha256")
        current_evidence_sha256_raw = evidence.snapshot.get("sha256_raw_bytes") or current_evidence_sha256
        current_evidence_sha256_normalized = evidence.snapshot.get("sha256_normalized_text")
        raw_mismatch, normalized_mismatch, cosmetic_change = compute_signoff_mismatches(
            signoff_raw_sha=signoff_evidence_sha256_raw,
            signoff_normalized_sha=signoff_evidence_sha256_normalized,
            current_raw_sha=current_evidence_sha256_raw,
            current_normalized_sha=current_evidence_sha256_normalized,
            text_extraction_failed=bool(evidence.snapshot.get("text_extraction_failed")),
        )
        signoff_is_stale = resolve_evidence_change(
            raw_mismatch,
            normalized_mismatch,
            cosmetic_change,
            cfg.license_map.evidence_change_policy,
            cfg.license_map.cosmetic_change_policy,
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
            "evidence_headers_used": redact_headers_for_manifest(cfg.headers),
            "license_change_detected": evidence.license_change_detected,
            "signoff_evidence_sha256": signoff_evidence_sha256,
            "signoff_evidence_sha256_raw_bytes": signoff_evidence_sha256_raw,
            "signoff_evidence_sha256_normalized_text": signoff_evidence_sha256_normalized,
            "current_evidence_sha256": current_evidence_sha256,
            "current_evidence_sha256_raw_bytes": current_evidence_sha256_raw,
            "current_evidence_sha256_normalized_text": current_evidence_sha256_normalized,
            "signoff_is_stale": signoff_is_stale,
            "signoff_cosmetic_change": cosmetic_change,
            "evidence_raw_changed": evidence.snapshot.get("raw_changed_from_previous"),
            "evidence_normalized_changed": evidence.snapshot.get("normalized_changed_from_previous"),
            "evidence_cosmetic_change": evidence.snapshot.get("cosmetic_change"),
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
        license_map: LicenseMap,
        evidence: EvidenceResult,
        restriction_hits: list[str],
        resolved: str,
        resolved_confidence: float,
        eff_bucket: str,
        review_required: bool,
        out_pool: str,
    ) -> dict[str, Any]:
        signoff_evidence_sha256 = (ctx.signoff or {}).get("license_evidence_sha256")
        signoff_evidence_sha256_raw = (ctx.signoff or {}).get("license_evidence_sha256_raw_bytes") or signoff_evidence_sha256
        signoff_evidence_sha256_normalized = (ctx.signoff or {}).get("license_evidence_sha256_normalized_text")
        current_evidence_sha256 = evidence.snapshot.get("sha256")
        current_evidence_sha256_raw = evidence.snapshot.get("sha256_raw_bytes") or current_evidence_sha256
        current_evidence_sha256_normalized = evidence.snapshot.get("sha256_normalized_text")
        raw_mismatch, normalized_mismatch, cosmetic_change = compute_signoff_mismatches(
            signoff_raw_sha=signoff_evidence_sha256_raw,
            signoff_normalized_sha=signoff_evidence_sha256_normalized,
            current_raw_sha=current_evidence_sha256_raw,
            current_normalized_sha=current_evidence_sha256_normalized,
            text_extraction_failed=bool(evidence.snapshot.get("text_extraction_failed")),
        )
        signoff_is_stale = resolve_evidence_change(
            raw_mismatch,
            normalized_mismatch,
            cosmetic_change,
            license_map.evidence_change_policy,
            license_map.cosmetic_change_policy,
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
            "signoff_evidence_sha256_raw_bytes": signoff_evidence_sha256_raw,
            "signoff_evidence_sha256_normalized_text": signoff_evidence_sha256_normalized,
            "current_evidence_sha256": current_evidence_sha256,
            "current_evidence_sha256_raw_bytes": current_evidence_sha256_raw,
            "current_evidence_sha256_normalized_text": current_evidence_sha256_normalized,
            "signoff_is_stale": signoff_is_stale,
            "signoff_cosmetic_change": cosmetic_change,
            "evidence_raw_changed": evidence.snapshot.get("raw_changed_from_previous"),
            "evidence_normalized_changed": evidence.snapshot.get("normalized_changed_from_previous"),
            "evidence_cosmetic_change": evidence.snapshot.get("cosmetic_change"),
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
        failed_targets = [
            {
                "id": warning.get("target_id", "unknown"),
                "error": warning.get("message") or warning.get("type") or "warning",
            }
            for warning in results.warnings
        ]
        counts = Counter(
            {
                "targets_total": len(cfg.targets),
                "queued_green": len(results.green_rows),
                "queued_yellow": len(results.yellow_rows),
                "queued_red": len(results.red_rows),
                "warnings": len(results.warnings),
                "failed": len(failed_targets),
            }
        )
        summary = {
            "run_at_utc": utc_now(),
            "pipeline_version": VERSION,
            "targets_total": len(cfg.targets),
            "queued_green": len(results.green_rows),
            "queued_yellow": len(results.yellow_rows),
            "queued_red": len(results.red_rows),
            "targets_path": str(cfg.targets_path),
            "license_map_path": [str(p) for p in cfg.license_map_path],
            "manifests_root": str(cfg.manifests_root),
            "queues_root": str(cfg.queues_root),
            "warnings": results.warnings,
            "counts": dict(counts),
            "failed_targets": failed_targets,
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
            help=f"Path to {cls.TARGETS_LABEL} (v0.9)",
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
        ap.add_argument(
            "--allow-private-evidence-hosts",
            action="store_true",
            help="Allow evidence URLs that resolve to private, loopback, or link-local IPs (unsafe).",
        )
        ap.add_argument(
            "--strict",
            action="store_true",
            help="Treat config warnings (such as unknown gates) as errors.",
        )
        ap.add_argument("--quiet", action="store_true", help="Suppress dry-run report output")
        add_logging_args(ap)
        return ap

    @classmethod
    def main(cls) -> None:
        args = cls.build_arg_parser().parse_args()
        configure_logging(level=args.log_level, fmt=args.log_format)
        cls().run(args)
