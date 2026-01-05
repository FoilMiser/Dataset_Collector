from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import os
import re
import time
from pathlib import Path
from typing import Any

import requests

from collector_core.__version__ import __version__ as VERSION
from collector_core.config_validator import read_yaml


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

    if "no_restrictions" in gates and restriction_hits:
        bucket = "YELLOW"

    if "manual_review" in gates:
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
        for ext in [".html", ".pdf", ".txt", ".json"]:
            candidate = manifest_dir / f"license_evidence{ext}"
            if candidate.exists():
                existing_path = candidate
                previous_digest = sha256_file(candidate)
                break

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
        out_path.write_bytes(content)
        result["saved_path"] = str(out_path)

        write_json(manifest_dir / "license_evidence_meta.json", result)
        return result

    def run(self, args: argparse.Namespace) -> None:
        retry_max_env = os.getenv("PIPELINE_RETRY_MAX")
        retry_backoff_env = os.getenv("PIPELINE_RETRY_BACKOFF")
        retry_max = args.retry_max if args.retry_max is not None else args.max_retries
        if retry_max is None:
            retry_max = int(retry_max_env) if retry_max_env else 3
        retry_backoff = (
            args.retry_backoff
            if args.retry_backoff is not None
            else (float(retry_backoff_env) if retry_backoff_env else 2.0)
        )

        headers: dict[str, str] = {}
        for raw in args.evidence_header:
            if "=" not in raw:
                continue
            k, v = raw.split("=", 1)
            if k.strip():
                headers[k.strip()] = v.strip()

        targets_path = Path(args.targets).resolve()
        targets_cfg = read_yaml(targets_path, schema_name="targets")

        companion = targets_cfg.get("companion_files", {}) or {}
        license_map_path = Path(args.license_map or companion.get("license_map", "./license_map.yaml")).resolve()
        license_map = load_license_map(license_map_path)

        denylist_path = Path(companion.get("denylist", "./denylist.yaml")).resolve()
        denylist = load_denylist(denylist_path)

        globals_cfg = targets_cfg.get("globals", {}) or {}
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
        ensure_dir(manifests_root)
        ensure_dir(queues_root)

        default_gates = globals_cfg.get("default_gates", []) or []
        targets = targets_cfg.get("targets", []) or []

        # v0.9: Global setting to require signoff for all YELLOW items
        require_yellow_signoff = bool(globals_cfg.get("require_yellow_signoff", False))

        green_rows: list[dict[str, Any]] = []
        yellow_rows: list[dict[str, Any]] = []
        red_rows: list[dict[str, Any]] = []
        warnings: list[dict[str, Any]] = []

        for t in targets:
            enabled = bool(t.get("enabled", True))
            tid = str(t.get("id", "")).strip() or "unknown_id"
            name = str(t.get("name", tid))
            profile = str(t.get("license_profile", "unknown"))
            if profile not in license_map.profiles:
                warnings.append(
                    {
                        "type": "unknown_license_profile",
                        "target_id": tid,
                        "license_profile": profile,
                        "known_profiles": sorted(license_map.profiles.keys()),
                        "message": (
                            f"Target {tid} uses license_profile '{profile}' not present in license_map profiles."
                        ),
                    }
                )
            evidence = t.get("license_evidence", {}) or {}
            spdx_hint = str(evidence.get("spdx_hint", "UNKNOWN"))
            evidence_url = str(evidence.get("url", ""))

            # Flatten download config into a searchable blob for denylist scanning
            download_cfg = t.get("download", {}) or {}
            download_blob = json.dumps(download_cfg, ensure_ascii=False)
            review_required = bool(t.get("review_required", False))

            gates = merge_gates(default_gates, t.get("gates_override", {}) or {})

            target_manifest_dir = manifests_root / tid
            ensure_dir(target_manifest_dir)

            signoff = read_review_signoff(target_manifest_dir)
            review_status = str(signoff.get("status", "") or "").lower()  # approved | rejected | deferred
            promote_to = str(signoff.get("promote_to", "") or "").upper()

            # Build haystack for denylist scanning (fixed: dl_hits moved after definition)
            dl_hay = {
                "id": tid,
                "name": name,
                "license_evidence_url": evidence_url,
                "download_blob": download_blob,
                "publisher": str(t.get("publisher", "") or ""),  # v0.9: publisher metadata for denylist
            }
            dl_hits = denylist_hits(denylist, dl_hay)

            evidence_snapshot = {"status": "skipped", "url": evidence_url}
            evidence_text = ""
            license_change_detected = False

            # v0.9: Check for existing evidence snapshot in --no-fetch mode
            no_fetch_missing_evidence = False
            if "snapshot_terms" in gates and not args.no_fetch:
                evidence_snapshot = self.snapshot_evidence(
                    target_manifest_dir,
                    evidence_url,
                    max_retries=retry_max,
                    backoff_base=retry_backoff,
                    headers=headers,
                )
                evidence_text = extract_text_for_scanning(evidence_snapshot)
                license_change_detected = bool(evidence_snapshot.get("changed_from_previous"))
            elif "snapshot_terms" in gates and args.no_fetch:
                # v0.9: In offline mode, check for existing snapshot
                existing_evidence_path = None
                for ext in [".html", ".pdf", ".txt", ".json"]:
                    candidate = target_manifest_dir / f"license_evidence{ext}"
                    if candidate.exists():
                        existing_evidence_path = candidate
                        break

                if existing_evidence_path:
                    evidence_snapshot = {
                        "status": "ok",
                        "url": evidence_url,
                        "saved_path": str(existing_evidence_path),
                        "fetched_at_utc": utc_now(),
                        "offline_mode": True,
                    }
                    evidence_text = extract_text_for_scanning(evidence_snapshot)
                else:
                    evidence_snapshot = {"status": "offline_missing", "url": evidence_url}
                    no_fetch_missing_evidence = True

            # Restriction scan
            restriction_hits = contains_any(evidence_text, license_map.restriction_phrases)

            # SPDX normalization + confidence scoring
            resolved, resolved_confidence, confidence_reason = resolve_spdx_with_confidence(
                license_map, evidence_text, spdx_hint
            )

            eff_bucket = compute_effective_bucket(
                license_map,
                gates,
                resolved,
                restriction_hits,
                evidence_snapshot,
                args.min_license_confidence,
                resolved_confidence,
            )

            denylist_forced_bucket = None
            for hit in dl_hits:
                severity = hit.get("severity", "hard_red")
                if severity == "hard_red":
                    denylist_forced_bucket = "RED"
                    break
                if severity == "force_yellow":
                    denylist_forced_bucket = "YELLOW"

            if denylist_forced_bucket:
                eff_bucket = denylist_forced_bucket

            # v0.9: --no-fetch safety: force YELLOW if missing evidence in offline mode
            if no_fetch_missing_evidence and eff_bucket == "GREEN":
                eff_bucket = "YELLOW"

            # Manual review gating:
            # - If explicitly rejected: force RED
            # - If review_required and not approved: at least YELLOW (unless already RED)
            # - If approved with promote_to=GREEN: allow promotion to GREEN (conservative: only if no restriction hits)
            if review_status == "rejected":
                eff_bucket = "RED"
            elif review_required and eff_bucket != "RED" and review_status != "approved":
                if eff_bucket == "GREEN":
                    eff_bucket = "YELLOW"
            elif review_status == "approved" and promote_to == "GREEN" and not restriction_hits and eff_bucket != "RED":
                eff_bucket = "GREEN"

            # v0.9: require_yellow_signoff - if enabled and bucket is YELLOW without signoff, stay YELLOW
            if require_yellow_signoff and eff_bucket == "YELLOW" and review_status not in {
                "approved",
                "rejected",
            }:
                review_required = True  # Ensure review_required is set

            # v0.9: Dataset-aware splitting support
            split_group_id = str(t.get("split_group_id", "") or tid)

            # Output pool selection (difficulty recorded as metadata only)
            out_pool = (t.get("output", {}) or {}).get("pool")
            if not out_pool:
                if profile == "copyleft":
                    out_pool = "copyleft"
                elif eff_bucket == "GREEN":
                    out_pool = "permissive"
                else:
                    out_pool = "quarantine"

            routing = self.resolve_routing_fields(t)
            evaluation = {
                "id": tid,
                "name": name,
                "enabled": enabled,
                "evaluated_at_utc": utc_now(),
                "pipeline_version": VERSION,
                "review_required": review_required,
                "review_signoff": signoff or None,
                "review_status": review_status or "pending",
                "denylist_hits": dl_hits,
                "license_profile": profile,
                "spdx_hint": spdx_hint,
                "resolved_spdx": resolved,
                "resolved_spdx_confidence": resolved_confidence,
                "resolved_spdx_confidence_reason": confidence_reason,
                "restriction_hits": restriction_hits,
                "gates": gates,
                "effective_bucket": eff_bucket,
                "queue_bucket": eff_bucket,
                "license_evidence_url": evidence_url,
                "evidence_snapshot": evidence_snapshot,
                "evidence_headers_used": headers,
                "license_change_detected": license_change_detected,
                "download": t.get("download", {}),
                "build": t.get("build", {}),
                "data_type": t.get("data_type", []),
                "priority": t.get("priority", None),
                "statistics": t.get("statistics", {}),
                # v0.9: New fields
                "split_group_id": split_group_id,  # For dataset-aware splitting
                "no_fetch_missing_evidence": no_fetch_missing_evidence,
                "require_yellow_signoff": require_yellow_signoff,
                "output_pool": out_pool,
            }
            evaluation.update(self.build_evaluation_extras(t, routing))
            evaluation["routing"] = routing
            write_json(target_manifest_dir / "evaluation.json", evaluation)

            row = {
                "id": tid,
                "name": name,
                "effective_bucket": eff_bucket,
                "queue_bucket": eff_bucket,
                "license_profile": profile,
                "resolved_spdx": resolved,
                "resolved_spdx_confidence": resolved_confidence,
                "restriction_hits": restriction_hits,
                "license_evidence_url": evidence_url,
                "manifest_dir": str(target_manifest_dir),
                "download": t.get("download", {}),
                "build": t.get("build", {}),
                "data_type": t.get("data_type", []),
                "priority": t.get("priority", None),
                "enabled": enabled,
                "statistics": t.get("statistics", {}),
                # v0.9: New fields
                "split_group_id": split_group_id,
                "denylist_hits": dl_hits,
                "review_required": review_required,
                "license_change_detected": license_change_detected,
                "output_pool": out_pool,
                # Generic routing (v2)
                "routing_subject": routing.get("subject"),
                "routing_domain": routing.get("domain"),
                "routing_category": routing.get("category"),
                "routing_level": routing.get("level"),
                "routing_granularity": routing.get("granularity"),
                "routing_confidence": routing.get("confidence"),
                "routing_reason": routing.get("reason"),
            }
            row.update(self.build_row_extras(t, routing))
            if self.INCLUDE_ROUTING_DICT_IN_ROW and "routing" not in row:
                row["routing"] = routing

            if not enabled:
                continue

            if eff_bucket == "GREEN":
                green_rows.append(row)
            elif eff_bucket == "YELLOW":
                yellow_rows.append(row)
            else:
                red_rows.append(row)

        def sort_key(r: dict[str, Any]) -> tuple[int, str]:
            p = r.get("priority", None)
            try:
                pi = int(p) if p is not None else -999999
            except Exception:
                pi = -999999
            return (-pi, str(r.get("id", "")))

        green_rows.sort(key=sort_key)
        yellow_rows.sort(key=sort_key)
        red_rows.sort(key=sort_key)

        write_jsonl(queues_root / "green_download.jsonl", green_rows)
        write_jsonl(queues_root / "yellow_pipeline.jsonl", yellow_rows)
        write_jsonl(queues_root / "red_rejected.jsonl", red_rows)

        summary = {
            "run_at_utc": utc_now(),
            "pipeline_version": VERSION,
            "targets_total": len(targets),
            "queued_green": len(green_rows),
            "queued_yellow": len(yellow_rows),
            "queued_red": len(red_rows),
            "targets_path": str(targets_path),
            "license_map_path": str(license_map_path),
            "manifests_root": str(manifests_root),
            "queues_root": str(queues_root),
            "warnings": warnings,
        }
        write_json(queues_root / "run_summary.json", summary)

        report = generate_dry_run_report(
            queues_root=queues_root,
            targets=targets,
            green_rows=green_rows,
            yellow_rows=yellow_rows,
            red_rows=red_rows,
            warnings=warnings,
        )
        if not args.quiet:
            print(report)

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
        return ap

    @classmethod
    def main(cls) -> None:
        args = cls.build_arg_parser().parse_args()
        cls().run(args)
