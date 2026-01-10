"""
collector_core/denylist_matcher.py

Denylist pattern matching and management.
Extracted from pipeline_driver_base.py for improved modularity.

This module handles:
- Domain and URL pattern matching
- Denylist loading and normalization
- Hit detection against target metadata
"""
from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any

from collector_core.companion_files import read_denylist_raw

logger = logging.getLogger(__name__)


def extract_domain(url: str) -> str:
    """Extract domain from URL for domain-based denylist matching."""
    try:
        from urllib.parse import urlparse

        parsed = urlparse(url)
        return parsed.hostname or ""
    except Exception:
        logger.debug("Failed to extract domain from URL: %r", url)
        return ""


def _domain_matches(host: str, target: str) -> bool:
    """Check if hostname matches target domain (exact or subdomain)."""
    if not host or not target:
        return False
    host_l = host.lower()
    target_l = target.lower()
    return host_l == target_l or host_l.endswith(f".{target_l}")


def _normalize_denylist(data: dict[str, Any]) -> dict[str, Any]:
    """Normalize v0.9 denylist format with patterns, domain_patterns, publisher_patterns."""
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
    """Iterate over field values, handling lists and single values."""
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


def build_denylist_haystack(
    tid: str,
    name: str,
    evidence_url: str,
    download_urls: list[str],
    target: dict[str, Any],
) -> dict[str, Any]:
    """Build a haystack dictionary for denylist matching from target metadata."""
    download_blob = " ".join(download_urls)
    return {
        "id": tid,
        "name": name,
        "license_evidence_url": evidence_url,
        "download_blob": download_blob,
        "download_urls": download_urls,
        "publisher": str(target.get("publisher", "") or ""),
    }
