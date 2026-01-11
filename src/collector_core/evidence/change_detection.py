from __future__ import annotations

from typing import Any

import re

from collector_core.stability import stable_api
from collector_core.utils import normalize_whitespace, sha256_bytes

EVIDENCE_CHANGE_POLICIES = {"raw", "normalized", "either"}
COSMETIC_CHANGE_POLICIES = {"warn_only", "treat_as_changed"}


_URL_QUERYSTRING_RE = re.compile(r"(https?://[^\s?#]+)\?[^\s#]+")
_TIMESTAMP_PATTERNS = [
    re.compile(
        r"\b\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?\b"
    ),
    re.compile(r"\b\d{4}-\d{2}-\d{2}\b"),
    re.compile(r"\b\d{1,2}/\d{1,2}/\d{4}\b"),
    re.compile(r"\b\d{2}:\d{2}:\d{2}\b"),
    re.compile(r"\b\d{2}:\d{2}\b"),
]


@stable_api
def normalize_evidence_text(text: str) -> str:
    cleaned = _URL_QUERYSTRING_RE.sub(r"\1", text or "")
    for pattern in _TIMESTAMP_PATTERNS:
        cleaned = pattern.sub(" ", cleaned)
    return normalize_whitespace(cleaned)


@stable_api
def normalize_evidence_change_policy(value: Any) -> str:
    policy = str(value or "").strip().lower()
    if policy in EVIDENCE_CHANGE_POLICIES:
        return policy
    return "normalized"


@stable_api
def normalize_cosmetic_change_policy(value: Any) -> str:
    policy = str(value or "").strip().lower()
    if policy in COSMETIC_CHANGE_POLICIES:
        return policy
    return "warn_only"


@stable_api
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


@stable_api
def compute_normalized_text_hash(text: str) -> str:
    normalized = normalize_evidence_text(text)
    return sha256_bytes(normalized.encode("utf-8"))


@stable_api
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



@stable_api
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


@stable_api
def normalize_evidence_fetch_status(
    evidence_snapshot: dict[str, Any],
) -> tuple[str, str | None]:
    status = str(evidence_snapshot.get("status") or "unknown")
    reason = None
    if status == "ok":
        return status, None
    if status in {"error", "blocked_url", "needs_manual_evidence"}:
        reason = str(evidence_snapshot.get("error") or status)
    elif status in {"offline_missing", "no_url", "response_too_large", "skipped"}:
        reason = status
    else:
        reason = status
    return status, reason
