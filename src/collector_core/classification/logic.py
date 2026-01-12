from __future__ import annotations

import re
from typing import Any

from collector_core.evidence.change_detection import normalize_evidence_fetch_status
from collector_core.stability import stable_api
from collector_core.utils.text import lower, normalize_whitespace

if False:  # pragma: no cover - type checking
    from collector_core.pipeline_driver_base import EvidenceResult, LicenseMap, TargetContext


@stable_api
def resolve_spdx_with_confidence(
    license_map: "LicenseMap", evidence_text: str, spdx_hint: str
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


@stable_api
def spdx_bucket(license_map: "LicenseMap", spdx: str) -> str:
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


@stable_api
def compute_effective_bucket(
    license_map: "LicenseMap",
    license_gates: list[str],
    resolved_spdx: str,
    restriction_hits: list[str],
    evidence_snapshot: dict[str, Any],
    min_confidence: float,
    resolved_confidence: float,
) -> str:
    """Compute effective bucket based on gates and scan results."""
    bucket = spdx_bucket(license_map, resolved_spdx)
    evidence_status, _ = normalize_evidence_fetch_status(evidence_snapshot)

    # Confidence gate: if confidence is too low, force YELLOW
    if resolved_confidence < min_confidence and bucket == "GREEN":
        bucket = license_map.gating.get("low_confidence_bucket", "YELLOW")

    if "snapshot_terms" in license_gates and evidence_status != "ok":
        # If we require snapshot and failed, force YELLOW
        bucket = "YELLOW"

    if evidence_snapshot.get("changed_from_previous"):
        bucket = "YELLOW"

    if (
        "restriction_phrase_scan" in license_gates or "no_restrictions" in license_gates
    ) and restriction_hits:
        bucket = "YELLOW"
    if (
        "restriction_phrase_scan" in license_gates or "no_restrictions" in license_gates
    ) and evidence_snapshot.get("pdf_text_extraction_failed"):
        bucket = "YELLOW"

    if "manual_legal_review" in license_gates or "manual_review" in license_gates:
        bucket = "YELLOW"

    return bucket


@stable_api
def apply_denylist_bucket(dl_hits: list[dict[str, Any]], eff_bucket: str) -> str:
    for hit in dl_hits:
        severity = hit.get("severity", "hard_red")
        if severity == "hard_red":
            return "RED"
        if severity == "force_yellow":
            eff_bucket = "YELLOW"
    return eff_bucket


@stable_api
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
    if (
        review_status == "approved"
        and promote_to == "GREEN"
        and not restriction_hits
        and eff_bucket != "RED"
    ):
        return "GREEN"
    return eff_bucket


@stable_api
def resolve_effective_bucket(
    license_map: "LicenseMap",
    license_gates: list[str],
    evidence: "EvidenceResult",
    spdx: str,
    restriction_hits: list[str],
    min_confidence: float,
    resolved_confidence: float,
    review_required: bool,
    review_status: str,
    promote_to: str,
    denylist_hits: list[dict[str, Any]],
    strict_snapshot: bool,
) -> str:
    eff_bucket = compute_effective_bucket(
        license_map,
        license_gates,
        spdx,
        restriction_hits,
        evidence.snapshot,
        min_confidence,
        resolved_confidence,
    )
    eff_bucket = apply_denylist_bucket(denylist_hits, eff_bucket)
    evidence_status, _ = normalize_evidence_fetch_status(evidence.snapshot)
    if (
        strict_snapshot
        and "snapshot_terms" in license_gates
        and evidence_status != "ok"
        and eff_bucket == "GREEN"
    ):
        eff_bucket = "YELLOW"
    if evidence.no_fetch_missing_evidence and eff_bucket == "GREEN":
        eff_bucket = "YELLOW"
    return apply_review_gates(
        eff_bucket, review_required, review_status, promote_to, restriction_hits
    )


@stable_api
def apply_yellow_signoff_requirement(
    eff_bucket: str,
    review_status: str,
    review_required: bool,
    require_yellow_signoff: bool,
) -> bool:
    if (
        require_yellow_signoff
        and eff_bucket == "YELLOW"
        and review_status not in {"approved", "rejected"}
    ):
        return True
    return review_required


@stable_api
def resolve_output_pool(profile: str, eff_bucket: str, target: dict[str, Any]) -> str:
    out_pool = (target.get("output", {}) or {}).get("pool")
    if out_pool:
        return out_pool
    if profile == "copyleft":
        return "copyleft"
    if eff_bucket == "GREEN":
        return "permissive"
    return "quarantine"


@stable_api
def summarize_denylist_hits(dl_hits: list[dict[str, Any]]) -> tuple[bool, bool]:
    hard_red = False
    force_yellow = False
    for hit in dl_hits:
        severity = hit.get("severity", "hard_red")
        if severity == "hard_red":
            hard_red = True
        elif severity == "force_yellow":
            force_yellow = True
    return hard_red, force_yellow


@stable_api
def build_bucket_signals(
    *,
    ctx: "TargetContext",
    license_map: "LicenseMap",
    evidence: "EvidenceResult",
    restriction_hits: list[str],
    resolved: str,
    resolved_confidence: float,
    eff_bucket: str,
    review_required: bool,
    review_status: str,
    promote_to: str,
    min_confidence: float,
    require_yellow_signoff: bool,
    action: str,
    action_checks: list[str],
    strict_snapshot: bool,
) -> tuple[str, dict[str, Any]]:
    spdx_bucket_value = spdx_bucket(license_map, resolved)
    low_confidence = resolved_confidence < min_confidence
    snapshot_required = "snapshot_terms" in ctx.license_gates
    evidence_status, fetch_failure_reason = normalize_evidence_fetch_status(
        evidence.snapshot
    )
    snapshot_missing = bool(evidence.no_fetch_missing_evidence) or (
        snapshot_required and evidence_status != "ok"
    )
    evidence_changed = bool(evidence.snapshot.get("changed_from_previous"))
    pdf_text_failed = bool(evidence.snapshot.get("pdf_text_extraction_failed"))
    manual_review_gate = bool(
        "manual_legal_review" in ctx.license_gates or "manual_review" in ctx.license_gates
    )
    restriction_gate = bool(
        "restriction_phrase_scan" in ctx.license_gates or "no_restrictions" in ctx.license_gates
    )
    restriction_present = bool(restriction_hits)
    review_pending = review_status not in {"approved", "rejected"}
    hard_red, force_yellow = summarize_denylist_hits(ctx.dl_hits)

    signals = {
        "spdx": {
            "resolved": resolved,
            "confidence": resolved_confidence,
            "bucket": spdx_bucket_value,
            "min_confidence": min_confidence,
            "low_confidence": low_confidence,
        },
        "evidence": {
            "status": evidence_status,
            "snapshot_required": snapshot_required,
            "snapshot_missing": snapshot_missing,
            "changed_from_previous": evidence_changed,
            "pdf_text_extraction_failed": pdf_text_failed,
            "no_fetch_missing_evidence": evidence.no_fetch_missing_evidence,
            "fetch_failure_reason": fetch_failure_reason,
            "strict_snapshot_failure": bool(
                strict_snapshot and snapshot_required and evidence_status != "ok"
            ),
        },
        "license_gates": list(ctx.license_gates),
        "restriction_hits": restriction_hits,
        "review": {
            "required": review_required,
            "status": review_status,
            "pending": review_pending,
            "promote_to": promote_to,
            "require_yellow_signoff": require_yellow_signoff,
        },
        "denylist": {
            "hits": ctx.dl_hits,
            "hard_red": hard_red,
            "force_yellow": force_yellow,
        },
        "content_check": {"action": action, "checks": action_checks},
    }

    bucket_reason = "policy_default"
    if eff_bucket == "RED":
        if action == "block":
            bucket_reason = "content_check_block"
        elif hard_red:
            bucket_reason = "denylist_hard_red"
        elif review_status == "rejected":
            bucket_reason = "review_rejected"
        elif spdx_bucket_value == "RED":
            bucket_reason = "spdx_deny"
        else:
            bucket_reason = "policy_red"
    elif eff_bucket == "YELLOW":
        if action == "quarantine":
            bucket_reason = "content_check_quarantine"
        elif force_yellow:
            bucket_reason = "denylist_force_yellow"
        elif review_required and review_pending:
            bucket_reason = "review_required"
        elif snapshot_missing:
            bucket_reason = "snapshot_missing"
        elif evidence_changed:
            bucket_reason = "evidence_changed"
        elif restriction_gate and restriction_present:
            bucket_reason = "restriction_hits"
        elif pdf_text_failed:
            bucket_reason = "text_extraction_failed"
        elif manual_review_gate:
            bucket_reason = "manual_review_gate"
        elif low_confidence:
            bucket_reason = "low_confidence"
        elif spdx_bucket_value == "YELLOW":
            bucket_reason = (
                "unknown_spdx"
                if str(resolved).strip().upper() in {"", "UNKNOWN"}
                else "conditional_spdx"
            )
        else:
            bucket_reason = "policy_yellow"
    elif eff_bucket == "GREEN":
        if review_status == "approved" and promote_to == "GREEN":
            bucket_reason = "review_approved"
        elif spdx_bucket_value == "GREEN":
            bucket_reason = "spdx_allow"
        else:
            bucket_reason = "policy_green"

    return bucket_reason, signals
