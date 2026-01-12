"""
Evidence Policy - License evidence change detection and re-review policy.

Issue 4.3 (v3.0): When evidence hash changes:
- Target is moved to re-review queue
- Merge blocks until re-approved (or equivalent conservative policy)
- Behavior is documented + tested
"""

from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from enum import Enum
from pathlib import Path
from typing import Any

from collector_core.evidence.change_detection import (
    compute_signoff_mismatches,
    normalize_evidence_change_policy,
    normalize_cosmetic_change_policy,
    resolve_evidence_change,
)
from collector_core.stability import stable_api
from collector_core.utils.io import append_jsonl
from collector_core.utils.logging import utc_now
from collector_core.utils.paths import ensure_dir


class EvidenceChangeAction(str, Enum):
    """Actions to take when evidence changes."""

    NONE = "none"  # No change detected
    COSMETIC_WARN = "cosmetic_warn"  # Cosmetic change, warn only
    DEMOTE_TO_YELLOW = "demote_to_yellow"  # Demote GREEN to YELLOW for re-review
    BLOCK_MERGE = "block_merge"  # Block merge until re-approved
    RE_REVIEW_REQUIRED = "re_review_required"  # Requires new manual review


@stable_api
@dataclass
class EvidenceChangeResult:
    """Result of evidence change detection."""

    target_id: str
    action: EvidenceChangeAction
    raw_changed: bool
    normalized_changed: bool
    cosmetic_change: bool

    signoff_raw_sha: str | None
    signoff_normalized_sha: str | None
    current_raw_sha: str | None
    current_normalized_sha: str | None

    detected_at_utc: str
    message: str

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["action"] = self.action.value
        return result

    @property
    def requires_action(self) -> bool:
        return self.action not in (EvidenceChangeAction.NONE, EvidenceChangeAction.COSMETIC_WARN)


@stable_api
@dataclass
class EvidencePolicyConfig:
    """Configuration for evidence change policy.

    Attributes:
        evidence_policy: How to compare evidence ("raw", "normalized", "either")
        cosmetic_policy: How to handle cosmetic changes ("warn_only", "treat_as_changed")
        demote_on_change: Whether to demote GREEN to YELLOW when evidence changes
        block_merge_on_change: Whether to block merge when evidence changes
        re_review_grace_days: Days before requiring re-review after change
    """

    evidence_policy: str = "normalized"
    cosmetic_policy: str = "warn_only"
    demote_on_change: bool = True
    block_merge_on_change: bool = True
    re_review_grace_days: int = 0

    @classmethod
    def from_config(cls, cfg: dict[str, Any]) -> "EvidencePolicyConfig":
        """Create from pipeline/targets configuration."""
        policy_cfg = cfg.get("globals", {}).get("evidence_policy", {})
        return cls(
            evidence_policy=normalize_evidence_change_policy(
                policy_cfg.get("comparison", "normalized")
            ),
            cosmetic_policy=normalize_cosmetic_change_policy(
                policy_cfg.get("cosmetic_handling", "warn_only")
            ),
            demote_on_change=bool(policy_cfg.get("demote_on_change", True)),
            block_merge_on_change=bool(policy_cfg.get("block_merge_on_change", True)),
            re_review_grace_days=int(policy_cfg.get("re_review_grace_days", 0)),
        )


@stable_api
def detect_evidence_change(
    target_id: str,
    signoff: dict[str, Any] | None,
    current_evidence: dict[str, Any] | None,
    policy: EvidencePolicyConfig,
) -> EvidenceChangeResult:
    """
    Detect if license evidence has changed since signoff.

    Args:
        target_id: Target identifier
        signoff: Signoff record with evidence hashes
        current_evidence: Current evidence snapshot with hashes
        policy: Evidence policy configuration

    Returns:
        EvidenceChangeResult with action to take
    """
    now = utc_now()

    # If no signoff exists, no change can be detected
    if not signoff:
        return EvidenceChangeResult(
            target_id=target_id,
            action=EvidenceChangeAction.NONE,
            raw_changed=False,
            normalized_changed=False,
            cosmetic_change=False,
            signoff_raw_sha=None,
            signoff_normalized_sha=None,
            current_raw_sha=current_evidence.get("raw_sha256") if current_evidence else None,
            current_normalized_sha=current_evidence.get("normalized_sha256")
            if current_evidence
            else None,
            detected_at_utc=now,
            message="No signoff exists, cannot detect change",
        )

    # Extract hashes from signoff
    signoff_evidence = signoff.get("evidence", {}) or {}
    signoff_raw = signoff_evidence.get("raw_sha256") or signoff.get("evidence_raw_sha256")
    signoff_norm = signoff_evidence.get("normalized_sha256") or signoff.get(
        "evidence_normalized_sha256"
    )

    # Extract current hashes
    current_raw = current_evidence.get("raw_sha256") if current_evidence else None
    current_norm = current_evidence.get("normalized_sha256") if current_evidence else None
    text_extraction_failed = bool(
        current_evidence.get("text_extraction_failed") if current_evidence else False
    )

    # Compute mismatches
    raw_mismatch, normalized_mismatch, cosmetic_change = compute_signoff_mismatches(
        signoff_raw_sha=signoff_raw,
        signoff_normalized_sha=signoff_norm,
        current_raw_sha=current_raw,
        current_normalized_sha=current_norm,
        text_extraction_failed=text_extraction_failed,
    )

    # Determine if evidence has changed based on policy
    evidence_changed = resolve_evidence_change(
        raw_changed=raw_mismatch,
        normalized_changed=normalized_mismatch,
        cosmetic_change=cosmetic_change,
        evidence_policy=policy.evidence_policy,
        cosmetic_policy=policy.cosmetic_policy,
    )

    # Determine action
    action = EvidenceChangeAction.NONE
    message = "No evidence change detected"

    if evidence_changed:
        if policy.block_merge_on_change:
            action = EvidenceChangeAction.BLOCK_MERGE
            message = "Evidence changed - merge blocked until re-approved"
        elif policy.demote_on_change:
            action = EvidenceChangeAction.DEMOTE_TO_YELLOW
            message = "Evidence changed - demoting to YELLOW for re-review"
        else:
            action = EvidenceChangeAction.RE_REVIEW_REQUIRED
            message = "Evidence changed - re-review required"
    elif cosmetic_change:
        action = EvidenceChangeAction.COSMETIC_WARN
        message = "Cosmetic evidence change detected (whitespace/formatting only)"

    return EvidenceChangeResult(
        target_id=target_id,
        action=action,
        raw_changed=raw_mismatch,
        normalized_changed=normalized_mismatch,
        cosmetic_change=cosmetic_change,
        signoff_raw_sha=signoff_raw,
        signoff_normalized_sha=signoff_norm,
        current_raw_sha=current_raw,
        current_normalized_sha=current_norm,
        detected_at_utc=now,
        message=message,
    )


@stable_api
def record_evidence_change(
    result: EvidenceChangeResult,
    ledger_root: Path,
    re_review_queue_path: Path | None = None,
) -> None:
    """
    Record an evidence change to the ledger and optionally add to re-review queue.

    Args:
        result: Evidence change detection result
        ledger_root: Path to ledger directory
        re_review_queue_path: Path to re-review queue (if applicable)
    """
    ensure_dir(ledger_root)

    # Record to evidence change ledger
    ledger_entry = {
        "target_id": result.target_id,
        "action": result.action.value,
        "raw_changed": result.raw_changed,
        "normalized_changed": result.normalized_changed,
        "cosmetic_change": result.cosmetic_change,
        "detected_at_utc": result.detected_at_utc,
        "message": result.message,
    }
    append_jsonl(ledger_root / "evidence_changes.jsonl", [ledger_entry])

    # Add to re-review queue if action requires it
    if result.requires_action and re_review_queue_path:
        ensure_dir(re_review_queue_path.parent)
        queue_entry = {
            "id": result.target_id,
            "reason": result.action.value,
            "evidence_raw_changed": result.raw_changed,
            "evidence_normalized_changed": result.normalized_changed,
            "added_at_utc": result.detected_at_utc,
            "bucket": "yellow",  # Demoted to yellow for re-review
            "enabled": True,
        }
        append_jsonl(re_review_queue_path, [queue_entry])


@stable_api
def check_merge_eligibility(
    target_id: str,
    signoff: dict[str, Any] | None,
    current_evidence: dict[str, Any] | None,
    policy: EvidencePolicyConfig,
) -> tuple[bool, str]:
    """
    Check if a target is eligible for merge based on evidence policy.

    Args:
        target_id: Target identifier
        signoff: Signoff record with evidence hashes
        current_evidence: Current evidence snapshot with hashes
        policy: Evidence policy configuration

    Returns:
        Tuple of (eligible, reason)
    """
    result = detect_evidence_change(target_id, signoff, current_evidence, policy)

    if result.action == EvidenceChangeAction.BLOCK_MERGE:
        return False, result.message

    if result.action == EvidenceChangeAction.RE_REVIEW_REQUIRED:
        return False, result.message

    return True, "Eligible for merge"


__all__ = [
    "EvidenceChangeAction",
    "EvidenceChangeResult",
    "EvidencePolicyConfig",
    "check_merge_eligibility",
    "detect_evidence_change",
    "record_evidence_change",
]
