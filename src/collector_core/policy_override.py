"""
Policy Override - Scoped allow/override mechanism with required rationale.

Issue 4.4 (v3.0): Overrides are:
- Target-scoped
- Require justification + link
- Recorded in decision bundle
"""

from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from enum import Enum
from pathlib import Path
from typing import Any

from collector_core.stability import stable_api
from collector_core.utils.io import read_jsonl, append_jsonl
from collector_core.utils.logging import utc_now
from collector_core.utils.paths import ensure_dir


class OverrideType(str, Enum):
    """Types of policy overrides."""

    DENYLIST_EXCEPTION = "denylist_exception"  # Exception to a denylist rule
    LICENSE_EXCEPTION = "license_exception"  # Exception to license requirement
    CONTENT_CHECK_EXCEPTION = "content_check_exception"  # Exception to content check
    FORCE_GREEN = "force_green"  # Force GREEN classification
    FORCE_YELLOW = "force_yellow"  # Force YELLOW classification


@stable_api
@dataclass
class PolicyOverride:
    """
    A scoped policy override for a specific target.

    Overrides allow exceptions to standard policy rules when there is
    a documented justification and reference link.
    """

    override_id: str  # Unique identifier
    target_id: str  # Target this override applies to
    override_type: OverrideType
    rule_pattern: str | None  # Pattern of rule being overridden (e.g., "denylist.domain.*")

    # Required documentation
    justification: str  # Why this override is necessary
    reference_link: str  # Link to issue/discussion/approval
    approved_by: str  # Who approved this override

    # Audit trail
    created_at_utc: str
    expires_at_utc: str | None = None  # Optional expiration
    revoked: bool = False
    revoked_at_utc: str | None = None
    revoked_by: str | None = None
    revoked_reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["override_type"] = self.override_type.value
        return result

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PolicyOverride":
        return cls(
            override_id=data["override_id"],
            target_id=data["target_id"],
            override_type=OverrideType(data["override_type"]),
            rule_pattern=data.get("rule_pattern"),
            justification=data["justification"],
            reference_link=data["reference_link"],
            approved_by=data["approved_by"],
            created_at_utc=data["created_at_utc"],
            expires_at_utc=data.get("expires_at_utc"),
            revoked=data.get("revoked", False),
            revoked_at_utc=data.get("revoked_at_utc"),
            revoked_by=data.get("revoked_by"),
            revoked_reason=data.get("revoked_reason"),
        )

    def is_active(self, as_of_utc: str | None = None) -> bool:
        """Check if the override is currently active."""
        if self.revoked:
            return False
        if self.expires_at_utc:
            check_time = as_of_utc or utc_now()
            if check_time > self.expires_at_utc:
                return False
        return True

    def matches_rule(self, rule_id: str) -> bool:
        """Check if this override applies to a given rule ID."""
        if not self.rule_pattern:
            return True  # No pattern means matches all rules of this type
        if self.rule_pattern.endswith("*"):
            prefix = self.rule_pattern[:-1]
            return rule_id.startswith(prefix)
        return rule_id == self.rule_pattern


@stable_api
@dataclass
class OverrideRegistry:
    """Registry of policy overrides for a pipeline."""

    overrides: list[PolicyOverride]
    _by_target: dict[str, list[PolicyOverride]] | None = None

    def _index_by_target(self) -> None:
        if self._by_target is None:
            self._by_target = {}
            for override in self.overrides:
                if override.target_id not in self._by_target:
                    self._by_target[override.target_id] = []
                self._by_target[override.target_id].append(override)

    def get_overrides_for_target(self, target_id: str) -> list[PolicyOverride]:
        """Get all active overrides for a target."""
        self._index_by_target()
        target_overrides = self._by_target.get(target_id, [])
        return [o for o in target_overrides if o.is_active()]

    def find_override_for_rule(
        self,
        target_id: str,
        rule_id: str,
        override_type: OverrideType | None = None,
    ) -> PolicyOverride | None:
        """Find an override that applies to a specific rule."""
        for override in self.get_overrides_for_target(target_id):
            if override_type and override.override_type != override_type:
                continue
            if override.matches_rule(rule_id):
                return override
        return None

    def add_override(self, override: PolicyOverride) -> None:
        """Add an override to the registry."""
        self.overrides.append(override)
        self._by_target = None  # Invalidate index

    def revoke_override(
        self,
        override_id: str,
        revoked_by: str,
        reason: str,
    ) -> bool:
        """Revoke an override by ID."""
        for override in self.overrides:
            if override.override_id == override_id and not override.revoked:
                override.revoked = True
                override.revoked_at_utc = utc_now()
                override.revoked_by = revoked_by
                override.revoked_reason = reason
                return True
        return False


@stable_api
def create_override(
    target_id: str,
    override_type: OverrideType,
    justification: str,
    reference_link: str,
    approved_by: str,
    rule_pattern: str | None = None,
    expires_at_utc: str | None = None,
) -> PolicyOverride:
    """Create a new policy override with required documentation."""
    if not justification or len(justification) < 10:
        raise ValueError("Justification must be at least 10 characters")
    if not reference_link or not reference_link.startswith(("http://", "https://")):
        raise ValueError("Reference link must be a valid URL")
    if not approved_by:
        raise ValueError("Approved by must be specified")

    import hashlib

    override_id = hashlib.sha256(
        f"{target_id}:{override_type.value}:{rule_pattern}:{utc_now()}".encode()
    ).hexdigest()[:16]

    return PolicyOverride(
        override_id=override_id,
        target_id=target_id,
        override_type=override_type,
        rule_pattern=rule_pattern,
        justification=justification,
        reference_link=reference_link,
        approved_by=approved_by,
        created_at_utc=utc_now(),
        expires_at_utc=expires_at_utc,
    )


@stable_api
def load_override_registry(path: Path) -> OverrideRegistry:
    """Load override registry from a JSONL file."""
    if not path.exists():
        return OverrideRegistry(overrides=[])

    overrides = []
    for row in read_jsonl(path):
        try:
            overrides.append(PolicyOverride.from_dict(row))
        except (KeyError, ValueError):
            continue  # Skip malformed entries

    return OverrideRegistry(overrides=overrides)


@stable_api
def save_override_registry(registry: OverrideRegistry, path: Path) -> None:
    """Save override registry to a JSONL file."""
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8") as f:
        for override in registry.overrides:
            f.write(json.dumps(override.to_dict()) + "\n")


@stable_api
def apply_override_to_decision(
    target_id: str,
    current_decision: str,
    rule_id: str,
    registry: OverrideRegistry,
) -> tuple[str, PolicyOverride | None]:
    """
    Apply any matching override to a routing decision.

    Args:
        target_id: Target identifier
        current_decision: Current decision (GREEN, YELLOW, RED)
        rule_id: Rule that produced the current decision
        registry: Override registry to check

    Returns:
        Tuple of (new_decision, applied_override)
    """
    # Check for force overrides first
    for override_type in [OverrideType.FORCE_GREEN, OverrideType.FORCE_YELLOW]:
        override = registry.find_override_for_rule(target_id, rule_id, override_type)
        if override:
            new_decision = "GREEN" if override_type == OverrideType.FORCE_GREEN else "YELLOW"
            return new_decision, override

    # Check for exception overrides (e.g., denylist exception)
    exception_types = [
        OverrideType.DENYLIST_EXCEPTION,
        OverrideType.LICENSE_EXCEPTION,
        OverrideType.CONTENT_CHECK_EXCEPTION,
    ]

    for exception_type in exception_types:
        override = registry.find_override_for_rule(target_id, rule_id, exception_type)
        if override:
            # Exception overrides upgrade RED to YELLOW (for review) not GREEN
            if current_decision == "RED":
                return "YELLOW", override
            # YELLOW stays YELLOW with override recorded
            return current_decision, override

    return current_decision, None


@stable_api
def record_override_usage(
    override: PolicyOverride,
    target_id: str,
    original_decision: str,
    new_decision: str,
    ledger_root: Path,
) -> None:
    """Record usage of an override to the audit ledger."""
    ensure_dir(ledger_root)
    usage_entry = {
        "override_id": override.override_id,
        "target_id": target_id,
        "original_decision": original_decision,
        "new_decision": new_decision,
        "override_type": override.override_type.value,
        "rule_pattern": override.rule_pattern,
        "justification": override.justification,
        "reference_link": override.reference_link,
        "approved_by": override.approved_by,
        "used_at_utc": utc_now(),
    }
    append_jsonl(ledger_root / "override_usage.jsonl", [usage_entry])


__all__ = [
    "OverrideRegistry",
    "OverrideType",
    "PolicyOverride",
    "apply_override_to_decision",
    "create_override",
    "load_override_registry",
    "record_override_usage",
    "save_override_registry",
]
