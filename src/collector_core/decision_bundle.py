"""
Decision Bundle - Audit trail for routing decisions.

Issue 4.2 (v3.0): Every routing decision (GREEN/YELLOW/RED) stores:
- Rule IDs that fired
- Evidence URLs + hash + timestamp
- Denylist matches + restriction phrase matches

This enables reviewers to answer "why was this target red/yellow?" from artifacts alone.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

from collector_core.__version__ import __version__ as VERSION
from collector_core.stability import stable_api
from collector_core.utils.logging import utc_now


RoutingDecision = Literal["GREEN", "YELLOW", "RED", "UNKNOWN"]


@stable_api
@dataclass
class RuleFired:
    """Record of a rule that influenced a routing decision."""

    rule_id: str  # Unique identifier (e.g., "denylist.domain.sci-hub", "license.deny.CC-BY-NC")
    rule_type: str  # Type of rule (denylist, license, evidence, content_check)
    severity: str  # Severity level (hard_red, force_yellow, warn)
    field: str | None  # Field that triggered the rule
    pattern: str | None  # Pattern that matched
    reason: str  # Human-readable explanation
    link: str | None = None  # Reference link/provenance

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@stable_api
@dataclass
class EvidenceSnapshot:
    """Snapshot of license evidence at decision time."""

    url: str | None
    fetched_at_utc: str | None
    raw_sha256: str | None
    normalized_sha256: str | None
    status: str  # ok, error, blocked_url, skipped, etc.
    error: str | None = None
    text_excerpt: str | None = None  # First N chars of evidence text

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@stable_api
@dataclass
class DecisionBundle:
    """
    Complete audit trail for a target routing decision.

    Issue 4.2: Every routing decision stores enough information for a reviewer
    to understand why a target was classified as GREEN/YELLOW/RED.
    """

    target_id: str
    decision: RoutingDecision
    decided_at_utc: str
    decided_by: str  # "pipeline_driver" or specific stage name

    # Rule audit trail
    rules_fired: list[RuleFired] = field(default_factory=list)
    primary_rule: str | None = None  # Most significant rule that drove the decision

    # Evidence audit trail
    evidence_snapshot: EvidenceSnapshot | None = None
    evidence_changed_since_signoff: bool = False

    # Denylist audit trail
    denylist_matches: list[dict[str, Any]] = field(default_factory=list)

    # Content check results
    content_checks: dict[str, Any] = field(default_factory=dict)

    # Metadata
    collector_version: str = VERSION
    bundle_schema_version: str = "1.0.0"
    signoff_status: str | None = None
    signoff_by: str | None = None
    signoff_at_utc: str | None = None

    # Override information (Issue 4.4)
    override_applied: bool = False
    override_rule_id: str | None = None
    override_justification: str | None = None
    override_link: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        data = {
            "target_id": self.target_id,
            "decision": self.decision,
            "decided_at_utc": self.decided_at_utc,
            "decided_by": self.decided_by,
            "primary_rule": self.primary_rule,
            "evidence_changed_since_signoff": self.evidence_changed_since_signoff,
            "collector_version": self.collector_version,
            "bundle_schema_version": self.bundle_schema_version,
        }

        if self.rules_fired:
            data["rules_fired"] = [r.to_dict() for r in self.rules_fired]

        if self.evidence_snapshot:
            data["evidence_snapshot"] = self.evidence_snapshot.to_dict()

        if self.denylist_matches:
            data["denylist_matches"] = self.denylist_matches

        if self.content_checks:
            data["content_checks"] = self.content_checks

        if self.signoff_status:
            data["signoff"] = {
                "status": self.signoff_status,
                "by": self.signoff_by,
                "at_utc": self.signoff_at_utc,
            }

        if self.override_applied:
            data["override"] = {
                "applied": True,
                "rule_id": self.override_rule_id,
                "justification": self.override_justification,
                "link": self.override_link,
            }

        return data

    def add_rule(self, rule: RuleFired) -> None:
        """Add a rule that fired during evaluation."""
        self.rules_fired.append(rule)
        # Update primary rule if this is more severe
        severity_order = {"hard_red": 0, "force_yellow": 1, "warn": 2}
        if self.primary_rule is None:
            self.primary_rule = rule.rule_id
        else:
            current_severity = next(
                (r.severity for r in self.rules_fired if r.rule_id == self.primary_rule),
                "warn",
            )
            if severity_order.get(rule.severity, 3) < severity_order.get(current_severity, 3):
                self.primary_rule = rule.rule_id

    def set_evidence(
        self,
        url: str | None,
        fetched_at: str | None,
        raw_sha: str | None,
        normalized_sha: str | None,
        status: str,
        error: str | None = None,
        text_excerpt: str | None = None,
    ) -> None:
        """Set the evidence snapshot."""
        self.evidence_snapshot = EvidenceSnapshot(
            url=url,
            fetched_at_utc=fetched_at,
            raw_sha256=raw_sha,
            normalized_sha256=normalized_sha,
            status=status,
            error=error,
            text_excerpt=text_excerpt[:500] if text_excerpt else None,
        )

    def get_explanation(self) -> str:
        """Generate a human-readable explanation of the decision."""
        lines = [f"Target: {self.target_id}", f"Decision: {self.decision}"]

        if self.primary_rule:
            primary = next((r for r in self.rules_fired if r.rule_id == self.primary_rule), None)
            if primary:
                lines.append(f"Primary Rule: {primary.rule_id}")
                lines.append(f"  Reason: {primary.reason}")
                if primary.link:
                    lines.append(f"  Reference: {primary.link}")

        if self.denylist_matches:
            lines.append(f"Denylist Matches: {len(self.denylist_matches)}")
            for match in self.denylist_matches[:3]:  # Show first 3
                lines.append(f"  - {match.get('pattern')}: {match.get('reason')}")

        if self.evidence_changed_since_signoff:
            lines.append("WARNING: Evidence has changed since last signoff!")

        if self.override_applied:
            lines.append(f"Override Applied: {self.override_rule_id}")
            if self.override_justification:
                lines.append(f"  Justification: {self.override_justification}")

        return "\n".join(lines)


@stable_api
def create_decision_bundle(
    target_id: str,
    decision: RoutingDecision,
    decided_by: str = "pipeline_driver",
) -> DecisionBundle:
    """Create a new decision bundle for a target."""
    return DecisionBundle(
        target_id=target_id,
        decision=decision,
        decided_at_utc=utc_now(),
        decided_by=decided_by,
    )


@stable_api
def save_decision_bundle(bundle: DecisionBundle, output_dir: Path) -> Path:
    """Save a decision bundle to the output directory."""
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"decision_bundle_{bundle.target_id}.json"
    output_path.write_text(json.dumps(bundle.to_dict(), indent=2))
    return output_path


@stable_api
def load_decision_bundle(path: Path) -> DecisionBundle | None:
    """Load a decision bundle from a file."""
    if not path.exists():
        return None
    # P1.2H: Handle file read and JSON decode errors
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        bundle = DecisionBundle(
            target_id=data["target_id"],
            decision=data["decision"],
            decided_at_utc=data["decided_at_utc"],
            decided_by=data["decided_by"],
            primary_rule=data.get("primary_rule"),
            evidence_changed_since_signoff=data.get("evidence_changed_since_signoff", False),
            collector_version=data.get("collector_version", VERSION),
            bundle_schema_version=data.get("bundle_schema_version", "1.0.0"),
        )

        # Load rules
        for rule_data in data.get("rules_fired", []):
            bundle.rules_fired.append(
                RuleFired(
                    rule_id=rule_data["rule_id"],
                    rule_type=rule_data["rule_type"],
                    severity=rule_data["severity"],
                    field=rule_data.get("field"),
                    pattern=rule_data.get("pattern"),
                    reason=rule_data["reason"],
                    link=rule_data.get("link"),
                )
            )

        # Load evidence snapshot
        ev = data.get("evidence_snapshot")
        if ev:
            bundle.evidence_snapshot = EvidenceSnapshot(
                url=ev.get("url"),
                fetched_at_utc=ev.get("fetched_at_utc"),
                raw_sha256=ev.get("raw_sha256"),
                normalized_sha256=ev.get("normalized_sha256"),
                status=ev.get("status", "unknown"),
                error=ev.get("error"),
                text_excerpt=ev.get("text_excerpt"),
            )

        # Load denylist matches
        bundle.denylist_matches = data.get("denylist_matches", [])

        # Load content checks
        bundle.content_checks = data.get("content_checks", {})

        # Load signoff info
        signoff = data.get("signoff", {})
        if signoff:
            bundle.signoff_status = signoff.get("status")
            bundle.signoff_by = signoff.get("by")
            bundle.signoff_at_utc = signoff.get("at_utc")

        # Load override info
        override = data.get("override", {})
        if override.get("applied"):
            bundle.override_applied = True
            bundle.override_rule_id = override.get("rule_id")
            bundle.override_justification = override.get("justification")
            bundle.override_link = override.get("link")

        return bundle
    except (json.JSONDecodeError, KeyError, OSError, TypeError):
        # P1.2H: Handle file read, JSON decode, and key errors
        return None


@stable_api
def bundle_from_denylist_hits(
    target_id: str,
    hits: list[dict[str, Any]],
) -> DecisionBundle:
    """Create a decision bundle from denylist hits."""
    decision: RoutingDecision = "GREEN"

    for hit in hits:
        severity = hit.get("severity", "hard_red")
        if severity == "hard_red":
            decision = "RED"
            break
        elif severity == "force_yellow" and decision != "RED":
            decision = "YELLOW"

    bundle = create_decision_bundle(target_id, decision, decided_by="denylist_matcher")
    bundle.denylist_matches = hits

    for hit in hits:
        bundle.add_rule(
            RuleFired(
                rule_id=f"denylist.{hit.get('type', 'substring')}.{hit.get('pattern', 'unknown')}",
                rule_type="denylist",
                severity=hit.get("severity", "hard_red"),
                field=hit.get("field"),
                pattern=hit.get("pattern"),
                reason=hit.get("reason") or hit.get("rationale", "Denylist match"),
                link=hit.get("link"),
            )
        )

    return bundle


__all__ = [
    "DecisionBundle",
    "EvidenceSnapshot",
    "RuleFired",
    "RoutingDecision",
    "bundle_from_denylist_hits",
    "create_decision_bundle",
    "load_decision_bundle",
    "save_decision_bundle",
]
