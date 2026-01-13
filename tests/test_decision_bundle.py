"""Tests for collector_core/decision_bundle.py.

P3.1D: Tests for decision bundle functionality including:
- to_dict() serialization
- from_dict() deserialization with missing fields
- Nested data structures
- Rule severity handling
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from collector_core.decision_bundle import (
    DecisionBundle,
    EvidenceSnapshot,
    RuleFired,
    create_decision_bundle,
    save_decision_bundle,
    load_decision_bundle,
    bundle_from_denylist_hits,
)


class TestRuleFired:
    """Tests for RuleFired dataclass."""

    def test_to_dict(self):
        """Should serialize to dict."""
        rule = RuleFired(
            rule_id="denylist.domain.test",
            rule_type="denylist",
            severity="hard_red",
            field="license_evidence_url",
            pattern="example.com",
            reason="Test reason",
            link="https://docs.example.com/policy",
        )
        result = rule.to_dict()
        assert result["rule_id"] == "denylist.domain.test"
        assert result["rule_type"] == "denylist"
        assert result["severity"] == "hard_red"
        assert result["field"] == "license_evidence_url"
        assert result["pattern"] == "example.com"
        assert result["reason"] == "Test reason"
        assert result["link"] == "https://docs.example.com/policy"

    def test_to_dict_with_none_fields(self):
        """Should handle None optional fields."""
        rule = RuleFired(
            rule_id="test",
            rule_type="license",
            severity="warn",
            field=None,
            pattern=None,
            reason="Test",
        )
        result = rule.to_dict()
        assert result["field"] is None
        assert result["pattern"] is None
        assert result["link"] is None


class TestEvidenceSnapshot:
    """Tests for EvidenceSnapshot dataclass."""

    def test_to_dict(self):
        """Should serialize to dict."""
        snapshot = EvidenceSnapshot(
            url="https://example.com/license.txt",
            fetched_at_utc="2024-01-15T10:00:00Z",
            raw_sha256="abc123",
            normalized_sha256="def456",
            status="ok",
            error=None,
            text_excerpt="MIT License...",
        )
        result = snapshot.to_dict()
        assert result["url"] == "https://example.com/license.txt"
        assert result["fetched_at_utc"] == "2024-01-15T10:00:00Z"
        assert result["status"] == "ok"
        assert result["text_excerpt"] == "MIT License..."

    def test_to_dict_with_error(self):
        """Should include error when present."""
        snapshot = EvidenceSnapshot(
            url="https://example.com/404",
            fetched_at_utc=None,
            raw_sha256=None,
            normalized_sha256=None,
            status="error",
            error="HTTP 404",
        )
        result = snapshot.to_dict()
        assert result["status"] == "error"
        assert result["error"] == "HTTP 404"


class TestDecisionBundle:
    """Tests for DecisionBundle dataclass."""

    def test_to_dict_minimal(self):
        """Should serialize minimal bundle."""
        bundle = DecisionBundle(
            target_id="test-target",
            decision="GREEN",
            decided_at_utc="2024-01-15T10:00:00Z",
            decided_by="pipeline_driver",
        )
        result = bundle.to_dict()
        assert result["target_id"] == "test-target"
        assert result["decision"] == "GREEN"
        assert result["decided_at_utc"] == "2024-01-15T10:00:00Z"
        assert result["decided_by"] == "pipeline_driver"
        assert "rules_fired" not in result  # Empty list not included
        assert "evidence_snapshot" not in result  # None not included

    def test_to_dict_with_rules(self):
        """Should include rules when present."""
        bundle = DecisionBundle(
            target_id="test-target",
            decision="RED",
            decided_at_utc="2024-01-15T10:00:00Z",
            decided_by="pipeline_driver",
        )
        bundle.add_rule(RuleFired(
            rule_id="test.rule",
            rule_type="denylist",
            severity="hard_red",
            field="id",
            pattern="test",
            reason="Test reason",
        ))
        result = bundle.to_dict()
        assert "rules_fired" in result
        assert len(result["rules_fired"]) == 1
        assert result["rules_fired"][0]["rule_id"] == "test.rule"

    def test_to_dict_with_evidence(self):
        """Should include evidence when set."""
        bundle = DecisionBundle(
            target_id="test",
            decision="GREEN",
            decided_at_utc="2024-01-15T10:00:00Z",
            decided_by="pipeline_driver",
        )
        bundle.set_evidence(
            url="https://example.com/license",
            fetched_at="2024-01-15T10:00:00Z",
            raw_sha="abc",
            normalized_sha="def",
            status="ok",
        )
        result = bundle.to_dict()
        assert "evidence_snapshot" in result
        assert result["evidence_snapshot"]["url"] == "https://example.com/license"

    def test_to_dict_with_signoff(self):
        """Should include signoff when present."""
        bundle = DecisionBundle(
            target_id="test",
            decision="GREEN",
            decided_at_utc="2024-01-15T10:00:00Z",
            decided_by="pipeline_driver",
            signoff_status="approved",
            signoff_by="reviewer@example.com",
            signoff_at_utc="2024-01-15T11:00:00Z",
        )
        result = bundle.to_dict()
        assert "signoff" in result
        assert result["signoff"]["status"] == "approved"
        assert result["signoff"]["by"] == "reviewer@example.com"

    def test_to_dict_with_override(self):
        """Should include override when applied."""
        bundle = DecisionBundle(
            target_id="test",
            decision="GREEN",
            decided_at_utc="2024-01-15T10:00:00Z",
            decided_by="pipeline_driver",
            override_applied=True,
            override_rule_id="policy.exception.test",
            override_justification="Approved exception",
            override_link="https://policy.example.com/exception/123",
        )
        result = bundle.to_dict()
        assert "override" in result
        assert result["override"]["applied"] is True
        assert result["override"]["rule_id"] == "policy.exception.test"

    def test_add_rule_sets_primary(self):
        """First rule should become primary rule."""
        bundle = create_decision_bundle("test", "RED")
        bundle.add_rule(RuleFired(
            rule_id="first.rule",
            rule_type="denylist",
            severity="hard_red",
            field=None,
            pattern=None,
            reason="First",
        ))
        assert bundle.primary_rule == "first.rule"

    def test_add_rule_updates_primary_by_severity(self):
        """More severe rule should become primary."""
        bundle = create_decision_bundle("test", "RED")
        bundle.add_rule(RuleFired(
            rule_id="warn.rule",
            rule_type="license",
            severity="warn",
            field=None,
            pattern=None,
            reason="Warning",
        ))
        bundle.add_rule(RuleFired(
            rule_id="red.rule",
            rule_type="denylist",
            severity="hard_red",
            field=None,
            pattern=None,
            reason="Hard red",
        ))
        assert bundle.primary_rule == "red.rule"

    def test_set_evidence_truncates_excerpt(self):
        """Should truncate long text excerpts."""
        bundle = create_decision_bundle("test", "GREEN")
        long_text = "A" * 1000
        bundle.set_evidence(
            url="https://example.com",
            fetched_at="2024-01-15T10:00:00Z",
            raw_sha="abc",
            normalized_sha="def",
            status="ok",
            text_excerpt=long_text,
        )
        assert len(bundle.evidence_snapshot.text_excerpt) == 500

    def test_get_explanation(self):
        """Should generate human-readable explanation."""
        bundle = create_decision_bundle("my-target", "RED")
        bundle.add_rule(RuleFired(
            rule_id="denylist.domain.piracy",
            rule_type="denylist",
            severity="hard_red",
            field="url",
            pattern="piracy.com",
            reason="Known piracy site",
            link="https://policy.example.com/denylist",
        ))
        explanation = bundle.get_explanation()
        assert "my-target" in explanation
        assert "RED" in explanation
        assert "denylist.domain.piracy" in explanation
        assert "Known piracy site" in explanation

    def test_get_explanation_with_evidence_change(self):
        """Should warn about evidence changes."""
        bundle = create_decision_bundle("test", "YELLOW")
        bundle.evidence_changed_since_signoff = True
        explanation = bundle.get_explanation()
        assert "changed since last signoff" in explanation


class TestCreateDecisionBundle:
    """Tests for create_decision_bundle function."""

    def test_creates_bundle(self):
        """Should create bundle with timestamp."""
        bundle = create_decision_bundle("test-id", "GREEN")
        assert bundle.target_id == "test-id"
        assert bundle.decision == "GREEN"
        assert bundle.decided_by == "pipeline_driver"
        assert bundle.decided_at_utc is not None

    def test_custom_decided_by(self):
        """Should allow custom decided_by."""
        bundle = create_decision_bundle("test", "RED", decided_by="custom_stage")
        assert bundle.decided_by == "custom_stage"


class TestSaveLoadDecisionBundle:
    """Tests for save and load functions."""

    def test_save_and_load(self, tmp_path):
        """Should save and load bundle correctly."""
        bundle = create_decision_bundle("test-target", "GREEN")
        bundle.add_rule(RuleFired(
            rule_id="test.rule",
            rule_type="license",
            severity="warn",
            field="license",
            pattern="CC-BY-NC",
            reason="Non-commercial license",
        ))
        bundle.set_evidence(
            url="https://example.com/license.txt",
            fetched_at="2024-01-15T10:00:00Z",
            raw_sha="abc123",
            normalized_sha="def456",
            status="ok",
        )

        output_path = save_decision_bundle(bundle, tmp_path)
        assert output_path.exists()

        loaded = load_decision_bundle(output_path)
        assert loaded is not None
        assert loaded.target_id == "test-target"
        assert loaded.decision == "GREEN"
        assert len(loaded.rules_fired) == 1
        assert loaded.rules_fired[0].rule_id == "test.rule"
        assert loaded.evidence_snapshot is not None
        assert loaded.evidence_snapshot.url == "https://example.com/license.txt"

    def test_load_nonexistent(self, tmp_path):
        """Should return None for nonexistent file."""
        result = load_decision_bundle(tmp_path / "nonexistent.json")
        assert result is None

    def test_load_invalid_json(self, tmp_path):
        """Should return None for invalid JSON."""
        bad_file = tmp_path / "bad.json"
        bad_file.write_text("not valid json{")
        result = load_decision_bundle(bad_file)
        assert result is None

    def test_load_missing_required_fields(self, tmp_path):
        """Should return None for missing required fields."""
        bad_file = tmp_path / "incomplete.json"
        bad_file.write_text('{"target_id": "test"}')  # Missing decision, etc.
        result = load_decision_bundle(bad_file)
        assert result is None

    def test_load_with_signoff(self, tmp_path):
        """Should load signoff information."""
        bundle = create_decision_bundle("test", "GREEN")
        bundle.signoff_status = "approved"
        bundle.signoff_by = "reviewer"
        bundle.signoff_at_utc = "2024-01-15T10:00:00Z"

        output_path = save_decision_bundle(bundle, tmp_path)
        loaded = load_decision_bundle(output_path)

        assert loaded.signoff_status == "approved"
        assert loaded.signoff_by == "reviewer"

    def test_load_with_override(self, tmp_path):
        """Should load override information."""
        bundle = create_decision_bundle("test", "GREEN")
        bundle.override_applied = True
        bundle.override_rule_id = "policy.exception"
        bundle.override_justification = "Approved"
        bundle.override_link = "https://example.com"

        output_path = save_decision_bundle(bundle, tmp_path)
        loaded = load_decision_bundle(output_path)

        assert loaded.override_applied is True
        assert loaded.override_rule_id == "policy.exception"


class TestBundleFromDenylistHits:
    """Tests for bundle_from_denylist_hits function."""

    def test_green_when_no_hits(self):
        """Should return GREEN when no denylist hits."""
        bundle = bundle_from_denylist_hits("test", [])
        assert bundle.decision == "GREEN"
        assert len(bundle.rules_fired) == 0

    def test_red_for_hard_red_hit(self):
        """Should return RED for hard_red severity."""
        hits = [
            {"type": "domain", "pattern": "piracy.com", "severity": "hard_red", "field": "url"}
        ]
        bundle = bundle_from_denylist_hits("test", hits)
        assert bundle.decision == "RED"
        assert len(bundle.rules_fired) == 1
        assert bundle.denylist_matches == hits

    def test_yellow_for_force_yellow_hit(self):
        """Should return YELLOW for force_yellow severity."""
        hits = [
            {"type": "substring", "pattern": "warning", "severity": "force_yellow", "field": "name"}
        ]
        bundle = bundle_from_denylist_hits("test", hits)
        assert bundle.decision == "YELLOW"

    def test_red_overrides_yellow(self):
        """RED should override YELLOW when both present."""
        hits = [
            {"type": "substring", "pattern": "warning", "severity": "force_yellow", "field": "name"},
            {"type": "domain", "pattern": "piracy.com", "severity": "hard_red", "field": "url"},
        ]
        bundle = bundle_from_denylist_hits("test", hits)
        assert bundle.decision == "RED"

    def test_rules_include_reason(self):
        """Rules should include reason from hits."""
        hits = [
            {
                "type": "domain",
                "pattern": "test.com",
                "severity": "hard_red",
                "field": "url",
                "reason": "Custom reason",
                "link": "https://policy.example.com",
            }
        ]
        bundle = bundle_from_denylist_hits("test", hits)
        assert bundle.rules_fired[0].reason == "Custom reason"
        assert bundle.rules_fired[0].link == "https://policy.example.com"

    def test_rule_id_format(self):
        """Rule IDs should follow expected format."""
        hits = [
            {"type": "domain", "pattern": "example.com", "severity": "hard_red", "field": "url"}
        ]
        bundle = bundle_from_denylist_hits("test", hits)
        assert bundle.rules_fired[0].rule_id == "denylist.domain.example.com"
