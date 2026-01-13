"""Tests for collector_core.policy_override module."""

from __future__ import annotations

import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from collector_core.policy_override import (
    OverrideType,
    PolicyOverride,
    OverrideRegistry,
    create_override,
    load_override_registry,
    save_override_registry,
    apply_override_to_decision,
    record_override_usage,
)


class TestPolicyOverride:
    """Test PolicyOverride dataclass."""

    def create_sample_override(
        self,
        override_id: str = "test123",
        target_id: str = "target_1",
        override_type: OverrideType = OverrideType.DENYLIST_EXCEPTION,
        rule_pattern: str | None = "denylist.domain.*",
        expires_at_utc: str | None = None,
        revoked: bool = False,
    ) -> PolicyOverride:
        """Create a sample override for testing."""
        return PolicyOverride(
            override_id=override_id,
            target_id=target_id,
            override_type=override_type,
            rule_pattern=rule_pattern,
            justification="This is a valid justification for the override",
            reference_link="https://github.com/example/issue/123",
            approved_by="test@example.com",
            created_at_utc="2024-01-15T12:00:00Z",
            expires_at_utc=expires_at_utc,
            revoked=revoked,
        )

    def test_to_dict(self) -> None:
        """Test serialization to dictionary."""
        override = self.create_sample_override()
        d = override.to_dict()

        assert d["override_id"] == "test123"
        assert d["target_id"] == "target_1"
        assert d["override_type"] == "denylist_exception"
        assert d["rule_pattern"] == "denylist.domain.*"
        assert d["revoked"] is False

    def test_from_dict(self) -> None:
        """Test deserialization from dictionary."""
        data = {
            "override_id": "abc123",
            "target_id": "my_target",
            "override_type": "force_green",
            "rule_pattern": "license.*",
            "justification": "A good reason for this override",
            "reference_link": "https://github.com/example/issue/456",
            "approved_by": "admin@example.com",
            "created_at_utc": "2024-02-01T10:00:00Z",
            "expires_at_utc": "2024-12-31T23:59:59Z",
            "revoked": False,
        }
        override = PolicyOverride.from_dict(data)

        assert override.override_id == "abc123"
        assert override.target_id == "my_target"
        assert override.override_type == OverrideType.FORCE_GREEN
        assert override.rule_pattern == "license.*"
        assert override.expires_at_utc == "2024-12-31T23:59:59Z"

    def test_from_dict_missing_optional_fields(self) -> None:
        """Test deserialization handles missing optional fields."""
        data = {
            "override_id": "abc123",
            "target_id": "my_target",
            "override_type": "force_yellow",
            "justification": "A good reason",
            "reference_link": "https://example.com/issue/1",
            "approved_by": "admin",
            "created_at_utc": "2024-01-01T00:00:00Z",
        }
        override = PolicyOverride.from_dict(data)

        assert override.rule_pattern is None
        assert override.expires_at_utc is None
        assert override.revoked is False
        assert override.revoked_at_utc is None

    def test_is_active_not_revoked(self) -> None:
        """Test is_active returns True for non-revoked override."""
        override = self.create_sample_override(revoked=False)
        assert override.is_active() is True

    def test_is_active_revoked(self) -> None:
        """Test is_active returns False for revoked override."""
        override = self.create_sample_override(revoked=True)
        assert override.is_active() is False

    def test_is_active_not_expired(self) -> None:
        """Test is_active returns True when not expired."""
        override = self.create_sample_override(expires_at_utc="2099-12-31T23:59:59Z")
        assert override.is_active(as_of_utc="2024-06-15T12:00:00Z") is True

    def test_is_active_expired(self) -> None:
        """Test is_active returns False when expired."""
        override = self.create_sample_override(expires_at_utc="2024-01-01T00:00:00Z")
        assert override.is_active(as_of_utc="2024-06-15T12:00:00Z") is False

    def test_is_active_exactly_at_expiration(self) -> None:
        """Test is_active at exact expiration boundary."""
        override = self.create_sample_override(expires_at_utc="2024-06-15T12:00:00Z")
        # At the same time as expiration, it's still expired (> not >=)
        assert override.is_active(as_of_utc="2024-06-15T12:00:01Z") is False

    def test_matches_rule_no_pattern(self) -> None:
        """Test matches_rule returns True when no pattern (matches all)."""
        override = self.create_sample_override(rule_pattern=None)
        assert override.matches_rule("any.rule.id") is True
        assert override.matches_rule("another.rule") is True

    def test_matches_rule_exact_match(self) -> None:
        """Test matches_rule with exact pattern match."""
        override = self.create_sample_override(rule_pattern="denylist.domain.example.com")
        assert override.matches_rule("denylist.domain.example.com") is True
        assert override.matches_rule("denylist.domain.other.com") is False

    def test_matches_rule_wildcard_suffix(self) -> None:
        """Test matches_rule with wildcard suffix pattern."""
        override = self.create_sample_override(rule_pattern="denylist.domain.*")
        assert override.matches_rule("denylist.domain.example.com") is True
        assert override.matches_rule("denylist.domain.other.org") is True
        assert override.matches_rule("denylist.publisher.example") is False

    def test_matches_rule_empty_wildcard(self) -> None:
        """Test matches_rule with pattern that's just a wildcard."""
        override = self.create_sample_override(rule_pattern="*")
        # Pattern "*" means prefix is empty, so matches anything
        assert override.matches_rule("anything") is True


class TestOverrideRegistry:
    """Test OverrideRegistry class."""

    def create_test_registry(self) -> OverrideRegistry:
        """Create a test registry with sample overrides."""
        overrides = [
            PolicyOverride(
                override_id="override1",
                target_id="target_a",
                override_type=OverrideType.DENYLIST_EXCEPTION,
                rule_pattern="denylist.*",
                justification="Valid reason for target_a",
                reference_link="https://example.com/1",
                approved_by="admin",
                created_at_utc="2024-01-01T00:00:00Z",
            ),
            PolicyOverride(
                override_id="override2",
                target_id="target_a",
                override_type=OverrideType.FORCE_GREEN,
                rule_pattern="license.check",
                justification="Force green for license check",
                reference_link="https://example.com/2",
                approved_by="admin",
                created_at_utc="2024-01-01T00:00:00Z",
            ),
            PolicyOverride(
                override_id="override3",
                target_id="target_b",
                override_type=OverrideType.LICENSE_EXCEPTION,
                rule_pattern=None,
                justification="License exception for target_b",
                reference_link="https://example.com/3",
                approved_by="admin",
                created_at_utc="2024-01-01T00:00:00Z",
            ),
            PolicyOverride(
                override_id="override4",
                target_id="target_a",
                override_type=OverrideType.FORCE_YELLOW,
                rule_pattern="content.*",
                justification="Revoked override",
                reference_link="https://example.com/4",
                approved_by="admin",
                created_at_utc="2024-01-01T00:00:00Z",
                revoked=True,
            ),
        ]
        return OverrideRegistry(overrides=overrides)

    def test_get_overrides_for_target(self) -> None:
        """Test getting active overrides for a target."""
        registry = self.create_test_registry()
        overrides = registry.get_overrides_for_target("target_a")

        # Should have 2 active overrides (override4 is revoked)
        assert len(overrides) == 2
        override_ids = {o.override_id for o in overrides}
        assert "override1" in override_ids
        assert "override2" in override_ids
        assert "override4" not in override_ids  # revoked

    def test_get_overrides_for_nonexistent_target(self) -> None:
        """Test getting overrides for target with no overrides."""
        registry = self.create_test_registry()
        overrides = registry.get_overrides_for_target("nonexistent_target")
        assert overrides == []

    def test_find_override_for_rule(self) -> None:
        """Test finding override for a specific rule."""
        registry = self.create_test_registry()
        override = registry.find_override_for_rule("target_a", "denylist.something")

        assert override is not None
        assert override.override_id == "override1"

    def test_find_override_for_rule_with_type(self) -> None:
        """Test finding override for rule with specific type."""
        registry = self.create_test_registry()

        # Should find FORCE_GREEN override
        override = registry.find_override_for_rule(
            "target_a", "license.check", OverrideType.FORCE_GREEN
        )
        assert override is not None
        assert override.override_id == "override2"

        # Should not find DENYLIST_EXCEPTION for license.check
        override = registry.find_override_for_rule(
            "target_a", "license.check", OverrideType.DENYLIST_EXCEPTION
        )
        assert override is None

    def test_find_override_returns_none_when_no_match(self) -> None:
        """Test find_override_for_rule returns None when no match."""
        registry = self.create_test_registry()
        override = registry.find_override_for_rule("target_a", "no.such.rule")
        assert override is None

    def test_add_override(self) -> None:
        """Test adding new override to registry."""
        registry = OverrideRegistry(overrides=[])
        new_override = PolicyOverride(
            override_id="new1",
            target_id="new_target",
            override_type=OverrideType.FORCE_GREEN,
            rule_pattern=None,
            justification="New override",
            reference_link="https://example.com/new",
            approved_by="admin",
            created_at_utc="2024-06-01T00:00:00Z",
        )

        registry.add_override(new_override)

        assert len(registry.overrides) == 1
        overrides = registry.get_overrides_for_target("new_target")
        assert len(overrides) == 1

    def test_revoke_override(self) -> None:
        """Test revoking an override."""
        registry = self.create_test_registry()

        success = registry.revoke_override(
            "override1", revoked_by="revoker@example.com", reason="No longer needed"
        )

        assert success is True
        override = next(o for o in registry.overrides if o.override_id == "override1")
        assert override.revoked is True
        assert override.revoked_by == "revoker@example.com"
        assert override.revoked_reason == "No longer needed"
        assert override.revoked_at_utc is not None

    def test_revoke_nonexistent_override(self) -> None:
        """Test revoking non-existent override returns False."""
        registry = self.create_test_registry()
        success = registry.revoke_override("nonexistent", "admin", "reason")
        assert success is False

    def test_revoke_already_revoked_override(self) -> None:
        """Test revoking already revoked override returns False."""
        registry = self.create_test_registry()
        # override4 is already revoked
        success = registry.revoke_override("override4", "admin", "reason")
        assert success is False


class TestCreateOverride:
    """Test create_override function."""

    def test_create_override_success(self) -> None:
        """Test successful override creation."""
        override = create_override(
            target_id="my_target",
            override_type=OverrideType.FORCE_GREEN,
            justification="This is a sufficiently long justification for the override",
            reference_link="https://github.com/example/issue/123",
            approved_by="admin@example.com",
            rule_pattern="some.rule.*",
        )

        assert override.target_id == "my_target"
        assert override.override_type == OverrideType.FORCE_GREEN
        assert override.rule_pattern == "some.rule.*"
        assert len(override.override_id) == 16  # SHA256 prefix

    def test_create_override_with_expiration(self) -> None:
        """Test override creation with expiration date."""
        override = create_override(
            target_id="my_target",
            override_type=OverrideType.LICENSE_EXCEPTION,
            justification="Long enough justification for override",
            reference_link="https://example.com/issue/1",
            approved_by="admin",
            expires_at_utc="2024-12-31T23:59:59Z",
        )

        assert override.expires_at_utc == "2024-12-31T23:59:59Z"

    def test_create_override_short_justification_fails(self) -> None:
        """Test that short justification raises ValueError."""
        with pytest.raises(ValueError, match="at least 10 characters"):
            create_override(
                target_id="target",
                override_type=OverrideType.FORCE_GREEN,
                justification="short",
                reference_link="https://example.com/1",
                approved_by="admin",
            )

    def test_create_override_empty_justification_fails(self) -> None:
        """Test that empty justification raises ValueError."""
        with pytest.raises(ValueError, match="at least 10 characters"):
            create_override(
                target_id="target",
                override_type=OverrideType.FORCE_GREEN,
                justification="",
                reference_link="https://example.com/1",
                approved_by="admin",
            )

    def test_create_override_invalid_reference_link(self) -> None:
        """Test that invalid reference link raises ValueError."""
        with pytest.raises(ValueError, match="valid URL"):
            create_override(
                target_id="target",
                override_type=OverrideType.FORCE_GREEN,
                justification="This is a valid long justification",
                reference_link="not-a-url",
                approved_by="admin",
            )

    def test_create_override_empty_reference_link(self) -> None:
        """Test that empty reference link raises ValueError."""
        with pytest.raises(ValueError, match="valid URL"):
            create_override(
                target_id="target",
                override_type=OverrideType.FORCE_GREEN,
                justification="This is a valid long justification",
                reference_link="",
                approved_by="admin",
            )

    def test_create_override_empty_approved_by(self) -> None:
        """Test that empty approved_by raises ValueError."""
        with pytest.raises(ValueError, match="must be specified"):
            create_override(
                target_id="target",
                override_type=OverrideType.FORCE_GREEN,
                justification="This is a valid long justification",
                reference_link="https://example.com/1",
                approved_by="",
            )


class TestLoadSaveRegistry:
    """Test load and save registry functions."""

    def test_load_nonexistent_file(self) -> None:
        """Test loading from non-existent file returns empty registry."""
        registry = load_override_registry(Path("/nonexistent/path/overrides.jsonl"))
        assert registry.overrides == []

    def test_save_and_load_roundtrip(self) -> None:
        """Test saving and loading preserves data."""
        override = PolicyOverride(
            override_id="test123",
            target_id="target_1",
            override_type=OverrideType.FORCE_GREEN,
            rule_pattern="test.*",
            justification="Test justification for roundtrip",
            reference_link="https://example.com/test",
            approved_by="tester",
            created_at_utc="2024-01-01T00:00:00Z",
        )
        registry = OverrideRegistry(overrides=[override])

        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "overrides.jsonl"
            save_override_registry(registry, path)

            loaded = load_override_registry(path)

            assert len(loaded.overrides) == 1
            loaded_override = loaded.overrides[0]
            assert loaded_override.override_id == "test123"
            assert loaded_override.override_type == OverrideType.FORCE_GREEN

    def test_load_skips_malformed_entries(self) -> None:
        """Test that loading skips malformed JSON entries."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "overrides.jsonl"
            path.write_text(
                '{"override_id": "valid", "target_id": "t1", "override_type": "force_green", '
                '"justification": "ok", "reference_link": "https://x.com", "approved_by": "a", '
                '"created_at_utc": "2024-01-01T00:00:00Z"}\n'
                '{"invalid": "missing required fields"}\n'
            )

            registry = load_override_registry(path)
            # Only the valid entry should be loaded
            assert len(registry.overrides) == 1


class TestApplyOverrideToDecision:
    """Test apply_override_to_decision function."""

    def create_registry_with_overrides(self) -> OverrideRegistry:
        """Create registry with various override types."""
        overrides = [
            PolicyOverride(
                override_id="force_green_1",
                target_id="target_a",
                override_type=OverrideType.FORCE_GREEN,
                rule_pattern="license.*",
                justification="Force green for license",
                reference_link="https://example.com/1",
                approved_by="admin",
                created_at_utc="2024-01-01T00:00:00Z",
            ),
            PolicyOverride(
                override_id="force_yellow_1",
                target_id="target_b",
                override_type=OverrideType.FORCE_YELLOW,
                rule_pattern="content.*",
                justification="Force yellow for content",
                reference_link="https://example.com/2",
                approved_by="admin",
                created_at_utc="2024-01-01T00:00:00Z",
            ),
            PolicyOverride(
                override_id="denylist_exc_1",
                target_id="target_c",
                override_type=OverrideType.DENYLIST_EXCEPTION,
                rule_pattern="denylist.*",
                justification="Denylist exception",
                reference_link="https://example.com/3",
                approved_by="admin",
                created_at_utc="2024-01-01T00:00:00Z",
            ),
        ]
        return OverrideRegistry(overrides=overrides)

    def test_force_green_override(self) -> None:
        """Test FORCE_GREEN override changes decision to GREEN."""
        registry = self.create_registry_with_overrides()
        decision, override = apply_override_to_decision(
            "target_a", "RED", "license.check", registry
        )

        assert decision == "GREEN"
        assert override is not None
        assert override.override_id == "force_green_1"

    def test_force_yellow_override(self) -> None:
        """Test FORCE_YELLOW override changes decision to YELLOW."""
        registry = self.create_registry_with_overrides()
        decision, override = apply_override_to_decision(
            "target_b", "RED", "content.check", registry
        )

        assert decision == "YELLOW"
        assert override is not None
        assert override.override_id == "force_yellow_1"

    def test_denylist_exception_red_to_yellow(self) -> None:
        """Test DENYLIST_EXCEPTION upgrades RED to YELLOW."""
        registry = self.create_registry_with_overrides()
        decision, override = apply_override_to_decision(
            "target_c", "RED", "denylist.domain", registry
        )

        assert decision == "YELLOW"
        assert override is not None
        assert override.override_id == "denylist_exc_1"

    def test_denylist_exception_yellow_stays_yellow(self) -> None:
        """Test DENYLIST_EXCEPTION keeps YELLOW as YELLOW."""
        registry = self.create_registry_with_overrides()
        decision, override = apply_override_to_decision(
            "target_c", "YELLOW", "denylist.domain", registry
        )

        assert decision == "YELLOW"
        assert override is not None

    def test_no_matching_override(self) -> None:
        """Test no override when no match found."""
        registry = self.create_registry_with_overrides()
        decision, override = apply_override_to_decision(
            "target_x", "RED", "some.rule", registry
        )

        assert decision == "RED"
        assert override is None

    def test_force_overrides_take_priority(self) -> None:
        """Test that FORCE_GREEN/YELLOW take priority over exception types."""
        overrides = [
            PolicyOverride(
                override_id="exception_1",
                target_id="target_1",
                override_type=OverrideType.DENYLIST_EXCEPTION,
                rule_pattern="rule.*",
                justification="Exception override",
                reference_link="https://example.com/1",
                approved_by="admin",
                created_at_utc="2024-01-01T00:00:00Z",
            ),
            PolicyOverride(
                override_id="force_green_1",
                target_id="target_1",
                override_type=OverrideType.FORCE_GREEN,
                rule_pattern="rule.*",
                justification="Force green override",
                reference_link="https://example.com/2",
                approved_by="admin",
                created_at_utc="2024-01-01T00:00:00Z",
            ),
        ]
        registry = OverrideRegistry(overrides=overrides)

        decision, override = apply_override_to_decision(
            "target_1", "RED", "rule.check", registry
        )

        # FORCE_GREEN should be applied, not the exception
        assert decision == "GREEN"
        assert override.override_id == "force_green_1"


class TestRecordOverrideUsage:
    """Test record_override_usage function."""

    def test_record_override_usage(self) -> None:
        """Test recording override usage to ledger."""
        override = PolicyOverride(
            override_id="test123",
            target_id="target_1",
            override_type=OverrideType.FORCE_GREEN,
            rule_pattern="test.*",
            justification="Test override justification",
            reference_link="https://example.com/test",
            approved_by="tester",
            created_at_utc="2024-01-01T00:00:00Z",
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            ledger_root = Path(tmpdir) / "ledger"

            record_override_usage(
                override=override,
                target_id="target_1",
                original_decision="RED",
                new_decision="GREEN",
                ledger_root=ledger_root,
            )

            # Check that ledger file was created
            ledger_file = ledger_root / "override_usage.jsonl"
            assert ledger_file.exists()

            # Check content
            import json
            content = ledger_file.read_text()
            entry = json.loads(content.strip())

            assert entry["override_id"] == "test123"
            assert entry["target_id"] == "target_1"
            assert entry["original_decision"] == "RED"
            assert entry["new_decision"] == "GREEN"
            assert entry["override_type"] == "force_green"
