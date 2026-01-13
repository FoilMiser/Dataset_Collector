"""Tests for collector_core.evidence_policy module."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from collector_core.evidence_policy import (
    EvidenceChangeAction,
    EvidenceChangeResult,
    EvidencePolicyConfig,
    detect_evidence_change,
    record_evidence_change,
    check_merge_eligibility,
)


class TestEvidenceChangeResult:
    """Test EvidenceChangeResult dataclass."""

    def test_to_dict(self) -> None:
        """Test serialization to dictionary."""
        result = EvidenceChangeResult(
            target_id="target_1",
            action=EvidenceChangeAction.BLOCK_MERGE,
            raw_changed=True,
            normalized_changed=True,
            cosmetic_change=False,
            signoff_raw_sha="abc123",
            signoff_normalized_sha="def456",
            current_raw_sha="ghi789",
            current_normalized_sha="jkl012",
            detected_at_utc="2024-06-15T12:00:00Z",
            message="Evidence changed - merge blocked",
        )
        d = result.to_dict()

        assert d["target_id"] == "target_1"
        assert d["action"] == "block_merge"
        assert d["raw_changed"] is True
        assert d["normalized_changed"] is True

    def test_requires_action_true_for_block_merge(self) -> None:
        """Test requires_action returns True for BLOCK_MERGE."""
        result = EvidenceChangeResult(
            target_id="target_1",
            action=EvidenceChangeAction.BLOCK_MERGE,
            raw_changed=True,
            normalized_changed=True,
            cosmetic_change=False,
            signoff_raw_sha=None,
            signoff_normalized_sha=None,
            current_raw_sha=None,
            current_normalized_sha=None,
            detected_at_utc="2024-01-01T00:00:00Z",
            message="Test",
        )
        assert result.requires_action is True

    def test_requires_action_true_for_demote(self) -> None:
        """Test requires_action returns True for DEMOTE_TO_YELLOW."""
        result = EvidenceChangeResult(
            target_id="target_1",
            action=EvidenceChangeAction.DEMOTE_TO_YELLOW,
            raw_changed=True,
            normalized_changed=False,
            cosmetic_change=False,
            signoff_raw_sha=None,
            signoff_normalized_sha=None,
            current_raw_sha=None,
            current_normalized_sha=None,
            detected_at_utc="2024-01-01T00:00:00Z",
            message="Test",
        )
        assert result.requires_action is True

    def test_requires_action_true_for_re_review(self) -> None:
        """Test requires_action returns True for RE_REVIEW_REQUIRED."""
        result = EvidenceChangeResult(
            target_id="target_1",
            action=EvidenceChangeAction.RE_REVIEW_REQUIRED,
            raw_changed=True,
            normalized_changed=True,
            cosmetic_change=False,
            signoff_raw_sha=None,
            signoff_normalized_sha=None,
            current_raw_sha=None,
            current_normalized_sha=None,
            detected_at_utc="2024-01-01T00:00:00Z",
            message="Test",
        )
        assert result.requires_action is True

    def test_requires_action_false_for_none(self) -> None:
        """Test requires_action returns False for NONE."""
        result = EvidenceChangeResult(
            target_id="target_1",
            action=EvidenceChangeAction.NONE,
            raw_changed=False,
            normalized_changed=False,
            cosmetic_change=False,
            signoff_raw_sha=None,
            signoff_normalized_sha=None,
            current_raw_sha=None,
            current_normalized_sha=None,
            detected_at_utc="2024-01-01T00:00:00Z",
            message="Test",
        )
        assert result.requires_action is False

    def test_requires_action_false_for_cosmetic_warn(self) -> None:
        """Test requires_action returns False for COSMETIC_WARN."""
        result = EvidenceChangeResult(
            target_id="target_1",
            action=EvidenceChangeAction.COSMETIC_WARN,
            raw_changed=True,
            normalized_changed=False,
            cosmetic_change=True,
            signoff_raw_sha=None,
            signoff_normalized_sha=None,
            current_raw_sha=None,
            current_normalized_sha=None,
            detected_at_utc="2024-01-01T00:00:00Z",
            message="Test",
        )
        assert result.requires_action is False


class TestEvidencePolicyConfig:
    """Test EvidencePolicyConfig dataclass."""

    def test_default_values(self) -> None:
        """Test default configuration values."""
        config = EvidencePolicyConfig()

        assert config.evidence_policy == "normalized"
        assert config.cosmetic_policy == "warn_only"
        assert config.demote_on_change is True
        assert config.block_merge_on_change is True
        assert config.re_review_grace_days == 0

    def test_from_config_with_globals(self) -> None:
        """Test creating config from targets configuration."""
        cfg = {
            "globals": {
                "evidence_policy": {
                    "comparison": "raw",
                    "cosmetic_handling": "treat_as_changed",
                    "demote_on_change": False,
                    "block_merge_on_change": False,
                    "re_review_grace_days": 7,
                }
            }
        }
        config = EvidencePolicyConfig.from_config(cfg)

        assert config.evidence_policy == "raw"
        assert config.cosmetic_policy == "treat_as_changed"
        assert config.demote_on_change is False
        assert config.block_merge_on_change is False
        assert config.re_review_grace_days == 7

    def test_from_config_empty(self) -> None:
        """Test creating config from empty configuration uses defaults."""
        config = EvidencePolicyConfig.from_config({})

        assert config.evidence_policy == "normalized"
        assert config.cosmetic_policy == "warn_only"
        assert config.demote_on_change is True

    def test_from_config_invalid_policy_uses_default(self) -> None:
        """Test invalid policy values fall back to defaults."""
        cfg = {
            "globals": {
                "evidence_policy": {
                    "comparison": "invalid_policy",
                    "cosmetic_handling": "also_invalid",
                }
            }
        }
        config = EvidencePolicyConfig.from_config(cfg)

        assert config.evidence_policy == "normalized"
        assert config.cosmetic_policy == "warn_only"


class TestDetectEvidenceChange:
    """Test detect_evidence_change function."""

    def test_no_signoff_returns_no_change(self) -> None:
        """Test that missing signoff results in no change detected."""
        policy = EvidencePolicyConfig()
        result = detect_evidence_change(
            target_id="target_1",
            signoff=None,
            current_evidence={"raw_sha256": "abc123", "normalized_sha256": "def456"},
            policy=policy,
        )

        assert result.action == EvidenceChangeAction.NONE
        assert result.raw_changed is False
        assert result.normalized_changed is False
        assert "No signoff exists" in result.message

    def test_no_change_when_hashes_match(self) -> None:
        """Test no change when all hashes match."""
        policy = EvidencePolicyConfig()
        signoff = {
            "evidence": {
                "raw_sha256": "abc123",
                "normalized_sha256": "def456",
            }
        }
        current = {
            "raw_sha256": "abc123",
            "normalized_sha256": "def456",
        }
        result = detect_evidence_change("target_1", signoff, current, policy)

        assert result.action == EvidenceChangeAction.NONE
        assert result.raw_changed is False
        assert result.normalized_changed is False

    def test_block_merge_when_evidence_changed(self) -> None:
        """Test BLOCK_MERGE action when evidence changed with blocking enabled."""
        policy = EvidencePolicyConfig(
            evidence_policy="normalized",
            block_merge_on_change=True,
        )
        signoff = {
            "evidence": {
                "raw_sha256": "abc123",
                "normalized_sha256": "def456",
            }
        }
        current = {
            "raw_sha256": "xyz789",
            "normalized_sha256": "uvw012",
        }
        result = detect_evidence_change("target_1", signoff, current, policy)

        assert result.action == EvidenceChangeAction.BLOCK_MERGE
        assert result.normalized_changed is True
        assert "merge blocked" in result.message.lower()

    def test_demote_when_evidence_changed_no_blocking(self) -> None:
        """Test DEMOTE_TO_YELLOW when blocking disabled but demote enabled."""
        policy = EvidencePolicyConfig(
            evidence_policy="normalized",
            block_merge_on_change=False,
            demote_on_change=True,
        )
        signoff = {
            "evidence": {
                "raw_sha256": "abc123",
                "normalized_sha256": "def456",
            }
        }
        current = {
            "raw_sha256": "xyz789",
            "normalized_sha256": "uvw012",
        }
        result = detect_evidence_change("target_1", signoff, current, policy)

        assert result.action == EvidenceChangeAction.DEMOTE_TO_YELLOW
        assert "demoting to YELLOW" in result.message

    def test_re_review_when_both_disabled(self) -> None:
        """Test RE_REVIEW_REQUIRED when both blocking and demote disabled."""
        policy = EvidencePolicyConfig(
            evidence_policy="normalized",
            block_merge_on_change=False,
            demote_on_change=False,
        )
        signoff = {
            "evidence": {
                "raw_sha256": "abc123",
                "normalized_sha256": "def456",
            }
        }
        current = {
            "raw_sha256": "xyz789",
            "normalized_sha256": "uvw012",
        }
        result = detect_evidence_change("target_1", signoff, current, policy)

        assert result.action == EvidenceChangeAction.RE_REVIEW_REQUIRED
        assert "re-review required" in result.message.lower()

    def test_cosmetic_change_warn_only(self) -> None:
        """Test cosmetic change with warn_only policy."""
        policy = EvidencePolicyConfig(
            evidence_policy="normalized",
            cosmetic_policy="warn_only",
        )
        signoff = {
            "evidence": {
                "raw_sha256": "abc123",
                "normalized_sha256": "def456",
            }
        }
        # Raw changed but normalized same = cosmetic change
        current = {
            "raw_sha256": "xyz789",  # Different
            "normalized_sha256": "def456",  # Same
        }
        result = detect_evidence_change("target_1", signoff, current, policy)

        assert result.action == EvidenceChangeAction.COSMETIC_WARN
        assert result.cosmetic_change is True
        assert "cosmetic" in result.message.lower()

    def test_raw_policy_detects_raw_changes(self) -> None:
        """Test raw evidence policy detects raw-only changes."""
        policy = EvidencePolicyConfig(
            evidence_policy="raw",
            block_merge_on_change=True,
        )
        signoff = {
            "evidence": {
                "raw_sha256": "abc123",
                "normalized_sha256": "def456",
            }
        }
        current = {
            "raw_sha256": "xyz789",
            "normalized_sha256": "def456",  # Normalized unchanged
        }
        result = detect_evidence_change("target_1", signoff, current, policy)

        assert result.action == EvidenceChangeAction.BLOCK_MERGE
        assert result.raw_changed is True

    def test_signoff_evidence_from_alternative_keys(self) -> None:
        """Test signoff evidence extraction from alternative key names."""
        policy = EvidencePolicyConfig()
        # Use flat keys instead of nested evidence dict
        signoff = {
            "evidence_raw_sha256": "abc123",
            "evidence_normalized_sha256": "def456",
        }
        current = {
            "raw_sha256": "abc123",
            "normalized_sha256": "def456",
        }
        result = detect_evidence_change("target_1", signoff, current, policy)

        assert result.action == EvidenceChangeAction.NONE
        assert result.signoff_raw_sha == "abc123"
        assert result.signoff_normalized_sha == "def456"

    def test_current_evidence_none(self) -> None:
        """Test handling of None current evidence."""
        policy = EvidencePolicyConfig()
        signoff = {
            "evidence": {
                "raw_sha256": "abc123",
                "normalized_sha256": "def456",
            }
        }
        result = detect_evidence_change("target_1", signoff, None, policy)

        assert result.current_raw_sha is None
        assert result.current_normalized_sha is None


class TestRecordEvidenceChange:
    """Test record_evidence_change function."""

    def test_record_to_ledger(self) -> None:
        """Test recording evidence change to ledger."""
        result = EvidenceChangeResult(
            target_id="target_1",
            action=EvidenceChangeAction.BLOCK_MERGE,
            raw_changed=True,
            normalized_changed=True,
            cosmetic_change=False,
            signoff_raw_sha="abc123",
            signoff_normalized_sha="def456",
            current_raw_sha="ghi789",
            current_normalized_sha="jkl012",
            detected_at_utc="2024-06-15T12:00:00Z",
            message="Evidence changed",
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            ledger_root = Path(tmpdir) / "ledger"

            record_evidence_change(result, ledger_root)

            ledger_file = ledger_root / "evidence_changes.jsonl"
            assert ledger_file.exists()

            import json
            content = ledger_file.read_text()
            entry = json.loads(content.strip())

            assert entry["target_id"] == "target_1"
            assert entry["action"] == "block_merge"
            assert entry["raw_changed"] is True

    def test_record_adds_to_re_review_queue(self) -> None:
        """Test recording adds to re-review queue when action requires it."""
        result = EvidenceChangeResult(
            target_id="target_1",
            action=EvidenceChangeAction.DEMOTE_TO_YELLOW,
            raw_changed=True,
            normalized_changed=True,
            cosmetic_change=False,
            signoff_raw_sha="abc123",
            signoff_normalized_sha="def456",
            current_raw_sha="ghi789",
            current_normalized_sha="jkl012",
            detected_at_utc="2024-06-15T12:00:00Z",
            message="Demoted to yellow",
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            ledger_root = Path(tmpdir) / "ledger"
            queue_path = Path(tmpdir) / "queue" / "re_review.jsonl"

            record_evidence_change(result, ledger_root, re_review_queue_path=queue_path)

            assert queue_path.exists()

            import json
            content = queue_path.read_text()
            entry = json.loads(content.strip())

            assert entry["id"] == "target_1"
            assert entry["reason"] == "demote_to_yellow"
            assert entry["bucket"] == "yellow"

    def test_record_does_not_add_to_queue_for_cosmetic(self) -> None:
        """Test cosmetic changes don't get added to re-review queue."""
        result = EvidenceChangeResult(
            target_id="target_1",
            action=EvidenceChangeAction.COSMETIC_WARN,
            raw_changed=True,
            normalized_changed=False,
            cosmetic_change=True,
            signoff_raw_sha="abc123",
            signoff_normalized_sha="def456",
            current_raw_sha="ghi789",
            current_normalized_sha="def456",
            detected_at_utc="2024-06-15T12:00:00Z",
            message="Cosmetic change",
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            ledger_root = Path(tmpdir) / "ledger"
            queue_path = Path(tmpdir) / "queue" / "re_review.jsonl"

            record_evidence_change(result, ledger_root, re_review_queue_path=queue_path)

            # Queue should not exist since cosmetic change doesn't require action
            assert not queue_path.exists()


class TestCheckMergeEligibility:
    """Test check_merge_eligibility function."""

    def test_eligible_when_no_change(self) -> None:
        """Test merge eligible when no evidence change."""
        policy = EvidencePolicyConfig()
        signoff = {
            "evidence": {
                "raw_sha256": "abc123",
                "normalized_sha256": "def456",
            }
        }
        current = {
            "raw_sha256": "abc123",
            "normalized_sha256": "def456",
        }

        eligible, reason = check_merge_eligibility("target_1", signoff, current, policy)

        assert eligible is True
        assert "Eligible" in reason

    def test_not_eligible_when_blocked(self) -> None:
        """Test merge not eligible when BLOCK_MERGE action."""
        policy = EvidencePolicyConfig(block_merge_on_change=True)
        signoff = {
            "evidence": {
                "raw_sha256": "abc123",
                "normalized_sha256": "def456",
            }
        }
        current = {
            "raw_sha256": "xyz789",
            "normalized_sha256": "uvw012",
        }

        eligible, reason = check_merge_eligibility("target_1", signoff, current, policy)

        assert eligible is False
        assert "blocked" in reason.lower()

    def test_not_eligible_when_re_review_required(self) -> None:
        """Test merge not eligible when RE_REVIEW_REQUIRED action."""
        policy = EvidencePolicyConfig(
            block_merge_on_change=False,
            demote_on_change=False,
        )
        signoff = {
            "evidence": {
                "raw_sha256": "abc123",
                "normalized_sha256": "def456",
            }
        }
        current = {
            "raw_sha256": "xyz789",
            "normalized_sha256": "uvw012",
        }

        eligible, reason = check_merge_eligibility("target_1", signoff, current, policy)

        assert eligible is False
        assert "re-review" in reason.lower()

    def test_eligible_when_only_demoted(self) -> None:
        """Test merge eligible when only demoted (not blocked)."""
        policy = EvidencePolicyConfig(
            block_merge_on_change=False,
            demote_on_change=True,
        )
        signoff = {
            "evidence": {
                "raw_sha256": "abc123",
                "normalized_sha256": "def456",
            }
        }
        current = {
            "raw_sha256": "xyz789",
            "normalized_sha256": "uvw012",
        }

        eligible, reason = check_merge_eligibility("target_1", signoff, current, policy)

        # Demote action is not BLOCK_MERGE, so technically eligible
        # But the function checks for both BLOCK_MERGE and RE_REVIEW_REQUIRED
        assert eligible is True

    def test_eligible_with_cosmetic_change(self) -> None:
        """Test merge eligible with cosmetic-only change."""
        policy = EvidencePolicyConfig(
            evidence_policy="normalized",
            cosmetic_policy="warn_only",
        )
        signoff = {
            "evidence": {
                "raw_sha256": "abc123",
                "normalized_sha256": "def456",
            }
        }
        current = {
            "raw_sha256": "xyz789",  # Different raw
            "normalized_sha256": "def456",  # Same normalized
        }

        eligible, reason = check_merge_eligibility("target_1", signoff, current, policy)

        assert eligible is True


class TestEvidenceChangeAction:
    """Test EvidenceChangeAction enum."""

    def test_action_values(self) -> None:
        """Test all expected action values exist."""
        assert EvidenceChangeAction.NONE.value == "none"
        assert EvidenceChangeAction.COSMETIC_WARN.value == "cosmetic_warn"
        assert EvidenceChangeAction.DEMOTE_TO_YELLOW.value == "demote_to_yellow"
        assert EvidenceChangeAction.BLOCK_MERGE.value == "block_merge"
        assert EvidenceChangeAction.RE_REVIEW_REQUIRED.value == "re_review_required"
