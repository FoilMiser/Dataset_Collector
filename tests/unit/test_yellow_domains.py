"""
Unit tests for yellow screen domain modules.

Issue 3.3 (v3.0): Tests cover at least one positive and one negative example per domain.
"""

from __future__ import annotations

import pytest
from pathlib import Path
from unittest.mock import MagicMock

# Import base components
from collector_core.yellow.base import (
    FilterDecision,
    DomainContext,
    standard_filter,
    standard_transform,
)
from collector_core.yellow_screen_common import (
    Roots,
    PitchConfig,
    ScreeningConfig,
)

# Import domain modules
from collector_core.yellow.domains import chem, nlp, safety, econ, kg_nav


@pytest.fixture
def mock_roots(tmp_path: Path) -> Roots:
    """Create mock Roots structure for testing."""
    return Roots(
        raw_root=tmp_path / "raw",
        screened_root=tmp_path / "screened",
        ledger_root=tmp_path / "ledger",
        pitches_root=tmp_path / "pitches",
        manifests_root=tmp_path / "manifests",
    )


@pytest.fixture
def mock_pitch_cfg() -> PitchConfig:
    """Create mock PitchConfig for testing."""
    return PitchConfig(
        sample_limit=10,
        text_limit=1000,
    )


@pytest.fixture
def permissive_screen_cfg() -> ScreeningConfig:
    """Create a permissive screening config that allows most content."""
    return ScreeningConfig(
        min_chars=1,
        max_chars=1_000_000,
        text_fields=["text", "content", "body"],
        license_fields=["license", "license_spdx"],
        require_record_license=False,
        allow_spdx=None,  # Allow all licenses
        deny_phrases=[],
    )


@pytest.fixture
def strict_screen_cfg() -> ScreeningConfig:
    """Create a strict screening config with deny phrases."""
    return ScreeningConfig(
        min_chars=100,
        max_chars=10000,
        text_fields=["text", "content"],
        license_fields=["license", "license_spdx"],
        require_record_license=True,
        allow_spdx=["CC0-1.0", "CC-BY-4.0", "MIT"],
        deny_phrases=["confidential", "proprietary", "do not distribute"],
    )


def make_ctx(
    roots: Roots,
    pitch_cfg: PitchConfig,
    screen_cfg: ScreeningConfig,
    target_id: str = "test_target",
) -> DomainContext:
    """Create a DomainContext for testing."""
    return DomainContext(
        cfg={"globals": {}, "targets": []},
        roots=roots,
        pitch_cfg=pitch_cfg,
        screen_cfg=screen_cfg,
        target_id=target_id,
        target_cfg={},
        queue_row={"id": target_id},
        pool="permissive",
        execute=False,
    )


class TestStandardFilter:
    """Tests for standard_filter function."""

    def test_allows_valid_record(
        self, mock_roots: Roots, mock_pitch_cfg: PitchConfig, permissive_screen_cfg: ScreeningConfig
    ) -> None:
        """POSITIVE: Valid record passes standard filter."""
        ctx = make_ctx(mock_roots, mock_pitch_cfg, permissive_screen_cfg)
        raw = {
            "text": "This is a valid text content with sufficient length.",
            "license": "CC0-1.0",
        }

        decision = standard_filter(raw, ctx)

        assert decision.allow is True
        assert decision.text is not None
        assert "valid text" in decision.text

    def test_rejects_empty_text(
        self, mock_roots: Roots, mock_pitch_cfg: PitchConfig, permissive_screen_cfg: ScreeningConfig
    ) -> None:
        """NEGATIVE: Record with no text is rejected."""
        ctx = make_ctx(mock_roots, mock_pitch_cfg, permissive_screen_cfg)
        raw = {"license": "CC0-1.0"}

        decision = standard_filter(raw, ctx)

        assert decision.allow is False
        assert decision.reason == "no_text"

    def test_rejects_short_text(
        self, mock_roots: Roots, mock_pitch_cfg: PitchConfig, strict_screen_cfg: ScreeningConfig
    ) -> None:
        """NEGATIVE: Record with text below min_chars is rejected."""
        ctx = make_ctx(mock_roots, mock_pitch_cfg, strict_screen_cfg)
        raw = {"text": "Too short", "license": "CC0-1.0"}

        decision = standard_filter(raw, ctx)

        assert decision.allow is False
        assert decision.reason == "length_bounds"

    def test_rejects_missing_license(
        self, mock_roots: Roots, mock_pitch_cfg: PitchConfig, strict_screen_cfg: ScreeningConfig
    ) -> None:
        """NEGATIVE: Record without required license is rejected."""
        ctx = make_ctx(mock_roots, mock_pitch_cfg, strict_screen_cfg)
        raw = {"text": "A" * 200}  # Long enough but no license

        decision = standard_filter(raw, ctx)

        assert decision.allow is False
        assert decision.reason == "missing_record_license"

    def test_rejects_non_allowed_license(
        self, mock_roots: Roots, mock_pitch_cfg: PitchConfig, strict_screen_cfg: ScreeningConfig
    ) -> None:
        """NEGATIVE: Record with non-allowlisted license is rejected."""
        ctx = make_ctx(mock_roots, mock_pitch_cfg, strict_screen_cfg)
        raw = {
            "text": "A" * 200,
            "license": "Proprietary",
        }

        decision = standard_filter(raw, ctx)

        assert decision.allow is False
        assert decision.reason == "license_not_allowlisted"
        assert decision.license_spdx == "Proprietary"

    def test_rejects_deny_phrase(
        self, mock_roots: Roots, mock_pitch_cfg: PitchConfig, strict_screen_cfg: ScreeningConfig
    ) -> None:
        """NEGATIVE: Record containing deny phrase is rejected."""
        ctx = make_ctx(mock_roots, mock_pitch_cfg, strict_screen_cfg)
        raw = {
            "text": "A" * 100 + " This document is confidential " + "A" * 100,
            "license": "CC0-1.0",
        }

        decision = standard_filter(raw, ctx)

        assert decision.allow is False
        assert decision.reason == "deny_phrase"


class TestStandardTransform:
    """Tests for standard_transform function."""

    def test_transforms_valid_record(
        self, mock_roots: Roots, mock_pitch_cfg: PitchConfig, permissive_screen_cfg: ScreeningConfig
    ) -> None:
        """POSITIVE: Valid decision produces canonical record."""
        ctx = make_ctx(mock_roots, mock_pitch_cfg, permissive_screen_cfg)
        raw = {"text": "Test content", "source": {"target_id": "test"}}
        decision = FilterDecision(allow=True, text="Test content", license_spdx="CC0-1.0")

        result = standard_transform(raw, ctx, decision, license_profile="permissive")

        assert result is not None
        assert "record_id" in result
        assert result["text"] == "Test content"
        assert result["source"]["license_spdx"] == "CC0-1.0"
        assert result["source"]["license_profile"] == "permissive"

    def test_returns_none_for_no_text(
        self, mock_roots: Roots, mock_pitch_cfg: PitchConfig, permissive_screen_cfg: ScreeningConfig
    ) -> None:
        """NEGATIVE: Decision with no text returns None."""
        ctx = make_ctx(mock_roots, mock_pitch_cfg, permissive_screen_cfg)
        raw = {}
        decision = FilterDecision(allow=True, text=None)

        result = standard_transform(raw, ctx, decision, license_profile="permissive")

        assert result is None


class TestChemDomain:
    """Tests for chemistry domain module."""

    def test_filter_allows_valid_chem_record(
        self, mock_roots: Roots, mock_pitch_cfg: PitchConfig, permissive_screen_cfg: ScreeningConfig
    ) -> None:
        """POSITIVE: Valid chemistry record passes filter."""
        ctx = make_ctx(mock_roots, mock_pitch_cfg, permissive_screen_cfg)
        raw = {
            "text": "The synthesis of benzene derivatives requires careful temperature control.",
            "license": "CC-BY-4.0",
        }

        decision = chem.filter_record(raw, ctx)

        assert decision.allow is True
        assert decision.text is not None

    def test_filter_rejects_empty_chem_record(
        self, mock_roots: Roots, mock_pitch_cfg: PitchConfig, permissive_screen_cfg: ScreeningConfig
    ) -> None:
        """NEGATIVE: Empty chemistry record is rejected."""
        ctx = make_ctx(mock_roots, mock_pitch_cfg, permissive_screen_cfg)
        raw = {}

        decision = chem.filter_record(raw, ctx)

        assert decision.allow is False
        assert decision.reason == "no_text"


class TestNlpDomain:
    """Tests for NLP domain module."""

    def test_filter_allows_valid_nlp_record(
        self, mock_roots: Roots, mock_pitch_cfg: PitchConfig, permissive_screen_cfg: ScreeningConfig
    ) -> None:
        """POSITIVE: Valid NLP record passes filter."""
        ctx = make_ctx(mock_roots, mock_pitch_cfg, permissive_screen_cfg)
        raw = {
            "text": "Natural language processing is a field of artificial intelligence.",
            "license": "MIT",
        }

        decision = nlp.filter_record(raw, ctx)

        assert decision.allow is True
        assert "Natural language" in (decision.text or "")

    def test_filter_rejects_short_nlp_record(
        self, mock_roots: Roots, mock_pitch_cfg: PitchConfig, strict_screen_cfg: ScreeningConfig
    ) -> None:
        """NEGATIVE: Short NLP record is rejected."""
        ctx = make_ctx(mock_roots, mock_pitch_cfg, strict_screen_cfg)
        raw = {"text": "NLP", "license": "MIT"}

        decision = nlp.filter_record(raw, ctx)

        assert decision.allow is False


class TestSafetyDomain:
    """Tests for safety incident domain module."""

    def test_filter_allows_valid_safety_record(
        self, mock_roots: Roots, mock_pitch_cfg: PitchConfig, permissive_screen_cfg: ScreeningConfig
    ) -> None:
        """POSITIVE: Valid safety incident record passes filter."""
        ctx = make_ctx(mock_roots, mock_pitch_cfg, permissive_screen_cfg)
        raw = {
            "text": "An industrial accident occurred due to equipment failure. No injuries reported.",
            "license": "CC0-1.0",
        }

        decision = safety.filter_record(raw, ctx)

        assert decision.allow is True

    def test_filter_rejects_empty_safety_record(
        self, mock_roots: Roots, mock_pitch_cfg: PitchConfig, permissive_screen_cfg: ScreeningConfig
    ) -> None:
        """NEGATIVE: Empty safety record is rejected."""
        ctx = make_ctx(mock_roots, mock_pitch_cfg, permissive_screen_cfg)
        raw = {"category": "incident"}  # No text

        decision = safety.filter_record(raw, ctx)

        assert decision.allow is False


class TestEconDomain:
    """Tests for economics domain module."""

    def test_filter_allows_valid_econ_record(
        self, mock_roots: Roots, mock_pitch_cfg: PitchConfig, permissive_screen_cfg: ScreeningConfig
    ) -> None:
        """POSITIVE: Valid economics record passes filter."""
        ctx = make_ctx(mock_roots, mock_pitch_cfg, permissive_screen_cfg)
        raw = {
            "text": "The GDP growth rate indicates economic expansion in the third quarter.",
            "license": "CC-BY-4.0",
        }

        decision = econ.filter_record(raw, ctx)

        assert decision.allow is True

    def test_filter_rejects_empty_econ_record(
        self, mock_roots: Roots, mock_pitch_cfg: PitchConfig, permissive_screen_cfg: ScreeningConfig
    ) -> None:
        """NEGATIVE: Empty economics record is rejected."""
        ctx = make_ctx(mock_roots, mock_pitch_cfg, permissive_screen_cfg)
        raw = {}

        decision = econ.filter_record(raw, ctx)

        assert decision.allow is False


class TestKgNavDomain:
    """Tests for knowledge graph & navigation domain module."""

    def test_filter_allows_valid_kg_record(
        self, mock_roots: Roots, mock_pitch_cfg: PitchConfig, permissive_screen_cfg: ScreeningConfig
    ) -> None:
        """POSITIVE: Valid knowledge graph record passes filter."""
        ctx = make_ctx(mock_roots, mock_pitch_cfg, permissive_screen_cfg)
        raw = {
            "text": "The entity 'Paris' has a relation 'capital_of' with entity 'France'.",
            "license": "CC0-1.0",
        }

        decision = kg_nav.filter_record(raw, ctx)

        assert decision.allow is True

    def test_filter_rejects_empty_kg_record(
        self, mock_roots: Roots, mock_pitch_cfg: PitchConfig, permissive_screen_cfg: ScreeningConfig
    ) -> None:
        """NEGATIVE: Empty knowledge graph record is rejected."""
        ctx = make_ctx(mock_roots, mock_pitch_cfg, permissive_screen_cfg)
        raw = {"entity_type": "location"}  # No text

        decision = kg_nav.filter_record(raw, ctx)

        assert decision.allow is False


class TestFilterDecisionDataclass:
    """Tests for FilterDecision dataclass."""

    def test_allows_decision_creation(self) -> None:
        """POSITIVE: FilterDecision can be created with all fields."""
        decision = FilterDecision(
            allow=True,
            reason=None,
            text="Sample text",
            license_spdx="CC0-1.0",
            extra={"key": "value"},
            sample_extra={"sample_key": "sample_value"},
        )

        assert decision.allow is True
        assert decision.text == "Sample text"
        assert decision.license_spdx == "CC0-1.0"

    def test_rejection_decision(self) -> None:
        """NEGATIVE: FilterDecision can represent rejection."""
        decision = FilterDecision(
            allow=False,
            reason="test_rejection",
            text=None,
        )

        assert decision.allow is False
        assert decision.reason == "test_rejection"


class TestDomainContext:
    """Tests for DomainContext dataclass."""

    def test_context_creation(
        self, mock_roots: Roots, mock_pitch_cfg: PitchConfig, permissive_screen_cfg: ScreeningConfig
    ) -> None:
        """POSITIVE: DomainContext can be created with state."""
        ctx = make_ctx(mock_roots, mock_pitch_cfg, permissive_screen_cfg)

        assert ctx.target_id == "test_target"
        assert ctx.pool == "permissive"
        assert ctx.execute is False
        assert ctx.state == {}

    def test_context_state_modification(
        self, mock_roots: Roots, mock_pitch_cfg: PitchConfig, permissive_screen_cfg: ScreeningConfig
    ) -> None:
        """POSITIVE: DomainContext state can be modified."""
        ctx = make_ctx(mock_roots, mock_pitch_cfg, permissive_screen_cfg)
        ctx.state["processed_count"] = 100

        assert ctx.state["processed_count"] == 100
