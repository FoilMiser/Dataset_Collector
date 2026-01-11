"""
test_license_normalization.py

Unit tests for license normalization and SPDX resolution.
"""
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest
import yaml


def load_license_map():
    """Load the license_map.yaml for testing."""
    license_map_path = Path(__file__).parent.parent / "license_map.yaml"
    return yaml.safe_load(license_map_path.read_text(encoding="utf-8"))


class TestLicenseAllowlist:
    """Tests for license allowlist classification."""

    def test_permissive_licenses_in_allow_list(self):
        """Common permissive licenses should be in the allow list."""
        lm = load_license_map()
        allow = lm["spdx"]["allow"]

        assert "CC0-1.0" in allow
        assert "CC-BY-4.0" in allow
        assert "MIT" in allow
        assert "Apache-2.0" in allow
        assert "BSD-3-Clause" in allow
        assert "US-PUBLIC-DOMAIN" in allow

    def test_copyleft_licenses_in_conditional(self):
        """Copyleft licenses should be in the conditional list."""
        lm = load_license_map()
        conditional = lm["spdx"]["conditional"]

        assert "CC-BY-SA-4.0" in conditional
        assert "GPL-3.0-only" in conditional or "GPL-3.0-or-later" in conditional
        assert "LGPL-3.0-only" in conditional or "LGPL-3.0-or-later" in conditional

    def test_restrictive_prefixes_denied(self):
        """Non-commercial and restrictive licenses should be denied."""
        lm = load_license_map()
        deny_prefixes = lm["spdx"]["deny_prefixes"]

        assert "CC-BY-NC" in deny_prefixes
        assert "CC-BY-ND" in deny_prefixes
        assert "Proprietary" in deny_prefixes


class TestNormalizationRules:
    """Tests for SPDX normalization rules."""

    def test_cc0_normalization(self):
        """CC0 variations should normalize correctly."""
        lm = load_license_map()
        rules = lm["normalization"]["rules"]

        # Find CC0 rule
        cc0_rule = next((r for r in rules if r.get("spdx") == "CC0-1.0"), None)
        assert cc0_rule is not None
        assert any("CC0" in m for m in cc0_rule.get("match_any", []))

    def test_cc_by_normalization(self):
        """CC-BY variations should normalize correctly."""
        lm = load_license_map()
        rules = lm["normalization"]["rules"]

        # Find CC-BY-4.0 rule
        ccby_rule = next((r for r in rules if r.get("spdx") == "CC-BY-4.0"), None)
        assert ccby_rule is not None
        assert any("CC-BY 4.0" in m or "CC BY 4.0" in m for m in ccby_rule.get("match_any", []))

    def test_restrictive_detection(self):
        """Restrictive license patterns should be detected."""
        lm = load_license_map()
        rules = lm["normalization"]["rules"]

        # Find non-commercial rule
        nc_rule = next((r for r in rules if "NC" in str(r.get("spdx", ""))), None)
        assert nc_rule is not None
        assert any("non-commercial" in m.lower() or "noncommercial" in m.lower()
                   for m in nc_rule.get("match_any", []))


class TestRestrictionPhrases:
    """Tests for restriction phrase scanning."""

    def test_ai_restriction_phrases(self):
        """AI/ML restriction phrases should be present."""
        lm = load_license_map()
        phrases = lm["restriction_scan"]["phrases"]
        phrases_lower = [p.lower() for p in phrases]

        assert any("no ai" in p for p in phrases_lower)
        assert any("no machine learning" in p or "no ml" in p for p in phrases_lower)
        assert any("no llm" in p or "no large language model" in p for p in phrases_lower)

    def test_tdm_restriction_phrases(self):
        """Text and data mining restriction phrases should be present."""
        lm = load_license_map()
        phrases = lm["restriction_scan"]["phrases"]
        phrases_lower = [p.lower() for p in phrases]

        assert any("no text and data mining" in p or "no tdm" in p for p in phrases_lower)
        assert any("no text mining" in p for p in phrases_lower)

    def test_commercial_restriction_phrases(self):
        """Commercial use restriction phrases should be present."""
        lm = load_license_map()
        phrases = lm["restriction_scan"]["phrases"]
        phrases_lower = [p.lower() for p in phrases]

        assert any("no commercial use" in p or "non-commercial use only" in p
                   for p in phrases_lower)


class TestGatingBehavior:
    """Tests for license gating behavior configuration."""

    def test_gating_defaults(self):
        """Gating configuration should have proper defaults."""
        lm = load_license_map()
        gating = lm["gating"]

        assert gating["conditional_spdx_bucket"] == "YELLOW"
        assert gating["unknown_spdx_bucket"] == "YELLOW"
        assert gating["deny_spdx_bucket"] == "RED"
        assert gating["restriction_phrase_bucket"] == "YELLOW"

    def test_profile_defaults(self):
        """Profile defaults should map correctly to buckets."""
        lm = load_license_map()
        profiles = lm["profiles"]

        assert profiles["permissive"]["default_bucket"] == "GREEN"
        assert profiles["copyleft"]["default_bucket"] == "YELLOW"
        assert profiles["record_level"]["default_bucket"] == "YELLOW"
        assert profiles["deny"]["default_bucket"] == "RED"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
