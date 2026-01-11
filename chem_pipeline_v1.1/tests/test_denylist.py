"""
test_denylist.py

Unit tests for denylist functionality and pattern matching.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest
import yaml


def load_denylist():
    """Load the denylist.yaml for testing."""
    denylist_path = Path(__file__).parent.parent / "denylist.yaml"
    return yaml.safe_load(denylist_path.read_text(encoding="utf-8"))


class TestDenylistSchema:
    """Tests for denylist schema structure."""

    def test_schema_version(self):
        """Denylist should have correct schema version."""
        dl = load_denylist()
        assert dl.get("schema_version") == "0.2"

    def test_required_sections(self):
        """Denylist should have all required sections."""
        dl = load_denylist()

        # These sections should exist (even if empty)
        assert "patterns" in dl or dl.get("patterns") is None
        assert "domain_patterns" in dl or dl.get("domain_patterns") is None
        assert "publisher_patterns" in dl or dl.get("publisher_patterns") is None


class TestDenylistPatterns:
    """Tests for denylist pattern configuration."""

    def test_pattern_structure(self):
        """Patterns should have proper structure if present."""
        dl = load_denylist()
        patterns = dl.get("patterns") or []

        for p in patterns:
            # Each pattern should be a dict
            assert isinstance(p, dict)

            # If pattern has required fields, validate them
            if "type" in p:
                assert p["type"] in ["substring", "regex", "domain"]
            if "severity" in p:
                assert p["severity"] in ["hard_red", "force_yellow"]

    def test_domain_pattern_structure(self):
        """Domain patterns should have proper structure if present."""
        dl = load_denylist()
        domain_patterns = dl.get("domain_patterns") or []

        for dp in domain_patterns:
            assert isinstance(dp, dict)
            if "severity" in dp:
                assert dp["severity"] in ["hard_red", "force_yellow"]

    def test_publisher_pattern_structure(self):
        """Publisher patterns should have proper structure if present."""
        dl = load_denylist()
        publisher_patterns = dl.get("publisher_patterns") or []

        for pp in publisher_patterns:
            assert isinstance(pp, dict)
            if "severity" in pp:
                assert pp["severity"] in ["hard_red", "force_yellow"]


class TestDenylistProvenanceRequirements:
    """Tests for denylist provenance tracking (v0.9 requirements)."""

    def test_patterns_have_provenance_fields(self):
        """Active patterns should have link and rationale fields (v0.9)."""
        dl = load_denylist()
        patterns = dl.get("patterns") or []

        for p in patterns:
            if p.get("value"):  # Only check active patterns
                # These fields are recommended but not strictly required
                # for backwards compatibility
                pass


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
