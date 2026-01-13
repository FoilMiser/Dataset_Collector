"""Tests for distribution statement content check."""

from __future__ import annotations

import pytest
from collector_core.checks.implementations.distribution_statement import (
    check, extract_distribution_statement, check_name,
)
from collector_core.checks.implementations.base import CheckResult


class TestExtractDistributionStatement:
    """Tests for extract_distribution_statement function."""

    def test_statement_a_public(self) -> None:
        """Distribution Statement A is detected as public."""
        text = "Distribution Statement A: Approved for public release."
        result = extract_distribution_statement(text)
        assert result["statement_type"] == "A"
        assert result["is_public"] is True
        assert result["is_restricted"] is False

    def test_statement_b_restricted(self) -> None:
        """Distribution Statement B is detected as restricted."""
        text = "Distribution Statement B: Distribution authorized to U.S. Government agencies."
        result = extract_distribution_statement(text)
        assert result["statement_type"] == "B"
        assert result["is_public"] is False
        assert result["is_restricted"] is True

    def test_statement_c_restricted(self) -> None:
        """Distribution Statement C is detected."""
        text = "Distribution Statement C."
        result = extract_distribution_statement(text)
        assert result["statement_type"] == "C"
        assert result["is_restricted"] is True

    def test_no_statement(self) -> None:
        """No statement returns None."""
        text = "Regular content without distribution statement."
        result = extract_distribution_statement(text)
        assert result["statement_type"] is None

    def test_export_control_itar(self) -> None:
        """ITAR is detected."""
        text = "This document is controlled under ITAR regulations."
        result = extract_distribution_statement(text)
        assert result["export_controlled"] is True

    def test_export_control_ear(self) -> None:
        """EAR is detected."""
        text = "Subject to EAR export controls."
        result = extract_distribution_statement(text)
        assert result["export_controlled"] is True

    def test_classification_secret(self) -> None:
        """SECRET classification is detected."""
        text = "SECRET - This document is classified."
        result = extract_distribution_statement(text)
        assert result["classification_marking"] == "SECRET"

    def test_classification_unclassified(self) -> None:
        """UNCLASSIFIED marking is detected."""
        text = "UNCLASSIFIED - For public release."
        result = extract_distribution_statement(text)
        assert result["classification_marking"] == "UNCLASSIFIED"

    def test_cui_marking(self) -> None:
        """CUI is detected as export controlled."""
        text = "This document contains CUI information."
        result = extract_distribution_statement(text)
        assert result["export_controlled"] is True


class TestDistributionCheck:
    """Tests for check function."""

    def test_public_release_passes(self) -> None:
        """Public release content passes."""
        record = {"text": "Distribution Statement A: Approved for public release."}
        config = {}

        result = check(record, config)

        assert result.passed is True
        assert result.action == "keep"

    def test_restricted_distribution_rejects(self) -> None:
        """Restricted distribution rejects by default."""
        record = {"text": "Distribution Statement B: Authorized to US Government."}
        config = {}

        result = check(record, config)

        assert result.passed is False
        assert result.action == "reject"
        assert "restricted_distribution" in (result.reason or "")

    def test_allow_restricted(self) -> None:
        """Restricted can be allowed with config."""
        record = {"text": "Distribution Statement B."}
        config = {"reject_restricted": False}

        result = check(record, config)

        assert result.passed is True

    def test_export_controlled_rejects(self) -> None:
        """Export controlled content rejects by default."""
        record = {"text": "This document is controlled under ITAR."}
        config = {}

        result = check(record, config)

        assert result.passed is False
        assert result.action == "reject"
        assert "export_controlled" in (result.reason or "")

    def test_allow_export_controlled(self) -> None:
        """Export controlled can be allowed with config."""
        record = {"text": "Subject to EAR."}
        config = {"reject_export_controlled": False}

        result = check(record, config)

        assert result.passed is True

    def test_classified_rejects(self) -> None:
        """Classified content rejects by default."""
        record = {"text": "SECRET - Classified information."}
        config = {}

        result = check(record, config)

        assert result.passed is False
        assert result.action == "reject"
        assert "classified" in (result.reason or "")

    def test_unclassified_passes(self) -> None:
        """UNCLASSIFIED marking passes."""
        record = {"text": "UNCLASSIFIED - Public document."}
        config = {}

        result = check(record, config)

        assert result.passed is True

    def test_require_public(self) -> None:
        """Require public release can be enforced."""
        record = {"text": "Distribution Statement B."}
        config = {"require_public": True, "reject_restricted": False}

        result = check(record, config)

        # Should fail because not public, but not reject because restricted allowed
        assert result.passed is False
        assert result.action == "filter"

    def test_no_statement_passes(self) -> None:
        """Content without statement passes by default."""
        record = {"text": "Normal content without any distribution markings."}
        config = {}

        result = check(record, config)

        assert result.passed is True

    def test_details_include_distribution_info(self) -> None:
        """Details include distribution information."""
        record = {"text": "Distribution Statement A: Public release."}
        config = {}

        result = check(record, config)

        assert "distribution_info" in result.details
        dist_info = result.details["distribution_info"]
        assert "statement_type" in dist_info
        assert "is_public" in dist_info


class TestCheckName:
    """Test check name constant."""

    def test_check_name(self) -> None:
        """Check name is set correctly."""
        assert check_name == "distribution_statement"
