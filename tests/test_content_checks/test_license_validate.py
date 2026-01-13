"""Tests for license validation content check."""

from __future__ import annotations

import pytest
from collector_core.checks.implementations.license_validate import (
    check, extract_spdx, classify_license, check_name,
)
from collector_core.checks.implementations.base import CheckResult


class TestExtractSpdx:
    """Tests for extract_spdx function."""

    def test_extracts_spdx_identifier(self) -> None:
        """SPDX identifier is extracted."""
        text = "// SPDX-License-Identifier: MIT\nsome code"
        result = extract_spdx(text)
        assert result == "MIT"

    def test_extracts_compound_spdx(self) -> None:
        """Compound SPDX expressions are extracted."""
        text = "# SPDX-License-Identifier: MIT OR Apache-2.0"
        result = extract_spdx(text)
        assert result == "MIT OR Apache-2.0"

    def test_extracts_with_exception(self) -> None:
        """SPDX with exception is extracted."""
        text = "/* SPDX-License-Identifier: GPL-2.0 WITH Classpath-exception-2.0 */"
        result = extract_spdx(text)
        assert "GPL-2.0" in result
        assert "WITH" in result

    def test_no_spdx_returns_none(self) -> None:
        """Missing SPDX returns None."""
        text = "This file has no license information."
        result = extract_spdx(text)
        assert result is None

    def test_case_insensitive(self) -> None:
        """SPDX extraction is case insensitive."""
        text = "spdx-license-identifier: mit"
        result = extract_spdx(text)
        assert result.lower() == "mit"


class TestClassifyLicense:
    """Tests for classify_license function."""

    def test_permissive_licenses(self) -> None:
        """Permissive licenses are classified correctly."""
        assert classify_license("MIT") == "permissive"
        assert classify_license("Apache-2.0") == "permissive"
        assert classify_license("BSD-3-Clause") == "permissive"
        assert classify_license("CC0-1.0") == "permissive"

    def test_copyleft_licenses(self) -> None:
        """Copyleft licenses are classified correctly."""
        assert classify_license("GPL-3.0") == "copyleft"
        assert classify_license("LGPL-2.1") == "copyleft"
        assert classify_license("AGPL-3.0") == "copyleft"

    def test_restrictive_licenses(self) -> None:
        """Restrictive licenses are classified correctly."""
        assert classify_license("CC-BY-NC-4.0") == "restrictive"
        assert classify_license("Proprietary") == "restrictive"

    def test_unknown_licenses(self) -> None:
        """Unknown licenses are classified as unknown."""
        assert classify_license("SomeRandomLicense") == "unknown"
        assert classify_license("XYZ-1.0") == "unknown"


class TestLicenseCheck:
    """Tests for check function."""

    def test_permissive_license_passes(self) -> None:
        """Permissive license passes default check."""
        record = {"text": "// SPDX-License-Identifier: MIT\ncode"}
        config = {}

        result = check(record, config)

        assert result.passed is True
        assert result.action == "keep"

    def test_copyleft_license_passes_default(self) -> None:
        """Copyleft license passes with default config."""
        record = {"license": "GPL-3.0"}
        config = {}

        result = check(record, config)

        assert result.passed is True

    def test_restrictive_license_fails_default(self) -> None:
        """Restrictive license fails with default config."""
        record = {"license": "CC-BY-NC-4.0"}
        config = {}

        result = check(record, config)

        assert result.passed is False

    def test_deny_list_rejects(self) -> None:
        """License on deny list is rejected."""
        record = {"license": "GPL-3.0"}
        config = {"deny_spdx": ["GPL"]}

        result = check(record, config)

        assert result.passed is False
        assert result.action == "reject"

    def test_allow_list_filters(self) -> None:
        """License not on allow list is filtered."""
        record = {"license": "Apache-2.0"}
        config = {"allowed_spdx": ["MIT"]}

        result = check(record, config)

        assert result.passed is False
        assert result.action == "filter"

    def test_allow_list_passes(self) -> None:
        """License on allow list passes."""
        record = {"license": "MIT"}
        config = {"allowed_spdx": ["MIT", "Apache-2.0"]}

        result = check(record, config)

        assert result.passed is True

    def test_missing_required_license(self) -> None:
        """Missing required license flags."""
        record = {"text": "code without license"}
        config = {"require_license": True}

        result = check(record, config)

        assert result.passed is False
        assert result.action == "flag"

    def test_missing_optional_license(self) -> None:
        """Missing optional license passes."""
        record = {"text": "code without license"}
        config = {"require_license": False}

        result = check(record, config)

        assert result.passed is True

    def test_license_from_record_field(self) -> None:
        """License extracted from record field."""
        record = {"text": "some content", "license_spdx": "MIT"}
        config = {}

        result = check(record, config)

        assert result.passed is True
        assert result.details["detected_spdx"] == "MIT"

    def test_unknown_license_flags(self) -> None:
        """Unknown license flags the record."""
        record = {"license": "SomeUnknownLicense"}
        config = {}

        result = check(record, config)

        assert result.passed is False
        assert result.action == "flag"


class TestCheckName:
    """Test check name constant."""

    def test_check_name(self) -> None:
        """Check name is set correctly."""
        assert check_name == "license_validate"
