from __future__ import annotations

from collector_core.yellow.domains import code


def test_code_allows_legitimate_code(domain_ctx) -> None:
    raw = {
        "content": '''
# SPDX-License-Identifier: MIT

"""Example module with docstring."""

def test_function(param: str) -> bool:
    """Test function with type hints."""
    try:
        return len(param) > 0
    except Exception:
        return False
''',
        "license": "MIT",
    }

    decision = code.filter_record(raw, domain_ctx)

    assert decision.allow is True
    assert decision.extra is not None
    license_info = decision.extra.get("license_info", {})
    assert license_info.get("has_license_header") is True
    assert license_info.get("detected_spdx") == "MIT"


def test_code_extracts_spdx_license(domain_ctx) -> None:
    raw = {
        "content": "// SPDX-License-Identifier: Apache-2.0\npackage main",
        "license": "Apache-2.0",
    }

    decision = code.filter_record(raw, domain_ctx)

    assert decision.allow is True
    assert decision.extra is not None
    license_info = decision.extra.get("license_info", {})
    assert license_info.get("detected_spdx") == "Apache-2.0"
    assert license_info.get("confidence") == 1.0


def test_code_detects_license_from_header(domain_ctx) -> None:
    raw = {
        "content": '''
/*
 * Licensed under the Apache License, Version 2.0
 */

public class Example {}
''',
        "license": "Apache-2.0",
    }

    decision = code.filter_record(raw, domain_ctx)

    assert decision.allow is True
    assert decision.extra is not None
    license_info = decision.extra.get("license_info", {})
    assert license_info.get("has_license_header") is True
    assert license_info.get("detected_license") == "Apache-2.0"


def test_code_rejects_malware_patterns(domain_ctx) -> None:
    raw = {
        "content": "import os; eval(request.GET['cmd'])",
        "license": "MIT",
    }

    decision = code.filter_record(raw, domain_ctx)

    assert decision.allow is False
    assert decision.reason == "malware_pattern_detected"
    assert decision.extra is not None
    assert decision.extra["rejection_type"] == "security"


def test_code_rejects_sql_injection(domain_ctx) -> None:
    raw = {
        "content": "query = 'SELECT * FROM users WHERE id=' + ' OR 1=1 --",
        "license": "MIT",
    }

    decision = code.filter_record(raw, domain_ctx)

    assert decision.allow is False
    assert decision.reason == "malware_pattern_detected"


def test_code_flags_secrets(domain_ctx) -> None:
    raw = {
        "content": '''
API_KEY = "sk_live_abcdefghij1234567890"
AWS_ACCESS_KEY_ID = "AKIAIOSFODNN7EXAMPLE"
''',
        "license": "MIT",
    }

    decision = code.filter_record(raw, domain_ctx)

    # Secrets are flagged but not auto-rejected
    assert decision.extra is not None
    assert decision.extra["secrets_found"] >= 1
    assert "api_key" in decision.extra["secret_types"] or "aws_access_key" in decision.extra["secret_types"]


def test_code_flags_private_key(domain_ctx) -> None:
    raw = {
        "content": '''
-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEA0Z3VS5JJcds3...
-----END RSA PRIVATE KEY-----
''',
        "license": "MIT",
    }

    decision = code.filter_record(raw, domain_ctx)

    assert decision.extra is not None
    assert decision.extra["secrets_found"] >= 1
    assert "private_key" in decision.extra["secret_types"]


def test_code_assesses_quality(domain_ctx) -> None:
    raw = {
        "content": '''
"""Module with docstring."""

import logging

logger = logging.getLogger(__name__)

def example_function(param: str) -> bool:
    """Function with docstring."""
    try:
        logger.info("Processing")
        return True
    except Exception:
        raise ValueError("Error")

def test_example():
    assert example_function("test")
''',
        "license": "MIT",
    }

    decision = code.filter_record(raw, domain_ctx)

    assert decision.allow is True
    assert decision.extra is not None
    quality = decision.extra.get("quality_info", {})
    assert quality.get("has_tests") is True
    assert quality.get("has_docstrings") is True
    assert quality.get("has_logging") is True
    assert quality.get("has_error_handling") is True


def test_code_transform_adds_license(domain_ctx) -> None:
    raw = {
        "content": "// SPDX-License-Identifier: MIT\nconst x = 1;",
        "license": "MIT",
    }

    decision = code.filter_record(raw, domain_ctx)
    result = code.transform_record(raw, domain_ctx, decision, license_profile="permissive")

    assert result is not None
    assert "screening" in result
    assert result["screening"]["domain"] == "code"
    assert result.get("detected_spdx") == "MIT"


def test_extract_license_info_no_license() -> None:
    result = code.extract_license_info("def main(): pass")
    assert result["has_license_header"] is False
    assert result["detected_spdx"] is None
    assert result["detected_license"] is None


def test_detect_secrets_finds_api_key() -> None:
    findings = code.detect_secrets('api_key = "abcdefghij1234567890abcd"')
    assert len(findings) >= 1
    assert any(f["type"] == "api_key" for f in findings)


def test_detect_malware_patterns_finds_shell_injection() -> None:
    findings = code.detect_malware_patterns("os.system('; rm -rf /')")
    assert len(findings) >= 1


def test_assess_code_quality() -> None:
    code_text = '''
def test_foo():
    """Docstring."""
    pass
'''
    result = code.assess_code_quality(code_text)
    assert result["has_tests"] is True
    assert result["has_docstrings"] is True
    assert result["quality_score"] >= 2
