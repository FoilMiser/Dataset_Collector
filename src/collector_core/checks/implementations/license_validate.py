"""License validation content check."""

from __future__ import annotations

import re
from typing import Any

from collector_core.checks.implementations.base import CheckResult

check_name = "license_validate"

# SPDX License Identifier pattern
SPDX_PATTERN = re.compile(
    r"SPDX-License-Identifier:\s*([A-Za-z0-9\-\.+]+(?:\s+(?:AND|OR|WITH)\s+[A-Za-z0-9\-\.+]+)*)",
    re.IGNORECASE,
)

# Common permissive licenses
PERMISSIVE_LICENSES = {
    "MIT", "Apache-2.0", "BSD-2-Clause", "BSD-3-Clause", "ISC",
    "Unlicense", "CC0-1.0", "WTFPL", "Zlib", "0BSD",
}

# Copyleft licenses (require careful handling)
COPYLEFT_LICENSES = {
    "GPL-2.0", "GPL-3.0", "LGPL-2.1", "LGPL-3.0", "AGPL-3.0",
    "MPL-2.0", "EPL-1.0", "EPL-2.0",
}

# Non-commercial / restrictive
RESTRICTIVE_LICENSES = {
    "CC-BY-NC-4.0", "CC-BY-NC-SA-4.0", "CC-BY-NC-ND-4.0",
    "Proprietary", "All-Rights-Reserved",
}


def extract_spdx(text: str) -> str | None:
    """Extract SPDX license identifier from text."""
    match = SPDX_PATTERN.search(text[:2000])  # Check header only
    if match:
        return match.group(1)
    return None


def classify_license(spdx_id: str) -> str:
    """Classify a license as permissive, copyleft, or restrictive."""
    normalized = spdx_id.upper().replace("_", "-")
    
    for license_id in PERMISSIVE_LICENSES:
        if license_id.upper() in normalized:
            return "permissive"
    
    for license_id in COPYLEFT_LICENSES:
        if license_id.upper() in normalized:
            return "copyleft"
    
    for license_id in RESTRICTIVE_LICENSES:
        if license_id.upper() in normalized:
            return "restrictive"
    
    return "unknown"


def check(record: dict[str, Any], config: dict[str, Any]) -> CheckResult:
    """Run license validation check.
    
    Config options:
        allowed_categories: List of allowed categories [permissive, copyleft, restrictive]
        allowed_spdx: List of specific allowed SPDX identifiers
        deny_spdx: List of denied SPDX identifiers
        require_license: Whether a license is required (default: False)
    """
    text = record.get("text", "") or record.get("content", "") or ""
    record_license = record.get("license") or record.get("license_spdx")
    
    # Try to extract from text if not in record
    spdx_id = record_license or extract_spdx(text)
    
    require_license = config.get("require_license", False)
    allowed_categories = config.get("allowed_categories", ["permissive", "copyleft"])
    allowed_spdx = config.get("allowed_spdx", [])
    deny_spdx = config.get("deny_spdx", [])
    
    details: dict[str, Any] = {"detected_spdx": spdx_id}
    
    if not spdx_id:
        if require_license:
            return CheckResult(
                passed=False, action="flag", reason="no_license_detected",
                details=details, confidence=0.5,
            )
        return CheckResult(
            passed=True, action="keep", reason="no_license_required",
            details=details, confidence=0.5,
        )
    
    # Check deny list first
    if any(d.lower() in spdx_id.lower() for d in deny_spdx):
        return CheckResult(
            passed=False, action="reject", reason=f"denied_license: {spdx_id}",
            details=details, confidence=1.0,
        )
    
    # Check allow list if specified
    if allowed_spdx:
        if any(a.lower() in spdx_id.lower() for a in allowed_spdx):
            details["license_category"] = classify_license(spdx_id)
            return CheckResult(
                passed=True, action="keep", details=details, confidence=1.0,
            )
        return CheckResult(
            passed=False, action="filter", reason=f"license_not_in_allowlist: {spdx_id}",
            details=details, confidence=1.0,
        )
    
    # Check category
    category = classify_license(spdx_id)
    details["license_category"] = category
    
    if category in allowed_categories:
        return CheckResult(passed=True, action="keep", details=details, confidence=0.9)
    
    if category == "unknown":
        return CheckResult(
            passed=False, action="flag", reason=f"unknown_license: {spdx_id}",
            details=details, confidence=0.6,
        )
    
    return CheckResult(
        passed=False, action="filter",
        reason=f"license_category_not_allowed: {category}",
        details=details, confidence=0.9,
    )


__all__ = ["check_name", "check", "extract_spdx", "classify_license"]
