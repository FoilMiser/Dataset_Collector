"""Distribution statement extraction check."""

from __future__ import annotations

import re
from typing import Any

from collector_core.checks.implementations.base import CheckResult

check_name = "distribution_statement"

# US Government distribution statement patterns
DISTRIBUTION_PATTERNS = {
    "A": re.compile(
        r"Distribution\s+Statement\s+A[:\.]?\s*"
        r"(?:Approved\s+for\s+public\s+release[;,]?\s*(?:distribution\s+(?:is\s+)?unlimited)?)?",
        re.IGNORECASE,
    ),
    "B": re.compile(
        r"Distribution\s+Statement\s+B[:\.]?\s*"
        r"(?:Distribution\s+authorized\s+to\s+U\.?S\.?\s+Government\s+agencies)?",
        re.IGNORECASE,
    ),
    "C": re.compile(
        r"Distribution\s+Statement\s+C[:\.]?\s*"
        r"(?:Distribution\s+authorized\s+to\s+U\.?S\.?\s+Government\s+agencies\s+and\s+their\s+contractors)?",
        re.IGNORECASE,
    ),
    "D": re.compile(
        r"Distribution\s+Statement\s+D[:\.]?\s*"
        r"(?:Distribution\s+authorized\s+to\s+(?:the\s+)?Department\s+of\s+Defense)?",
        re.IGNORECASE,
    ),
    "E": re.compile(
        r"Distribution\s+Statement\s+E[:\.]?\s*"
        r"(?:Distribution\s+authorized\s+to\s+(?:the\s+)?DoD\s+components)?",
        re.IGNORECASE,
    ),
    "F": re.compile(
        r"Distribution\s+Statement\s+F[:\.]?\s*"
        r"(?:Further\s+dissemination\s+only\s+as\s+directed)?",
        re.IGNORECASE,
    ),
}

# Export control patterns
EXPORT_CONTROL_PATTERNS = [
    re.compile(r"\bITAR\b", re.IGNORECASE),
    re.compile(r"\bEAR\b"),
    re.compile(r"\bexport\s+controlled?\b", re.IGNORECASE),
    re.compile(r"\bCUI\b"),  # Controlled Unclassified Information
    re.compile(r"\bFOUO\b"),  # For Official Use Only
]

# Classification markings
CLASSIFICATION_PATTERNS = [
    re.compile(r"\b(SECRET|TOP\s+SECRET|CONFIDENTIAL)\b"),
    re.compile(r"\bUNCLASSIFIED\b"),
    re.compile(r"\bCLASSIFIED\b"),
]


def extract_distribution_statement(text: str) -> dict[str, Any]:
    """Extract distribution statement information from text."""
    result: dict[str, Any] = {
        "statement_type": None,
        "is_public": False,
        "is_restricted": False,
        "export_controlled": False,
        "classification_marking": None,
    }
    
    # Check for distribution statements
    for stmt_type, pattern in DISTRIBUTION_PATTERNS.items():
        if pattern.search(text):
            result["statement_type"] = stmt_type
            result["is_public"] = (stmt_type == "A")
            result["is_restricted"] = (stmt_type in "BCDEF")
            break
    
    # Check export control
    for pattern in EXPORT_CONTROL_PATTERNS:
        if pattern.search(text):
            result["export_controlled"] = True
            break
    
    # Check classification markings
    for pattern in CLASSIFICATION_PATTERNS:
        match = pattern.search(text)
        if match:
            result["classification_marking"] = match.group(0).upper()
            break
    
    return result


def check(record: dict[str, Any], config: dict[str, Any]) -> CheckResult:
    """Run distribution statement check.
    
    Config options:
        require_public: Require Distribution Statement A (default: False)
        reject_restricted: Reject restricted distribution (B-F) (default: True)
        reject_export_controlled: Reject export controlled content (default: True)
        reject_classified: Reject classified content (default: True)
    """
    text = record.get("text", "") or record.get("content", "") or ""
    
    require_public = config.get("require_public", False)
    reject_restricted = config.get("reject_restricted", True)
    reject_export_controlled = config.get("reject_export_controlled", True)
    reject_classified = config.get("reject_classified", True)
    
    dist_info = extract_distribution_statement(text)
    details = {"distribution_info": dist_info}
    
    # Check classification first
    if reject_classified and dist_info["classification_marking"]:
        if dist_info["classification_marking"] not in ("UNCLASSIFIED",):
            return CheckResult(
                passed=False, action="reject",
                reason=f"classified_content: {dist_info['classification_marking']}",
                details=details, confidence=1.0,
            )
    
    # Check export control
    if reject_export_controlled and dist_info["export_controlled"]:
        return CheckResult(
            passed=False, action="reject",
            reason="export_controlled_content",
            details=details, confidence=0.95,
        )
    
    # Check restricted distribution
    if reject_restricted and dist_info["is_restricted"]:
        return CheckResult(
            passed=False, action="reject",
            reason=f"restricted_distribution: Statement {dist_info['statement_type']}",
            details=details, confidence=1.0,
        )
    
    # Check if public release is required
    if require_public and not dist_info["is_public"]:
        if dist_info["statement_type"]:
            return CheckResult(
                passed=False, action="filter",
                reason="not_public_release",
                details=details, confidence=0.9,
            )
    
    return CheckResult(passed=True, action="keep", details=details, confidence=0.9)


__all__ = ["check_name", "check", "extract_distribution_statement"]
