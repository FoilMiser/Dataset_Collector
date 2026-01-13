"""PII detection content check.

This module provides heuristic detection of personally identifiable
information (PII) in text content, including:
- Email addresses
- Phone numbers
- Social Security Numbers (SSN)
- Credit card numbers
- IP addresses

The check returns a result with match counts and masked examples.
"""

from __future__ import annotations

import re
from typing import Any

# Patterns for PII detection
EMAIL_PATTERN = re.compile(
    r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b",
    re.IGNORECASE,
)

PHONE_PATTERN = re.compile(
    r"\b(?:\+?1[\s.-]?)?(?:\(\s*\d{3}\s*\)|\d{3})[\s.-]?\d{3}[\s.-]?\d{4}\b"
)

SSN_PATTERN = re.compile(r"\b\d{3}-\d{2}-\d{4}\b")

# Credit card patterns (Visa, MasterCard, Amex, Discover)
CREDIT_CARD_PATTERN = re.compile(
    r"\b(?:4[0-9]{12}(?:[0-9]{3})?|"  # Visa
    r"5[1-5][0-9]{14}|"  # MasterCard
    r"3[47][0-9]{13}|"  # Amex
    r"6(?:011|5[0-9]{2})[0-9]{12})\b"  # Discover
)

# IP address pattern
IP_ADDRESS_PATTERN = re.compile(
    r"\b(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}"
    r"(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\b"
)

check_name = "pii_detect"


def _mask_value(value: str, keep: int = 2) -> str:
    """Mask a PII value, keeping only a few characters visible."""
    if len(value) <= keep * 2:
        return "*" * len(value)
    return f"{value[:keep]}***{value[-keep:]}"


def _get_text_content(record: dict[str, Any]) -> str:
    """Extract text content from a record."""
    return (
        record.get("text", "")
        or record.get("content", "")
        or record.get("body", "")
        or ""
    )


def check(record: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
    """Run PII detection check on a record.

    Args:
        record: The record to check.
        config: Check configuration.

    Returns:
        Check result with status and findings.
    """
    text = _get_text_content(record)

    if not text:
        return {
            "status": "ok",
            "match_count": 0,
            "message": "No text content to check",
        }

    # Find matches for each pattern
    emails = EMAIL_PATTERN.findall(text)
    phones = PHONE_PATTERN.findall(text)
    ssns = SSN_PATTERN.findall(text)
    credit_cards = CREDIT_CARD_PATTERN.findall(text)
    ip_addresses = IP_ADDRESS_PATTERN.findall(text)

    total_matches = (
        len(emails) + len(phones) + len(ssns) +
        len(credit_cards) + len(ip_addresses)
    )

    if total_matches == 0:
        return {
            "status": "ok",
            "match_count": 0,
            "message": "No PII detected",
        }

    # Determine action based on config
    action = config.get("action", "warn")
    status = "warn" if action == "warn" else action

    # Build findings summary with masked values
    findings: dict[str, Any] = {
        "status": status,
        "action": action,
        "match_count": total_matches,
    }

    if emails:
        findings["emails"] = [_mask_value(e) for e in emails[:5]]
        findings["email_count"] = len(emails)

    if phones:
        findings["phones"] = [_mask_value(p) for p in phones[:5]]
        findings["phone_count"] = len(phones)

    if ssns:
        findings["ssns"] = [_mask_value(s) for s in ssns[:5]]
        findings["ssn_count"] = len(ssns)

    if credit_cards:
        findings["credit_cards"] = [_mask_value(c) for c in credit_cards[:5]]
        findings["credit_card_count"] = len(credit_cards)

    if ip_addresses:
        findings["ip_addresses"] = ip_addresses[:5]
        findings["ip_address_count"] = len(ip_addresses)

    return findings


__all__ = ["check", "check_name"]
