from __future__ import annotations

from collector_core.checks.implementations import pii_detect


def test_pii_detect_no_pii() -> None:
    record = {"text": "This is a normal document with no personal information."}
    result = pii_detect.check(record, {})

    assert result["status"] == "ok"
    assert result["match_count"] == 0


def test_pii_detect_finds_email() -> None:
    record = {"text": "Contact us at support@example.com for help."}
    result = pii_detect.check(record, {})

    assert result["match_count"] >= 1
    assert "emails" in result
    assert result["email_count"] == 1


def test_pii_detect_finds_phone() -> None:
    record = {"text": "Call us at 555-123-4567 for help."}
    result = pii_detect.check(record, {})

    assert result["match_count"] >= 1
    assert "phones" in result
    assert result["phone_count"] >= 1


def test_pii_detect_finds_ssn() -> None:
    record = {"text": "SSN: 123-45-6789 is not valid."}
    result = pii_detect.check(record, {})

    assert result["match_count"] >= 1
    assert "ssns" in result
    assert result["ssn_count"] == 1


def test_pii_detect_finds_credit_card() -> None:
    record = {"text": "Visa card: 4111111111111111"}
    result = pii_detect.check(record, {})

    assert result["match_count"] >= 1
    assert "credit_cards" in result
    assert result["credit_card_count"] == 1


def test_pii_detect_finds_ip_address() -> None:
    record = {"text": "Server IP: 192.168.1.1"}
    result = pii_detect.check(record, {})

    assert result["match_count"] >= 1
    assert "ip_addresses" in result
    assert result["ip_address_count"] == 1


def test_pii_detect_masks_values() -> None:
    record = {"text": "Email: john.doe@example.com"}
    result = pii_detect.check(record, {})

    # Check that the email is masked
    assert result["emails"][0] != "john.doe@example.com"
    assert "***" in result["emails"][0]


def test_pii_detect_empty_text() -> None:
    record = {"text": ""}
    result = pii_detect.check(record, {})

    assert result["status"] == "ok"
    assert result["match_count"] == 0


def test_pii_detect_no_text_field() -> None:
    record = {}
    result = pii_detect.check(record, {})

    assert result["status"] == "ok"
    assert result["match_count"] == 0


def test_pii_detect_uses_content_field() -> None:
    record = {"content": "Email: test@example.org"}
    result = pii_detect.check(record, {})

    assert result["match_count"] >= 1


def test_pii_detect_respects_action_config() -> None:
    record = {"text": "Email: test@example.org"}
    result = pii_detect.check(record, {"action": "block"})

    assert result["status"] == "block"
    assert result["action"] == "block"


def test_pii_detect_multiple_types() -> None:
    record = {
        "text": (
            "Contact: john@example.com, phone: 555-123-4567, "
            "SSN: 123-45-6789, IP: 10.0.0.1"
        )
    }
    result = pii_detect.check(record, {})

    assert result["match_count"] == 4
    assert "emails" in result
    assert "phones" in result
    assert "ssns" in result
    assert "ip_addresses" in result
