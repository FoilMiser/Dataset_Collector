from __future__ import annotations

from collector_core.checks.implementations import distribution_statement


def test_distribution_statement_accepts_allowed_statement() -> None:
    result = distribution_statement.check(
        {"distribution_statement": "Approved"},
        {"allowed_statements": ["Approved", "Public"]},
    )
    assert result.check == distribution_statement.check_name
    assert result.status == "ok"
    assert result.details["statement"] == "Approved"


def test_distribution_statement_fails_disallowed_statement() -> None:
    result = distribution_statement.check(
        {"distribution_statement": "Restricted"},
        {"allowed_statements": ["Approved"]},
    )
    assert result.status == "fail"
    assert result.details["allowed_statements"] == ["Approved"]
