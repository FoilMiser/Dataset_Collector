from __future__ import annotations

from collector_core.checks.actions import (
    normalize_content_check_actions,
    resolve_content_check_action,
)


def test_normalize_content_check_actions_filters_empty() -> None:
    actions = {"CheckA": "Block", "": "block", "CheckB": ""}
    normalized = normalize_content_check_actions(actions)
    assert normalized == {"CheckA": "block"}


def test_resolve_content_check_action_prioritizes_block() -> None:
    results = [
        {"check": "scan", "status": "quarantine"},
        {"check": "pii", "status": "block"},
        {"check": "other", "status": "ok"},
    ]
    action, checks = resolve_content_check_action(results)
    assert action == "block"
    assert checks == ["scan", "pii"]
