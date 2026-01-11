from __future__ import annotations

from typing import Any


def normalize_content_check_actions(
    actions: dict[str, Any] | None,
) -> dict[str, str]:
    if not isinstance(actions, dict):
        return {}
    normalized: dict[str, str] = {}
    for key, value in actions.items():
        key_name = str(key).strip()
        value_name = str(value).strip().lower()
        if key_name and value_name:
            normalized[key_name] = value_name
    return normalized


def resolve_content_check_action(
    results: list[dict[str, Any]],
) -> tuple[str | None, list[str]]:
    action: str | None = None
    checks: list[str] = []
    for result in results:
        status = str(result.get("status", "") or "").lower()
        if status not in {"block", "quarantine"}:
            continue
        if status == "block":
            action = "block"
        elif action != "block":
            action = "quarantine"
        check_name = str(result.get("check", "") or "").strip()
        if check_name:
            checks.append(check_name)
    return action, checks
