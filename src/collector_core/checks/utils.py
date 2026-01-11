from __future__ import annotations

import json
from typing import Any

from collector_core.checks.base import CheckContext

MAX_TEXT_LENGTH = 200_000


def _collect_strings(value: Any, output: list[str]) -> None:
    if value is None:
        return
    if isinstance(value, str):
        if value:
            output.append(value)
        return
    if isinstance(value, bytes):
        try:
            output.append(value.decode("utf-8", errors="ignore"))
        except Exception:
            return
        return
    if isinstance(value, dict):
        for key, item in value.items():
            if isinstance(key, str):
                output.append(key)
            _collect_strings(item, output)
        return
    if isinstance(value, (list, tuple, set)):
        for item in value:
            _collect_strings(item, output)
        return
    try:
        output.append(str(value))
    except Exception:
        return


def build_text_blob(ctx: CheckContext) -> str:
    parts: list[str] = []
    _collect_strings(ctx.target, parts)
    _collect_strings(ctx.row, parts)
    _collect_strings(ctx.extra, parts)
    blob = "\n".join(p for p in parts if p)
    if len(blob) > MAX_TEXT_LENGTH:
        return blob[:MAX_TEXT_LENGTH]
    return blob


def resolve_check_action(ctx: CheckContext, check_name: str) -> str | None:
    action: str | None = None
    for source in (ctx.extra, ctx.target, ctx.row):
        if not isinstance(source, dict):
            continue
        actions = source.get("content_check_actions")
        if isinstance(actions, str):
            try:
                actions = json.loads(actions)
            except Exception:
                actions = None
        if isinstance(actions, dict) and check_name in actions:
            value = str(actions[check_name]).strip().lower()
            if value:
                action = value
    return action
