from __future__ import annotations

import json
from typing import Any

from collector_core.output_contract import normalize_output_record, validate_output_contract
from collector_core.stability import stable_api
from collector_core.utils import sha256_text, utc_now


@stable_api
def resolve_canonicalize_config(
    cfg: dict[str, Any],
    target_cfg: dict[str, Any] | None,
) -> tuple[list[str], int | None]:
    g = cfg.get("globals", {}) or {}
    g_canon = g.get("canonicalize", {}) or {}
    g_screen = g.get("screening", {}) or {}
    t_screen = (target_cfg.get("yellow_screen", {}) or {}) if target_cfg else {}
    t_canon = (target_cfg.get("canonicalize", {}) or {}) if target_cfg else {}
    candidates = list(
        t_canon.get("text_field_candidates")
        or t_screen.get("text_field_candidates")
        or g_canon.get("text_field_candidates")
        or g_screen.get("text_field_candidates")
        or ["text"]
    )
    max_chars_value = t_canon.get(
        "max_chars",
        t_screen.get("max_chars", g_canon.get("max_chars", g_screen.get("max_chars"))),
    )
    max_chars = int(max_chars_value) if max_chars_value is not None else None
    return candidates, max_chars


@stable_api
def coerce_text(value: Any) -> str:
    if isinstance(value, (list, tuple)):
        return "\n".join(map(str, value))
    return str(value)


@stable_api
def extract_text(row: dict[str, Any], candidates: list[str]) -> str | None:
    if "text" in row and row["text"]:
        return coerce_text(row["text"])
    for key in candidates:
        if key == "text":
            continue
        if key in row and row[key]:
            return coerce_text(row[key])
    string_fields = [v for v in row.values() if isinstance(v, str) and v]
    if string_fields:
        return "\n".join(string_fields)
    try:
        return json.dumps(row, ensure_ascii=False)
    except Exception:
        return str(row)


@stable_api
def resolve_routing(raw: dict[str, Any]) -> dict[str, Any]:
    if raw.get("routing") or raw.get("route"):
        return raw.get("routing") or raw.get("route") or {}
    routing_keys = sorted(k for k in raw.keys() if k.endswith("_routing"))
    for key in routing_keys:
        if raw.get(key):
            return raw.get(key) or {}
    return {}


@stable_api
def canonicalize_row(
    raw: dict[str, Any],
    target_id: str,
    pool: str,
    candidates: list[str],
    max_chars: int | None,
    target_meta: dict[str, Any] | None,
    *,
    pipeline_id: str,
) -> tuple[dict[str, Any] | None, str | None]:
    if not isinstance(raw, dict):
        return None, "unsupported_row_type"
    text = extract_text(raw, candidates)
    if not text:
        return None, "missing_text"
    if max_chars is not None and max_chars > 0 and len(text) > max_chars:
        text = text[:max_chars]
    record = dict(raw)
    record.setdefault("text", text)
    record_id = str(
        record.get("record_id") or record.get("id") or sha256_text(f"{target_id}:{text}")
    )
    record.setdefault("record_id", record_id)
    record = normalize_record(
        record,
        target_id=target_id,
        pool=pool,
        pipeline_id=pipeline_id,
        target_meta=target_meta,
        context=f"green/{target_id}",
    )
    return record, None


@stable_api
def normalize_record(
    record: dict[str, Any],
    *,
    target_id: str,
    pool: str,
    pipeline_id: str,
    target_meta: dict[str, Any] | None,
    context: str,
) -> dict[str, Any]:
    meta = target_meta or {}
    normalized = normalize_output_record(
        record,
        target_id=target_id,
        pool=pool,
        pipeline=pipeline_id,
        dataset_id=meta.get("dataset_id"),
        config=meta.get("config"),
        now=utc_now(),
    )
    validate_output_contract(normalized, context)
    return normalized
