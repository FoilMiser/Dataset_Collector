from __future__ import annotations

from typing import Any

from collector_core.utils.hash import sha256_text
from collector_core.utils.logging import utc_now

REQUIRED_FIELDS: dict[str, type] = {
    "dataset_id": str,
    "split": str,
    "config": str,
    "row_id": str,
    "license_spdx": str,
    "license_profile": str,
    "source_urls": list,
    "reviewer_notes": str,
    "content_sha256": str,
    "normalized_sha256": str,
    "pool": str,
    "pipeline": str,
    "target_name": str,
    "timestamp_created": str,
    "timestamp_updated": str,
}


def normalize_output_record(
    record: dict[str, Any],
    *,
    target_id: str,
    pool: str | None,
    pipeline: str,
    dataset_id: str | None = None,
    config: str | None = None,
    now: str | None = None,
) -> dict[str, Any]:
    out = dict(record)
    timestamp = now or utc_now()

    text = out.get("text")
    content_hash = out.get("content_sha256") or (out.get("hash") or {}).get("content_sha256")
    if not content_hash and text is not None:
        content_hash = sha256_text(str(text))
    if content_hash:
        out["content_sha256"] = content_hash

    normalized_hash = out.get("normalized_sha256") or (out.get("hash") or {}).get(
        "normalized_sha256"
    )
    if not normalized_hash and content_hash:
        normalized_hash = content_hash
    if normalized_hash:
        out["normalized_sha256"] = normalized_hash

    if "dataset_id" not in out:
        out["dataset_id"] = dataset_id or target_id

    if "split" not in out:
        source = out.get("source") or {}
        if source.get("split"):
            out["split"] = source.get("split")
        else:
            out["split"] = "train"

    if "config" not in out:
        out["config"] = config or "default"

    if "row_id" not in out:
        candidate = out.get("record_id") or out.get("id")
        if candidate is None and content_hash:
            candidate = content_hash
        if candidate is not None:
            out["row_id"] = str(candidate)

    if "license_spdx" not in out:
        license_obj = out.get("license") or {}
        source = out.get("source") or {}
        spdx = license_obj.get("spdx") or source.get("license_spdx")
        out["license_spdx"] = spdx or "NOASSERTION"

    if "license_profile" not in out:
        license_obj = out.get("license") or {}
        source = out.get("source") or {}
        profile = license_obj.get("profile") or source.get("license_profile") or pool
        out["license_profile"] = profile or "quarantine"

    if "source_urls" not in out:
        source_url = out.get("source_url") or out.get("url")
        source = out.get("source") or {}
        if not source_url:
            source_url = source.get("source_url") or source.get("url")
        if source_url:
            out["source_urls"] = [source_url]
        else:
            out["source_urls"] = []
    elif isinstance(out.get("source_urls"), str):
        out["source_urls"] = [out["source_urls"]]

    if "reviewer_notes" not in out:
        if isinstance(out.get("notes"), str):
            out["reviewer_notes"] = out["notes"]
        else:
            out["reviewer_notes"] = ""

    if pool is not None:
        out.setdefault("pool", pool)
    out.setdefault("pipeline", pipeline)
    out.setdefault("target_name", target_id)
    out.setdefault("timestamp_created", timestamp)
    out.setdefault("timestamp_updated", out["timestamp_created"])

    return out


def validate_output_contract(record: dict[str, Any], context: str) -> None:
    missing: list[str] = []
    invalid: list[str] = []
    for field, expected_type in REQUIRED_FIELDS.items():
        if field not in record or record[field] is None:
            missing.append(field)
            continue
        value = record[field]
        if field == "source_urls":
            if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
                invalid.append("source_urls")
        elif not isinstance(value, expected_type):
            invalid.append(field)

    if missing or invalid:
        parts = []
        if missing:
            parts.append(f"missing required fields: {', '.join(sorted(missing))}")
        if invalid:
            parts.append(f"invalid field types: {', '.join(sorted(set(invalid)))}")
        detail = "; ".join(parts)
        raise ValueError(f"Output contract validation failed for {context}: {detail}")
