from __future__ import annotations

from collector_core.merge.contract import canonicalize_row, normalize_record, resolve_canonicalize_config


def test_canonicalize_row_uses_candidates_and_limits() -> None:
    cfg = {"globals": {"canonicalize": {"text_field_candidates": ["title"], "max_chars": 4}}}
    candidates, max_chars = resolve_canonicalize_config(cfg, None)

    raw = {"title": "hello world", "source": {"license_profile": "permissive"}}
    record, reason = canonicalize_row(
        raw,
        "t1",
        "permissive",
        candidates,
        max_chars,
        {"dataset_id": "ds1", "config": "cfg1"},
        pipeline_id="pipe",
    )
    assert reason is None
    assert record is not None
    assert record["text"] == "hell"
    assert record["dataset_id"] == "ds1"
    assert record["config"] == "cfg1"


def test_normalize_record_enforces_contract() -> None:
    raw = {"text": "hello", "source": {"license_profile": "permissive"}}
    record = normalize_record(
        raw,
        target_id="t1",
        pool="permissive",
        pipeline_id="pipe",
        target_meta={"dataset_id": "ds1", "config": "cfg1"},
        context="green/t1",
    )
    assert record["content_sha256"]
    assert record["license_profile"] == "permissive"
    assert record["dataset_id"] == "ds1"
