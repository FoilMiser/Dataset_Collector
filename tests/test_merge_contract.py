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


def test_canonicalize_row_returns_error_for_non_dict() -> None:
    """Test that non-dict input returns unsupported_row_type error."""
    candidates = ["text"]
    record, reason = canonicalize_row(
        "not a dict",  # type: ignore
        "t1",
        "permissive",
        candidates,
        None,
        None,
        pipeline_id="pipe",
    )
    assert record is None
    assert reason == "unsupported_row_type"


def test_canonicalize_row_truncates_text() -> None:
    """Test that text is truncated when exceeding max_chars."""
    raw = {"text": "hello world this is a very long text"}
    candidates = ["text"]
    record, reason = canonicalize_row(
        raw,
        "t1",
        "permissive",
        candidates,
        10,  # max_chars
        None,
        pipeline_id="pipe",
    )
    assert reason is None
    assert record is not None
    assert len(record["text"]) == 10


def test_resolve_canonicalize_config_empty() -> None:
    """Test resolve_canonicalize_config with empty config."""
    cfg = {}
    candidates, max_chars = resolve_canonicalize_config(cfg, None)
    assert candidates == ["text"]
    assert max_chars is None


def test_resolve_canonicalize_config_target_override() -> None:
    """Test that target config overrides global config."""
    cfg = {"globals": {"canonicalize": {"text_field_candidates": ["content"], "max_chars": 100}}}
    target_cfg = {"canonicalize": {"text_field_candidates": ["body"], "max_chars": 50}}
    candidates, max_chars = resolve_canonicalize_config(cfg, target_cfg)
    assert candidates == ["body"]
    assert max_chars == 50


def test_canonicalize_row_with_list_text() -> None:
    """Test canonicalize_row handles list values for text field."""
    from collector_core.merge.contract import coerce_text

    # Test coerce_text with list
    result = coerce_text(["line1", "line2", "line3"])
    assert result == "line1\nline2\nline3"


def test_canonicalize_row_generates_record_id() -> None:
    """Test that record_id is generated if not present."""
    raw = {"text": "hello"}
    candidates = ["text"]
    record, reason = canonicalize_row(
        raw,
        "t1",
        "permissive",
        candidates,
        None,
        None,
        pipeline_id="pipe",
    )
    assert reason is None
    assert record is not None
    assert "record_id" in record
    assert record["record_id"]  # Should be non-empty


def test_canonicalize_row_preserves_existing_record_id() -> None:
    """Test that existing record_id is preserved."""
    raw = {"text": "hello", "record_id": "existing_id"}
    candidates = ["text"]
    record, reason = canonicalize_row(
        raw,
        "t1",
        "permissive",
        candidates,
        None,
        None,
        pipeline_id="pipe",
    )
    assert reason is None
    assert record is not None
    assert record["record_id"] == "existing_id"


def test_resolve_routing_with_routing_key() -> None:
    """Test resolve_routing extracts routing from 'routing' key."""
    from collector_core.merge.contract import resolve_routing

    raw = {"routing": {"key": "value"}, "text": "hello"}
    result = resolve_routing(raw)
    assert result == {"key": "value"}


def test_resolve_routing_with_route_key() -> None:
    """Test resolve_routing extracts routing from 'route' key."""
    from collector_core.merge.contract import resolve_routing

    raw = {"route": {"path": "/api"}, "text": "hello"}
    result = resolve_routing(raw)
    assert result == {"path": "/api"}


def test_resolve_routing_with_custom_routing_key() -> None:
    """Test resolve_routing finds keys ending with '_routing'."""
    from collector_core.merge.contract import resolve_routing

    raw = {"math_routing": {"formula": "x+y"}, "text": "hello"}
    result = resolve_routing(raw)
    assert result == {"formula": "x+y"}


def test_resolve_routing_empty_when_no_routing() -> None:
    """Test resolve_routing returns empty dict when no routing present."""
    from collector_core.merge.contract import resolve_routing

    raw = {"text": "hello", "other": "value"}
    result = resolve_routing(raw)
    assert result == {}
