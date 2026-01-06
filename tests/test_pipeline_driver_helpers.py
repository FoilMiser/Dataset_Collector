from __future__ import annotations

from types import SimpleNamespace

import pytest

from collector_core.pipeline_driver_base import (
    EvidenceResult,
    LicenseMap,
    apply_denylist_bucket,
    apply_yellow_signoff_requirement,
    build_denylist_haystack,
    build_evidence_headers,
    build_target_identity,
    resolve_effective_bucket,
    resolve_retry_config,
    sort_queue_rows,
)


def test_build_evidence_headers_filters_invalid() -> None:
    headers = build_evidence_headers(["Authorization=token", "X-Test=1", "invalid"])
    assert headers == {"Authorization": "token", "X-Test": "1"}


def test_resolve_retry_config_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    args = SimpleNamespace(retry_max=None, retry_backoff=None, max_retries=None)
    monkeypatch.setenv("PIPELINE_RETRY_MAX", "5")
    monkeypatch.setenv("PIPELINE_RETRY_BACKOFF", "4.5")
    retry_max, retry_backoff = resolve_retry_config(args, {})
    assert retry_max == 5
    assert retry_backoff == 4.5


def test_sort_queue_rows_orders_by_priority_and_id() -> None:
    rows = [
        {"id": "b", "priority": 1},
        {"id": "a", "priority": 10},
        {"id": "c"},
    ]
    sorted_rows = sort_queue_rows(rows)
    assert [row["id"] for row in sorted_rows] == ["a", "b", "c"]


def test_apply_denylist_bucket_prioritizes_red() -> None:
    hits = [{"severity": "force_yellow"}, {"severity": "hard_red"}]
    assert apply_denylist_bucket(hits, "GREEN") == "RED"


def test_resolve_effective_bucket_applies_no_fetch_guard() -> None:
    license_map = LicenseMap(
        allow=["MIT"],
        conditional=[],
        deny_prefixes=[],
        normalization_rules=[],
        restriction_phrases=[],
        gating={"low_confidence_bucket": "YELLOW"},
        profiles={"permissive": {"default_bucket": "GREEN"}},
    )
    evidence = EvidenceResult(
        snapshot={"status": "ok"},
        text="",
        license_change_detected=False,
        no_fetch_missing_evidence=True,
    )
    bucket = resolve_effective_bucket(
        license_map,
        gates=[],
        evidence=evidence,
        spdx="MIT",
        restriction_hits=[],
        min_confidence=0.6,
        resolved_confidence=0.9,
        review_required=False,
        review_status="pending",
        promote_to="",
        denylist_hits=[],
    )
    assert bucket == "YELLOW"


def test_resolve_effective_bucket_denies_hard_red() -> None:
    license_map = LicenseMap(
        allow=["MIT"],
        conditional=[],
        deny_prefixes=[],
        normalization_rules=[],
        restriction_phrases=[],
        gating={"low_confidence_bucket": "YELLOW"},
        profiles={"permissive": {"default_bucket": "GREEN"}},
    )
    evidence = EvidenceResult(
        snapshot={"status": "ok"},
        text="",
        license_change_detected=False,
        no_fetch_missing_evidence=False,
    )
    bucket = resolve_effective_bucket(
        license_map,
        gates=[],
        evidence=evidence,
        spdx="MIT",
        restriction_hits=[],
        min_confidence=0.6,
        resolved_confidence=0.9,
        review_required=False,
        review_status="pending",
        promote_to="",
        denylist_hits=[{"severity": "hard_red"}],
    )
    assert bucket == "RED"


def test_build_target_identity_warns_on_unknown_profile() -> None:
    license_map = LicenseMap(
        allow=["MIT"],
        conditional=[],
        deny_prefixes=[],
        normalization_rules=[],
        restriction_phrases=[],
        gating={},
        profiles={"permissive": {"default_bucket": "GREEN"}},
    )
    tid, name, profile, enabled, warnings = build_target_identity(
        {"id": "t1", "name": "Target", "license_profile": "unknown"},
        license_map,
    )
    assert tid == "t1"
    assert name == "Target"
    assert profile == "unknown"
    assert enabled is True
    assert warnings and warnings[0]["type"] == "unknown_license_profile"


def test_build_denylist_haystack_includes_publisher() -> None:
    haystack = build_denylist_haystack(
        "t1",
        "Target",
        "https://example.test/terms",
        "{}",
        {"publisher": "Example Pub"},
    )
    assert haystack["publisher"] == "Example Pub"


def test_apply_yellow_signoff_requirement_flags_review() -> None:
    review_required = apply_yellow_signoff_requirement(
        "YELLOW",
        "pending",
        False,
        True,
    )
    assert review_required is True
