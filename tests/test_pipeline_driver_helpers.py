from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from collector_core.pipeline_driver_base import (
    BasePipelineDriver,
    EvidenceResult,
    LicenseMap,
    apply_denylist_bucket,
    apply_yellow_signoff_requirement,
    build_denylist_haystack,
    build_evidence_headers,
    build_target_identity,
    denylist_hits,
    extract_download_urls,
    redact_headers_for_manifest,
    resolve_effective_bucket,
    resolve_retry_config,
    sort_queue_rows,
)
from collector_core.secrets import REDACTED


def test_build_evidence_headers_filters_invalid() -> None:
    headers = build_evidence_headers(["Authorization=token", "X-Test=1", "invalid"])
    assert headers == {"Authorization": "token", "X-Test": "1"}


def test_redact_headers_for_manifest_scrubs_sensitive_values() -> None:
    headers = {"Authorization": "Bearer secret", "X-Api-Key": "abc123", "User-Agent": "demo"}
    redacted = redact_headers_for_manifest(headers)
    assert redacted["Authorization"] == REDACTED
    assert redacted["X-Api-Key"] == REDACTED
    assert redacted["User-Agent"] == "demo"


def test_snapshot_evidence_manifest_redacts_headers(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    driver = BasePipelineDriver()
    secret = "supersecret"
    headers = {"Authorization": f"Bearer {secret}", "X-Api-Key": secret}

    def fake_fetch(
        url: str,
        timeout_s: float | tuple[float, float] = (15.0, 60.0),
        max_retries: int = 3,
        backoff_base: float = 2.0,
        headers: dict[str, str] | None = None,
        max_bytes: int | None = None,
    ) -> tuple[bytes | None, str | None, dict[str, object]]:
        return b"ok", "text/plain", {"retries": 0, "errors": [], "final_url": url}

    monkeypatch.setattr(driver, "fetch_url_with_retry", fake_fetch)

    manifest_dir = tmp_path / "manifest"
    driver.snapshot_evidence(manifest_dir, "https://example.test/terms", headers=headers)
    meta_text = (manifest_dir / "license_evidence_meta.json").read_text(encoding="utf-8")
    assert secret not in meta_text
    assert REDACTED in meta_text


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
        evidence_change_policy="normalized",
        cosmetic_change_policy="warn_only",
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
        evidence_change_policy="normalized",
        cosmetic_change_policy="warn_only",
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
        evidence_change_policy="normalized",
        cosmetic_change_policy="warn_only",
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
        [],
        {"publisher": "Example Pub"},
    )
    assert haystack["publisher"] == "Example Pub"


def test_denylist_hits_match_download_url() -> None:
    target = {"download": {"url": "https://blocked.example.com/file"}}
    denylist = {
        "domain_patterns": [
            {
                "domain": "blocked.example.com",
                "severity": "hard_red",
            }
        ]
    }
    download_urls = extract_download_urls(target)
    haystack = build_denylist_haystack("t1", "Target", "", download_urls, target)
    hits = denylist_hits(denylist, haystack)
    assert hits and hits[0]["type"] == "domain"


def test_apply_yellow_signoff_requirement_flags_review() -> None:
    review_required = apply_yellow_signoff_requirement(
        "YELLOW",
        "pending",
        False,
        True,
    )
    assert review_required is True
