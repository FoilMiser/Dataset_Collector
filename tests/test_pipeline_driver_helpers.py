from __future__ import annotations
from pathlib import Path

import socket
from types import SimpleNamespace

import pytest

import collector_core.pipeline_driver_base as pipeline_driver_base
from collector_core.pipeline_driver_base import (
    BasePipelineDriver,
    EvidenceResult,
    LicenseMap,
    apply_denylist_bucket,
    apply_yellow_signoff_requirement,
    build_denylist_haystack,
    build_evidence_headers,
    build_target_identity,
    compute_file_hashes,
    compute_signoff_mismatches,
    denylist_hits,
    extract_download_urls,
    redact_headers_for_manifest,
    resolve_effective_bucket,
    resolve_evidence_change,
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


def test_snapshot_evidence_manifest_redacts_headers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
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
        allow_private_hosts: bool = False,
    ) -> tuple[bytes | None, str | None, dict[str, object]]:
        return b"ok", "text/plain", {"retries": 0, "errors": [], "final_url": url}

    monkeypatch.setattr(driver, "fetch_url_with_retry", fake_fetch)

    manifest_dir = tmp_path / "manifest"
    driver.snapshot_evidence(manifest_dir, "https://example.test/terms", headers=headers)
    meta_text = (manifest_dir / "license_evidence_meta.json").read_text(encoding="utf-8")
    assert secret not in meta_text
    assert REDACTED in meta_text


def test_snapshot_evidence_write_mismatch_marks_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    driver = BasePipelineDriver()

    def fake_fetch(
        url: str,
        timeout_s: float | tuple[float, float] = (15.0, 60.0),
        max_retries: int = 3,
        backoff_base: float = 2.0,
        headers: dict[str, str] | None = None,
        max_bytes: int | None = None,
        allow_private_hosts: bool = False,
    ) -> tuple[bytes | None, str | None, dict[str, object]]:
        return b"ok", "text/plain", {"retries": 0, "errors": [], "final_url": url}

    original_write_bytes = Path.write_bytes

    def partial_write(self: Path, data: bytes) -> int:
        if self.name.endswith(".part"):
            return original_write_bytes(self, data[:1])
        return original_write_bytes(self, data)

    monkeypatch.setattr(driver, "fetch_url_with_retry", fake_fetch)
    monkeypatch.setattr(Path, "write_bytes", partial_write)

    manifest_dir = tmp_path / "manifest"
    result = driver.snapshot_evidence(manifest_dir, "https://example.test/terms")
    assert result["status"] == "error"
    assert "verification" in result.get("error", "").lower()


def test_snapshot_evidence_removes_stale_siblings(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    driver = BasePipelineDriver()

    def fake_fetch(
        url: str,
        timeout_s: float | tuple[float, float] = (15.0, 60.0),
        max_retries: int = 3,
        backoff_base: float = 2.0,
        headers: dict[str, str] | None = None,
        max_bytes: int | None = None,
        allow_private_hosts: bool = False,
    ) -> tuple[bytes | None, str | None, dict[str, object]]:
        return b"%PDF-1.4\nbody", "application/pdf", {"retries": 0, "errors": [], "final_url": url}

    monkeypatch.setattr(driver, "fetch_url_with_retry", fake_fetch)

    manifest_dir = tmp_path / "manifest"
    manifest_dir.mkdir()
    (manifest_dir / "license_evidence.html").write_text("<html>old</html>", encoding="utf-8")
    (manifest_dir / "license_evidence.txt").write_text("stale", encoding="utf-8")

    driver.snapshot_evidence(manifest_dir, "https://example.test/terms")

    current_files = sorted(
        path.name
        for path in manifest_dir.glob("license_evidence.*")
        if not path.name.startswith("license_evidence.prev_")
    )
    assert current_files == ["license_evidence.pdf"]
    assert any(path.suffix == ".html" for path in manifest_dir.glob("license_evidence.prev_*"))


def test_fetch_url_with_retry_blocks_loopback(monkeypatch: pytest.MonkeyPatch) -> None:
    driver = BasePipelineDriver()

    def unexpected_get(*args: object, **kwargs: object) -> None:
        raise AssertionError("requests.get should not be called for blocked URLs")

    monkeypatch.setattr(pipeline_driver_base.requests, "get", unexpected_get)

    content, info, meta = driver.fetch_url_with_retry("http://127.0.0.1/terms")
    assert content is None
    assert info == "blocked_url"
    assert meta["blocked_url"] == "http://127.0.0.1/terms"
    assert "blocked_ip" in meta["blocked_reason"]


def test_fetch_url_with_retry_blocks_private_redirect(monkeypatch: pytest.MonkeyPatch) -> None:
    driver = BasePipelineDriver()

    class RedirectResponse:
        def __init__(self, url: str, location: str) -> None:
            self.url = url
            self.headers = {"Location": location}

    class DummyResponse:
        def __init__(self, url: str, history: list[RedirectResponse]) -> None:
            self.url = url
            self.headers = {"Content-Type": "text/plain"}
            self.status_code = 200
            self.history = history

        def raise_for_status(self) -> None:
            return None

        def iter_content(self, chunk_size: int = 1024) -> list[bytes]:
            return [b"ok"]

        def __enter__(self) -> DummyResponse:
            return self

        def __exit__(self, exc_type: object, exc: object, tb: object) -> bool:
            return False

    redirect = RedirectResponse("https://example.test/start", "http://127.0.0.1/private")
    response = DummyResponse("http://127.0.0.1/private", [redirect])

    def fake_getaddrinfo(host: str, *args: object, **kwargs: object) -> list[tuple[object, ...]]:
        if host == "example.test":
            return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 0))]
        return []

    monkeypatch.setattr(pipeline_driver_base.socket, "getaddrinfo", fake_getaddrinfo)
    monkeypatch.setattr(pipeline_driver_base.requests, "get", lambda *args, **kwargs: response)

    content, info, meta = driver.fetch_url_with_retry("https://example.test/start")
    assert content is None
    assert info == "blocked_url"
    assert meta["blocked_url"] == "http://127.0.0.1/private"


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
        license_gates=[],
        evidence=evidence,
        spdx="MIT",
        restriction_hits=[],
        min_confidence=0.6,
        resolved_confidence=0.9,
        review_required=False,
        review_status="pending",
        promote_to="",
        denylist_hits=[],
        strict_snapshot=False,
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
        license_gates=[],
        evidence=evidence,
        spdx="MIT",
        restriction_hits=[],
        min_confidence=0.6,
        resolved_confidence=0.9,
        review_required=False,
        review_status="pending",
        promote_to="",
        denylist_hits=[{"severity": "hard_red"}],
        strict_snapshot=False,
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


def test_denylist_domain_matches_subdomains_only() -> None:
    target = {"download": {"url": "https://notgov.com/file"}}
    denylist = {
        "domain_patterns": [
            {"domain": "example.com", "severity": "hard_red"},
            {"domain": "gov", "severity": "hard_red"},
        ]
    }
    download_urls = extract_download_urls(target)
    haystack = build_denylist_haystack(
        "t1",
        "Target",
        "https://sub.example.com",
        download_urls,
        target,
    )
    hits = denylist_hits(denylist, haystack)
    patterns = {hit["pattern"] for hit in hits if hit["type"] == "domain"}
    assert patterns == {"example.com"}


def test_apply_yellow_signoff_requirement_flags_review() -> None:
    review_required = apply_yellow_signoff_requirement(
        "YELLOW",
        "pending",
        False,
        True,
    )
    assert review_required is True


def test_text_extraction_failure_fallback_and_evidence_change_policies(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pdf_path = tmp_path / "evidence.pdf"
    pdf_path.write_bytes(b"%PDF-1.4\nexample")
    monkeypatch.setattr(pipeline_driver_base, "PdfReader", None)

    evidence: dict[str, object] = {}
    raw_hash, normalized_hash = compute_file_hashes(pdf_path, evidence)

    assert raw_hash
    assert raw_hash == normalized_hash
    assert evidence["normalized_hash_fallback"] == "raw_bytes"
    assert evidence["text_extraction_failed"] is True

    raw_mismatch, normalized_mismatch, cosmetic_change = compute_signoff_mismatches(
        signoff_raw_sha="deadbeef",
        signoff_normalized_sha="beadfeed",
        current_raw_sha=raw_hash,
        current_normalized_sha=normalized_hash,
        text_extraction_failed=True,
    )
    assert raw_mismatch is True
    assert normalized_mismatch is True
    assert (
        resolve_evidence_change(
            raw_mismatch,
            normalized_mismatch,
            cosmetic_change,
            evidence_policy="normalized",
            cosmetic_policy="warn_only",
        )
        is True
    )

    assert (
        resolve_evidence_change(
            raw_changed=True,
            normalized_changed=False,
            cosmetic_change=False,
            evidence_policy="normalized",
            cosmetic_policy="warn_only",
        )
        is False
    )
    assert (
        resolve_evidence_change(
            raw_changed=True,
            normalized_changed=False,
            cosmetic_change=False,
            evidence_policy="either",
            cosmetic_policy="warn_only",
        )
        is True
    )
