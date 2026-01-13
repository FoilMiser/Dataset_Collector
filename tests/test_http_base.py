from __future__ import annotations

import hashlib

from collector_core.acquire.context import InternalMirrorAllowlist
from collector_core.acquire.strategies.http_base import HttpDownloadBase


def test_validate_download_url_blocks_loopback() -> None:
    result = HttpDownloadBase.validate_download_url(
        "http://127.0.0.1/data.txt", allow_non_global_hosts=False
    )
    assert not result.allowed
    assert result.reason is not None
    assert result.reason.startswith("blocked_ip:127.0.0.1:")


def test_validate_download_url_allows_loopback_when_flagged() -> None:
    result = HttpDownloadBase.validate_download_url(
        "http://127.0.0.1/data.txt", allow_non_global_hosts=True
    )
    assert result.allowed
    assert result.reason is None


def test_parse_content_disposition_filename() -> None:
    header = 'attachment; filename="report.csv"'
    assert HttpDownloadBase.parse_content_disposition_filename(header) == "report.csv"


def test_parse_content_disposition_filename_star() -> None:
    header = "attachment; filename*=UTF-8''%E2%82%AC%20rates.txt"
    assert (
        HttpDownloadBase.parse_content_disposition_filename(header) == "€ rates.txt"
    )


def test_sha256_file(tmp_path) -> None:
    payload = b"hello world"
    file_path = tmp_path / "payload.bin"
    file_path.write_bytes(payload)
    expected = hashlib.sha256(payload).hexdigest()
    assert HttpDownloadBase.sha256_file(file_path) == expected


def test_parse_content_length_from_content_range() -> None:
    headers = {"Content-Range": "bytes 0-9/30"}
    assert HttpDownloadBase.parse_content_length(headers, status_code=206, existing=10) == 30


def test_parse_content_length_with_resume() -> None:
    headers = {"content-length": "15"}
    assert HttpDownloadBase.parse_content_length(headers, status_code=206, existing=5) == 20


def test_valid_content_range_matches_start_offset() -> None:
    assert HttpDownloadBase.valid_content_range("bytes 10-19/30", start_offset=10) is True
    assert HttpDownloadBase.valid_content_range("bytes 9-19/30", start_offset=10) is False


def test_validate_download_url_allows_allowlisted_host() -> None:
    allowlist = InternalMirrorAllowlist(hosts=frozenset({".example.com"}))
    result = HttpDownloadBase.validate_download_url(
        "https://files.example.com/data.csv",
        allow_non_global_hosts=False,
        internal_mirror_allowlist=allowlist,
    )
    assert result.allowed is True


def test_validate_redirect_urls_reports_blocked_redirect() -> None:
    result = HttpDownloadBase.validate_redirect_urls(
        ["https://example.com/data", "http://127.0.0.1/data"],
        allow_non_global_hosts=False,
    )
    assert result.allowed is False
    assert result.blocked_url == "http://127.0.0.1/data"
