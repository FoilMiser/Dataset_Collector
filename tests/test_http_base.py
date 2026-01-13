from __future__ import annotations

import hashlib

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
