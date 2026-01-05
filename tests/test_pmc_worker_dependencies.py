from __future__ import annotations

import pytest

from collector_core import pmc_worker


def test_http_get_bytes_requires_requests(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(pmc_worker, "requests", None)
    with pytest.raises(RuntimeError, match="missing dependency: requests"):
        pmc_worker.http_get_bytes("https://example.com")


def test_ftp_get_bytes_requires_ftplib(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(pmc_worker, "FTP", None)
    with pytest.raises(RuntimeError, match="missing dependency: ftplib"):
        pmc_worker.ftp_get_bytes("example.com", "/path/file.txt")
