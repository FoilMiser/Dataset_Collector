from __future__ import annotations

import pytest

from collector_core import pmc_worker
from collector_core.exceptions import DependencyMissingError


def test_http_get_bytes_requires_requests(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(pmc_worker, "requests", None)
    with pytest.raises(DependencyMissingError, match="missing dependency: requests") as excinfo:
        pmc_worker.http_get_bytes("https://example.com")
    assert excinfo.value.code == "missing_dependency"
    assert excinfo.value.context["dependency"] == "requests"


def test_ftp_get_bytes_requires_ftplib(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(pmc_worker, "FTP", None)
    with pytest.raises(DependencyMissingError, match="missing dependency: ftplib") as excinfo:
        pmc_worker.ftp_get_bytes("example.com", "/path/file.txt")
    assert excinfo.value.code == "missing_dependency"
    assert excinfo.value.context["dependency"] == "ftplib"
