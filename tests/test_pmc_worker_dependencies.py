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


def test_fetch_pmc_package_logs_dependency_error(monkeypatch: pytest.MonkeyPatch) -> None:
    def _raise_missing(*args: object, **kwargs: object) -> tuple[bytes, dict]:
        raise DependencyMissingError(
            "missing dependency: requests",
            dependency="requests",
            install="pip install requests",
        )

    monkeypatch.setattr(pmc_worker, "http_get_bytes", _raise_missing)
    pkg, meta = pmc_worker.fetch_pmc_package(
        "http://example.com/file.tar.gz",
        max_bytes=10,
        cache_dir=None,
    )
    assert pkg is None
    assert meta["status"] == "error"
    assert meta["error_code"] == "missing_dependency"
    assert meta["error_context"]["dependency"] == "requests"
