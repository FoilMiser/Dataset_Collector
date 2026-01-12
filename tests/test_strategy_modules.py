from __future__ import annotations

import hashlib
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import requests

import collector_core.acquire_strategies as aw
from collector_core.acquire.strategies import (
    dataverse,
    figshare,
    ftp,
    git,
    github_release,
    hf,
    http,
    s3,
    torrent,
    zenodo,
)
from collector_core.rate_limit import RateLimiterConfig


class StreamResponse:
    def __init__(
        self,
        content: bytes,
        status_code: int = 200,
        headers: dict[str, str] | None = None,
        url: str = "https://example.com/file.txt",
        history: list[Any] | None = None,
    ) -> None:
        self._content = content
        self.content = content
        self.status_code = status_code
        self.headers = headers or {"Content-Length": str(len(content))}
        self.url = url
        self.history = history or []

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            err = requests.exceptions.HTTPError(f"HTTP {self.status_code}")
            err.response = self
            raise err

    def iter_content(self, chunk_size: int = 1024 * 1024):
        yield self._content

    def __enter__(self) -> StreamResponse:
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


class JsonResponse:
    def __init__(self, payload: dict[str, Any] | list[Any], status_code: int = 200) -> None:
        self._payload = payload
        self.status_code = status_code
        self.headers = {}
        self.url = "https://example.com/api"

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            err = requests.exceptions.HTTPError(f"HTTP {self.status_code}")
            err.response = self
            raise err

    def json(self) -> dict[str, Any] | list[Any]:
        return self._payload


class RedirectResponse:
    def __init__(self, url: str, location: str) -> None:
        self.url = url
        self.headers = {"Location": location}


class FakeLimiter:
    def __init__(self) -> None:
        self.calls = 0

    def acquire(self) -> float:
        self.calls += 1
        return 0.0


class FakeDataset:
    def __init__(self) -> None:
        self.saved: list[Path] = []

    def save_to_disk(self, path: str) -> None:
        target = Path(path)
        target.mkdir(parents=True, exist_ok=True)
        (target / "state.json").write_text("{}", encoding="utf-8")
        self.saved.append(target)


class FakeFTP:
    def __init__(self, host: str) -> None:
        self.host = host
        self.cwd_path: str | None = None
        self.raise_timeout = False

    def __enter__(self) -> FakeFTP:
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def login(self) -> None:
        return None

    def cwd(self, path: str) -> None:
        self.cwd_path = path

    def nlst(self, glob: str) -> list[str]:
        return ["dataset.csv"]

    def retrbinary(self, cmd: str, callback) -> None:
        if self.raise_timeout:
            raise TimeoutError("FTP timeout")
        callback(b"ftp-data")


def make_ctx(
    tmp_path: Path,
    *,
    execute: bool = True,
    enable_resume: bool = False,
    verify_sha256: bool = False,
    verify_zenodo_md5: bool = False,
    max_attempts: int = 1,
    cfg: dict[str, Any] | None = None,
    allow_non_global_download_hosts: bool = True,
) -> aw.AcquireContext:
    roots = aw.Roots(
        raw_root=tmp_path / "raw",
        manifests_root=tmp_path / "_manifests",
        ledger_root=tmp_path / "_ledger",
        logs_root=tmp_path / "_logs",
    )
    limits = aw.Limits(limit_targets=None, limit_files=None, max_bytes_per_target=None)
    mode = aw.RunMode(
        execute=execute,
        overwrite=True,
        verify_sha256=verify_sha256,
        verify_zenodo_md5=verify_zenodo_md5,
        enable_resume=enable_resume,
        workers=1,
    )
    retry = aw.RetryConfig(max_attempts=max_attempts, backoff_base=0.0, backoff_max=0.0)
    run_budget = aw.build_run_budget(None)
    return aw.AcquireContext(
        roots=roots,
        limits=limits,
        mode=mode,
        retry=retry,
        run_budget=run_budget,
        cfg=cfg,
        allow_non_global_download_hosts=allow_non_global_download_hosts,
    )


def test_http_strategy_success(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    response = StreamResponse(b"hello")

    def fake_get(url: str, stream: bool, headers: dict, timeout: tuple[int, int]):
        return response

    monkeypatch.setattr(http, "requests", SimpleNamespace(get=fake_get, exceptions=requests.exceptions))

    handler = http.resolve_http_handler("single")
    ctx = make_ctx(tmp_path)
    out_dir = tmp_path / "out"
    result = handler(ctx, {"id": "http", "download": {"url": "https://example.com/file.txt"}}, out_dir)

    assert result[0]["status"] == "ok"
    assert Path(result[0]["path"]).read_bytes() == b"hello"


def test_http_strategy_timeout(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_get(url: str, stream: bool, headers: dict, timeout: tuple[int, int]):
        raise requests.exceptions.Timeout("timeout")

    monkeypatch.setattr(aw.requests, "get", fake_get)

    handler = http.resolve_http_handler("single")
    ctx = make_ctx(tmp_path, max_attempts=1)
    out_dir = tmp_path / "out"

    with pytest.raises(requests.exceptions.Timeout):
        handler(ctx, {"id": "http", "download": {"url": "https://example.com/file.txt"}}, out_dir)


def test_http_strategy_checksum_mismatch(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    response = StreamResponse(b"content")

    def fake_get(url: str, stream: bool, headers: dict, timeout: tuple[int, int]):
        return response

    monkeypatch.setattr(aw.requests, "get", fake_get)

    handler = http.resolve_http_handler("single")
    ctx = make_ctx(tmp_path)
    out_dir = tmp_path / "out"
    result = handler(
        ctx,
        {"id": "http", "download": {"url": "https://example.com/file.txt", "expected_sha256": "deadbeef"}},
        out_dir,
    )

    assert result[0]["status"] == "error"
    assert result[0]["error"] == "sha256_mismatch"


def test_http_strategy_redirect_blocked(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    redirect = RedirectResponse("https://example.test/start", "http://127.0.0.1/private")
    response = StreamResponse(b"payload", url="http://127.0.0.1/private", history=[redirect])

    def fake_get(url: str, stream: bool, headers: dict, timeout: tuple[int, int]):
        return response

    def fake_getaddrinfo(host: str, *args: object, **kwargs: object):
        if host == "example.test":
            return [(aw.socket.AF_INET, aw.socket.SOCK_STREAM, 6, "", ("93.184.216.34", 0))]
        return []

    monkeypatch.setattr(aw.requests, "get", fake_get)
    monkeypatch.setattr(aw.socket, "getaddrinfo", fake_getaddrinfo)

    handler = http.resolve_http_handler("single")
    ctx = make_ctx(tmp_path)
    out_dir = tmp_path / "out"
    result = handler(ctx, {"id": "http", "download": {"url": "https://example.test/start"}}, out_dir)

    assert result[0]["status"] == "error"
    assert result[0]["error"] == "blocked_url"
    assert result[0]["blocked_url"] == "http://127.0.0.1/private"


def test_http_strategy_resume(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    partial = tmp_path / "out" / "file.txt.part"
    partial.parent.mkdir(parents=True)
    partial.write_bytes(b"hello")

    def fake_get(url: str, stream: bool, headers: dict, timeout: tuple[int, int]):
        assert headers.get("Range") == "bytes=5-"
        return StreamResponse(
            b" world",
            status_code=206,
            headers={"Content-Range": "bytes 5-10/11", "Content-Length": "6"},
            url=url,
        )

    monkeypatch.setattr(aw.requests, "get", fake_get)

    handler = http.resolve_http_handler("single")
    ctx = make_ctx(tmp_path, enable_resume=True)
    out_dir = tmp_path / "out"
    result = handler(ctx, {"id": "http", "download": {"url": "https://example.com/file.txt"}}, out_dir)

    assert result[0]["status"] == "ok"
    assert Path(result[0]["path"]).read_bytes() == b"hello world"


def test_ftp_strategy_success(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    fake_ftp = FakeFTP("example.com")
    monkeypatch.setattr(aw, "FTP", lambda host: fake_ftp)

    handler = ftp.get_handler()
    ctx = make_ctx(tmp_path)
    out_dir = tmp_path / "out"
    result = handler(ctx, {"id": "ftp", "download": {"base_url": "ftp://example.com/data"}}, out_dir)

    assert result[0]["status"] == "ok"
    assert Path(result[0]["path"]).read_bytes() == b"ftp-data"


def test_ftp_strategy_timeout(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    fake_ftp = FakeFTP("example.com")
    fake_ftp.raise_timeout = True
    monkeypatch.setattr(aw, "FTP", lambda host: fake_ftp)

    handler = ftp.get_handler()
    ctx = make_ctx(tmp_path)
    out_dir = tmp_path / "out"

    with pytest.raises(TimeoutError):
        handler(ctx, {"id": "ftp", "download": {"base_url": "ftp://example.com/data"}}, out_dir)


def test_git_strategy_success(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[list[str]] = []

    def fake_run_cmd(cmd: list[str], cwd: Path | None = None) -> str:
        calls.append(cmd)
        if cmd[-2:] == ["rev-parse", "HEAD"]:
            return "abc123"
        return "ok"

    monkeypatch.setattr(aw, "run_cmd", fake_run_cmd)

    handler = git.get_handler()
    ctx = make_ctx(tmp_path)
    out_dir = tmp_path / "repo"
    result = handler(ctx, {"id": "git", "download": {"repo": "https://example.com/repo.git"}}, out_dir)

    assert result[0]["status"] == "ok"
    assert result[0]["git_commit"] == "abc123"
    assert calls[0][0:2] == ["git", "clone"]


def test_zenodo_strategy_success(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    api_payload = {"files": [{"links": {"self": "https://example.com/file.txt"}, "key": "file.txt"}]}

    def fake_get(url: str, timeout: int):
        return JsonResponse(api_payload)

    def fake_download(ctx: aw.AcquireContext, url: str, out_path: Path, *_args: Any) -> dict[str, Any]:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_bytes(b"zenodo")
        return {
            "status": "ok",
            "path": str(out_path),
            "resolved_url": url,
            "content_length": 6,
            "sha256": hashlib.sha256(b"zenodo").hexdigest(),
        }

    monkeypatch.setattr(aw.requests, "get", fake_get)
    monkeypatch.setattr(aw, "_http_download_with_resume", fake_download)

    handler = zenodo.get_handler()
    ctx = make_ctx(tmp_path)
    out_dir = tmp_path / "out"
    result = handler(ctx, {"id": "zenodo", "download": {"record_id": "123"}}, out_dir)

    assert result[0]["status"] == "ok"
    assert Path(result[0]["path"]).read_bytes() == b"zenodo"


def test_zenodo_strategy_timeout(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_get(url: str, timeout: int):
        raise requests.exceptions.Timeout("timeout")

    monkeypatch.setattr(aw.requests, "get", fake_get)

    handler = zenodo.get_handler()
    ctx = make_ctx(tmp_path, max_attempts=1)
    out_dir = tmp_path / "out"

    with pytest.raises(requests.exceptions.Timeout):
        handler(ctx, {"id": "zenodo", "download": {"record_id": "123"}}, out_dir)


def test_zenodo_strategy_checksum_mismatch(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    api_payload = {
        "files": [
            {
                "links": {"self": "https://example.com/file.txt"},
                "key": "file.txt",
                "checksum": "md5:deadbeef",
            }
        ]
    }

    def fake_get(url: str, timeout: int):
        return JsonResponse(api_payload)

    def fake_download(ctx: aw.AcquireContext, url: str, out_path: Path, *_args: Any) -> dict[str, Any]:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_bytes(b"zenodo")
        return {"status": "ok", "path": str(out_path), "resolved_url": url, "content_length": 6}

    monkeypatch.setattr(aw.requests, "get", fake_get)
    monkeypatch.setattr(aw, "_http_download_with_resume", fake_download)

    handler = zenodo.get_handler()
    ctx = make_ctx(tmp_path, verify_zenodo_md5=True)
    out_dir = tmp_path / "out"
    result = handler(ctx, {"id": "zenodo", "download": {"record_id": "123"}}, out_dir)

    assert result[0]["status"] == "error"
    assert result[0]["error"] == "md5_mismatch"


def test_dataverse_strategy_success(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    response = StreamResponse(b"data", headers={"Content-Length": "4"}, url="https://example.com/file.csv")

    def fake_get(url: str, allow_redirects: bool, timeout: int):
        return response

    monkeypatch.setattr(aw.requests, "get", fake_get)

    handler = dataverse.get_handler()
    ctx = make_ctx(tmp_path)
    out_dir = tmp_path / "out"
    result = handler(ctx, {"id": "dv", "download": {"persistent_id": "doi:123"}}, out_dir)

    assert result[0]["status"] == "ok"
    assert Path(result[0]["path"]).read_bytes() == b"data"


def test_dataverse_strategy_timeout(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_get(url: str, allow_redirects: bool, timeout: int):
        raise requests.exceptions.Timeout("timeout")

    monkeypatch.setattr(aw.requests, "get", fake_get)

    handler = dataverse.get_handler()
    ctx = make_ctx(tmp_path)
    out_dir = tmp_path / "out"

    with pytest.raises(requests.exceptions.Timeout):
        handler(ctx, {"id": "dv", "download": {"persistent_id": "doi:123"}}, out_dir)


def test_dataverse_strategy_checksum_mismatch(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    response = StreamResponse(b"data", headers={"Content-Length": "4"}, url="https://example.com/file.csv")

    def fake_get(url: str, allow_redirects: bool, timeout: int):
        return response

    monkeypatch.setattr(aw.requests, "get", fake_get)

    handler = dataverse.get_handler()
    ctx = make_ctx(tmp_path)
    out_dir = tmp_path / "out"
    result = handler(
        ctx,
        {"id": "dv", "download": {"persistent_id": "doi:123", "expected_sha256": "deadbeef"}},
        out_dir,
    )

    assert result[0]["status"] == "error"
    assert result[0]["error"] == "sha256_mismatch"


def test_figshare_strategy_success(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    meta = {"files": [{"download_url": "https://example.com/file.csv", "name": "file.csv"}]}

    def fake_get(url: str, timeout: int):
        return JsonResponse(meta)

    def fake_download(ctx: aw.AcquireContext, url: str, out_path: Path, *_args: Any) -> dict[str, Any]:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_bytes(b"figshare")
        return {
            "status": "ok",
            "path": str(out_path),
            "resolved_url": url,
            "content_length": 8,
            "sha256": hashlib.sha256(b"figshare").hexdigest(),
        }

    monkeypatch.setattr(aw.requests, "get", fake_get)
    monkeypatch.setattr(aw, "_http_download_with_resume", fake_download)

    handler = figshare.resolve_figshare_handler("article")
    ctx = make_ctx(tmp_path)
    out_dir = tmp_path / "out"
    result = handler(ctx, {"id": "figshare", "download": {"article_id": 1}}, out_dir)

    assert result[0]["status"] == "ok"


def test_figshare_strategy_rate_limit_retry(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    limiter = FakeLimiter()
    rate_config = RateLimiterConfig(retry_on_429=True)
    meta = {"files": [{"download_url": "https://example.com/file.csv", "name": "file.csv"}]}
    calls = {"count": 0}

    def fake_get(url: str, timeout: int):
        calls["count"] += 1
        if calls["count"] == 1:
            return JsonResponse({}, status_code=429)
        return JsonResponse(meta)

    def fake_get_resolver_rate_limiter(cfg: dict[str, Any] | None, name: str):
        return limiter, rate_config

    monkeypatch.setattr(aw.requests, "get", fake_get)
    monkeypatch.setattr(aw, "get_resolver_rate_limiter", fake_get_resolver_rate_limiter)
    monkeypatch.setattr(
        aw,
        "_http_download_with_resume",
        lambda ctx, url, out_path, *_args: {
            "status": "ok",
            "path": str(out_path),
            "resolved_url": url,
            "content_length": 0,
            "sha256": hashlib.sha256(b"").hexdigest(),
        },
    )

    handler = figshare.resolve_figshare_handler("article")
    ctx = make_ctx(tmp_path, max_attempts=2, cfg={"resolvers": {"figshare": {"rate_limit": {}}}})
    out_dir = tmp_path / "out"
    result = handler(ctx, {"id": "figshare", "download": {"article_id": 1}}, out_dir)

    assert result[0]["status"] == "ok"
    assert limiter.calls == 2
    assert calls["count"] == 2


def test_github_release_strategy_success(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    payload = {
        "assets": [
            {"browser_download_url": "https://example.com/file.zip", "name": "file.zip", "size": 4}
        ]
    }

    def fake_get(url: str, headers: dict[str, str], timeout: int):
        return JsonResponse(payload)

    def fake_download(ctx: aw.AcquireContext, url: str, out_path: Path, *_args: Any) -> dict[str, Any]:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_bytes(b"data")
        return {
            "status": "ok",
            "path": str(out_path),
            "resolved_url": url,
            "content_length": 4,
            "sha256": hashlib.sha256(b"data").hexdigest(),
        }

    monkeypatch.setattr(aw.requests, "get", fake_get)
    monkeypatch.setattr(aw, "_http_download_with_resume", fake_download)

    handler = github_release.resolve_handler("owner/repo")
    ctx = make_ctx(tmp_path)
    out_dir = tmp_path / "out"
    result = handler(ctx, {"id": "gh", "download": {"repo": "owner/repo"}}, out_dir)

    assert result[0]["status"] == "ok"


def test_github_release_strategy_rate_limit_retry(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    limiter = FakeLimiter()
    rate_config = RateLimiterConfig(retry_on_403=True)
    payload = {
        "assets": [
            {"browser_download_url": "https://example.com/file.zip", "name": "file.zip", "size": 0}
        ]
    }
    calls = {"count": 0}

    def fake_get(url: str, headers: dict[str, str], timeout: int):
        calls["count"] += 1
        if calls["count"] == 1:
            return JsonResponse({}, status_code=403)
        return JsonResponse(payload)

    def fake_get_resolver_rate_limiter(cfg: dict[str, Any] | None, name: str):
        return limiter, rate_config

    monkeypatch.setattr(aw.requests, "get", fake_get)
    monkeypatch.setattr(aw, "get_resolver_rate_limiter", fake_get_resolver_rate_limiter)
    monkeypatch.setattr(
        aw,
        "_http_download_with_resume",
        lambda ctx, url, out_path, *_args: {
            "status": "ok",
            "path": str(out_path),
            "resolved_url": url,
            "content_length": 0,
            "sha256": hashlib.sha256(b"").hexdigest(),
        },
    )

    handler = github_release.resolve_handler("owner/repo")
    ctx = make_ctx(tmp_path, max_attempts=2, cfg={"resolvers": {"github": {"rate_limit": {}}}})
    out_dir = tmp_path / "out"
    result = handler(ctx, {"id": "gh", "download": {"repo": "owner/repo"}}, out_dir)

    assert result[0]["status"] == "ok"
    assert limiter.calls == 2
    assert calls["count"] == 2


def test_hf_strategy_success(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_load_dataset(*_args: Any, **_kwargs: Any) -> FakeDataset:
        return FakeDataset()

    monkeypatch.setitem(sys.modules, "datasets", SimpleNamespace(load_dataset=fake_load_dataset))

    handler = hf.get_handler()
    ctx = make_ctx(tmp_path)
    out_dir = tmp_path / "out"
    result = handler(ctx, {"id": "hf", "download": {"dataset_id": "sample"}}, out_dir)

    assert result[0]["status"] == "ok"


def test_s3_sync_strategy_success(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_run_cmd(cmd: list[str], cwd: Path | None = None) -> str:
        out_dir = Path(cmd[-1])
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / "file.txt").write_bytes(b"s3")
        return "synced"

    monkeypatch.setattr(aw, "run_cmd", fake_run_cmd)

    handler = s3.get_sync_handler()
    ctx = make_ctx(tmp_path)
    out_dir = tmp_path / "out"
    result = handler(ctx, {"id": "s3", "download": {"urls": ["s3://bucket/path"]}}, out_dir)

    assert result[0]["status"] == "ok"


def test_aws_requester_pays_strategy_success(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_run_cmd(cmd: list[str], cwd: Path | None = None) -> str:
        dest = Path(cmd[cmd.index("--request-payer") - 1])
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(b"aws")
        return "downloaded"

    monkeypatch.setattr(aw, "run_cmd", fake_run_cmd)

    handler = s3.get_requester_pays_handler()
    ctx = make_ctx(tmp_path)
    out_dir = tmp_path / "out"
    result = handler(
        ctx,
        {"id": "aws", "download": {"bucket": "bucket", "key": "path/file.txt"}},
        out_dir,
    )

    assert result[0]["status"] == "ok"


def test_aws_requester_pays_checksum_mismatch(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_run_cmd(cmd: list[str], cwd: Path | None = None) -> str:
        dest = Path(cmd[cmd.index("--request-payer") - 1])
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(b"aws")
        return "downloaded"

    monkeypatch.setattr(aw, "run_cmd", fake_run_cmd)

    handler = s3.get_requester_pays_handler()
    ctx = make_ctx(tmp_path)
    out_dir = tmp_path / "out"
    result = handler(
        ctx,
        {
            "id": "aws",
            "download": {
                "bucket": "bucket",
                "key": "path/file.txt",
                "expected_sha256": "deadbeef",
            },
        },
        out_dir,
    )

    assert result[0]["status"] == "error"
    assert result[0]["error"] == "sha256_mismatch"


def test_torrent_strategy_success(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_run_cmd(cmd: list[str], cwd: Path | None = None) -> str:
        out_dir = Path(cmd[3])
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / "file.bin").write_bytes(b"torrent")
        return "downloaded"

    monkeypatch.setattr(aw, "run_cmd", fake_run_cmd)

    handler = torrent.get_handler()
    ctx = make_ctx(tmp_path)
    out_dir = tmp_path / "out"
    result = handler(ctx, {"id": "torrent", "download": {"magnet": "magnet:?"}}, out_dir)

    assert result[0]["status"] == "ok"


def test_torrent_strategy_timeout(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_run_cmd(cmd: list[str], cwd: Path | None = None) -> str:
        raise TimeoutError("timeout")

    monkeypatch.setattr(aw, "run_cmd", fake_run_cmd)

    handler = torrent.get_handler()
    ctx = make_ctx(tmp_path)
    out_dir = tmp_path / "out"
    result = handler(ctx, {"id": "torrent", "download": {"magnet": "magnet:?"}}, out_dir)

    assert result[0]["status"] == "error"
    assert "TimeoutError" in result[0]["error"]
