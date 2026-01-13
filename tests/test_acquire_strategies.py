from __future__ import annotations

import hashlib
import json
import socket
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

import collector_core.acquire_strategies as aw  # noqa: E402
import collector_core.acquire.strategies.http as http_mod  # noqa: E402
import collector_core.acquire.strategies.ftp as ftp_mod  # noqa: E402
import collector_core.acquire.strategies.git as git_mod  # noqa: E402
import collector_core.acquire.strategies.zenodo as zenodo_mod  # noqa: E402
import collector_core.acquire.strategies.figshare as figshare_mod  # noqa: E402
import collector_core.acquire.strategies.s3 as s3_mod  # noqa: E402
import collector_core.acquire.strategies.torrent as torrent_mod  # noqa: E402


class StreamResponse:
    def __init__(self, content: bytes, status_code: int = 200, url: str = "") -> None:
        self._content = content
        self.status_code = status_code
        self.url = url or "https://example.com/file.txt"
        self.history: list = []
        self.headers: dict = {}

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def iter_content(self, chunk_size: int = 1024 * 1024):
        yield self._content

    def __enter__(self) -> StreamResponse:
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


class RedirectResponse:
    def __init__(self, url: str, location: str) -> None:
        self.url = url
        self.headers = {"Location": location}


class DummyResponse:
    def __init__(self, url: str, history: list[RedirectResponse], content: bytes = b"ok") -> None:
        self.url = url
        self.headers = {"Content-Type": "text/plain"}
        self.status_code = 200
        self.history = history
        self._content = content

    def raise_for_status(self) -> None:
        return None

    def iter_content(self, chunk_size: int = 1024) -> list[bytes]:
        return [self._content]

    def __enter__(self) -> DummyResponse:
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


class JsonResponse:
    def __init__(self, payload: dict | list) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict | list:
        return self._payload


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
        callback(b"ftp-data")


def make_ctx(
    tmp_path: Path,
    *,
    execute: bool = True,
    verify_sha256: bool = False,
    verify_zenodo_md5: bool = False,
    max_attempts: int = 1,
    max_bytes_per_target: int | None = None,
    run_byte_budget: int | None = None,
    allow_non_global_download_hosts: bool = True,
) -> aw.AcquireContext:
    roots = aw.Roots(
        raw_root=tmp_path / "raw",
        manifests_root=tmp_path / "_manifests",
        ledger_root=tmp_path / "_ledger",
        logs_root=tmp_path / "_logs",
    )
    limits = aw.Limits(
        limit_targets=None, limit_files=None, max_bytes_per_target=max_bytes_per_target
    )
    mode = aw.RunMode(
        execute=execute,
        overwrite=True,
        verify_sha256=verify_sha256,
        verify_zenodo_md5=verify_zenodo_md5,
        enable_resume=False,
        workers=1,
    )
    retry = aw.RetryConfig(max_attempts=max_attempts, backoff_base=0.0, backoff_max=0.0)
    run_budget = aw.build_run_budget(run_byte_budget)
    return aw.AcquireContext(
        roots=roots,
        limits=limits,
        mode=mode,
        retry=retry,
        run_budget=run_budget,
        allow_non_global_download_hosts=allow_non_global_download_hosts,
    )


def test_http_download_retries_and_sha256(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import requests as real_requests

    calls = {"count": 0}

    def fake_get(url: str, stream: bool, headers: dict, timeout: tuple[int, int]):
        calls["count"] += 1
        if calls["count"] == 1:
            raise real_requests.exceptions.ConnectionError("transient")
        return StreamResponse(b"hello")

    monkeypatch.setattr(
        http_mod, "requests", SimpleNamespace(get=fake_get, exceptions=real_requests.exceptions)
    )
    monkeypatch.setattr(http_mod.time, "sleep", lambda *_: None)

    ctx = make_ctx(tmp_path, verify_sha256=True, max_attempts=2)
    out_path = tmp_path / "out.txt"
    result = http_mod._http_download_with_resume(ctx, "https://example.com/file.txt", out_path)

    assert calls["count"] == 2
    assert result["status"] == "ok"
    assert result["sha256"] == hashlib.sha256(b"hello").hexdigest()
    assert out_path.read_bytes() == b"hello"


def test_http_download_size_mismatch(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_get(url: str, stream: bool, headers: dict, timeout: tuple[int, int]):
        return StreamResponse(b"hello")

    monkeypatch.setattr(http_mod, "requests", SimpleNamespace(get=fake_get))

    ctx = make_ctx(tmp_path, verify_sha256=True)
    out_path = tmp_path / "out.txt"
    result = http_mod._http_download_with_resume(
        ctx, "https://example.com/file.txt", out_path, expected_size=10
    )

    assert result["status"] == "error"
    assert result["error"] == "size_mismatch"


def test_http_download_blocks_private_redirect(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    redirect = RedirectResponse("https://example.test/start", "http://127.0.0.1/private")
    response = DummyResponse("http://127.0.0.1/private", [redirect])

    def fake_get(url: str, stream: bool, headers: dict, timeout: tuple[int, int]):
        return response

    def fake_getaddrinfo(host: str, *args: object, **kwargs: object) -> list[tuple[object, ...]]:
        if host == "example.test":
            return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 0))]
        return []

    import requests as real_requests

    monkeypatch.setattr(http_mod.socket, "getaddrinfo", fake_getaddrinfo)
    monkeypatch.setattr(
        http_mod, "requests", SimpleNamespace(get=fake_get, exceptions=real_requests.exceptions)
    )

    ctx = make_ctx(tmp_path, allow_non_global_download_hosts=False)
    out_path = tmp_path / "blocked.txt"
    result = http_mod._http_download_with_resume(ctx, "https://example.test/start", out_path)

    assert result["status"] == "error"
    assert result["error"] == "blocked_url"
    assert result["blocked_url"] == "http://127.0.0.1/private"


def test_http_download_blocks_dns_rebinding_redirect(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    redirect = RedirectResponse("https://rebind.test/start", "https://rebind.test/private")
    response = DummyResponse("https://rebind.test/private", [redirect])

    def fake_get(url: str, stream: bool, headers: dict, timeout: tuple[int, int]):
        return response

    call_state = {"count": 0}

    def fake_getaddrinfo(host: str, *args: object, **kwargs: object) -> list[tuple[object, ...]]:
        if host != "rebind.test":
            return []
        call_state["count"] += 1
        if call_state["count"] == 1:
            return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 0))]
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("10.0.0.8", 0))]

    import requests as real_requests

    monkeypatch.setattr(http_mod.socket, "getaddrinfo", fake_getaddrinfo)
    monkeypatch.setattr(
        http_mod, "requests", SimpleNamespace(get=fake_get, exceptions=real_requests.exceptions)
    )

    ctx = make_ctx(tmp_path, allow_non_global_download_hosts=False)
    out_path = tmp_path / "blocked.txt"
    result = http_mod._http_download_with_resume(ctx, "https://rebind.test/start", out_path)

    assert result["status"] == "error"
    assert result["error"] == "blocked_url"
    assert result["blocked_url"] == "https://rebind.test/private"


def test_zenodo_md5_mismatch(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    expected_md5 = hashlib.md5(b"expected").hexdigest()

    def fake_api_get(url: str, timeout: int = 60):
        return JsonResponse(
            {
                "files": [
                    {
                        "links": {"self": "https://zenodo.org/file.bin"},
                        "checksum": f"md5:{expected_md5}",
                        "key": "file.bin",
                    }
                ]
            }
        )

    def fake_download(
        ctx: aw.AcquireContext, url: str, out_path: Path, expected_size: int | None = None
    ):
        out_path.write_bytes(b"actual")
        return {"status": "ok", "path": str(out_path)}

    monkeypatch.setattr(zenodo_mod, "requests", SimpleNamespace(get=fake_api_get))
    monkeypatch.setattr(zenodo_mod, "_http_download_with_resume", fake_download)

    ctx = make_ctx(tmp_path, verify_zenodo_md5=True)
    row = {"id": "zenodo", "download": {"strategy": "zenodo", "record_id": "123"}}
    results = zenodo_mod.handle_zenodo(ctx, row, tmp_path / "zenodo")

    assert results[0]["status"] == "error"
    assert results[0]["error"] == "md5_mismatch"


def test_figshare_download(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_api_get(url: str, timeout: int = 120):
        return JsonResponse(
            [
                {
                    "name": "data.csv",
                    "download_url": "https://figshare.com/data.csv",
                }
            ]
        )

    def fake_download(
        ctx: aw.AcquireContext, url: str, out_path: Path, expected_size: int | None = None
    ):
        out_path.write_bytes(b"figshare")
        return {"status": "ok", "path": str(out_path)}

    monkeypatch.setattr(figshare_mod, "requests", SimpleNamespace(get=fake_api_get))
    monkeypatch.setattr(figshare_mod, "_http_download_with_resume", fake_download)

    ctx = make_ctx(tmp_path)
    row = {"id": "figshare", "download": {"strategy": "figshare", "article_id": "42"}}
    results = figshare_mod.handle_figshare_files(ctx, row, tmp_path / "figshare")

    assert results[0]["status"] == "ok"
    assert (tmp_path / "figshare" / "data.csv").exists()


def test_hf_datasets_splits(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple] = []

    def load_dataset(dataset_id: str, **kwargs):
        calls.append((dataset_id, kwargs))
        return FakeDataset()

    fake_module = SimpleNamespace(load_dataset=load_dataset)
    monkeypatch.setitem(sys.modules, "datasets", fake_module)

    ctx = make_ctx(tmp_path)
    row = {
        "id": "hf",
        "download": {
            "strategy": "huggingface_datasets",
            "dataset_id": "my-ds",
            "splits": ["train", "test"],
        },
    }
    out_dir = tmp_path / "hf"
    results = aw.handle_hf_datasets(ctx, row, out_dir)

    assert len(results) == 2
    assert (out_dir / "split_train" / "state.json").exists()
    assert (out_dir / "split_test" / "state.json").exists()
    assert calls[0][0] == "my-ds"


def test_ftp_download(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(ftp_mod, "FTP", FakeFTP)

    ctx = make_ctx(tmp_path)
    row = {
        "id": "ftp",
        "download": {
            "strategy": "ftp",
            "base_url": "ftp://example.com/data",
            "globs": ["*.csv"],
        },
    }
    out_dir = tmp_path / "ftp"
    results = ftp_mod.handle_ftp(ctx, row, out_dir)

    assert results[0]["status"] == "ok"
    assert (out_dir / "dataset.csv").read_bytes() == b"ftp-data"


def test_s3_sync_mock(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[list[str]] = []

    def fake_run_cmd(cmd: list[str], cwd: Path | None = None) -> str:
        calls.append(cmd)
        return "synced"

    monkeypatch.setattr(s3_mod, "run_cmd", fake_run_cmd)

    ctx = make_ctx(tmp_path)
    row = {
        "id": "s3",
        "download": {
            "strategy": "s3_sync",
            "urls": ["s3://bucket/data"],
            "no_sign_request": True,
            "request_payer": "requester",
            "extra_args": ["--exclude", "*.tmp"],
        },
    }
    out_dir = tmp_path / "s3"
    results = s3_mod.handle_s3_sync(ctx, row, out_dir)

    assert results[0]["status"] == "ok"
    assert calls[0][:4] == ["aws", "s3", "sync", "s3://bucket/data"]
    assert "--no-sign-request" in calls[0]
    assert "--request-payer" in calls[0]


def test_http_multi_respects_max_bytes_per_target(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def fake_download(
        ctx: aw.AcquireContext,
        url: str,
        out_path: Path,
        expected_size: int | None = None,
        expected_sha256=None,
    ):
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_bytes(b"x" * 10)
        return {"status": "ok", "path": str(out_path), "content_length": 10}

    monkeypatch.setattr(http_mod, "_http_download_with_resume", fake_download)

    ctx = make_ctx(tmp_path, max_bytes_per_target=5)
    row = {
        "id": "http-multi",
        "download": {
            "strategy": "http",
            "urls": ["https://example.com/a", "https://example.com/b"],
            "filenames": ["a.bin", "b.bin"],
        },
    }
    out_dir = tmp_path / "http"
    results = http_mod.handle_http_multi(ctx, row, out_dir)

    assert results[0]["status"] == "error"
    assert results[0]["error"] == "limit_exceeded"
    assert results[0]["limit_type"] == "bytes_per_target"
    assert results[0]["limit"] == 5
    assert results[0]["observed"] == 10
    assert not (out_dir / "a.bin").exists()
    assert results[1]["status"] == "error"
    assert results[1]["error"] == "limit_exceeded"
    assert results[1]["limit_type"] == "bytes_per_target"


def test_http_multi_respects_download_max_bytes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def fake_download(
        ctx: aw.AcquireContext,
        url: str,
        out_path: Path,
        expected_size: int | None = None,
        expected_sha256=None,
    ):
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_bytes(b"x" * 10)
        return {"status": "ok", "path": str(out_path), "content_length": 10}

    monkeypatch.setattr(http_mod, "_http_download_with_resume", fake_download)

    ctx = make_ctx(tmp_path)
    row = {
        "id": "http-max-bytes",
        "download": {
            "strategy": "http",
            "url": "https://example.com/a",
            "filename": "a.bin",
            "max_bytes": 5,
        },
    }
    out_dir = tmp_path / "http"
    results = http_mod.handle_http(ctx, row, out_dir)

    assert results[0]["status"] == "error"
    assert results[0]["error"] == "limit_exceeded"
    assert results[0]["limit_type"] == "bytes_per_target"
    assert results[0]["limit"] == 5
    assert results[0]["observed"] == 10


def test_http_multi_respects_run_byte_budget(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def fake_download(
        ctx: aw.AcquireContext,
        url: str,
        out_path: Path,
        expected_size: int | None = None,
        expected_sha256=None,
    ):
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_bytes(b"x" * 10)
        return {"status": "ok", "path": str(out_path), "content_length": 10}

    monkeypatch.setattr(http_mod, "_http_download_with_resume", fake_download)

    ctx = make_ctx(tmp_path, run_byte_budget=5)
    row = {
        "id": "http-run-budget",
        "download": {
            "strategy": "http",
            "url": "https://example.com/a",
            "filename": "a.bin",
        },
    }
    out_dir = tmp_path / "http"
    results = http_mod.handle_http(ctx, row, out_dir)

    assert results[0]["status"] == "error"
    assert results[0]["error"] == "limit_exceeded"
    assert results[0]["limit_type"] == "run_byte_budget"
    assert results[0]["limit"] == 5
    assert results[0]["observed"] == 10


def test_git_clone_post_check_respects_max_bytes_per_target(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def fake_run_cmd(cmd: list[str], cwd: Path | None = None) -> str:
        if cmd[:2] == ["git", "clone"]:
            dest = Path(cmd[-1])
            dest.mkdir(parents=True, exist_ok=True)
            (dest / "big.bin").write_bytes(b"x" * 10)
            return "cloned"
        if "rev-parse" in cmd:
            return "deadbeef"
        return ""

    monkeypatch.setattr(git_mod, "run_cmd", fake_run_cmd)

    ctx = make_ctx(tmp_path, max_bytes_per_target=5)
    row = {"id": "git", "download": {"strategy": "git", "repo": "https://example.com/repo.git"}}
    out_dir = tmp_path / "repo"
    results = git_mod.handle_git(ctx, row, out_dir)

    assert results[0]["status"] == "error"
    assert results[0]["error"] == "limit_exceeded"
    assert results[0]["limit_type"] == "bytes_per_target"
    assert results[0]["limit"] == 5
    assert results[0]["observed"] == 10
    assert not out_dir.exists()


def test_s3_sync_post_check_respects_max_bytes_per_target(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def fake_run_cmd(cmd: list[str], cwd: Path | None = None) -> str:
        out_dir = Path(cmd[-1])
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / "payload.bin").write_bytes(b"x" * 10)
        return "synced"

    monkeypatch.setattr(s3_mod, "run_cmd", fake_run_cmd)

    ctx = make_ctx(tmp_path, max_bytes_per_target=5)
    row = {"id": "s3", "download": {"strategy": "s3_sync", "urls": ["s3://bucket/data"]}}
    out_dir = tmp_path / "s3"
    results = s3_mod.handle_s3_sync(ctx, row, out_dir)

    assert results[0]["status"] == "error"
    assert results[0]["error"] == "limit_exceeded"
    assert results[0]["limit_type"] == "bytes_per_target"
    assert results[0]["limit"] == 5
    assert results[0]["observed"] == 10


@pytest.mark.parametrize(
    "handler,row,error",
    [
        (http_mod.handle_http, {"id": "http", "download": {"strategy": "http"}}, "missing url"),
        (
            figshare_mod.handle_figshare_files,
            {"id": "fig", "download": {"strategy": "figshare"}},
            "missing article_id",
        ),
        (
            aw.handle_hf_datasets,
            {"id": "hf", "download": {"strategy": "huggingface_datasets"}},
            "missing dataset_id",
        ),
        (s3_mod.handle_s3_sync, {"id": "s3", "download": {"strategy": "s3_sync"}}, "missing urls"),
    ],
)
def test_strategy_error_paths(
    tmp_path: Path,
    handler,
    row: dict,
    error: str,
) -> None:
    ctx = make_ctx(tmp_path)
    results = handler(ctx, row, tmp_path / "out")

    assert results[0]["status"] == "error"
    assert results[0]["error"] == error


def test_run_acquire_worker_strict_exits_on_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    queue_path = tmp_path / "queue.jsonl"
    queue_path.write_text(
        json.dumps({"id": "bad-target", "download": {"strategy": "fail"}}) + "\n", encoding="utf-8"
    )

    raw_root = tmp_path / "raw"
    manifests_root = tmp_path / "manifests"
    logs_root = tmp_path / "logs"

    def fail_handler(ctx: aw.AcquireContext, row: dict, out_dir: Path) -> list[dict[str, str]]:
        raise RuntimeError("boom")

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "dc run",
            "--queue",
            str(queue_path),
            "--bucket",
            "yellow",
            "--raw-root",
            str(raw_root),
            "--manifests-root",
            str(manifests_root),
            "--logs-root",
            str(logs_root),
            "--execute",
            "--strict",
        ],
    )

    with pytest.raises(SystemExit) as excinfo:
        aw.run_acquire_worker(
            defaults=aw.RootsDefaults(
                raw_root=str(raw_root),
                manifests_root=str(manifests_root),
                ledger_root=str(tmp_path / "_ledger"),
                logs_root=str(logs_root),
            ),
            targets_yaml_label="targets_kg_nav.yaml",
            strategy_handlers={"fail": fail_handler},
        )

    assert excinfo.value.code == 1
