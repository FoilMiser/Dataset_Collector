from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import os
import subprocess
import sys
import time
from collections import Counter
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from collector_core.__version__ import __version__ as VERSION
from collector_core.config_validator import read_yaml
from collector_core.dependencies import _try_import, requires
from collector_core.network_utils import _with_retries

requests = _try_import("requests")
FTP = _try_import("ftplib", "FTP")


StrategyHandler = Callable[["AcquireContext", dict[str, Any], Path], list[dict[str, Any]]]
PostProcessor = Callable[["AcquireContext", dict[str, Any], Path, str, dict[str, Any]], dict[str, Any] | None]


@dataclasses.dataclass(frozen=True)
class RootsDefaults:
    raw_root: str
    manifests_root: str
    logs_root: str


@dataclasses.dataclass
class Roots:
    raw_root: Path
    manifests_root: Path
    logs_root: Path


@dataclasses.dataclass
class Limits:
    limit_targets: int | None
    limit_files: int | None
    max_bytes_per_target: int | None


@dataclasses.dataclass
class RunMode:
    execute: bool
    overwrite: bool
    verify_sha256: bool
    verify_zenodo_md5: bool
    enable_resume: bool
    workers: int


@dataclasses.dataclass
class RetryConfig:
    max_attempts: int = 3
    backoff_base: float = 2.0
    backoff_max: float = 60.0


@dataclasses.dataclass
class AcquireContext:
    roots: Roots
    limits: Limits
    mode: RunMode
    retry: RetryConfig
    cfg: dict[str, Any] | None = None


def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def write_json(path: Path, obj: dict[str, Any]) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def safe_name(s: str) -> str:
    import re

    s = re.sub(r"[^a-zA-Z0-9._-]+", "_", (s or "").strip())
    return s[:200] if s else "file"


def normalize_download(download: dict[str, Any]) -> dict[str, Any]:
    d = dict(download or {})
    cfg = d.get("config")

    if isinstance(cfg, dict):
        merged = dict(cfg)
        merged.update({k: v for k, v in d.items() if k != "config"})
        d = merged

    if d.get("strategy") == "zenodo":
        if not d.get("record_id") and d.get("record"):
            d["record_id"] = d["record"]
        if not d.get("record_id") and isinstance(d.get("record_ids"), list) and d["record_ids"]:
            d["record_id"] = d["record_ids"][0]

    return d


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def md5_file(path: Path) -> str:
    h = hashlib.md5()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def run_cmd(cmd: list[str], cwd: Path | None = None) -> str:
    p = subprocess.run(cmd, cwd=str(cwd) if cwd else None, check=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    return p.stdout.decode("utf-8", errors="ignore")


def _parse_content_length(response: requests.Response, existing: int) -> int | None:
    content_range = response.headers.get("Content-Range")
    if content_range and "/" in content_range:
        total = content_range.split("/", 1)[1]
        if total.isdigit():
            return int(total)
    content_length = response.headers.get("Content-Length")
    if content_length and content_length.isdigit():
        length = int(content_length)
        if response.status_code == 206 and existing:
            return existing + length
        return length
    return None


def _http_download_with_resume(
    ctx: AcquireContext,
    url: str,
    out_path: Path,
    expected_size: int | None = None,
    expected_sha256: str | None = None,
) -> dict[str, Any]:
    missing = requires("requests", requests, install="pip install requests")
    if missing:
        raise RuntimeError(missing)
    ensure_dir(out_path.parent)
    temp_path = out_path.with_name(f"{out_path.name}.part")
    max_attempts = max(1, ctx.retry.max_attempts)

    content_length: int | None = None
    resolved_url: str | None = None

    def _stream_response(response: requests.Response, write_mode: str, existing_offset: int) -> None:
        nonlocal content_length, resolved_url
        resolved_url = response.url
        content_length = _parse_content_length(response, existing_offset)
        with temp_path.open(write_mode) as f:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)

    def _valid_content_range(header: str | None, start_offset: int) -> bool:
        if not header:
            return False
        if not header.startswith("bytes "):
            return False
        try:
            range_part = header.split(" ", 1)[1]
            span, _total = range_part.split("/", 1)
            start_str, _end_str = span.split("-", 1)
            return int(start_str) == start_offset
        except ValueError:
            return False

    def _is_transient_error(exc: Exception) -> bool:
        if isinstance(exc, requests.exceptions.HTTPError):
            status_code = exc.response.status_code if exc.response is not None else None
            return status_code is not None and status_code >= 500
        return isinstance(
            exc,
            (
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                requests.exceptions.ChunkedEncodingError,
                requests.exceptions.ContentDecodingError,
                requests.exceptions.TooManyRedirects,
            ),
        )

    for attempt in range(max_attempts):
        headers: dict[str, str] = {}
        mode = "wb"
        existing = 0
        if temp_path.exists() and ctx.mode.enable_resume:
            existing = temp_path.stat().st_size
        if existing and ctx.mode.enable_resume:
            headers["Range"] = f"bytes={existing}-"
            mode = "ab"
        content_length = None
        resolved_url = None
        try:
            with requests.get(url, stream=True, headers=headers, timeout=(15, 300)) as r:
                r.raise_for_status()
                if existing and ctx.mode.enable_resume:
                    content_range = r.headers.get("Content-Range")
                    valid_range = _valid_content_range(content_range, existing)
                    if r.status_code == 206:
                        if content_range and not valid_range:
                            raise RuntimeError("Invalid Content-Range for resumed download.")
                        _stream_response(r, mode, existing)
                    elif valid_range:
                        _stream_response(r, mode, existing)
                    else:
                        if r.status_code == 200:
                            with requests.get(url, stream=True, timeout=(15, 300)) as fresh:
                                fresh.raise_for_status()
                                _stream_response(fresh, "wb", 0)
                        else:
                            raise RuntimeError(
                                "Expected 206 Partial Content or a valid Content-Range for resumed download."
                            )
                else:
                    _stream_response(r, mode, existing)
        except Exception as exc:
            if not _is_transient_error(exc) or attempt >= max_attempts - 1:
                raise
            sleep_time = min(ctx.retry.backoff_base**attempt, ctx.retry.backoff_max)
            time.sleep(sleep_time)
            continue
        break
    actual_size = temp_path.stat().st_size
    if content_length is None:
        content_length = actual_size
    if expected_size is not None and actual_size != expected_size:
        temp_path.unlink(missing_ok=True)
        return {
            "status": "error",
            "error": "size_mismatch",
            "message": f"Expected size {expected_size} bytes but downloaded {actual_size} bytes.",
            "resolved_url": resolved_url,
            "content_length": content_length,
        }
    sha256 = sha256_file(temp_path)
    if expected_sha256 and sha256.lower() != expected_sha256.lower():
        temp_path.unlink(missing_ok=True)
        return {
            "status": "error",
            "error": "sha256_mismatch",
            "message": "Expected sha256 did not match downloaded content.",
            "expected_sha256": expected_sha256,
            "sha256": sha256,
            "resolved_url": resolved_url,
            "content_length": content_length,
        }
    temp_path.replace(out_path)
    result: dict[str, Any] = {
        "status": "ok",
        "path": str(out_path),
        "resolved_url": resolved_url,
        "content_length": content_length,
        "sha256": sha256,
    }
    if ctx.mode.verify_sha256 and "sha256" not in result:
        result["sha256"] = sha256_file(out_path)
    return result


# ---------------------------------
# Strategy handlers
# ---------------------------------

def handle_http_multi(ctx: AcquireContext, row: dict[str, Any], out_dir: Path) -> list[dict[str, Any]]:
    download = normalize_download(row.get("download", {}) or {})
    urls: list[str] = []
    if download.get("url"):
        urls.append(download["url"])
    urls.extend([u for u in download.get("urls") or [] if u])
    if not urls:
        return [{"status": "error", "error": "missing url"}]
    results: list[dict[str, Any]] = []
    filenames: list[str] = download.get("filenames") or []
    expected_sha256 = download.get("expected_sha256") or download.get("sha256")
    expected_sha256s: list[str | None] | None = None
    expected_sha256_map: dict[str, str] | None = None
    if isinstance(expected_sha256, list):
        expected_sha256s = expected_sha256
    elif isinstance(expected_sha256, dict):
        expected_sha256_map = expected_sha256
    for idx, url in enumerate(urls):
        if ctx.limits.limit_files and idx >= ctx.limits.limit_files:
            break
        filename = (
            (filenames[idx] if idx < len(filenames) else None)
            or download.get("filename")
            or safe_name(urlparse(url).path.split("/")[-1])
            or f"payload_{idx}.bin"
        )
        out_path = out_dir / filename
        if out_path.exists() and not ctx.mode.overwrite:
            results.append({"status": "ok", "path": str(out_path), "cached": True})
            continue
        if not ctx.mode.execute:
            results.append({"status": "noop", "path": str(out_path)})
            continue
        expected = expected_sha256
        if expected_sha256s is not None:
            expected = expected_sha256s[idx] if idx < len(expected_sha256s) else None
        elif expected_sha256_map is not None:
            expected = expected_sha256_map.get(filename) or expected_sha256_map.get(url)
        results.append(_http_download_with_resume(ctx, url, out_path, download.get("expected_size"), expected))
    return results


def handle_http_single(ctx: AcquireContext, row: dict[str, Any], out_dir: Path) -> list[dict[str, Any]]:
    download = normalize_download(row.get("download", {}) or {})
    url = download.get("url") or download.get("urls", [None])[0]
    if not url:
        return [{"status": "error", "error": "missing url"}]
    filename = download.get("filename") or safe_name(urlparse(url).path.split("/")[-1])
    if not filename:
        filename = "payload.bin"
    out_path = out_dir / filename
    if out_path.exists() and not ctx.mode.overwrite:
        return [{"status": "ok", "path": str(out_path), "cached": True}]
    if not ctx.mode.execute:
        return [{"status": "noop", "path": str(out_path)}]
    return [
        _http_download_with_resume(ctx, url, out_path, download.get("expected_size"), download.get("expected_sha256"))
    ]


def handle_http(ctx: AcquireContext, row: dict[str, Any], out_dir: Path) -> list[dict[str, Any]]:
    download = normalize_download(row.get("download", {}) or {})
    urls: list[str] = []
    if download.get("url"):
        urls.append(download["url"])
    urls.extend([u for u in download.get("urls") or [] if u])
    if len(urls) > 1:
        return handle_http_multi(ctx, row, out_dir)
    return handle_http_single(ctx, row, out_dir)


def handle_ftp(ctx: AcquireContext, row: dict[str, Any], out_dir: Path) -> list[dict[str, Any]]:
    download = normalize_download(row.get("download", {}) or {})
    base = download.get("base_url")
    globs = download.get("globs", ["*"])
    missing = requires("ftplib", FTP, install="use a standard Python build that includes ftplib")
    if missing:
        return [{"status": "error", "error": missing}]
    if not base:
        return [{"status": "error", "error": "missing base_url"}]
    url = urlparse(base)
    results: list[dict[str, Any]] = []
    if not ctx.mode.execute:
        return [{"status": "noop", "path": str(out_dir)}]
    with FTP(url.hostname) as ftp:
        ftp.login()
        ftp.cwd(url.path)
        for g in globs:
            files = ftp.nlst(g)
            for fname in files[: ctx.limits.limit_files or len(files)]:
                local = out_dir / fname
                ensure_dir(local.parent)
                temp_path = local.with_name(f"{local.name}.part")
                with temp_path.open("wb") as f:
                    ftp.retrbinary(f"RETR {fname}", f.write)
                content_length = temp_path.stat().st_size
                sha256 = sha256_file(temp_path)
                temp_path.replace(local)
                resolved_url = f"{base.rstrip('/')}/{fname}" if base else fname
                results.append(
                    {
                        "status": "ok",
                        "path": str(local),
                        "resolved_url": resolved_url,
                        "content_length": content_length,
                        "sha256": sha256,
                    }
                )
    return results


def handle_git(ctx: AcquireContext, row: dict[str, Any], out_dir: Path) -> list[dict[str, Any]]:
    download = normalize_download(row.get("download", {}) or {})
    repo = download.get("repo") or download.get("repo_url") or download.get("url") or download.get("url")
    branch = download.get("branch")
    commit = download.get("commit")
    tag = download.get("tag")
    revision = commit or tag
    if not repo:
        return [{"status": "error", "error": "missing repo"}]
    if not ctx.mode.execute:
        return [{"status": "noop", "path": str(out_dir)}]
    if out_dir.exists() and any(out_dir.iterdir()) and not ctx.mode.overwrite:
        git_dir = out_dir / ".git"
        if not git_dir.exists():
            return [{"status": "error", "error": "missing_git_repo", "path": str(out_dir)}]
        if revision:
            if tag:
                run_cmd(["git", "-C", str(out_dir), "fetch", "--tags", "--force"])
            else:
                run_cmd(["git", "-C", str(out_dir), "fetch", "--all", "--prune"])
            run_cmd(["git", "-C", str(out_dir), "checkout", revision])
        resolved = run_cmd(["git", "-C", str(out_dir), "rev-parse", "HEAD"]).strip()
        result = {"status": "ok", "path": str(out_dir), "cached": True, "git_commit": resolved}
        if revision:
            result["git_revision"] = revision
        return [result]
    ensure_dir(out_dir)
    cmd = ["git", "clone"]
    if branch and not revision:
        cmd += ["-b", branch]
    cmd += [repo, str(out_dir)]
    log = run_cmd(cmd)
    if revision:
        run_cmd(["git", "-C", str(out_dir), "checkout", revision])
    resolved = run_cmd(["git", "-C", str(out_dir), "rev-parse", "HEAD"]).strip()
    result = {"status": "ok", "path": str(out_dir), "log": log, "git_commit": resolved}
    if revision:
        result["git_revision"] = revision
    return [result]


def handle_zenodo(ctx: AcquireContext, row: dict[str, Any], out_dir: Path) -> list[dict[str, Any]]:
    download = normalize_download(row.get("download", {}) or {})
    api_url = download.get("api") or download.get("record_url")
    record_id = download.get("record_id")
    doi = download.get("doi")
    url = download.get("url")
    if not api_url:
        if record_id:
            api_url = f"https://zenodo.org/api/records/{record_id}"
        elif doi:
            api_url = f"https://zenodo.org/api/records/?q=doi:{doi}"
        elif url and "/api/records/" in url:
            api_url = url
    if not api_url:
        return [{"status": "error", "error": "missing api/record_url/record_id/doi/url"}]
    missing = requires("requests", requests, install="pip install requests")
    if missing:
        return [{"status": "error", "error": missing}]
    if not ctx.mode.execute:
        return [{"status": "noop", "path": str(out_dir)}]
    def _fetch() -> requests.Response:
        resp = requests.get(api_url, timeout=60)
        resp.raise_for_status()
        return resp

    resp = _with_retries(
        _fetch,
        max_attempts=ctx.retry.max_attempts,
        backoff_base=ctx.retry.backoff_base,
        backoff_max=ctx.retry.backoff_max,
    )
    data = resp.json()
    hits = data.get("hits", {}).get("hits", [])
    if hits and not data.get("files"):
        data = hits[0]
    results: list[dict[str, Any]] = []
    files = data.get("files", []) or data.get("hits", {}).get("hits", [{}])[0].get("files", [])
    for f in files[: ctx.limits.limit_files or len(files)]:
        link = f.get("links", {}).get("self") or f.get("link")
        if not link:
            continue
        filename = f.get("key") or f.get("name") or safe_name(link)
        out_path = out_dir / filename
        ensure_dir(out_path.parent)
        r = _http_download_with_resume(ctx, link, out_path)
        if ctx.mode.verify_zenodo_md5 and f.get("checksum", "").startswith("md5:"):
            expected_md5 = f["checksum"].split(":", 1)[1]
            if md5_file(out_path) != expected_md5:
                r = {"status": "error", "error": "md5_mismatch"}
        results.append(r)
    return results or [{"status": "noop", "reason": "no files"}]


def handle_dataverse(ctx: AcquireContext, row: dict[str, Any], out_dir: Path) -> list[dict[str, Any]]:
    download = normalize_download(row.get("download", {}) or {})
    pid = download.get("persistent_id") or download.get("pid")
    instance = download.get("instance") or "https://dataverse.harvard.edu"
    if not pid:
        return [{"status": "error", "error": "missing persistent_id"}]
    missing = requires("requests", requests, install="pip install requests")
    if missing:
        return [{"status": "error", "error": missing}]
    if not ctx.mode.execute:
        return [{"status": "noop", "path": str(out_dir)}]
    url = f"{instance}/api/access/dvobject/{pid}"
    resp = requests.get(url, allow_redirects=True, timeout=60)
    resp.raise_for_status()
    filename = safe_name(urlparse(resp.url).path.split("/")[-1] or pid)
    out_path = out_dir / filename
    ensure_dir(out_path.parent)
    temp_path = out_path.with_name(f"{out_path.name}.part")
    with temp_path.open("wb") as f:
        f.write(resp.content)
    content_length = temp_path.stat().st_size
    sha256 = sha256_file(temp_path)
    expected_sha256 = download.get("expected_sha256")
    if expected_sha256 and sha256.lower() != expected_sha256.lower():
        temp_path.unlink(missing_ok=True)
        return [
            {
                "status": "error",
                "error": "sha256_mismatch",
                "message": "Expected sha256 did not match downloaded content.",
                "expected_sha256": expected_sha256,
                "sha256": sha256,
                "resolved_url": resp.url,
                "content_length": content_length,
            }
        ]
    temp_path.replace(out_path)
    return [
        {
            "status": "ok",
            "path": str(out_path),
            "resolved_url": resp.url,
            "content_length": content_length,
            "sha256": sha256,
        }
    ]


def handle_figshare_article(ctx: AcquireContext, row: dict[str, Any], out_dir: Path) -> list[dict[str, Any]]:
    missing = requires("requests", requests, install="pip install requests")
    if missing:
        return [{"status": "error", "error": missing}]
    download = normalize_download(row.get("download", {}) or {})
    article_id = download.get("article_id")
    if not article_id and download.get("article_url"):
        try:
            article_id = int(str(download["article_url"]).rstrip("/").split("/")[-1])
        except Exception:
            article_id = None
    if not article_id:
        return [{"status": "error", "error": "missing article_id"}]
    api_base = (download.get("api_base") or "https://api.figshare.com/v2").rstrip("/")
    endpoint = f"{api_base}/articles/{article_id}"
    if not ctx.mode.execute:
        return [{"status": "noop", "article_id": article_id, "path": str(out_dir)}]
    def _fetch() -> requests.Response:
        resp = requests.get(endpoint, timeout=60)
        resp.raise_for_status()
        return resp

    resp = _with_retries(
        _fetch,
        max_attempts=ctx.retry.max_attempts,
        backoff_base=ctx.retry.backoff_base,
        backoff_max=ctx.retry.backoff_max,
    )
    meta = resp.json()
    files = meta.get("files", []) or []
    results: list[dict[str, Any]] = []
    for idx, fmeta in enumerate(files):
        if ctx.limits.limit_files and idx >= ctx.limits.limit_files:
            break
        download_url = fmeta.get("download_url") or (fmeta.get("links") or {}).get("download")
        if not download_url:
            results.append({"status": "error", "error": "missing_download_url", "file": fmeta.get("name")})
            continue
        fname = safe_name(fmeta.get("name") or fmeta.get("id") or f"figshare_file_{idx}")
        expected_size = fmeta.get("size")
        results.append(_http_download_with_resume(ctx, download_url, out_dir / fname, expected_size))
    write_json(out_dir / "figshare_article.json", meta)
    return results


handle_figshare = handle_figshare_article


def handle_figshare_files(ctx: AcquireContext, row: dict[str, Any], out_dir: Path) -> list[dict[str, Any]]:
    download = normalize_download(row.get("download", {}) or {})
    article_id = download.get("article_id") or download.get("id")
    api = download.get("api") or (f"https://api.figshare.com/v2/articles/{article_id}/files" if article_id else None)
    if not article_id or not api:
        return [{"status": "error", "error": "missing article_id"}]
    missing = requires("requests", requests, install="pip install requests")
    if missing:
        return [{"status": "error", "error": missing}]
    if not ctx.mode.execute:
        return [{"status": "noop", "path": str(out_dir)}]
    def _fetch() -> requests.Response:
        resp = requests.get(api, timeout=120)
        resp.raise_for_status()
        return resp

    resp = _with_retries(
        _fetch,
        max_attempts=ctx.retry.max_attempts,
        backoff_base=ctx.retry.backoff_base,
        backoff_max=ctx.retry.backoff_max,
    )
    files = resp.json() or []
    ensure_dir(out_dir)
    results: list[dict[str, Any]] = []
    limit = ctx.limits.limit_files or len(files)
    for f in files[:limit]:
        link = f.get("download_url") or (f.get("links") or {}).get("download")
        if not link:
            continue
        filename = safe_name(f.get("name") or f.get("id") or str(article_id))
        out_path = out_dir / filename

        results.append(_http_download_with_resume(ctx, link, out_path))
    return results or [{"status": "noop", "reason": "no files"}]


def make_github_release_handler(user_agent: str) -> StrategyHandler:
    def _handle_github_release(ctx: AcquireContext, row: dict[str, Any], out_dir: Path) -> list[dict[str, Any]]:
        missing = requires("requests", requests, install="pip install requests")
        if missing:
            return [{"status": "error", "error": missing}]
        download = normalize_download(row.get("download", {}) or {})
        owner = download.get("owner")
        repo = download.get("repo") or download.get("repository")
        if repo and "/" in repo and not owner:
            owner, repo = repo.split("/", 1)
        tag = download.get("tag")
        release_id = download.get("release_id")
        if not owner or not repo:
            return [{"status": "error", "error": "missing owner/repo"}]
        headers = {"User-Agent": f"{user_agent}/{VERSION}"}
        token = (download.get("github_token") or os.environ.get("GITHUB_TOKEN") or "").strip()
        token_file = Path.home() / ".github_token"
        if not token and token_file.exists():
            try:
                token = token_file.read_text().strip()
            except Exception:
                token = ""
        if token:
            headers["Authorization"] = f"Bearer {token}"
        base = f"https://api.github.com/repos/{owner}/{repo}/releases"
        if release_id:
            url = f"{base}/{release_id}"
        elif tag:
            url = f"{base}/tags/{tag}"
        else:
            url = f"{base}/latest"
        if not ctx.mode.execute:
            return [{"status": "noop", "release_url": url, "path": str(out_dir)}]
        resp = requests.get(url, headers=headers, timeout=60)
        resp.raise_for_status()
        meta = resp.json()
        assets = meta.get("assets", []) or []
        results: list[dict[str, Any]] = []
        for idx, asset in enumerate(assets):
            if ctx.limits.limit_files and idx >= ctx.limits.limit_files:
                break
            download_url = asset.get("browser_download_url") or asset.get("url")
            if not download_url:
                results.append({"status": "error", "error": "missing_download_url", "asset": asset.get("name")})
                continue
            fname = safe_name(asset.get("name") or f"{repo}_asset_{idx}")
            results.append(_http_download_with_resume(ctx, download_url, out_dir / fname, asset.get("size")))
        write_json(out_dir / "github_release.json", meta)
        return results

    return _handle_github_release


def handle_hf_datasets(ctx: AcquireContext, row: dict[str, Any], out_dir: Path) -> list[dict[str, Any]]:
    download = normalize_download(row.get("download", {}) or {})
    dataset_id = download.get("dataset_id")
    if not dataset_id:
        return [{"status": "error", "error": "missing dataset_id"}]
    splits = download.get("splits") or download.get("split")
    if isinstance(splits, str):
        splits = [splits]
    load_kwargs = download.get("load_kwargs", {}) or {}
    cfg = download.get("config")
    hf_name = cfg if isinstance(cfg, str) else None
    if hf_name and "name" not in load_kwargs:
        load_kwargs["name"] = hf_name
    if not ctx.mode.execute:
        return [{"status": "noop", "path": str(out_dir)}]
    try:
        from datasets import load_dataset  # type: ignore
    except Exception as e:  # pragma: no cover - optional dep
        return [{"status": "error", "error": f"datasets import failed: {e}"}]

    results: list[dict[str, Any]] = []
    ensure_dir(out_dir)
    if splits:
        for sp in splits:
            ds = load_dataset(dataset_id, split=sp, **load_kwargs)
            sp_dir = out_dir / f"split_{safe_name(sp)}"
            ds.save_to_disk(str(sp_dir))
            results.append({"status": "ok", "dataset_id": dataset_id, "split": sp})
    else:
        ds = load_dataset(dataset_id, **load_kwargs)
        ds.save_to_disk(str(out_dir / "hf_dataset"))
        results.append({"status": "ok", "dataset_id": dataset_id})
    return results


def handle_s3_sync(ctx: AcquireContext, row: dict[str, Any], out_dir: Path) -> list[dict[str, Any]]:
    download = normalize_download(row.get("download", {}) or {})
    urls = download.get("urls") or []
    if not urls:
        return [{"status": "error", "error": "missing urls"}]
    if not ctx.mode.execute:
        return [{"status": "noop", "path": str(out_dir)}]
    ensure_dir(out_dir)
    results: list[dict[str, Any]] = []
    extra_args = download.get("extra_args", []) or []
    for url in urls:
        cmd = ["aws", "s3", "sync", url, str(out_dir)]
        if download.get("no_sign_request"):
            cmd.append("--no-sign-request")
        if download.get("request_payer"):
            cmd += ["--request-payer", str(download.get("request_payer"))]
        cmd += [str(a) for a in extra_args]
        log = run_cmd(cmd)
        results.append({"status": "ok", "path": str(out_dir), "log": log})
    return results


def handle_aws_requester_pays(ctx: AcquireContext, row: dict[str, Any], out_dir: Path) -> list[dict[str, Any]]:
    download = normalize_download(row.get("download", {}) or {})
    bucket = download.get("bucket")
    key = download.get("key")
    if not bucket or not key:
        return [{"status": "error", "error": "missing bucket/key"}]
    dest_filename = download.get("dest_filename") or safe_name(Path(key).name)
    out_path = out_dir / dest_filename
    if out_path.exists() and not ctx.mode.overwrite:
        return [{"status": "ok", "path": str(out_path), "cached": True}]
    if not ctx.mode.execute:
        return [{"status": "noop", "path": str(out_path)}]
    ensure_dir(out_path.parent)
    payer = download.get("request_payer", "requester")
    temp_path = out_path.with_name(f"{out_path.name}.part")
    cmd = ["aws", "s3api", "get-object", "--bucket", bucket, "--key", key, str(temp_path), "--request-payer", payer]
    log = run_cmd(cmd)
    content_length = temp_path.stat().st_size
    sha256 = sha256_file(temp_path)
    expected_sha256 = download.get("expected_sha256")
    if expected_sha256 and sha256.lower() != expected_sha256.lower():
        temp_path.unlink(missing_ok=True)
        return [
            {
                "status": "error",
                "error": "sha256_mismatch",
                "message": "Expected sha256 did not match downloaded content.",
                "expected_sha256": expected_sha256,
                "sha256": sha256,
                "resolved_url": f"s3://{bucket}/{key}",
                "content_length": content_length,
                "log": log,
            }
        ]
    temp_path.replace(out_path)
    result = {
        "status": "ok",
        "path": str(out_path),
        "log": log,
        "resolved_url": f"s3://{bucket}/{key}",
        "content_length": content_length,
        "sha256": sha256,
    }
    return [result]


def handle_torrent(ctx: AcquireContext, row: dict[str, Any], out_dir: Path) -> list[dict[str, Any]]:
    download = normalize_download(row.get("download", {}) or {})
    magnet = download.get("magnet") or download.get("torrent")
    if not magnet:
        return [{"status": "error", "error": "missing magnet/torrent"}]
    if not ctx.mode.execute:
        return [{"status": "noop", "path": str(out_dir)}]
    ensure_dir(out_dir)
    try:
        log = run_cmd(["aria2c", "--seed-time=0", "-d", str(out_dir), magnet])
        return [{"status": "ok", "path": str(out_dir), "log": log}]
    except Exception as e:
        return [{"status": "error", "error": repr(e)}]


LICENSE_POOL_MAP = {
    "permissive": "permissive",
    "public_domain": "permissive",
    "record_level": "permissive",
    "copyleft": "copyleft",
    "unknown": "quarantine",
    "quarantine": "quarantine",
    "deny": "quarantine",
}


def resolve_license_pool(row: dict[str, Any]) -> str:
    lp = str(row.get("license_profile") or row.get("license_pool") or "quarantine").lower()
    return LICENSE_POOL_MAP.get(lp, "quarantine")


def resolve_output_dir(ctx: AcquireContext, bucket: str, pool: str, target_id: str) -> Path:
    bucket = (bucket or "yellow").strip().lower()
    pool = (pool or "quarantine").strip().lower()
    out = ctx.roots.raw_root / bucket / pool / safe_name(target_id)
    ensure_dir(out)
    return out


def write_done_marker(
    ctx: AcquireContext, target_id: str, bucket: str, status: str, extra: dict[str, Any] | None = None
) -> None:
    marker = ctx.roots.manifests_root / safe_name(target_id) / "acquire_done.json"
    payload = {
        "target_id": target_id,
        "bucket": bucket,
        "status": status,
        "written_at_utc": utc_now(),
        "version": VERSION,
    }
    if extra:
        payload.update(extra)
    write_json(marker, payload)


def run_target(
    ctx: AcquireContext,
    bucket: str,
    row: dict[str, Any],
    strategy_handlers: dict[str, StrategyHandler],
    postprocess: PostProcessor | None = None,
) -> dict[str, Any]:
    tid = row["id"]
    pool = resolve_license_pool(row)
    strat = (row.get("download", {}) or {}).get("strategy", "none")
    out_dir = resolve_output_dir(ctx, bucket, pool, tid)
    manifest = {
        "id": tid,
        "name": row.get("name", tid),
        "bucket": bucket,
        "license_pool": pool,
        "strategy": strat,
        "started_at_utc": utc_now(),
        "pipeline_version": VERSION,
        "output_dir": str(out_dir),
        "results": [],
    }

    handler = strategy_handlers.get(strat)
    if not handler or strat in {"none", ""}:
        manifest["results"] = [{"status": "noop", "reason": f"unsupported: {strat}"}]
    else:
        try:
            manifest["results"] = handler(ctx, row, out_dir)
            if not manifest["results"]:
                manifest["results"] = [
                    {"status": "failed", "reason": "handler_returned_no_results"}
                ]
        except Exception as e:
            manifest["results"] = [{"status": "error", "error": repr(e)}]

    post_processors: dict[str, Any] | None = None
    if postprocess:
        post_processors = postprocess(ctx, row, out_dir, bucket, manifest)
        if post_processors:
            manifest["post_processors"] = post_processors

    manifest["finished_at_utc"] = utc_now()
    git_info: dict[str, Any] | None = None
    for result in manifest["results"]:
        if result.get("git_commit"):
            git_info = {"git_commit": result["git_commit"]}
            if result.get("git_revision"):
                git_info["git_revision"] = result["git_revision"]
            break
    if git_info:
        manifest.update(git_info)
    write_json(out_dir / "download_manifest.json", manifest)

    status = "ok" if any(r.get("status") == "ok" for r in manifest["results"]) else manifest["results"][0].get("status", "error")
    if post_processors:
        for proc in post_processors.values():
            if isinstance(proc, dict) and proc.get("status") not in {"ok", "noop"}:
                status = proc.get("status", status)
    if ctx.mode.execute:
        write_done_marker(ctx, tid, bucket, status, git_info)
    return {"id": tid, "status": status, "bucket": bucket, "license_pool": pool, "strategy": strat}


def load_config(targets_path: Path | None) -> dict[str, Any]:
    cfg: dict[str, Any] = {}
    if targets_path and targets_path.exists():
        cfg = read_yaml(targets_path, schema_name="targets") or {}
    return cfg


def load_roots(cfg: dict[str, Any], overrides: argparse.Namespace, defaults: RootsDefaults) -> Roots:
    g = (cfg.get("globals", {}) or {})
    raw_root = Path(overrides.raw_root or g.get("raw_root", defaults.raw_root))
    manifests_root = Path(overrides.manifests_root or g.get("manifests_root", defaults.manifests_root))
    logs_root = Path(overrides.logs_root or g.get("logs_root", defaults.logs_root))
    return Roots(
        raw_root=raw_root.expanduser().resolve(),
        manifests_root=manifests_root.expanduser().resolve(),
        logs_root=logs_root.expanduser().resolve(),
    )


def run_acquire_worker(
    *,
    defaults: RootsDefaults,
    targets_yaml_label: str,
    strategy_handlers: dict[str, StrategyHandler],
    postprocess: PostProcessor | None = None,
) -> None:
    ap = argparse.ArgumentParser(description=f"Acquire Worker v{VERSION}")
    ap.add_argument("--queue", required=True, help="Queue JSONL emitted by pipeline_driver.py")
    ap.add_argument("--targets-yaml", default=None, help=f"Path to {targets_yaml_label} for roots")
    ap.add_argument("--bucket", required=True, choices=["green", "yellow"], help="Bucket being processed")
    ap.add_argument("--raw-root", default=None, help="Override raw root")
    ap.add_argument("--manifests-root", default=None, help="Override manifests root")
    ap.add_argument("--logs-root", default=None, help="Override logs root")
    ap.add_argument("--execute", action="store_true", help="Perform downloads")
    ap.add_argument("--overwrite", action="store_true", help="Overwrite existing outputs")
    ap.add_argument("--verify-sha256", action="store_true", help="Compute sha256 for http downloads")
    ap.add_argument("--verify-zenodo-md5", action="store_true", help="Verify Zenodo md5")
    ap.add_argument(
        "--resume",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Resume partial downloads (default: resume).",
    )
    ap.add_argument("--limit-targets", type=int, default=None)
    ap.add_argument("--limit-files", type=int, default=None)
    ap.add_argument("--max-bytes-per-target", type=int, default=None)
    ap.add_argument("--workers", type=int, default=1)
    ap.add_argument("--retry-max", type=int, default=3)
    ap.add_argument("--retry-backoff", type=float, default=2.0)
    ap.add_argument("--strict", "--fail-on-error", dest="strict", action="store_true")
    args = ap.parse_args()

    queue_path = Path(args.queue).expanduser().resolve()
    rows = read_jsonl(queue_path)

    targets_path = Path(args.targets_yaml).expanduser().resolve() if args.targets_yaml else None
    cfg = load_config(targets_path)
    roots = load_roots(cfg, args, defaults)
    ensure_dir(roots.logs_root)

    ctx = AcquireContext(
        roots=roots,
        limits=Limits(args.limit_targets, args.limit_files, args.max_bytes_per_target),
        mode=RunMode(
            args.execute,
            args.overwrite,
            args.verify_sha256,
            args.verify_zenodo_md5,
            args.resume,
            max(1, args.workers),
        ),
        retry=RetryConfig(args.retry_max, args.retry_backoff),
        cfg=cfg,
    )

    if ctx.limits.limit_targets:
        rows = rows[: ctx.limits.limit_targets]
    rows = [r for r in rows if r.get("enabled", True) and r.get("id")]

    summary = {
        "run_at_utc": utc_now(),
        "pipeline_version": VERSION,
        "queue": str(queue_path),
        "bucket": args.bucket,
        "execute": ctx.mode.execute,
        "results": [],
    }

    if ctx.mode.workers > 1 and ctx.mode.execute:
        with ThreadPoolExecutor(max_workers=ctx.mode.workers) as ex:
            futures = {ex.submit(run_target, ctx, args.bucket, row, strategy_handlers, postprocess): row for row in rows}
            for fut in as_completed(futures):
                try:
                    res = fut.result()
                except Exception as e:
                    res = {"id": futures[fut].get("id"), "status": "error", "error": repr(e)}
                summary["results"].append(res)
    else:
        for row in rows:
            res = run_target(ctx, args.bucket, row, strategy_handlers, postprocess)
            summary["results"].append(res)

    status_counts = Counter(result.get("status") or "unknown" for result in summary["results"])
    summary["counts"] = {"total": len(summary["results"]), **dict(status_counts)}
    summary["failed_targets"] = [
        {"id": result.get("id", "unknown"), "error": result.get("error", "unknown")}
        for result in summary["results"]
        if result.get("status") == "error"
    ]

    write_json(roots.logs_root / f"acquire_summary_{args.bucket}.json", summary)
    if ctx.mode.execute and args.strict and any(r.get("status") == "error" for r in summary["results"]):
        sys.exit(1)
