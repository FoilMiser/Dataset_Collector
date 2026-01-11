from __future__ import annotations

import argparse
import dataclasses
import hashlib
import ipaddress
import json
import os
import socket
import subprocess
import sys
import time
from collections import Counter
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

from collector_core.__version__ import __version__ as VERSION
from collector_core.acquire_limits import (
    RunByteBudget,
    build_target_limit_enforcer,
    build_run_budget,
    cleanup_path,
    resolve_result_bytes,
)
from collector_core.artifact_metadata import build_artifact_metadata
from collector_core.checks.runner import generate_run_id, run_checks_for_target
from collector_core.config_validator import read_yaml
from collector_core.dataset_root import ensure_data_root_allowed, resolve_dataset_root
from collector_core.dependencies import _try_import, requires
from collector_core.network_utils import _with_retries
from collector_core.rate_limit import get_resolver_rate_limiter
from collector_core.utils import (
    ensure_dir,
    safe_filename,
    utc_now,
    write_json,
)

# Alias for backwards compatibility
safe_name = safe_filename

requests = _try_import("requests")
FTP = _try_import("ftplib", "FTP")


StrategyHandler = Callable[["AcquireContext", dict[str, Any], Path], list[dict[str, Any]]]
PostProcessor = Callable[
    ["AcquireContext", dict[str, Any], Path, str, dict[str, Any]], dict[str, Any] | None
]


@dataclasses.dataclass(frozen=True)
class RootsDefaults:
    raw_root: str
    manifests_root: str
    ledger_root: str
    logs_root: str


@dataclasses.dataclass
class Roots:
    raw_root: Path
    manifests_root: Path
    ledger_root: Path
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
    run_budget: RunByteBudget | None = None
    allow_non_global_download_hosts: bool = False
    internal_mirror_allowlist: "InternalMirrorAllowlist" = dataclasses.field(
        default_factory=lambda: InternalMirrorAllowlist()
    )
    cfg: dict[str, Any] | None = None
    checks_run_id: str = ""


@dataclasses.dataclass(frozen=True)
class InternalMirrorAllowlist:
    hosts: frozenset[str] = frozenset()
    networks: tuple[ipaddress.IPv4Network | ipaddress.IPv6Network, ...] = ()

    def allows_host(self, hostname: str) -> bool:
        normalized = hostname.lower().rstrip(".")
        for host in self.hosts:
            if host.startswith("."):
                if normalized.endswith(host):
                    return True
            elif normalized == host:
                return True
        return False

    def allows_ip(self, ip_value: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
        return any(ip_value in network for network in self.networks)


def _normalize_internal_mirror_allowlist(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if str(item).strip()]
    return [str(value).strip()]


def _build_internal_mirror_allowlist(values: list[str]) -> InternalMirrorAllowlist:
    hosts: set[str] = set()
    networks: list[ipaddress.IPv4Network | ipaddress.IPv6Network] = []
    for entry in values:
        text = entry.strip()
        if not text:
            continue
        try:
            network = ipaddress.ip_network(text, strict=False)
        except ValueError:
            hosts.add(text.lower().rstrip("."))
        else:
            networks.append(network)
    return InternalMirrorAllowlist(hosts=frozenset(hosts), networks=tuple(networks))


def _read_jsonl_list(path: Path) -> list[dict[str, Any]]:
    """Read JSONL file and return as list (local non-gzip version for backwards compatibility)."""
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def _non_global_ip_reason(ip: ipaddress.IPv4Address | ipaddress.IPv6Address) -> str:
    if ip.is_private:
        return "private"
    if ip.is_loopback:
        return "loopback"
    if ip.is_link_local:
        return "link_local"
    if ip.is_multicast:
        return "multicast"
    if ip.is_reserved:
        return "reserved"
    if ip.is_unspecified:
        return "unspecified"
    return "non_global"


def _resolve_host_ips(hostname: str) -> list[str]:
    ips: set[str] = set()
    try:
        addrinfo = socket.getaddrinfo(hostname, None)
    except socket.gaierror:
        return []
    for item in addrinfo:
        sockaddr = item[4]
        if sockaddr:
            ips.add(sockaddr[0])
    return sorted(ips)


def validate_download_url(
    url: str,
    allow_non_global_hosts: bool,
    internal_mirror_allowlist: InternalMirrorAllowlist | None = None,
) -> tuple[bool, str | None]:
    parsed = urlparse(url)
    scheme = (parsed.scheme or "").lower()
    if scheme not in {"http", "https"}:
        return False, f"unsupported_scheme:{scheme or 'missing'}"
    if not parsed.hostname:
        return False, "missing_hostname"
    if allow_non_global_hosts:
        return True, None
    allowlist = internal_mirror_allowlist or InternalMirrorAllowlist()
    hostname = parsed.hostname
    if allowlist.allows_host(hostname):
        return True, None
    try:
        ip_value = ipaddress.ip_address(hostname)
    except ValueError:
        ips = _resolve_host_ips(hostname)
        if not ips:
            return False, "unresolvable_hostname"
        for ip_str in ips:
            ip_value = ipaddress.ip_address(ip_str)
            if ip_value.is_global:
                continue
            if allowlist.allows_ip(ip_value):
                continue
            return False, f"blocked_ip:{ip_str}:{_non_global_ip_reason(ip_value)}"
        return True, None
    if not ip_value.is_global and not allowlist.allows_ip(ip_value):
        return False, f"blocked_ip:{ip_value}:{_non_global_ip_reason(ip_value)}"
    return True, None


def _validate_redirect_chain(
    response: requests.Response,
    allow_non_global_hosts: bool,
    internal_mirror_allowlist: InternalMirrorAllowlist,
) -> tuple[bool, str | None, str | None]:
    redirect_urls: list[str] = []
    for resp in response.history:
        location = (resp.headers or {}).get("Location")
        if location:
            redirect_urls.append(urljoin(resp.url, location))
    redirect_urls.append(response.url)
    for redirect_url in redirect_urls:
        allowed, reason = validate_download_url(
            redirect_url, allow_non_global_hosts, internal_mirror_allowlist
        )
        if not allowed:
            return False, reason, redirect_url
    return True, None, None


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
    p = subprocess.run(
        cmd,
        cwd=str(cwd) if cwd else None,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
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
    blocked_url: str | None = None
    blocked_reason: str | None = None

    def _stream_response(
        response: requests.Response, write_mode: str, existing_offset: int
    ) -> None:
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

    allowed, reason = validate_download_url(
        url, ctx.allow_non_global_download_hosts, ctx.internal_mirror_allowlist
    )
    if not allowed:
        return {
            "status": "error",
            "error": "blocked_url",
            "reason": reason,
            "url": url,
        }

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
                allowed, reason, blocked = _validate_redirect_chain(
                    r, ctx.allow_non_global_download_hosts, ctx.internal_mirror_allowlist
                )
                if not allowed:
                    blocked_reason = reason
                    blocked_url = blocked
                    break
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
                                allowed, reason, blocked = _validate_redirect_chain(
                                    fresh,
                                    ctx.allow_non_global_download_hosts,
                                    ctx.internal_mirror_allowlist,
                                )
                                if not allowed:
                                    blocked_reason = reason
                                    blocked_url = blocked
                                    break
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
    if blocked_url:
        temp_path.unlink(missing_ok=True)
        return {
            "status": "error",
            "error": "blocked_url",
            "reason": blocked_reason,
            "url": url,
            "blocked_url": blocked_url,
        }
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


def handle_http_multi(
    ctx: AcquireContext, row: dict[str, Any], out_dir: Path
) -> list[dict[str, Any]]:
    download = normalize_download(row.get("download", {}) or {})
    enforcer = build_target_limit_enforcer(
        target_id=str(row.get("id", "unknown")),
        limit_files=ctx.limits.limit_files,
        max_bytes_per_target=ctx.limits.max_bytes_per_target,
        download=download,
        run_budget=ctx.run_budget,
    )
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
    expected_size = download.get("expected_size")
    expected_sizes: list[int | None] | None = None
    expected_size_map: dict[str, int] | None = None
    if isinstance(expected_size, list):
        expected_sizes = [int(s) if s is not None else None for s in expected_size]
    elif isinstance(expected_size, dict):
        expected_size_map = {str(k): int(v) for k, v in expected_size.items() if v is not None}
    for idx, url in enumerate(urls):
        filename = (
            (filenames[idx] if idx < len(filenames) else None)
            or download.get("filename")
            or safe_name(urlparse(url).path.split("/")[-1])
            or f"payload_{idx}.bin"
        )
        limit_error = enforcer.start_file(filename)
        if limit_error:
            results.append(limit_error)
            break
        limit_error = enforcer.check_remaining_bytes(filename)
        if limit_error:
            results.append(limit_error)
            break
        out_path = out_dir / filename
        if out_path.exists() and not ctx.mode.overwrite:
            result = {"status": "ok", "path": str(out_path), "cached": True}
            size_bytes = resolve_result_bytes(result, out_path)
            limit_error = enforcer.record_bytes(size_bytes, filename)
            if limit_error:
                results.append(limit_error)
            else:
                results.append(result)
            continue
        if not ctx.mode.execute:
            result = {"status": "noop", "path": str(out_path)}
            results.append(result)
            continue
        expected = expected_sha256
        if expected_sha256s is not None:
            expected = expected_sha256s[idx] if idx < len(expected_sha256s) else None
        elif expected_sha256_map is not None:
            expected = expected_sha256_map.get(filename) or expected_sha256_map.get(url)
        size_hint = None
        if expected_sizes is not None:
            size_hint = expected_sizes[idx] if idx < len(expected_sizes) else None
        elif expected_size_map is not None:
            size_hint = expected_size_map.get(filename) or expected_size_map.get(url)
        else:
            size_hint = int(expected_size) if expected_size is not None else None
        limit_error = enforcer.check_size_hint(size_hint, filename)
        if limit_error:
            results.append(limit_error)
            continue
        result = _http_download_with_resume(ctx, url, out_path, size_hint, expected)
        size_bytes = resolve_result_bytes(result, out_path)
        limit_error = enforcer.record_bytes(size_bytes, filename)
        if limit_error:
            if result.get("status") == "ok" and not result.get("cached"):
                cleanup_path(out_path)
            results.append(limit_error)
        else:
            results.append(result)
    return results


def handle_http_single(
    ctx: AcquireContext, row: dict[str, Any], out_dir: Path
) -> list[dict[str, Any]]:
    download = normalize_download(row.get("download", {}) or {})
    enforcer = build_target_limit_enforcer(
        target_id=str(row.get("id", "unknown")),
        limit_files=ctx.limits.limit_files,
        max_bytes_per_target=ctx.limits.max_bytes_per_target,
        download=download,
        run_budget=ctx.run_budget,
    )
    url = download.get("url") or download.get("urls", [None])[0]
    if not url:
        return [{"status": "error", "error": "missing url"}]
    filename = download.get("filename") or safe_name(urlparse(url).path.split("/")[-1])
    if not filename:
        filename = "payload.bin"
    limit_error = enforcer.start_file(filename)
    if limit_error:
        return [limit_error]
    limit_error = enforcer.check_remaining_bytes(filename)
    if limit_error:
        return [limit_error]
    size_hint = download.get("expected_size")
    limit_error = enforcer.check_size_hint(
        int(size_hint) if size_hint is not None else None, filename
    )
    if limit_error:
        return [limit_error]
    out_path = out_dir / filename
    if out_path.exists() and not ctx.mode.overwrite:
        result = {"status": "ok", "path": str(out_path), "cached": True}
        size_bytes = resolve_result_bytes(result, out_path)
        limit_error = enforcer.record_bytes(size_bytes, filename)
        return [limit_error] if limit_error else [result]
    if not ctx.mode.execute:
        return [{"status": "noop", "path": str(out_path)}]
    result = _http_download_with_resume(
        ctx, url, out_path, size_hint, download.get("expected_sha256")
    )
    size_bytes = resolve_result_bytes(result, out_path)
    limit_error = enforcer.record_bytes(size_bytes, filename)
    if limit_error:
        if result.get("status") == "ok" and not result.get("cached"):
            cleanup_path(out_path)
        return [limit_error]
    return [result]


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
    enforcer = build_target_limit_enforcer(
        target_id=str(row.get("id", "unknown")),
        limit_files=ctx.limits.limit_files,
        max_bytes_per_target=ctx.limits.max_bytes_per_target,
        download=download,
        run_budget=ctx.run_budget,
    )
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
            for fname in files:
                limit_error = enforcer.start_file(fname)
                if limit_error:
                    results.append(limit_error)
                    return results
                limit_error = enforcer.check_remaining_bytes(fname)
                if limit_error:
                    results.append(limit_error)
                    return results
                local = out_dir / fname
                ensure_dir(local.parent)
                temp_path = local.with_name(f"{local.name}.part")
                with temp_path.open("wb") as f:
                    ftp.retrbinary(f"RETR {fname}", f.write)
                content_length = temp_path.stat().st_size
                limit_error = enforcer.check_size_hint(content_length, fname)
                if limit_error:
                    temp_path.unlink(missing_ok=True)
                    results.append(limit_error)
                    continue
                sha256 = sha256_file(temp_path)
                limit_error = enforcer.record_bytes(content_length, fname)
                if limit_error:
                    temp_path.unlink(missing_ok=True)
                    results.append(limit_error)
                    continue
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
    enforcer = build_target_limit_enforcer(
        target_id=str(row.get("id", "unknown")),
        limit_files=ctx.limits.limit_files,
        max_bytes_per_target=ctx.limits.max_bytes_per_target,
        download=download,
        run_budget=ctx.run_budget,
    )
    repo = (
        download.get("repo")
        or download.get("repo_url")
        or download.get("url")
        or download.get("url")
    )
    branch = download.get("branch")
    commit = download.get("commit")
    tag = download.get("tag")
    revision = commit or tag
    if not repo:
        return [{"status": "error", "error": "missing repo"}]
    limit_error = enforcer.start_file(repo)
    if limit_error:
        return [limit_error]
    limit_error = enforcer.check_remaining_bytes(repo)
    if limit_error:
        return [limit_error]
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
        size_bytes = resolve_result_bytes(result, out_dir)
        limit_error = enforcer.record_bytes(size_bytes, repo)
        return [limit_error] if limit_error else [result]
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
    size_bytes = resolve_result_bytes(result, out_dir)
    limit_error = enforcer.record_bytes(size_bytes, repo)
    if limit_error:
        cleanup_path(out_dir)
        return [limit_error]
    return [result]


def handle_zenodo(ctx: AcquireContext, row: dict[str, Any], out_dir: Path) -> list[dict[str, Any]]:
    download = normalize_download(row.get("download", {}) or {})
    enforcer = build_target_limit_enforcer(
        target_id=str(row.get("id", "unknown")),
        limit_files=ctx.limits.limit_files,
        max_bytes_per_target=ctx.limits.max_bytes_per_target,
        download=download,
        run_budget=ctx.run_budget,
    )
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
    for f in files:
        link = f.get("links", {}).get("self") or f.get("link")
        if not link:
            continue
        filename = f.get("key") or f.get("name") or safe_name(link)
        limit_error = enforcer.start_file(filename)
        if limit_error:
            results.append(limit_error)
            break
        limit_error = enforcer.check_remaining_bytes(filename)
        if limit_error:
            results.append(limit_error)
            break
        size_hint = f.get("size")
        limit_error = enforcer.check_size_hint(
            int(size_hint) if size_hint is not None else None, filename
        )
        if limit_error:
            results.append(limit_error)
            continue
        out_path = out_dir / filename
        ensure_dir(out_path.parent)
        r = _http_download_with_resume(ctx, link, out_path)
        if ctx.mode.verify_zenodo_md5 and f.get("checksum", "").startswith("md5:"):
            expected_md5 = f["checksum"].split(":", 1)[1]
            if md5_file(out_path) != expected_md5:
                r = {"status": "error", "error": "md5_mismatch"}
        size_bytes = resolve_result_bytes(r, out_path)
        limit_error = enforcer.record_bytes(size_bytes, filename)
        if limit_error:
            if r.get("status") == "ok" and not r.get("cached"):
                cleanup_path(out_path)
            results.append(limit_error)
        else:
            results.append(r)
    return results or [{"status": "noop", "reason": "no files"}]


def handle_dataverse(
    ctx: AcquireContext, row: dict[str, Any], out_dir: Path
) -> list[dict[str, Any]]:
    download = normalize_download(row.get("download", {}) or {})
    enforcer = build_target_limit_enforcer(
        target_id=str(row.get("id", "unknown")),
        limit_files=ctx.limits.limit_files,
        max_bytes_per_target=ctx.limits.max_bytes_per_target,
        download=download,
        run_budget=ctx.run_budget,
    )
    pid = download.get("persistent_id") or download.get("pid")
    instance = download.get("instance") or "https://dataverse.harvard.edu"
    if not pid:
        return [{"status": "error", "error": "missing persistent_id"}]
    missing = requires("requests", requests, install="pip install requests")
    if missing:
        return [{"status": "error", "error": missing}]
    limit_error = enforcer.start_file(pid)
    if limit_error:
        return [limit_error]
    limit_error = enforcer.check_remaining_bytes(pid)
    if limit_error:
        return [limit_error]
    if not ctx.mode.execute:
        return [{"status": "noop", "path": str(out_dir)}]
    url = f"{instance}/api/access/dvobject/{pid}"
    resp = requests.get(url, allow_redirects=True, timeout=60)
    resp.raise_for_status()
    size_hint = resp.headers.get("Content-Length")
    limit_error = enforcer.check_size_hint(int(size_hint) if size_hint else None, pid)
    if limit_error:
        return [limit_error]
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
    limit_error = enforcer.record_bytes(content_length, filename)
    if limit_error:
        temp_path.unlink(missing_ok=True)
        return [limit_error]
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


def handle_figshare_article(
    ctx: AcquireContext, row: dict[str, Any], out_dir: Path
) -> list[dict[str, Any]]:
    missing = requires("requests", requests, install="pip install requests")
    if missing:
        return [{"status": "error", "error": missing}]
    download = normalize_download(row.get("download", {}) or {})
    enforcer = build_target_limit_enforcer(
        target_id=str(row.get("id", "unknown")),
        limit_files=ctx.limits.limit_files,
        max_bytes_per_target=ctx.limits.max_bytes_per_target,
        download=download,
        run_budget=ctx.run_budget,
    )
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

    # Get rate limiter from config
    rate_limiter, rate_config = get_resolver_rate_limiter(ctx.cfg, "figshare")

    def _fetch() -> requests.Response:
        # Acquire rate limit token before API request
        if rate_limiter:
            rate_limiter.acquire()
        resp = requests.get(endpoint, timeout=60)
        resp.raise_for_status()
        return resp

    resp = _with_retries(
        _fetch,
        max_attempts=ctx.retry.max_attempts,
        backoff_base=ctx.retry.backoff_base,
        backoff_max=ctx.retry.backoff_max,
        retry_on_429=rate_config.retry_on_429,
        retry_on_403=rate_config.retry_on_403,
    )
    meta = resp.json()
    files = meta.get("files", []) or []
    results: list[dict[str, Any]] = []
    for idx, fmeta in enumerate(files):
        download_url = fmeta.get("download_url") or (fmeta.get("links") or {}).get("download")
        if not download_url:
            results.append(
                {"status": "error", "error": "missing_download_url", "file": fmeta.get("name")}
            )
            continue
        fname = safe_name(fmeta.get("name") or fmeta.get("id") or f"figshare_file_{idx}")
        limit_error = enforcer.start_file(fname)
        if limit_error:
            results.append(limit_error)
            break
        limit_error = enforcer.check_remaining_bytes(fname)
        if limit_error:
            results.append(limit_error)
            break
        expected_size = fmeta.get("size")
        limit_error = enforcer.check_size_hint(
            int(expected_size) if expected_size is not None else None, fname
        )
        if limit_error:
            results.append(limit_error)
            continue
        out_path = out_dir / fname
        result = _http_download_with_resume(ctx, download_url, out_path, expected_size)
        size_bytes = resolve_result_bytes(result, out_path)
        limit_error = enforcer.record_bytes(size_bytes, fname)
        if limit_error:
            if result.get("status") == "ok" and not result.get("cached"):
                cleanup_path(out_path)
            results.append(limit_error)
        else:
            results.append(result)
    write_json(out_dir / "figshare_article.json", meta)
    return results


handle_figshare = handle_figshare_article


def handle_figshare_files(
    ctx: AcquireContext, row: dict[str, Any], out_dir: Path
) -> list[dict[str, Any]]:
    download = normalize_download(row.get("download", {}) or {})
    enforcer = build_target_limit_enforcer(
        target_id=str(row.get("id", "unknown")),
        limit_files=ctx.limits.limit_files,
        max_bytes_per_target=ctx.limits.max_bytes_per_target,
        download=download,
        run_budget=ctx.run_budget,
    )
    article_id = download.get("article_id") or download.get("id")
    api = download.get("api") or (
        f"https://api.figshare.com/v2/articles/{article_id}/files" if article_id else None
    )
    if not article_id or not api:
        return [{"status": "error", "error": "missing article_id"}]
    missing = requires("requests", requests, install="pip install requests")
    if missing:
        return [{"status": "error", "error": missing}]
    if not ctx.mode.execute:
        return [{"status": "noop", "path": str(out_dir)}]

    # Get rate limiter from config
    rate_limiter, rate_config = get_resolver_rate_limiter(ctx.cfg, "figshare")

    def _fetch() -> requests.Response:
        # Acquire rate limit token before API request
        if rate_limiter:
            rate_limiter.acquire()
        resp = requests.get(api, timeout=120)
        resp.raise_for_status()
        return resp

    resp = _with_retries(
        _fetch,
        max_attempts=ctx.retry.max_attempts,
        backoff_base=ctx.retry.backoff_base,
        backoff_max=ctx.retry.backoff_max,
        retry_on_429=rate_config.retry_on_429,
        retry_on_403=rate_config.retry_on_403,
    )
    files = resp.json() or []
    ensure_dir(out_dir)
    results: list[dict[str, Any]] = []
    for f in files:
        link = f.get("download_url") or (f.get("links") or {}).get("download")
        if not link:
            continue
        filename = safe_name(f.get("name") or f.get("id") or str(article_id))
        out_path = out_dir / filename
        limit_error = enforcer.start_file(filename)
        if limit_error:
            results.append(limit_error)
            break
        limit_error = enforcer.check_remaining_bytes(filename)
        if limit_error:
            results.append(limit_error)
            break
        size_hint = f.get("size")
        limit_error = enforcer.check_size_hint(
            int(size_hint) if size_hint is not None else None, filename
        )
        if limit_error:
            results.append(limit_error)
            continue
        result = _http_download_with_resume(ctx, link, out_path)
        size_bytes = resolve_result_bytes(result, out_path)
        limit_error = enforcer.record_bytes(size_bytes, filename)
        if limit_error:
            if result.get("status") == "ok" and not result.get("cached"):
                cleanup_path(out_path)
            results.append(limit_error)
        else:
            results.append(result)
    return results or [{"status": "noop", "reason": "no files"}]


def make_github_release_handler(user_agent: str) -> StrategyHandler:
    def _handle_github_release(
        ctx: AcquireContext, row: dict[str, Any], out_dir: Path
    ) -> list[dict[str, Any]]:
        missing = requires("requests", requests, install="pip install requests")
        if missing:
            return [{"status": "error", "error": missing}]
        download = normalize_download(row.get("download", {}) or {})
        enforcer = build_target_limit_enforcer(
            target_id=str(row.get("id", "unknown")),
            limit_files=ctx.limits.limit_files,
            max_bytes_per_target=ctx.limits.max_bytes_per_target,
            download=download,
            run_budget=ctx.run_budget,
        )
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

        # Get rate limiter from config - GitHub uses 403 for rate limits
        rate_limiter, rate_config = get_resolver_rate_limiter(ctx.cfg, "github")

        def _fetch() -> requests.Response:
            # Acquire rate limit token before API request
            if rate_limiter:
                rate_limiter.acquire()
            resp = requests.get(url, headers=headers, timeout=60)
            resp.raise_for_status()
            return resp

        resp = _with_retries(
            _fetch,
            max_attempts=ctx.retry.max_attempts,
            backoff_base=ctx.retry.backoff_base,
            backoff_max=ctx.retry.backoff_max,
            retry_on_429=rate_config.retry_on_429,
            retry_on_403=rate_config.retry_on_403,  # GitHub uses 403 for rate limits
        )
        meta = resp.json()
        assets = meta.get("assets", []) or []
        results: list[dict[str, Any]] = []
        for idx, asset in enumerate(assets):
            download_url = asset.get("browser_download_url") or asset.get("url")
            if not download_url:
                results.append(
                    {"status": "error", "error": "missing_download_url", "asset": asset.get("name")}
                )
                continue
            fname = safe_name(asset.get("name") or f"{repo}_asset_{idx}")
            limit_error = enforcer.start_file(fname)
            if limit_error:
                results.append(limit_error)
                break
            limit_error = enforcer.check_remaining_bytes(fname)
            if limit_error:
                results.append(limit_error)
                break
            size_hint = asset.get("size")
            limit_error = enforcer.check_size_hint(
                int(size_hint) if size_hint is not None else None, fname
            )
            if limit_error:
                results.append(limit_error)
                continue
            out_path = out_dir / fname
            result = _http_download_with_resume(ctx, download_url, out_path, size_hint)
            size_bytes = resolve_result_bytes(result, out_path)
            limit_error = enforcer.record_bytes(size_bytes, fname)
            if limit_error:
                if result.get("status") == "ok" and not result.get("cached"):
                    cleanup_path(out_path)
                results.append(limit_error)
            else:
                results.append(result)
        write_json(out_dir / "github_release.json", meta)
        return results

    return _handle_github_release


def handle_hf_datasets(
    ctx: AcquireContext, row: dict[str, Any], out_dir: Path
) -> list[dict[str, Any]]:
    download = normalize_download(row.get("download", {}) or {})
    enforcer = build_target_limit_enforcer(
        target_id=str(row.get("id", "unknown")),
        limit_files=ctx.limits.limit_files,
        max_bytes_per_target=ctx.limits.max_bytes_per_target,
        download=download,
        run_budget=ctx.run_budget,
    )
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
            file_label = f"{dataset_id}:{sp}"
            limit_error = enforcer.start_file(file_label)
            if limit_error:
                results.append(limit_error)
                break
            limit_error = enforcer.check_remaining_bytes(file_label)
            if limit_error:
                results.append(limit_error)
                break
            ds = load_dataset(dataset_id, split=sp, **load_kwargs)
            sp_dir = out_dir / f"split_{safe_name(sp)}"
            ds.save_to_disk(str(sp_dir))
            result = {"status": "ok", "dataset_id": dataset_id, "split": sp, "path": str(sp_dir)}
            size_bytes = resolve_result_bytes(result, sp_dir)
            limit_error = enforcer.record_bytes(size_bytes, file_label)
            if limit_error:
                cleanup_path(sp_dir)
                results.append(limit_error)
            else:
                results.append(result)
    else:
        file_label = dataset_id
        limit_error = enforcer.start_file(file_label)
        if limit_error:
            return [limit_error]
        limit_error = enforcer.check_remaining_bytes(file_label)
        if limit_error:
            return [limit_error]
        ds = load_dataset(dataset_id, **load_kwargs)
        ds_path = out_dir / "hf_dataset"
        ds.save_to_disk(str(ds_path))
        result = {"status": "ok", "dataset_id": dataset_id, "path": str(ds_path)}
        size_bytes = resolve_result_bytes(result, ds_path)
        limit_error = enforcer.record_bytes(size_bytes, file_label)
        if limit_error:
            cleanup_path(ds_path)
            return [limit_error]
        results.append(result)
    return results


DEFAULT_STRATEGY_HANDLERS: dict[str, StrategyHandler] = {
    "http": handle_http_multi,
    "ftp": handle_ftp,
    "git": handle_git,
    "zenodo": handle_zenodo,
    "dataverse": handle_dataverse,
    "huggingface_datasets": handle_hf_datasets,
}


def handle_s3_sync(ctx: AcquireContext, row: dict[str, Any], out_dir: Path) -> list[dict[str, Any]]:
    download = normalize_download(row.get("download", {}) or {})
    enforcer = build_target_limit_enforcer(
        target_id=str(row.get("id", "unknown")),
        limit_files=ctx.limits.limit_files,
        max_bytes_per_target=ctx.limits.max_bytes_per_target,
        download=download,
        run_budget=ctx.run_budget,
    )
    urls = download.get("urls") or []
    if not urls:
        return [{"status": "error", "error": "missing urls"}]
    if not ctx.mode.execute:
        return [{"status": "noop", "path": str(out_dir)}]
    ensure_dir(out_dir)
    results: list[dict[str, Any]] = []
    extra_args = download.get("extra_args", []) or []
    for url in urls:
        limit_error = enforcer.start_file(url)
        if limit_error:
            results.append(limit_error)
            break
        limit_error = enforcer.check_remaining_bytes(url)
        if limit_error:
            results.append(limit_error)
            break
        before_bytes = resolve_result_bytes({}, out_dir) or 0
        cmd = ["aws", "s3", "sync", url, str(out_dir)]
        if download.get("no_sign_request"):
            cmd.append("--no-sign-request")
        if download.get("request_payer"):
            cmd += ["--request-payer", str(download.get("request_payer"))]
        cmd += [str(a) for a in extra_args]
        log = run_cmd(cmd)
        after_bytes = resolve_result_bytes({}, out_dir) or before_bytes
        delta_bytes = max(0, after_bytes - before_bytes)
        limit_error = enforcer.record_bytes(delta_bytes, url)
        result = {"status": "ok", "path": str(out_dir), "log": log}
        if limit_error:
            results.append(limit_error)
        else:
            results.append(result)
    return results


def handle_aws_requester_pays(
    ctx: AcquireContext, row: dict[str, Any], out_dir: Path
) -> list[dict[str, Any]]:
    download = normalize_download(row.get("download", {}) or {})
    enforcer = build_target_limit_enforcer(
        target_id=str(row.get("id", "unknown")),
        limit_files=ctx.limits.limit_files,
        max_bytes_per_target=ctx.limits.max_bytes_per_target,
        download=download,
        run_budget=ctx.run_budget,
    )
    bucket = download.get("bucket")
    key = download.get("key")
    if not bucket or not key:
        return [{"status": "error", "error": "missing bucket/key"}]
    dest_filename = download.get("dest_filename") or safe_name(Path(key).name)
    limit_error = enforcer.start_file(dest_filename)
    if limit_error:
        return [limit_error]
    limit_error = enforcer.check_remaining_bytes(dest_filename)
    if limit_error:
        return [limit_error]
    out_path = out_dir / dest_filename
    if out_path.exists() and not ctx.mode.overwrite:
        result = {"status": "ok", "path": str(out_path), "cached": True}
        size_bytes = resolve_result_bytes(result, out_path)
        limit_error = enforcer.record_bytes(size_bytes, dest_filename)
        return [limit_error] if limit_error else [result]
    if not ctx.mode.execute:
        return [{"status": "noop", "path": str(out_path)}]
    ensure_dir(out_path.parent)
    payer = download.get("request_payer", "requester")
    temp_path = out_path.with_name(f"{out_path.name}.part")
    cmd = [
        "aws",
        "s3api",
        "get-object",
        "--bucket",
        bucket,
        "--key",
        key,
        str(temp_path),
        "--request-payer",
        payer,
    ]
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
    limit_error = enforcer.record_bytes(content_length, dest_filename)
    if limit_error:
        temp_path.unlink(missing_ok=True)
        return [limit_error]
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
    enforcer = build_target_limit_enforcer(
        target_id=str(row.get("id", "unknown")),
        limit_files=ctx.limits.limit_files,
        max_bytes_per_target=ctx.limits.max_bytes_per_target,
        download=download,
        run_budget=ctx.run_budget,
    )
    magnet = download.get("magnet") or download.get("torrent")
    if not magnet:
        return [{"status": "error", "error": "missing magnet/torrent"}]
    limit_error = enforcer.start_file(magnet)
    if limit_error:
        return [limit_error]
    limit_error = enforcer.check_remaining_bytes(magnet)
    if limit_error:
        return [limit_error]
    if not ctx.mode.execute:
        return [{"status": "noop", "path": str(out_dir)}]
    ensure_dir(out_dir)
    try:
        log = run_cmd(["aria2c", "--seed-time=0", "-d", str(out_dir), magnet])
        result = {"status": "ok", "path": str(out_dir), "log": log}
        size_bytes = resolve_result_bytes(result, out_dir)
        limit_error = enforcer.record_bytes(size_bytes, magnet)
        if limit_error:
            cleanup_path(out_dir)
            return [limit_error]
        return [result]
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
    ctx: AcquireContext,
    target_id: str,
    bucket: str,
    status: str,
    extra: dict[str, Any] | None = None,
) -> None:
    marker = ctx.roots.manifests_root / safe_name(target_id) / "acquire_done.json"
    payload = {
        "target_id": target_id,
        "bucket": bucket,
        "status": status,
        "written_at_utc": utc_now(),
        "version": VERSION,
    }
    payload.update(
        build_artifact_metadata(
            written_at_utc=payload["written_at_utc"],
            git_commit=extra.get("git_commit") if extra else None,
        )
    )
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
    content_checks = row.get("content_checks") or []
    if not isinstance(content_checks, list):
        content_checks = [str(content_checks)]
    strat = (row.get("download", {}) or {}).get("strategy", "none")
    out_dir = resolve_output_dir(ctx, bucket, pool, tid)
    manifest = {
        "id": tid,
        "name": row.get("name", tid),
        "bucket": bucket,
        "license_pool": pool,
        "strategy": strat,
        "started_at_utc": utc_now(),
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
    manifest.update(
        build_artifact_metadata(
            written_at_utc=manifest["finished_at_utc"],
            git_commit=git_info.get("git_commit") if git_info else None,
        )
    )
    write_json(out_dir / "download_manifest.json", manifest)

    status = (
        "ok"
        if any(r.get("status") == "ok" for r in manifest["results"])
        else manifest["results"][0].get("status", "error")
    )
    if post_processors:
        for proc in post_processors.values():
            if isinstance(proc, dict) and proc.get("status") not in {"ok", "noop"}:
                status = proc.get("status", status)
    if ctx.mode.execute:
        write_done_marker(ctx, tid, bucket, status, git_info)
    run_checks_for_target(
        content_checks=content_checks,
        ledger_root=ctx.roots.ledger_root,
        run_id=ctx.checks_run_id,
        target_id=tid,
        stage="acquire",
        row=row,
        extra={"bucket": bucket, "status": status},
    )
    return {"id": tid, "status": status, "bucket": bucket, "license_pool": pool, "strategy": strat}


def load_config(targets_path: Path | None) -> dict[str, Any]:
    cfg: dict[str, Any] = {}
    if targets_path and targets_path.exists():
        cfg = read_yaml(targets_path, schema_name="targets") or {}
    return cfg


def load_roots(
    cfg: dict[str, Any], overrides: argparse.Namespace, defaults: RootsDefaults
) -> Roots:
    allow_data_root = bool(getattr(overrides, "allow_data_root", False))
    dataset_root = resolve_dataset_root(overrides.dataset_root)
    if dataset_root:
        defaults = RootsDefaults(
            raw_root=str(dataset_root / "raw"),
            manifests_root=str(dataset_root / "_manifests"),
            ledger_root=str(dataset_root / "_ledger"),
            logs_root=str(dataset_root / "_logs"),
        )
    g = cfg.get("globals", {}) or {}
    raw_root = Path(overrides.raw_root or g.get("raw_root", defaults.raw_root))
    manifests_root = Path(
        overrides.manifests_root or g.get("manifests_root", defaults.manifests_root)
    )
    ledger_root = Path(overrides.ledger_root or g.get("ledger_root", defaults.ledger_root))
    logs_root = Path(overrides.logs_root or g.get("logs_root", defaults.logs_root))
    roots = Roots(
        raw_root=raw_root.expanduser().resolve(),
        manifests_root=manifests_root.expanduser().resolve(),
        ledger_root=ledger_root.expanduser().resolve(),
        logs_root=logs_root.expanduser().resolve(),
    )
    ensure_data_root_allowed(
        [roots.raw_root, roots.manifests_root, roots.ledger_root, roots.logs_root],
        allow_data_root,
    )
    return roots


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
    ap.add_argument(
        "--bucket", required=True, choices=["green", "yellow"], help="Bucket being processed"
    )
    ap.add_argument(
        "--dataset-root",
        default=None,
        help="Override dataset root (raw/_manifests/_ledger/_logs)",
    )
    ap.add_argument("--raw-root", default=None, help="Override raw root")
    ap.add_argument("--manifests-root", default=None, help="Override manifests root")
    ap.add_argument("--ledger-root", default=None, help="Override ledger root")
    ap.add_argument("--logs-root", default=None, help="Override logs root")
    ap.add_argument(
        "--allow-data-root",
        action="store_true",
        help="Allow /data defaults for outputs (default: disabled).",
    )
    ap.add_argument("--execute", action="store_true", help="Perform downloads")
    ap.add_argument("--overwrite", action="store_true", help="Overwrite existing outputs")
    ap.add_argument(
        "--verify-sha256", action="store_true", help="Compute sha256 for http downloads"
    )
    ap.add_argument("--verify-zenodo-md5", action="store_true", help="Verify Zenodo md5")
    ap.add_argument(
        "--allow-non-global-download-hosts",
        action="store_true",
        help="Allow downloads from non-global IPs (private/loopback/link-local/multicast/reserved/unspecified).",
    )
    ap.add_argument(
        "--internal-mirror-allowlist",
        action="append",
        default=None,
        help=(
            "Allow internal mirrors by hostname or IP/CIDR (repeatable or comma-separated). "
            "Use sparingly to permit private mirrors."
        ),
    )
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
    rows = _read_jsonl_list(queue_path)

    targets_path = Path(args.targets_yaml).expanduser().resolve() if args.targets_yaml else None
    cfg = load_config(targets_path)
    roots = load_roots(cfg, args, defaults)
    ensure_dir(roots.logs_root)
    ensure_dir(roots.ledger_root)
    globals_cfg = cfg.get("globals", {}) or {}
    cfg_allowlist = _normalize_internal_mirror_allowlist(globals_cfg.get("internal_mirror_allowlist"))
    arg_allowlist: list[str] = []
    for entry in args.internal_mirror_allowlist or []:
        arg_allowlist.extend(_normalize_internal_mirror_allowlist(entry))
    internal_mirror_allowlist = _build_internal_mirror_allowlist(
        sorted(set(cfg_allowlist + arg_allowlist))
    )
    run_budget = build_run_budget(globals_cfg.get("run_byte_budget"))

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
        run_budget=run_budget,
        allow_non_global_download_hosts=args.allow_non_global_download_hosts,
        internal_mirror_allowlist=internal_mirror_allowlist,
        cfg=cfg,
        checks_run_id=generate_run_id("acquire"),
    )

    if ctx.limits.limit_targets:
        rows = rows[: ctx.limits.limit_targets]
    rows = [r for r in rows if r.get("enabled", True) and r.get("id")]

    summary = {
        "checks_run_id": ctx.checks_run_id,
        "run_at_utc": utc_now(),
        "queue": str(queue_path),
        "bucket": args.bucket,
        "execute": ctx.mode.execute,
        "results": [],
    }
    summary.update(build_artifact_metadata(written_at_utc=summary["run_at_utc"]))

    if ctx.mode.workers > 1 and ctx.mode.execute:
        with ThreadPoolExecutor(max_workers=ctx.mode.workers) as ex:
            results_by_index = [None] * len(rows)
            futures: dict[object, tuple[int, dict[str, Any]]] = {}
            row_iter = iter(enumerate(rows))

            def submit_next() -> bool:
                if ctx.run_budget and ctx.run_budget.exhausted():
                    return False
                try:
                    idx, row = next(row_iter)
                except StopIteration:
                    return False
                fut = ex.submit(run_target, ctx, args.bucket, row, strategy_handlers, postprocess)
                futures[fut] = (idx, row)
                return True

            while len(futures) < ctx.mode.workers and submit_next():
                continue
            while futures:
                for fut in as_completed(futures):
                    idx, row = futures.pop(fut)
                    try:
                        res = fut.result()
                    except Exception as e:
                        res = {"id": row.get("id"), "status": "error", "error": repr(e)}
                    results_by_index[idx] = res
                    while len(futures) < ctx.mode.workers and submit_next():
                        continue
                    break
            summary["results"] = [result for result in results_by_index if result is not None]
    else:
        for row in rows:
            if ctx.run_budget and ctx.run_budget.exhausted():
                break
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
    if (
        ctx.mode.execute
        and args.strict
        and any(r.get("status") == "error" for r in summary["results"])
    ):
        sys.exit(1)
