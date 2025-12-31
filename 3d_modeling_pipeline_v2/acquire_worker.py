#!/usr/bin/env python3
"""
acquire_worker.py (v2.0)

Replaces download_worker.py with the v2 raw layout:
  raw/{green|yellow}/{license_pool}/{target_id}/...

Reads queue rows emitted by pipeline_driver.py and downloads payloads using the
configured strategy. Dry-run by default; pass --execute to write files. After a
successful run it writes a per-target `acquire_done.json` under the manifests
root.
"""

from __future__ import annotations

import argparse
import dataclasses
import fnmatch
import hashlib
import json
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser

import yaml

try:
    import requests
except ImportError:  # pragma: no cover - optional dependency
    requests = None

try:
    from ftplib import FTP
except ImportError:  # pragma: no cover - optional dependency
    FTP = None


VERSION = "2.0"


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


# ---------------------------------
# Strategy handlers
# ---------------------------------

def _http_download_with_resume(ctx: AcquireContext, url: str, out_path: Path, expected_size: int | None = None) -> dict[str, Any]:
    if requests is None:
        raise RuntimeError("requests is required for http downloads")
    ensure_dir(out_path.parent)
    headers = {}
    mode = "wb"
    existing = out_path.stat().st_size if out_path.exists() else 0
    if existing and ctx.mode.enable_resume:
        headers["Range"] = f"bytes={existing}-"
        mode = "ab"
    with requests.get(url, stream=True, headers=headers, timeout=(15, 300)) as r:
        r.raise_for_status()
        with out_path.open(mode) as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
    result: dict[str, Any] = {"status": "ok", "path": str(out_path)}
    if ctx.mode.verify_sha256 and expected_size and out_path.stat().st_size != expected_size:
        result = {"status": "error", "error": "size_mismatch"}
    elif ctx.mode.verify_sha256:
        result["sha256"] = sha256_file(out_path)
    return result


def handle_http(ctx: AcquireContext, row: dict[str, Any], out_dir: Path) -> list[dict[str, Any]]:
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
    return [_http_download_with_resume(ctx, url, out_path, download.get("expected_size"))]


def handle_ftp(ctx: AcquireContext, row: dict[str, Any], out_dir: Path) -> list[dict[str, Any]]:
    download = normalize_download(row.get("download", {}) or {})
    base = download.get("base_url")
    globs = download.get("globs", ["*"])
    if FTP is None:
        return [{"status": "error", "error": "ftplib missing"}]
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
                with local.open("wb") as f:
                    ftp.retrbinary(f"RETR {fname}", f.write)
                results.append({"status": "ok", "path": str(local)})
    return results


def handle_git(ctx: AcquireContext, row: dict[str, Any], out_dir: Path) -> list[dict[str, Any]]:
    download = normalize_download(row.get("download", {}) or {})
    repo = download.get("repo") or download.get("repo_url") or download.get("url")
    branch = download.get("branch")
    if not repo:
        return [{"status": "error", "error": "missing repo"}]
    if out_dir.exists() and any(out_dir.iterdir()) and not ctx.mode.overwrite:
        return [{"status": "ok", "path": str(out_dir), "cached": True}]
    if not ctx.mode.execute:
        return [{"status": "noop", "path": str(out_dir)}]
    ensure_dir(out_dir)
    cmd = ["git", "clone"]
    if branch:
        cmd += ["-b", branch]
    cmd += [repo, str(out_dir)]
    log = run_cmd(cmd)
    return [{"status": "ok", "path": str(out_dir), "log": log}]


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
    if requests is None:
        return [{"status": "error", "error": "requests missing"}]
    if not ctx.mode.execute:
        return [{"status": "noop", "path": str(out_dir)}]
    resp = requests.get(api_url, timeout=60)
    resp.raise_for_status()
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
    if requests is None:
        return [{"status": "error", "error": "requests missing"}]
    if not ctx.mode.execute:
        return [{"status": "noop", "path": str(out_dir)}]
    url = f"{instance}/api/access/dvobject/{pid}"
    resp = requests.get(url, allow_redirects=True, timeout=60)
    resp.raise_for_status()
    filename = safe_name(urlparse(resp.url).path.split("/")[-1] or pid)
    out_path = out_dir / filename
    ensure_dir(out_path.parent)
    with out_path.open("wb") as f:
        f.write(resp.content)
    return [{"status": "ok", "path": str(out_path)}]


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


def handle_api(ctx: AcquireContext, row: dict[str, Any], out_dir: Path) -> list[dict[str, Any]]:
    if requests is None:
        return [{"status": "error", "error": "requests not installed"}]

    download = normalize_download(row.get("download", {}) or {})
    base_url = (download.get("base_url") or "").strip()
    endpoints = download.get("endpoints") or download.get("paths") or [""]
    shared_query = download.get("query") or {}
    headers = download.get("headers") or {}
    delay = float(download.get("delay_seconds", 1.0))
    default_method = (download.get("method") or "GET").upper()

    pagination = download.get("pagination") or {}
    default_page_param = pagination.get("page_param", "page")
    default_page_size_param = pagination.get("page_size_param", None)
    default_page_size = pagination.get("page_size", None)
    default_page_start = int(pagination.get("page_start", 1))
    default_max_pages = int(pagination.get("max_pages", download.get("max_pages", 1)))
    stop_on_empty = bool(pagination.get("stop_on_empty", True))

    if not base_url:
        return [{"status": "noop", "reason": "no base_url"}]

    ensure_dir(out_dir)
    catalog_dir = out_dir / "_catalogs"
    ensure_dir(catalog_dir)

    def iter_endpoints():
        for ep in endpoints:
            if isinstance(ep, str):
                yield {"path": ep}
            elif isinstance(ep, dict):
                yield ep

    if not ctx.mode.execute:
        return [{"status": "planned", "base_url": base_url, "endpoints": list(iter_endpoints()), "query": shared_query}]

    results: list[dict[str, Any]] = []
    call_log: list[dict[str, Any]] = []

    for idx, ep in enumerate(iter_endpoints()):
        path = ep.get("path", "")
        name = ep.get("name") or safe_name(path or f"endpoint_{idx+1}")
        method = (ep.get("method") or default_method).upper()
        params = dict(shared_query)
        params.update(ep.get("params") or {})

        page_param = ep.get("page_param", default_page_param)
        page_size_param = ep.get("page_size_param", default_page_size_param)
        page_size = ep.get("page_size", default_page_size)
        page_start = int(ep.get("page_start", default_page_start))
        max_pages = int(ep.get("max_pages", default_max_pages))

        url = urljoin(base_url if base_url.endswith("/") or path.startswith("/") else f"{base_url}/", path)
        for page in range(max_pages):
            page_num = page_start + page
            call_params = dict(params)
            if max_pages > 1 or page_param:
                call_params.setdefault(page_param, page_num)
            if page_size_param and page_size:
                call_params.setdefault(page_size_param, page_size)

            result_meta: dict[str, Any] = {
                "endpoint": path,
                "url": url,
                "params": call_params,
                "method": method,
                "page": page_num if max_pages > 1 or page_param else None,
            }

            try:
                resp = requests.request(method, url, params=call_params, headers={"User-Agent": f"3d-modeling-api/{VERSION}", **headers}, timeout=30)
                result_meta["status_code"] = resp.status_code
                result_meta["final_url"] = resp.url
            except Exception as e:
                result_meta.update({"status": "error", "error": repr(e)})
                results.append(result_meta)
                call_log.append(result_meta)
                continue

            if resp.status_code >= 400:
                result_meta.update({"status": "error", "error": f"status {resp.status_code}"})
                results.append(result_meta)
                call_log.append(result_meta)
                if resp.status_code in {401, 403, 429}:
                    break
                continue

            content_type = resp.headers.get("Content-Type", "").lower()
            suffix = ".json" if "json" in content_type else ".html" if "html" in content_type else ".txt"
            dest = out_dir / f"{name}{'' if max_pages == 1 else f'_page{page_num}'}{suffix}"

            try:
                if "json" in content_type:
                    dest.write_text(json.dumps(resp.json(), indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
                elif "html" in content_type or "xml" in content_type:
                    dest.write_text(resp.text, encoding="utf-8")
                else:
                    dest.write_bytes(resp.content)
            except Exception:
                dest.write_bytes(resp.content)

            payload_size = dest.stat().st_size
            result_meta.update({"status": "ok", "path": str(dest), "bytes": payload_size, "sha256": sha256_file(dest)})
            results.append(result_meta)
            call_log.append(result_meta)

            if stop_on_empty and "json" in content_type:
                try:
                    parsed = resp.json()
                    if parsed == {} or (isinstance(parsed, list) and not parsed):
                        break
                except Exception:
                    pass
            time.sleep(delay)

    write_json(catalog_dir / "api_calls.json", {"base_url": base_url, "results": call_log})
    return results


def handle_s3_public(ctx: AcquireContext, row: dict[str, Any], out_dir: Path) -> list[dict[str, Any]]:
    download = normalize_download(row.get("download", {}) or {})
    bucket = download.get("bucket")
    prefix = download.get("prefix", "").lstrip("/")
    include_globs = download.get("include_globs") or ["**"]

    if not bucket:
        return [{"status": "noop", "reason": "no bucket"}]

    try:
        import boto3
        from botocore import UNSIGNED
        from botocore.config import Config
    except ImportError:
        return [{"status": "error", "error": "boto3 not installed (required for s3_public)"}]

    if not ctx.mode.execute:
        return [{"status": "planned", "bucket": bucket, "prefix": prefix, "include_globs": include_globs}]

    ensure_dir(out_dir)
    catalog_dir = out_dir / "_catalogs"
    ensure_dir(catalog_dir)

    s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))

    def list_objects() -> list[dict[str, Any]]:
        objects: list[dict[str, Any]] = []
        token = None
        while True:
            kwargs = {"Bucket": bucket, "Prefix": prefix, "MaxKeys": 1000}
            if token:
                kwargs["ContinuationToken"] = token
            resp = s3.list_objects_v2(**kwargs)
            for obj in resp.get("Contents", []):
                key = obj.get("Key", "")
                if any(fnmatch.fnmatch(key, g) for g in include_globs):
                    objects.append({
                        "key": key,
                        "size": obj.get("Size"),
                        "etag": obj.get("ETag", "").strip('"'),
                        "last_modified": obj.get("LastModified").isoformat() if obj.get("LastModified") else None,
                    })
            if not resp.get("IsTruncated"):
                break
            token = resp.get("NextContinuationToken")
        return objects

    results: list[dict[str, Any]] = []
    objects = list_objects()
    write_json(catalog_dir / "listing.json", {"bucket": bucket, "prefix": prefix, "objects": objects})

    for obj in objects:
        key = obj["key"]
        rel = key[len(prefix):].lstrip("/") if prefix else key
        dest = out_dir / rel
        ensure_dir(dest.parent)
        meta = {"status": "planned", "key": key, "dest": str(dest)}
        if not ctx.mode.execute:
            results.append(meta)
            continue
        try:
            s3.download_file(bucket, key, str(dest))
            meta.update({"status": "ok", "bytes": dest.stat().st_size, "sha256": sha256_file(dest), "etag": obj.get("etag")})
        except Exception as e:
            meta["status"] = "error"
            meta["error"] = repr(e)
        results.append(meta)
    return results


def handle_web_crawl(ctx: AcquireContext, row: dict[str, Any], out_dir: Path) -> list[dict[str, Any]]:
    if requests is None:
        return [{"status": "error", "error": "requests not installed"}]

    download = normalize_download(row.get("download", {}) or {})
    seeds = download.get("seed_urls") or []
    include_globs = download.get("include_globs") or ["*"]
    allow_domains = download.get("allow_domains")
    max_pages = int(download.get("max_pages", 50))
    delay = float(download.get("delay_seconds", 1.0))

    if not seeds:
        return [{"status": "noop", "reason": "no seed_urls"}]

    if not ctx.mode.execute:
        return [{"status": "planned", "seed_urls": seeds, "include_globs": include_globs}]

    ensure_dir(out_dir)
    catalog_dir = out_dir / "_catalogs"
    ensure_dir(catalog_dir)

    def domain_allowed(url: str) -> bool:
        dom = urlparse(url).netloc
        if allow_domains:
            return any(dom.endswith(ad) for ad in allow_domains)
        return True

    def robots_allows(url: str) -> bool:
        parsed = urlparse(url)
        robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
        rp = RobotFileParser()
        try:
            rp.set_url(robots_url)
            rp.read()
            return rp.can_fetch("*", url)
        except Exception:
            return True

    def snapshot_tos(url: str) -> None:
        try:
            resp = requests.get(url, timeout=15)
            fname = catalog_dir / f"tos_{safe_name(urlparse(url).netloc)}.html"
            fname.write_text(resp.text, encoding="utf-8")
        except Exception:
            pass

    results: list[dict[str, Any]] = []
    visited = set()
    queue = list(seeds)

    for seed in seeds:
        snapshot_tos(seed)

    while queue and len(visited) < max_pages:
        url = queue.pop(0)
        if url in visited or not domain_allowed(url):
            continue
        visited.add(url)
        if not robots_allows(url):
            results.append({"status": "skipped", "reason": "robots_disallow", "url": url})
            continue
        try:
            resp = requests.get(url, timeout=20, headers={"User-Agent": f"3d-modeling-crawler/{VERSION}"})
            if resp.status_code >= 400:
                results.append({"status": "error", "url": url, "error": f"status {resp.status_code}"})
                continue
            content_type = resp.headers.get("Content-Type", "")
            if "text/html" in content_type:
                import re
                links = re.findall(r'href=[\"\\\'](.*?)[\"\\\']', resp.text, flags=re.IGNORECASE)
                for link in links:
                    absolute = link
                    if absolute.startswith("//"):
                        absolute = f"{urlparse(url).scheme}:{absolute}"
                    elif absolute.startswith("/"):
                        absolute = f"{urlparse(url).scheme}://{urlparse(url).netloc}{absolute}"
                    if absolute not in visited and len(queue) + len(visited) < max_pages:
                        queue.append(absolute)
            parsed = urlparse(url)
            path_part = parsed.path.split("/")[-1]
            if path_part and any(fnmatch.fnmatch(path_part, g) for g in include_globs):
                dest = out_dir / safe_name(path_part)
                ensure_dir(dest.parent)
                dest.write_bytes(resp.content)
                results.append({"status": "ok", "url": url, "path": str(dest), "bytes": dest.stat().st_size, "sha256": sha256_file(dest)})
            else:
                results.append({"status": "visited", "url": url})
        except Exception as e:
            results.append({"status": "error", "url": url, "error": repr(e)})
        time.sleep(delay)

    write_json(catalog_dir / "crawl_run.json", {"seeds": seeds, "visited": list(visited), "results_count": len(results)})
    return results


def index_mesh_assets(row: dict[str, Any], out_dir: Path, results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    mesh_exts = {".stl", ".obj", ".fbx", ".ply", ".glb", ".gltf", ".stp", ".step", ".iges", ".igs", ".3mf"}
    index_rows: list[dict[str, Any]] = []
    routing = {
        "subject": row.get("routing_subject"),
        "domain": row.get("routing_domain"),
        "category": row.get("routing_category"),
        "level": row.get("routing_level"),
        "granularity": row.get("routing_granularity"),
        "confidence": row.get("routing_confidence"),
        "reason": row.get("routing_reason"),
    }
    for res in results:
        path = res.get("path")
        if res.get("status") != "ok" or not path:
            continue
        asset_path = Path(path)
        if asset_path.suffix.lower() not in mesh_exts:
            continue
        try:
            rel = asset_path.relative_to(out_dir)
        except ValueError:
            rel = asset_path.name
        index_rows.append(
            {
                "record_id": f"{row.get('id')}:{rel}",
                "asset_path": str(rel),
                "license_spdx": row.get("resolved_spdx") or row.get("license_spdx"),
                "license_profile": row.get("license_profile"),
                "source": {"target_id": row.get("id"), "source_url": row.get("license_evidence_url")},
                "routing": {k: v for k, v in routing.items() if v is not None},
                "hash": {"content_sha256": sha256_file(asset_path)},
            }
        )
    return index_rows


STRATEGY_HANDLERS = {
    "http": handle_http,
    "ftp": handle_ftp,
    "git": handle_git,
    "zenodo": handle_zenodo,
    "dataverse": handle_dataverse,
    "huggingface_datasets": handle_hf_datasets,
    "api": handle_api,
    "s3_public": handle_s3_public,
    "web_crawl": handle_web_crawl,
}


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


def write_done_marker(ctx: AcquireContext, target_id: str, bucket: str, status: str) -> None:
    marker = ctx.roots.manifests_root / safe_name(target_id) / "acquire_done.json"
    write_json(marker, {"target_id": target_id, "bucket": bucket, "status": status, "written_at_utc": utc_now(), "version": VERSION})


def run_target(ctx: AcquireContext, bucket: str, row: dict[str, Any]) -> dict[str, Any]:
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

    handler = STRATEGY_HANDLERS.get(strat)
    if not handler or strat in {"none", ""}:
        manifest["results"] = [{"status": "noop", "reason": f"unsupported: {strat}"}]
    else:
        try:
            manifest["results"] = handler(ctx, row, out_dir)
        except Exception as e:
            manifest["results"] = [{"status": "error", "error": repr(e)}]

    download = normalize_download(row.get("download", {}) or {})
    if ctx.mode.execute and (download.get("emit_index") or download.get("index_assets")):
        index_rows = index_mesh_assets(row, out_dir, manifest["results"])
        if index_rows:
            write_jsonl(out_dir / "records.index.jsonl", index_rows)

    manifest["finished_at_utc"] = utc_now()
    write_json(out_dir / "download_manifest.json", manifest)

    status = "ok" if any(r.get("status") == "ok" for r in manifest["results"]) else manifest["results"][0].get("status", "error")
    if ctx.mode.execute:
        write_done_marker(ctx, tid, bucket, status)
    return {"id": tid, "status": status, "bucket": bucket, "license_pool": pool, "strategy": strat}


def load_roots(targets_path: Path | None, overrides: argparse.Namespace) -> Roots:
    cfg: dict[str, Any] = {}
    if targets_path and targets_path.exists():
        cfg = yaml.safe_load(targets_path.read_text(encoding="utf-8")) or {}
    g = (cfg.get("globals", {}) or {})
    raw_root = Path(overrides.raw_root or g.get("raw_root", "/data/3d/raw"))
    manifests_root = Path(overrides.manifests_root or g.get("manifests_root", "/data/3d/_manifests"))
    logs_root = Path(overrides.logs_root or g.get("logs_root", "/data/3d/_logs"))
    return Roots(raw_root=raw_root.expanduser().resolve(), manifests_root=manifests_root.expanduser().resolve(), logs_root=logs_root.expanduser().resolve())


def main() -> None:
    ap = argparse.ArgumentParser(description=f"Acquire Worker v{VERSION}")
    ap.add_argument("--queue", required=True, help="Queue JSONL emitted by pipeline_driver.py")
    ap.add_argument("--targets-yaml", default=None, help="Path to targets_3d.yaml for roots")
    ap.add_argument("--bucket", required=True, choices=["green", "yellow"], help="Bucket being processed")
    ap.add_argument("--raw-root", default=None, help="Override raw root")
    ap.add_argument("--manifests-root", default=None, help="Override manifests root")
    ap.add_argument("--logs-root", default=None, help="Override logs root")
    ap.add_argument("--execute", action="store_true", help="Perform downloads")
    ap.add_argument("--overwrite", action="store_true", help="Overwrite existing outputs")
    ap.add_argument("--verify-sha256", action="store_true", help="Compute sha256 for http downloads")
    ap.add_argument("--verify-zenodo-md5", action="store_true", help="Verify Zenodo md5")
    ap.add_argument("--enable-resume", action="store_true", default=True)
    ap.add_argument("--no-resume", action="store_true")
    ap.add_argument("--limit-targets", type=int, default=None)
    ap.add_argument("--limit-files", type=int, default=None)
    ap.add_argument("--max-bytes-per-target", type=int, default=None)
    ap.add_argument("--workers", type=int, default=1)
    ap.add_argument("--retry-max", type=int, default=3)
    ap.add_argument("--retry-backoff", type=float, default=2.0)
    args = ap.parse_args()

    queue_path = Path(args.queue).expanduser().resolve()
    rows = read_jsonl(queue_path)

    targets_path = Path(args.targets_yaml).expanduser().resolve() if args.targets_yaml else None
    roots = load_roots(targets_path, args)
    ensure_dir(roots.logs_root)

    ctx = AcquireContext(
        roots=roots,
        limits=Limits(args.limit_targets, args.limit_files, args.max_bytes_per_target),
        mode=RunMode(args.execute, args.overwrite, args.verify_sha256, args.verify_zenodo_md5, args.enable_resume and not args.no_resume, max(1, args.workers)),
        retry=RetryConfig(args.retry_max, args.retry_backoff),
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
            futures = {ex.submit(run_target, ctx, args.bucket, row): row for row in rows}
            for fut in as_completed(futures):
                try:
                    res = fut.result()
                except Exception as e:
                    res = {"id": futures[fut].get("id"), "status": "error", "error": repr(e)}
                summary["results"].append(res)
    else:
        for row in rows:
            res = run_target(ctx, args.bucket, row)
            summary["results"].append(res)

    write_json(roots.logs_root / f"acquire_summary_{args.bucket}.json", summary)


if __name__ == "__main__":
    main()
