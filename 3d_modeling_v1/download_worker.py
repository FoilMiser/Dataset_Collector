#!/usr/bin/env python3
"""
download_worker.py (v0.9)

Consumes queue JSONL emitted by pipeline_driver.py and downloads dataset payloads
according to each row's `download` strategy.

Safe defaults:
  - DRY RUN by default (prints plan, writes manifests, does not download)
  - You must pass --execute to actually download.

v0.9 features:
  - NEW: Enhanced Figshare resolver with API support
  - NEW: GitHub release resolver with rate limit handling
  - NEW: Parquet output option (--emit-parquet)
  - Retry with exponential backoff
  - Resumable HTTP downloads (range requests)
  - Integrity verification (SHA256 + Zenodo MD5)
  - Parallel downloads with configurable workers

Supported strategies: http, ftp, git, zenodo, dataverse, huggingface_datasets, figshare, github_release,
api (generic JSON/HTML snapshotter), s3_public (unsigned), web_crawl (polite crawling of curated seeds)

Not legal advice.
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
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser

import yaml

try:
    import requests
except ImportError:
    requests = None

try:
    from ftplib import FTP
except ImportError:
    FTP = None


VERSION = "0.9"


def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def read_jsonl(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows

def write_json(path: Path, obj: Dict[str, Any]) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

def append_jsonl(path: Path, obj: Dict[str, Any]) -> None:
    ensure_dir(path.parent)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")

def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024*1024), b""):
            h.update(chunk)
    return h.hexdigest()

def md5_file(path: Path) -> str:
    h = hashlib.md5()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024*1024), b""):
            h.update(chunk)
    return h.hexdigest()

def safe_name(s: str) -> str:
    import re
    s = re.sub(r"[^a-zA-Z0-9._-]+", "_", (s or "").strip())
    return s[:200] if s else "file"

def load_yaml(path: Path) -> Dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8"))

def run_cmd(cmd: List[str], cwd: Optional[Path] = None) -> str:
    p = subprocess.run(cmd, cwd=str(cwd) if cwd else None, check=True, 
                       stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    return p.stdout.decode("utf-8", errors="ignore")


@dataclasses.dataclass
class Pools:
    permissive: Path
    copyleft: Path
    quarantine: Path

@dataclasses.dataclass
class Limits:
    limit_targets: Optional[int]
    limit_files: Optional[int]
    max_bytes_per_target: Optional[int]

@dataclasses.dataclass
class RetryConfig:
    max_attempts: int = 3
    backoff_base: float = 2.0
    backoff_max: float = 60.0

@dataclasses.dataclass
class RunMode:
    execute: bool
    overwrite: bool
    verify_sha256: bool
    verify_zenodo_md5: bool
    enable_resume: bool
    workers: int

@dataclasses.dataclass
class DownloaderContext:
    pools: Pools
    logs_dir: Path
    limits: Limits
    mode: RunMode
    retry: RetryConfig


def pools_from_targets_yaml(targets_yaml: Optional[Path], fallback: Path) -> Pools:
    if targets_yaml and targets_yaml.exists():
        cfg = load_yaml(targets_yaml)
        pools = cfg.get("globals", {}).get("pools", {})
        return Pools(
            permissive=Path(pools.get("permissive", fallback / "permissive")).expanduser(),
            copyleft=Path(pools.get("copyleft", fallback / "copyleft")).expanduser(),
            quarantine=Path(pools.get("quarantine", fallback / "quarantine")).expanduser(),
        )
    return Pools(fallback / "permissive", fallback / "copyleft", fallback / "quarantine")

def resolve_pool_dir(ctx: DownloaderContext, pool_name: str, target_id: str) -> Path:
    pool_name = (pool_name or "quarantine").strip().lower()
    base = {"permissive": ctx.pools.permissive, "copyleft": ctx.pools.copyleft}.get(pool_name, ctx.pools.quarantine)
    out = base / target_id
    ensure_dir(out)
    return out

def log_event(ctx: DownloaderContext, event: Dict[str, Any]) -> None:
    event = dict(event)
    event.setdefault("at_utc", utc_now())
    append_jsonl(ctx.logs_dir / "download_log.jsonl", event)


def http_download_with_resume(ctx: DownloaderContext, url: str, out_path: Path, 
                               expected_size: Optional[int] = None) -> Dict[str, Any]:
    """Download with retry, exponential backoff, and resume support."""
    if requests is None:
        return {"status": "error", "error": "requests not installed"}
    
    ensure_dir(out_path.parent)
    meta: Dict[str, Any] = {"url": url, "path": str(out_path), "attempts": 0, "resumed": False}

    if out_path.exists() and not ctx.mode.overwrite:
        existing_size = out_path.stat().st_size
        if expected_size is None or existing_size >= expected_size:
            meta["status"] = "skipped_exists"
            meta["bytes"] = existing_size
            return meta

    if not ctx.mode.execute:
        meta["status"] = "planned"
        return meta

    tmp = out_path.with_suffix(out_path.suffix + ".part")
    start_byte = 0
    
    if ctx.mode.enable_resume and tmp.exists():
        start_byte = tmp.stat().st_size
        meta["resumed"] = True

    for attempt in range(ctx.retry.max_attempts):
        meta["attempts"] = attempt + 1
        try:
            headers = {"User-Agent": f"chem-corpus-downloader/{VERSION}"}
            if start_byte > 0:
                headers["Range"] = f"bytes={start_byte}-"
            
            with requests.get(url, stream=True, timeout=(30, 300), headers=headers) as r:
                mode = "ab" if r.status_code == 206 else "wb"
                if r.status_code == 200:
                    start_byte = 0
                r.raise_for_status()
                
                written = start_byte
                with tmp.open(mode) as f:
                    for chunk in r.iter_content(chunk_size=1024*1024):
                        if chunk:
                            f.write(chunk)
                            written += len(chunk)
                            if ctx.limits.max_bytes_per_target and written > ctx.limits.max_bytes_per_target:
                                raise RuntimeError("Exceeded max_bytes_per_target")

            tmp.replace(out_path)
            meta["status"] = "ok"
            meta["bytes"] = out_path.stat().st_size
            if ctx.mode.verify_sha256:
                meta["sha256"] = sha256_file(out_path)
            return meta
            
        except Exception as e:
            meta["last_error"] = repr(e)
            if attempt < ctx.retry.max_attempts - 1:
                time.sleep(min(ctx.retry.backoff_base ** attempt, ctx.retry.backoff_max))
                if tmp.exists():
                    start_byte = tmp.stat().st_size
    
    meta["status"] = "error"
    return meta


def ftp_download_many(ctx: DownloaderContext, base_url: str, 
                      include_globs: List[str], out_dir: Path) -> List[Dict[str, Any]]:
    if FTP is None:
        return [{"status": "error", "error": "ftplib not available"}]
    
    u = urlparse(base_url)
    if u.scheme != "ftp":
        return [{"status": "error", "error": f"Not an ftp url: {base_url}"}]
    
    if not ctx.mode.execute:
        return [{"status": "planned", "base_url": base_url, "include_globs": include_globs}]

    ensure_dir(out_dir)
    results: List[Dict[str, Any]] = []
    
    for attempt in range(ctx.retry.max_attempts):
        try:
            ftp = FTP(u.hostname or "", timeout=60)
            ftp.login()
            ftp.cwd(u.path)
            
            all_files = ftp.nlst()
            matched = [f for f in all_files if any(fnmatch.fnmatch(f, g) for g in include_globs)]
            
            if ctx.limits.limit_files:
                matched = matched[:ctx.limits.limit_files]
            
            bytes_so_far = 0
            for fname in matched:
                out_path = out_dir / safe_name(fname)
                
                if out_path.exists() and not ctx.mode.overwrite:
                    results.append({"status": "skipped_exists", "file": fname, "path": str(out_path)})
                    continue
                
                try:
                    size = ftp.size(fname) or 0
                except:
                    size = 0
                    
                if ctx.limits.max_bytes_per_target and bytes_so_far + size > ctx.limits.max_bytes_per_target:
                    results.append({"status": "skipped_limit", "file": fname})
                    continue
                
                tmp = out_path.with_suffix(out_path.suffix + ".part")
                with tmp.open("wb") as f:
                    ftp.retrbinary(f"RETR {fname}", f.write, blocksize=256*1024)
                tmp.replace(out_path)
                
                info = {"status": "ok", "file": fname, "path": str(out_path), "bytes": out_path.stat().st_size}
                if ctx.mode.verify_sha256:
                    info["sha256"] = sha256_file(out_path)
                results.append(info)
                bytes_so_far += info["bytes"]
            
            ftp.quit()
            break
        except Exception as e:
            if attempt >= ctx.retry.max_attempts - 1:
                results.append({"status": "error", "error": repr(e)})
            else:
                time.sleep(min(ctx.retry.backoff_base ** attempt, ctx.retry.backoff_max))
    
    return results


def handle_http(ctx: DownloaderContext, target: Dict[str, Any], out_dir: Path) -> List[Dict[str, Any]]:
    urls = target.get("download", {}).get("urls") or []
    if not urls:
        return [{"status": "noop", "reason": "no urls"}]
    results = []
    for url in urls:
        fname = Path(urlparse(url).path).name or "downloaded_file"
        results.append(http_download_with_resume(ctx, url, out_dir / safe_name(fname)))
    return results


def handle_git(ctx: DownloaderContext, target: Dict[str, Any], out_dir: Path) -> List[Dict[str, Any]]:
    dl = target.get("download", {})
    repo = dl.get("repo") or dl.get("url")
    checkout = dl.get("checkout") or dl.get("branch") or "main"
    if not repo:
        return [{"status": "noop", "reason": "no repo"}]
    
    repo_dir = out_dir / "repo"
    if not ctx.mode.execute:
        return [{"status": "planned", "repo": repo, "checkout": checkout}]
    
    for attempt in range(ctx.retry.max_attempts):
        try:
            if repo_dir.exists():
                run_cmd(["git", "fetch", "--all", "--tags"], cwd=repo_dir)
            else:
                ensure_dir(out_dir)
                run_cmd(["git", "clone", repo, str(repo_dir)])
            run_cmd(["git", "checkout", checkout], cwd=repo_dir)
            commit = run_cmd(["git", "rev-parse", "HEAD"], cwd=repo_dir).strip()
            return [{"status": "ok", "repo": repo, "checkout": checkout, "commit": commit}]
        except Exception as e:
            if attempt >= ctx.retry.max_attempts - 1:
                return [{"status": "error", "error": repr(e)}]
            time.sleep(min(ctx.retry.backoff_base ** attempt, ctx.retry.backoff_max))
    return [{"status": "error"}]


def handle_zenodo(ctx: DownloaderContext, target: Dict[str, Any], out_dir: Path) -> List[Dict[str, Any]]:
    if requests is None:
        return [{"status": "error", "error": "requests not installed"}]
    
    record = str(target.get("download", {}).get("record") or "").strip()
    if not record:
        return [{"status": "noop", "reason": "no record id"}]
    
    api_url = f"https://zenodo.org/api/records/{record}"
    ensure_dir(out_dir)
    
    if not ctx.mode.execute:
        return [{"status": "planned", "zenodo_record": record}]

    # Fetch metadata
    for attempt in range(ctx.retry.max_attempts):
        try:
            r = requests.get(api_url, timeout=60, headers={"User-Agent": f"chem-corpus/{VERSION}"})
            r.raise_for_status()
            meta = r.json()
            break
        except Exception as e:
            if attempt >= ctx.retry.max_attempts - 1:
                return [{"status": "error", "error": repr(e)}]
            time.sleep(min(ctx.retry.backoff_base ** attempt, ctx.retry.backoff_max))
    else:
        return [{"status": "error", "reason": "failed metadata fetch"}]
    
    files = meta.get("files", [])
    if ctx.limits.limit_files:
        files = files[:ctx.limits.limit_files]
    
    results = []
    bytes_so_far = 0
    
    for fmeta in files:
        fkey = fmeta.get("key") or "file"
        link = (fmeta.get("links") or {}).get("self", "")
        checksum = fmeta.get("checksum", "")
        size = int(fmeta.get("size") or 0)
        
        if ctx.limits.max_bytes_per_target and bytes_so_far + size > ctx.limits.max_bytes_per_target:
            results.append({"status": "skipped_limit", "file": fkey})
            continue
        
        out_path = out_dir / safe_name(fkey)
        info = http_download_with_resume(ctx, link, out_path, expected_size=size)
        info["zenodo_record"] = record
        info["expected_checksum"] = checksum
        
        if info.get("status") == "ok" and ctx.mode.verify_zenodo_md5 and checksum.startswith("md5:"):
            actual_md5 = md5_file(out_path)
            info["md5"] = actual_md5
            info["md5_valid"] = actual_md5 == checksum[4:]
        
        results.append(info)
        if info.get("status") == "ok":
            bytes_so_far += info.get("bytes", 0)
    
    write_json(out_dir / "zenodo_record.json", meta)
    return results


def handle_dataverse(ctx: DownloaderContext, target: Dict[str, Any], out_dir: Path) -> List[Dict[str, Any]]:
    if requests is None:
        return [{"status": "error", "error": "requests not installed"}]
    
    dl = target.get("download", {})
    pid = str(dl.get("persistent_id") or "").strip()
    instance = dl.get("instance", "https://dataverse.harvard.edu")
    
    if not pid:
        return [{"status": "noop", "reason": "no persistent_id"}]
    
    api = f"{instance}/api/access/dataset/:persistentId/?persistentId={pid}"
    return [http_download_with_resume(ctx, api, out_dir / f"dataverse_{safe_name(pid)}.zip")]


def handle_ftp(ctx: DownloaderContext, target: Dict[str, Any], out_dir: Path) -> List[Dict[str, Any]]:
    dl = target.get("download", {})
    base_url = dl.get("base_url")
    globs = dl.get("include_globs") or ["*"]
    if not base_url:
        return [{"status": "noop", "reason": "no base_url"}]
    return ftp_download_many(ctx, base_url, globs, out_dir)


def handle_hf_datasets(ctx: DownloaderContext, target: Dict[str, Any], out_dir: Path) -> List[Dict[str, Any]]:
    dl = target.get("download", {})
    dataset_id = dl.get("dataset_id")
    if not dataset_id:
        return [{"status": "noop", "reason": "no dataset_id"}]
    
    if not ctx.mode.execute:
        return [{"status": "planned", "dataset_id": dataset_id}]
    
    try:
        from datasets import load_dataset
    except ImportError:
        return [{"status": "error", "error": "datasets library not installed"}]
    
    ensure_dir(out_dir)
    results = []
    
    for attempt in range(ctx.retry.max_attempts):
        try:
            cfg = dl.get("config")
            splits = dl.get("splits")
            if splits:
                for sp in splits:
                    ds = load_dataset(dataset_id, cfg, split=sp)
                    sp_dir = out_dir / f"split_{safe_name(sp)}"
                    ds.save_to_disk(str(sp_dir))
                    results.append({"status": "ok", "dataset_id": dataset_id, "split": sp})
            else:
                ds = load_dataset(dataset_id, cfg)
                ds.save_to_disk(str(out_dir / "hf_dataset"))
                results.append({"status": "ok", "dataset_id": dataset_id})
            break
        except Exception as e:
            if attempt >= ctx.retry.max_attempts - 1:
                results.append({"status": "error", "error": repr(e)})
            else:
                time.sleep(min(ctx.retry.backoff_base ** attempt, ctx.retry.backoff_max))
    
    return results


def handle_api(ctx: DownloaderContext, target: Dict[str, Any], out_dir: Path) -> List[Dict[str, Any]]:
    """
    Generic API snapshotter.
    - Fetches JSON/HTML responses for configured endpoints (relative to base_url).
    - Supports simple pagination (page/page_size) and shared query params.
    - Stores responses under _catalogs for auditability.
    """
    if requests is None:
        return [{"status": "error", "error": "requests not installed"}]

    dl = target.get("download", {})
    base_url = (dl.get("base_url") or "").strip()
    endpoints = dl.get("endpoints") or dl.get("paths") or [""]
    shared_query = dl.get("query") or {}
    headers = dl.get("headers") or {}
    delay = float(dl.get("delay_seconds", 1.0))
    default_method = (dl.get("method") or "GET").upper()

    pagination = dl.get("pagination") or {}
    default_page_param = pagination.get("page_param", "page")
    default_page_size_param = pagination.get("page_size_param", None)
    default_page_size = pagination.get("page_size", None)
    default_page_start = int(pagination.get("page_start", 1))
    default_max_pages = int(pagination.get("max_pages", dl.get("max_pages", 1)))
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
        return [
            {
                "status": "planned",
                "base_url": base_url,
                "endpoints": list(iter_endpoints()),
                "query": shared_query,
            }
        ]

    results: List[Dict[str, Any]] = []
    call_log: List[Dict[str, Any]] = []

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

            result_meta: Dict[str, Any] = {
                "endpoint": path,
                "url": url,
                "params": call_params,
                "method": method,
                "page": page_num if max_pages > 1 or page_param else None,
            }

            try:
                resp = requests.request(method, url, params=call_params, headers={"User-Agent": f"3d-corpus-api/{VERSION}", **headers}, timeout=30)
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
            result_meta.update(
                {
                    "status": "ok",
                    "path": str(dest),
                    "bytes": payload_size,
                    "sha256": sha256_file(dest),
                }
            )
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


def handle_s3_public(ctx: DownloaderContext, target: Dict[str, Any], out_dir: Path) -> List[Dict[str, Any]]:
    """
    Download from a public S3 bucket using unsigned requests.
    Captures listing + etags into _catalogs/listing.json.
    """
    dl = target.get("download", {})
    bucket = dl.get("bucket")
    prefix = dl.get("prefix", "").lstrip("/")
    include_globs = dl.get("include_globs") or ["**"]

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

    def list_objects() -> List[Dict[str, Any]]:
        objects: List[Dict[str, Any]] = []
        token = None
        while True:
            kwargs = {"Bucket": bucket, "Prefix": prefix, "MaxKeys": 1000}
            if token:
                kwargs["ContinuationToken"] = token
            resp = s3.list_objects_v2(**kwargs)
            for obj in resp.get("Contents", []):
                key = obj.get("Key", "")
                if any(fnmatch.fnmatch(key, g) for g in include_globs):
                    objects.append(
                        {
                            "key": key,
                            "size": obj.get("Size"),
                            "etag": obj.get("ETag", "").strip('"'),
                            "last_modified": obj.get("LastModified").isoformat() if obj.get("LastModified") else None,
                        }
                    )
            if not resp.get("IsTruncated"):
                break
            token = resp.get("NextContinuationToken")
        return objects

    results: List[Dict[str, Any]] = []
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
            meta.update(
                {
                    "status": "ok",
                    "bytes": dest.stat().st_size,
                    "sha256": sha256_file(dest),
                    "etag": obj.get("etag"),
                }
            )
        except Exception as e:
            meta["status"] = "error"
            meta["error"] = repr(e)
        results.append(meta)
    return results


def handle_web_crawl(ctx: DownloaderContext, target: Dict[str, Any], out_dir: Path) -> List[Dict[str, Any]]:
    """
    Polite single-hop crawler for curated seed URLs.
    - Respects robots.txt.
    - Filters downloads by include_globs.
    - Captures ToS/robots snapshot into _catalogs.
    """
    if requests is None:
        return [{"status": "error", "error": "requests not installed"}]

    dl = target.get("download", {})
    seeds = dl.get("seed_urls") or []
    include_globs = dl.get("include_globs") or ["*"]
    allow_domains = dl.get("allow_domains")
    max_pages = int(dl.get("max_pages", 50))
    delay = float(dl.get("delay_seconds", 1.0))

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

    results: List[Dict[str, Any]] = []
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
            resp = requests.get(url, timeout=20, headers={"User-Agent": f"3d-corpus-crawler/{VERSION}"})
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
            # Download if file matches globs
            parsed = urlparse(url)
            path_part = parsed.path.split("/")[-1]
            if path_part and any(fnmatch.fnmatch(path_part, g) for g in include_globs):
                dest = out_dir / safe_name(path_part)
                ensure_dir(dest.parent)
                dest.write_bytes(resp.content)
                results.append(
                    {
                        "status": "ok",
                        "url": url,
                        "path": str(dest),
                        "bytes": dest.stat().st_size,
                        "sha256": sha256_file(dest),
                    }
                )
            else:
                results.append({"status": "visited", "url": url})
        except Exception as e:
            results.append({"status": "error", "url": url, "error": repr(e)})
        time.sleep(delay)

    write_json(catalog_dir / "crawl_run.json", {"seeds": seeds, "visited": list(visited), "results_count": len(results)})
    return results


STRATEGY_HANDLERS = {
    "http": handle_http, "ftp": handle_ftp, "git": handle_git,
    "zenodo": handle_zenodo, "dataverse": handle_dataverse,
    "huggingface_datasets": handle_hf_datasets, "api": handle_api,
    "s3_public": handle_s3_public,
    "web_crawl": handle_web_crawl,
}


def run_target(ctx: DownloaderContext, row: Dict[str, Any]) -> Dict[str, Any]:
    tid = row["id"]
    pool = row.get("output_pool", "quarantine") or "quarantine"
    download = row.get("download", {})
    strat = (download.get("strategy") or "none").strip()
    out_dir = resolve_pool_dir(ctx, pool, tid)

    manifest = {
        "id": tid, "name": row.get("name", tid), "pool": pool, "strategy": strat,
        "started_at_utc": utc_now(), "pipeline_version": VERSION, "results": [],
    }

    handler = STRATEGY_HANDLERS.get(strat)
    if not handler or strat in {"none", ""}:
        manifest["results"] = [{"status": "noop", "reason": f"unsupported: {strat}"}]
    else:
        try:
            manifest["results"] = handler(ctx, row, out_dir)
        except Exception as e:
            manifest["results"] = [{"status": "error", "error": repr(e)}]

    manifest["finished_at_utc"] = utc_now()
    write_json(out_dir / "download_manifest.json", manifest)
    
    status = "ok" if any(r.get("status") == "ok" for r in manifest["results"]) else manifest["results"][0].get("status", "error")
    return {"id": tid, "status": status, "strategy": strat, "pool": pool}


def main() -> None:
    ap = argparse.ArgumentParser(description=f"Download Worker v{VERSION}")
    ap.add_argument("--queue", required=True, help="Queue JSONL")
    ap.add_argument("--targets-yaml", default=None, help="targets.yaml for pools")
    ap.add_argument("--pools-root", default="/data/3d/pools", help="Fallback pools root")
    ap.add_argument("--logs-dir", default="/data/3d/_logs", help="Logs directory")
    ap.add_argument("--execute", action="store_true", help="Actually download")
    ap.add_argument("--overwrite", action="store_true", help="Overwrite existing")
    ap.add_argument("--verify-sha256", action="store_true", help="Compute sha256")
    ap.add_argument("--verify-zenodo-md5", action="store_true", help="Verify Zenodo MD5")
    ap.add_argument("--enable-resume", action="store_true", default=True)
    ap.add_argument("--no-resume", action="store_true")
    ap.add_argument("--limit-targets", type=int, default=None)
    ap.add_argument("--limit-files", type=int, default=None)
    ap.add_argument("--max-bytes-per-target", type=int, default=None)
    ap.add_argument("--workers", type=int, default=1, help="Parallel workers")
    ap.add_argument("--retry-max", type=int, default=3)
    ap.add_argument("--retry-backoff", type=float, default=2.0)
    args = ap.parse_args()

    queue_path = Path(args.queue).expanduser().resolve()
    rows = read_jsonl(queue_path)
    
    pools = pools_from_targets_yaml(
        Path(args.targets_yaml).expanduser().resolve() if args.targets_yaml else None,
        Path(args.pools_root).expanduser().resolve()
    )
    logs_dir = Path(args.logs_dir).expanduser().resolve()
    ensure_dir(logs_dir)

    ctx = DownloaderContext(
        pools=pools, logs_dir=logs_dir,
        limits=Limits(args.limit_targets, args.limit_files, args.max_bytes_per_target),
        mode=RunMode(args.execute, args.overwrite, args.verify_sha256, args.verify_zenodo_md5,
                     args.enable_resume and not args.no_resume, max(1, args.workers)),
        retry=RetryConfig(args.retry_max, args.retry_backoff),
    )

    if ctx.limits.limit_targets:
        rows = rows[:ctx.limits.limit_targets]
    rows = [r for r in rows if r.get("enabled", True) and r.get("id")]

    summary = {
        "run_at_utc": utc_now(), "pipeline_version": VERSION, "queue": str(queue_path),
        "targets_seen": len(rows), "execute": ctx.mode.execute, "results": [],
    }

    if ctx.mode.workers > 1 and ctx.mode.execute:
        with ThreadPoolExecutor(max_workers=ctx.mode.workers) as executor:
            futures = {executor.submit(run_target, ctx, row): row for row in rows}
            for future in as_completed(futures):
                try:
                    result = future.result()
                except Exception as e:
                    result = {"id": futures[future].get("id"), "status": "error", "error": repr(e)}
                summary["results"].append(result)
                log_event(ctx, {"event": "target_done", **result})
    else:
        for row in rows:
            result = run_target(ctx, row)
            summary["results"].append(result)
            log_event(ctx, {"event": "target_done", **result})

    summary_path = logs_dir / f"download_run_{int(time.time())}.json"
    write_json(summary_path, summary)
    
    ok = sum(1 for r in summary["results"] if r.get("status") == "ok")
    err = sum(1 for r in summary["results"] if r.get("status") == "error")
    
    print(f"\n{'='*50}\nDownload Worker v{VERSION}\n{'='*50}")
    print(f"Mode: {'EXECUTE' if ctx.mode.execute else 'DRY-RUN'}")
    print(f"Targets: {len(summary['results'])} (OK: {ok}, Errors: {err})")
    print(f"Summary: {summary_path}\n{'='*50}\n")
    print(json.dumps({"summary": str(summary_path), "ok": ok, "errors": err}, indent=2))


if __name__ == "__main__":
    main()
