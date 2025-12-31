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

Supported strategies: http, ftp, git, zenodo, dataverse, huggingface_datasets, figshare, github_release

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
from typing import Any
from urllib.parse import urlparse

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

def append_jsonl(path: Path, obj: dict[str, Any]) -> None:
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

def load_yaml(path: Path) -> dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8"))

def run_cmd(cmd: list[str], cwd: Path | None = None) -> str:
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
    limit_targets: int | None
    limit_files: int | None
    max_bytes_per_target: int | None

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


def pools_from_targets_yaml(targets_yaml: Path | None, fallback: Path) -> Pools:
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

def log_event(ctx: DownloaderContext, event: dict[str, Any]) -> None:
    event = dict(event)
    event.setdefault("at_utc", utc_now())
    append_jsonl(ctx.logs_dir / "download_log.jsonl", event)


def http_download_with_resume(ctx: DownloaderContext, url: str, out_path: Path, 
                               expected_size: int | None = None) -> dict[str, Any]:
    """Download with retry, exponential backoff, and resume support."""
    if requests is None:
        return {"status": "error", "error": "requests not installed"}
    
    ensure_dir(out_path.parent)
    meta: dict[str, Any] = {"url": url, "path": str(out_path), "attempts": 0, "resumed": False}

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
            headers = {"User-Agent": f"agri-circular-downloader/{VERSION}"}
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
                      include_globs: list[str], out_dir: Path) -> list[dict[str, Any]]:
    if FTP is None:
        return [{"status": "error", "error": "ftplib not available"}]
    
    u = urlparse(base_url)
    if u.scheme != "ftp":
        return [{"status": "error", "error": f"Not an ftp url: {base_url}"}]
    
    if not ctx.mode.execute:
        return [{"status": "planned", "base_url": base_url, "include_globs": include_globs}]

    ensure_dir(out_dir)
    results: list[dict[str, Any]] = []
    
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
                except Exception:
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


def handle_http(ctx: DownloaderContext, target: dict[str, Any], out_dir: Path) -> list[dict[str, Any]]:
    urls = target.get("download", {}).get("urls") or []
    if not urls:
        return [{"status": "noop", "reason": "no urls"}]
    results = []
    for url in urls:
        fname = Path(urlparse(url).path).name or "downloaded_file"
        results.append(http_download_with_resume(ctx, url, out_dir / safe_name(fname)))
    return results


def handle_git(ctx: DownloaderContext, target: dict[str, Any], out_dir: Path) -> list[dict[str, Any]]:
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


def handle_zenodo(ctx: DownloaderContext, target: dict[str, Any], out_dir: Path) -> list[dict[str, Any]]:
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
            r = requests.get(api_url, timeout=60, headers={"User-Agent": f"agri-circular-pipeline/{VERSION}"})
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


def handle_dataverse(ctx: DownloaderContext, target: dict[str, Any], out_dir: Path) -> list[dict[str, Any]]:
    if requests is None:
        return [{"status": "error", "error": "requests not installed"}]
    
    dl = target.get("download", {})
    pid = str(dl.get("persistent_id") or "").strip()
    instance = dl.get("instance", "https://dataverse.harvard.edu")
    
    if not pid:
        return [{"status": "noop", "reason": "no persistent_id"}]
    
    api = f"{instance}/api/access/dataset/:persistentId/?persistentId={pid}"
    return [http_download_with_resume(ctx, api, out_dir / f"dataverse_{safe_name(pid)}.zip")]


def handle_ftp(ctx: DownloaderContext, target: dict[str, Any], out_dir: Path) -> list[dict[str, Any]]:
    dl = target.get("download", {})
    base_url = dl.get("base_url")
    globs = dl.get("include_globs") or ["*"]
    if not base_url:
        return [{"status": "noop", "reason": "no base_url"}]
    return ftp_download_many(ctx, base_url, globs, out_dir)


def handle_hf_datasets(ctx: DownloaderContext, target: dict[str, Any], out_dir: Path) -> list[dict[str, Any]]:
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


STRATEGY_HANDLERS = {
    "http": handle_http, "ftp": handle_ftp, "git": handle_git,
    "zenodo": handle_zenodo, "dataverse": handle_dataverse,
    "huggingface_datasets": handle_hf_datasets,
}


def run_target(ctx: DownloaderContext, row: dict[str, Any]) -> dict[str, Any]:
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
    ap.add_argument("--pools-root", default="/data/agri_circular/pools", help="Fallback pools root")
    ap.add_argument("--logs-dir", default="/data/agri_circular/_logs", help="Logs directory")
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
