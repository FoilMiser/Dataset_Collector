#!/usr/bin/env python3
"""
download_worker.py (v1.0)

Consumes queue JSONL emitted by pipeline_driver.py and downloads dataset payloads
according to each row's `download` strategy.

Safe defaults:
  - DRY RUN by default (prints plan, writes manifests, does not download)
  - You must pass --execute to actually download.

v1.0 features:
  - NEW: Difficulty-aware routing via difficulties_math.yaml + math_routing fields
  - Enhanced Figshare resolver with API support
  - GitHub release resolver with rate limit handling
  - Parquet output option (--emit-parquet)
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
from typing import Any, Dict, List, Optional
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


VERSION = "1.0"


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

def coerce_int(val: Any, default: int) -> int:
    try:
        return int(val)
    except Exception:
        return default

def clamp_level(level: Any, default: int) -> int:
    return max(1, min(10, coerce_int(level, default)))

def _default_route(diff_cfg: Dict[str, Any]) -> Dict[str, Any]:
    g = (diff_cfg.get("globals", {}) or {})
    default_level = coerce_int(g.get("default_level", 5), 5)
    return {
        "subject": g.get("default_subject", "math"),
        "domain": g.get("default_domain", "misc"),
        "category": g.get("default_category", "misc"),
        "level": default_level,
        "granularity": "target",
    }


def _apply_override(route: Dict[str, Any], overrides: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "subject": overrides.get("subject", route.get("subject")),
        "domain": overrides.get("domain", route.get("domain")),
        "category": overrides.get("category", route.get("category")),
        "level": coerce_int(overrides.get("level"), route.get("level")),
        "granularity": overrides.get("granularity", route.get("granularity")),
        "confidence": overrides.get("confidence", route.get("confidence")),
        "reason": overrides.get("reason", route.get("reason")),
    }


def _match_keyword_rules(blob: str, rules: List[Dict[str, Any]], fallback: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    for rule in rules or []:
        keywords = (rule.get("match_any", []) or [])
        if any(str(k).lower() in blob for k in keywords):
            r = (rule.get("route", {}) or {})
            return _apply_override(
                fallback,
                {
                    "subject": r.get("subject"),
                    "domain": r.get("domain"),
                    "category": r.get("category"),
                    "level": r.get("level"),
                    "granularity": r.get("granularity"),
                    "confidence": rule.get("confidence"),
                    "reason": r.get("reason"),
                },
            )
    return None


def resolve_route(row: Dict[str, Any], diff_cfg: Dict[str, Any]) -> Dict[str, Any]:
    base = _default_route(diff_cfg)
    g = (diff_cfg.get("globals", {}) or {})
    default_level = coerce_int(g.get("default_level", 5), 5)

    explicit = {
        "subject": row.get("routing_subject"),
        "domain": row.get("routing_domain"),
        "category": row.get("routing_category"),
        "level": row.get("routing_level"),
        "granularity": row.get("routing_granularity"),
        "confidence": row.get("routing_confidence"),
        "reason": row.get("routing_reason"),
    }
    if any(explicit.values()):
        base = _apply_override(base, explicit)
    else:
        legacy = {
            "subject": "math" if any((row.get("math_domain"), row.get("math_category"), row.get("difficulty_level") is not None)) else None,
            "domain": row.get("math_domain"),
            "category": row.get("math_category"),
            "level": row.get("difficulty_level"),
            "granularity": row.get("math_granularity"),
        }
        if any(legacy.values()):
            base = _apply_override(base, legacy)

    overrides = (diff_cfg.get("source_overrides", {}) or {})
    subject_overrides = overrides.get(base.get("subject"), {}) if isinstance(overrides.get(base.get("subject")), dict) else overrides
    if row.get("id") in subject_overrides:
        base = _apply_override(base, subject_overrides[row["id"]])

    dt = row.get("data_type", [])
    dt_blob = " ".join(dt) if isinstance(dt, list) else str(dt or "")
    blob = f"{row.get('name', '')} {dt_blob}".lower()

    rule_sets = (diff_cfg.get("rule_sets", {}) or {})
    matched = _match_keyword_rules(blob, (rule_sets.get("global", {}) or {}).get("keyword_rules"), base)
    if matched:
        return matched

    subject_rules = (rule_sets.get("subjects", {}) or {}).get(base.get("subject") or "math", {})
    matched = _match_keyword_rules(blob, subject_rules.get("keyword_rules"), base)
    if matched:
        return matched

    arxiv_map = (subject_rules.get("arxiv_primary_category_map") or {})
    apc = row.get("arxiv_primary_category")
    if apc and apc in arxiv_map:
        base = _apply_override(base, arxiv_map[apc])

    return {
        "subject": base.get("subject"),
        "domain": base.get("domain"),
        "category": base.get("category"),
        "level": clamp_level(base.get("level"), default_level),
        "granularity": base.get("granularity") or "target",
        "confidence": base.get("confidence"),
        "reason": base.get("reason"),
    }

def _sanitize_segment(val: str, enabled: bool) -> str:
    return safe_name(val) if enabled else (val or "")


def resolve_output_dir(ctx: "DownloaderContext", pool_name: str, route: Dict[str, Any], target_id: str) -> Path:
    pool_name = (pool_name or "quarantine").strip().lower()
    pool_path = {"permissive": ctx.pools.permissive, "copyleft": ctx.pools.copyleft}.get(pool_name, ctx.pools.quarantine)

    g = (ctx.difficulty_cfg.get("globals", {}) or {})
    level = clamp_level(route.get("level"), coerce_int(g.get("default_level", 5), 5))
    sanitize = g.get("sanitize_path_segments", True)
    subject = _sanitize_segment(route.get("subject") or g.get("default_subject", "math"), sanitize)
    domain = _sanitize_segment(route.get("domain") or g.get("default_domain", "misc"), sanitize)
    category = _sanitize_segment(route.get("category") or g.get("default_category", "misc"), sanitize)
    tid = _sanitize_segment(target_id, sanitize)

    folder_layout = g.get("folder_layout")
    base_root = ctx.pools.root or pool_path.parent
    if folder_layout and folder_layout.startswith("pools/") and pool_path.name in {"permissive", "copyleft", "quarantine"}:
        try:
            base_root = pool_path.parents[1]
        except IndexError:
            base_root = pool_path.parent

    if folder_layout:
        try:
            rendered = folder_layout.format(
                pool=pool_name,
                pool_path=str(pool_path),
                subject=subject,
                level=level,
                domain=domain,
                category=category,
                target_id=tid,
            )
            out = Path(rendered)
            if not out.is_absolute():
                out = base_root / out
        except Exception:
            out = pool_path / f"d{level:02d}" / domain / category / tid
    else:
        out = pool_path / f"d{level:02d}" / domain / category / tid

    ensure_dir(out)
    return out

def run_cmd(cmd: List[str], cwd: Optional[Path] = None) -> str:
    p = subprocess.run(cmd, cwd=str(cwd) if cwd else None, check=True, 
                       stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    return p.stdout.decode("utf-8", errors="ignore")


@dataclasses.dataclass
class Pools:
    permissive: Path
    copyleft: Path
    quarantine: Path
    root: Optional[Path] = None

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
    difficulty_cfg: Dict[str, Any]


def pools_from_targets_yaml(targets_yaml: Optional[Path], fallback: Path, preloaded_cfg: Optional[Dict[str, Any]] = None, difficulty_cfg: Optional[Dict[str, Any]] = None) -> Pools:
    cfg: Dict[str, Any] = preloaded_cfg or {}
    if targets_yaml and targets_yaml.exists():
        cfg = preloaded_cfg or load_yaml(targets_yaml)
    if cfg:
        pools = cfg.get("globals", {}).get("pools", {})
        root_path = Path(pools.get("root", fallback)).expanduser()
        return Pools(
            permissive=Path(pools.get("permissive", root_path / "permissive")).expanduser(),
            copyleft=Path(pools.get("copyleft", root_path / "copyleft")).expanduser(),
            quarantine=Path(pools.get("quarantine", root_path / "quarantine")).expanduser(),
            root=root_path,
        )
    g = (difficulty_cfg or {}).get("globals", {}) or {}
    root = Path(fallback)
    try:
        import platform
        if "microsoft" in platform.release().lower() and g.get("destination_root_wsl"):
            root = Path(g["destination_root_wsl"])
        elif platform.system().lower().startswith("windows") and g.get("destination_root_windows"):
            root = Path(g["destination_root_windows"])
    except Exception:
        pass
    return Pools(root / "permissive", root / "copyleft", root / "quarantine", root=root)

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
            headers = {"User-Agent": f"math-corpus-downloader/{VERSION}"}
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
            r = requests.get(api_url, timeout=60, headers={"User-Agent": f"math-corpus/{VERSION}"})
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


STRATEGY_HANDLERS = {
    "http": handle_http, "ftp": handle_ftp, "git": handle_git,
    "zenodo": handle_zenodo, "dataverse": handle_dataverse,
    "huggingface_datasets": handle_hf_datasets,
}


def run_target(ctx: DownloaderContext, row: Dict[str, Any]) -> Dict[str, Any]:
    tid = row["id"]
    pool = row.get("output_pool", "quarantine") or "quarantine"
    route = resolve_route(row, ctx.difficulty_cfg)
    download = row.get("download", {})
    strat = (download.get("strategy") or "none").strip()
    out_dir = resolve_output_dir(ctx, pool, route, tid)

    manifest = {
        "id": tid, "name": row.get("name", tid), "pool": pool, "strategy": strat,
        "started_at_utc": utc_now(), "pipeline_version": VERSION, "results": [],
        "route": route, "output_dir": str(out_dir),
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
    ap.add_argument("--targets-yaml", default=None, help="targets_math.yaml for pools")
    ap.add_argument("--pools-root", default="/data/math/pools", help="Fallback pools root")
    ap.add_argument("--difficulty-yaml", default=None, help="difficulties_math.yaml (domain/category/level map)")
    ap.add_argument("--logs-dir", default="/data/math/_logs", help="Logs directory")
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

    targets_path = Path(args.targets_yaml).expanduser().resolve() if args.targets_yaml else None
    targets_cfg: Dict[str, Any] = {}
    if targets_path and targets_path.exists():
        targets_cfg = load_yaml(targets_path) or {}

    comp = (targets_cfg.get("companion_files", {}) or {})
    diff_cfg: Dict[str, Any] = {}
    diff_path_str = args.difficulty_yaml or comp.get("difficulties_map")
    if diff_path_str:
        raw_diff_path = Path(diff_path_str).expanduser()
        candidates = [raw_diff_path.resolve()]
        if targets_path:
            candidates.insert(0, (targets_path.parent / raw_diff_path).resolve())
        for diff_path in candidates:
            if diff_path.exists():
                diff_cfg = load_yaml(diff_path) or {}
                break
        else:
            print(f"[WARN] difficulties map not found at {raw_diff_path}")

    pools = pools_from_targets_yaml(
        targets_path,
        Path(args.pools_root).expanduser().resolve(),
        targets_cfg,
        diff_cfg
    )
    logs_dir = Path(args.logs_dir).expanduser().resolve()
    ensure_dir(logs_dir)

    ctx = DownloaderContext(
        pools=pools, logs_dir=logs_dir,
        limits=Limits(args.limit_targets, args.limit_files, args.max_bytes_per_target),
        mode=RunMode(args.execute, args.overwrite, args.verify_sha256, args.verify_zenodo_md5,
                     args.enable_resume and not args.no_resume, max(1, args.workers)),
        retry=RetryConfig(args.retry_max, args.retry_backoff),
        difficulty_cfg=diff_cfg,
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
