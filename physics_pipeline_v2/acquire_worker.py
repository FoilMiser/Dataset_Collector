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


def safe_name(s: str) -> str:
    import re
    s = re.sub(r"[^a-zA-Z0-9._-]+", "_", (s or "").strip())
    return s[:200] if s else "file"


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


def run_cmd(cmd: List[str], cwd: Optional[Path] = None) -> str:
    p = subprocess.run(cmd, cwd=str(cwd) if cwd else None, check=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    return p.stdout.decode("utf-8", errors="ignore")


@dataclasses.dataclass
class Roots:
    raw_root: Path
    manifests_root: Path
    logs_root: Path


@dataclasses.dataclass
class Limits:
    limit_targets: Optional[int]
    limit_files: Optional[int]
    max_bytes_per_target: Optional[int]


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

def _http_download_with_resume(ctx: AcquireContext, url: str, out_path: Path, expected_size: Optional[int] = None) -> Dict[str, Any]:
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
    result: Dict[str, Any] = {"status": "ok", "path": str(out_path)}
    if ctx.mode.verify_sha256 and expected_size and out_path.stat().st_size != expected_size:
        result = {"status": "error", "error": "size_mismatch"}
    elif ctx.mode.verify_sha256:
        result["sha256"] = sha256_file(out_path)
    return result


def handle_http(ctx: AcquireContext, row: Dict[str, Any], out_dir: Path) -> List[Dict[str, Any]]:
    download = row.get("download", {}) or {}
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


def handle_ftp(ctx: AcquireContext, row: Dict[str, Any], out_dir: Path) -> List[Dict[str, Any]]:
    download = row.get("download", {}) or {}
    base = download.get("base_url")
    globs = download.get("globs", ["*"])
    if FTP is None:
        return [{"status": "error", "error": "ftplib missing"}]
    if not base:
        return [{"status": "error", "error": "missing base_url"}]
    url = urlparse(base)
    results: List[Dict[str, Any]] = []
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


def handle_git(ctx: AcquireContext, row: Dict[str, Any], out_dir: Path) -> List[Dict[str, Any]]:
    download = row.get("download", {}) or {}
    repo = download.get("repo")
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


def handle_zenodo(ctx: AcquireContext, row: Dict[str, Any], out_dir: Path) -> List[Dict[str, Any]]:
    download = row.get("download", {}) or {}
    api_url = download.get("api") or download.get("record_url")
    if not api_url:
        return [{"status": "error", "error": "missing api/record_url"}]
    if requests is None:
        return [{"status": "error", "error": "requests missing"}]
    if not ctx.mode.execute:
        return [{"status": "noop", "path": str(out_dir)}]
    resp = requests.get(api_url, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    results: List[Dict[str, Any]] = []
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


def handle_dataverse(ctx: AcquireContext, row: Dict[str, Any], out_dir: Path) -> List[Dict[str, Any]]:
    download = row.get("download", {}) or {}
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


def handle_hf_datasets(ctx: AcquireContext, row: Dict[str, Any], out_dir: Path) -> List[Dict[str, Any]]:
    download = row.get("download", {}) or {}
    dataset_id = download.get("dataset_id")
    if not dataset_id:
        return [{"status": "error", "error": "missing dataset_id"}]
    splits = download.get("splits")
    load_kwargs = download.get("load_kwargs", {}) or {}
    if not ctx.mode.execute:
        return [{"status": "noop", "path": str(out_dir)}]
    try:
        from datasets import load_dataset  # type: ignore
    except Exception as e:  # pragma: no cover - optional dep
        return [{"status": "error", "error": f"datasets import failed: {e}"}]

    results: List[Dict[str, Any]] = []
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


STRATEGY_HANDLERS = {
    "http": handle_http,
    "ftp": handle_ftp,
    "git": handle_git,
    "zenodo": handle_zenodo,
    "dataverse": handle_dataverse,
    "huggingface_datasets": handle_hf_datasets,
}


def resolve_license_pool(row: Dict[str, Any]) -> str:
    lp = str(row.get("license_profile") or row.get("license_pool") or "quarantine").lower()
    if lp not in {"permissive", "copyleft", "quarantine"}:
        lp = "quarantine"
    return lp


def resolve_output_dir(ctx: AcquireContext, bucket: str, pool: str, target_id: str) -> Path:
    bucket = (bucket or "yellow").strip().lower()
    pool = (pool or "quarantine").strip().lower()
    out = ctx.roots.raw_root / bucket / pool / safe_name(target_id)
    ensure_dir(out)
    return out


def write_done_marker(ctx: AcquireContext, target_id: str, bucket: str, status: str) -> None:
    marker = ctx.roots.manifests_root / safe_name(target_id) / "acquire_done.json"
    write_json(marker, {"target_id": target_id, "bucket": bucket, "status": status, "written_at_utc": utc_now(), "version": VERSION})


def run_target(ctx: AcquireContext, bucket: str, row: Dict[str, Any]) -> Dict[str, Any]:
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

    manifest["finished_at_utc"] = utc_now()
    write_json(out_dir / "download_manifest.json", manifest)

    status = "ok" if any(r.get("status") == "ok" for r in manifest["results"]) else manifest["results"][0].get("status", "error")
    if ctx.mode.execute:
        write_done_marker(ctx, tid, bucket, status)
    return {"id": tid, "status": status, "bucket": bucket, "license_pool": pool, "strategy": strat}


def load_roots(targets_path: Optional[Path], overrides: argparse.Namespace) -> Roots:
    cfg: Dict[str, Any] = {}
    if targets_path and targets_path.exists():
        cfg = yaml.safe_load(targets_path.read_text(encoding="utf-8")) or {}
    g = (cfg.get("globals", {}) or {})
    raw_root = Path(overrides.raw_root or g.get("raw_root", "/data/physics/raw"))
    manifests_root = Path(overrides.manifests_root or g.get("manifests_root", "/data/physics/_manifests"))
    logs_root = Path(overrides.logs_root or g.get("logs_root", "/data/physics/_logs"))
    return Roots(raw_root=raw_root.expanduser().resolve(), manifests_root=manifests_root.expanduser().resolve(), logs_root=logs_root.expanduser().resolve())


def main() -> None:
    ap = argparse.ArgumentParser(description=f"Acquire Worker v{VERSION}")
    ap.add_argument("--queue", required=True, help="Queue JSONL emitted by pipeline_driver.py")
    ap.add_argument("--targets-yaml", default=None, help="Path to targets_physics.yaml for roots")
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
