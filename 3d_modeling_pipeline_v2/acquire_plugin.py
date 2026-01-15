#!/usr/bin/env python3
"""
acquire_plugin.py (v2.0)

Replaces download_worker.py with the v2 raw layout:
  raw/{green|yellow}/{license_pool}/{target_id}/...

Reads queue rows emitted by pipeline_driver.py and downloads payloads using the
configured strategy. Dry-run by default; pass --execute to write files. After a
successful run it writes a per-target `acquire_done.json` under the manifests
root.
"""

from __future__ import annotations

from pathlib import Path

import fnmatch
import json
import logging
import time
from typing import Any
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser

logger = logging.getLogger(__name__)

from collector_core.__version__ import __version__ as VERSION
from collector_core.acquire_strategies import (
    DEFAULT_STRATEGY_HANDLERS,
    AcquireContext,
    RootsDefaults,
    ensure_dir,
    handle_http_single,
    normalize_download,
    run_acquire_worker,
    safe_name,
    sha256_file,
    write_json,
    write_jsonl,
)
from collector_core.dependencies import _try_import, requires

requests = _try_import("requests")


def handle_api(ctx: AcquireContext, row: dict[str, Any], out_dir: Path) -> list[dict[str, Any]]:
    missing = requires("requests", requests, install="pip install requests")
    if missing:
        return [{"status": "error", "error": missing}]

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
        return [
            {
                "status": "planned",
                "base_url": base_url,
                "endpoints": list(iter_endpoints()),
                "query": shared_query,
            }
        ]

    results: list[dict[str, Any]] = []
    call_log: list[dict[str, Any]] = []

    for idx, ep in enumerate(iter_endpoints()):
        path = ep.get("path", "")
        name = ep.get("name") or safe_name(path or f"endpoint_{idx + 1}")
        method = (ep.get("method") or default_method).upper()
        params = dict(shared_query)
        params.update(ep.get("params") or {})

        page_param = ep.get("page_param", default_page_param)
        page_size_param = ep.get("page_size_param", default_page_size_param)
        page_size = ep.get("page_size", default_page_size)
        page_start = int(ep.get("page_start", default_page_start))
        max_pages = int(ep.get("max_pages", default_max_pages))

        url = urljoin(
            base_url if base_url.endswith("/") or path.startswith("/") else f"{base_url}/", path
        )
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
                resp = requests.request(
                    method,
                    url,
                    params=call_params,
                    headers={"User-Agent": f"3d-modeling-api/{VERSION}", **headers},
                    timeout=30,
                )
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
            suffix = (
                ".json" if "json" in content_type else ".html" if "html" in content_type else ".txt"
            )
            dest = out_dir / f"{name}{'' if max_pages == 1 else f'_page{page_num}'}{suffix}"

            try:
                if "json" in content_type:
                    tmp_path = Path(f"{dest}.tmp")
                    tmp_path.write_text(
                        json.dumps(resp.json(), indent=2, ensure_ascii=False) + "\n",
                        encoding="utf-8",
                    )
                    tmp_path.replace(dest)
                elif "html" in content_type or "xml" in content_type:
                    tmp_path = dest.with_suffix(dest.suffix + ".tmp")
                    tmp_path.write_text(resp.text, encoding="utf-8")
                    tmp_path.replace(dest)
                else:
                    tmp_path = dest.with_suffix(dest.suffix + ".tmp")
                    tmp_path.write_bytes(resp.content)
                    tmp_path.replace(dest)
            except Exception:
                tmp_path = dest.with_suffix(dest.suffix + ".tmp")
                tmp_path.write_bytes(resp.content)
                tmp_path.replace(dest)

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
                except Exception as e:
                    logger.debug("Failed to parse API response as JSON (continuing): %s", e)
            time.sleep(delay)

    write_json(catalog_dir / "api_calls.json", {"base_url": base_url, "results": call_log})
    return results


def handle_s3_public(
    ctx: AcquireContext, row: dict[str, Any], out_dir: Path
) -> list[dict[str, Any]]:
    download = normalize_download(row.get("download", {}) or {})
    bucket = download.get("bucket")
    prefix = download.get("prefix", "").lstrip("/")
    include_globs = download.get("include_globs") or ["**"]

    if not bucket:
        return [{"status": "noop", "reason": "no bucket"}]

    boto3 = _try_import("boto3")
    unsigned = _try_import("botocore", "UNSIGNED")
    config_cls = _try_import("botocore.config", "Config")
    missing = requires("boto3", boto3, install="pip install boto3")
    if missing or unsigned is None or config_cls is None:
        hint = missing or "missing dependency: botocore (install: pip install boto3)"
        return [{"status": "error", "error": hint}]

    if not ctx.mode.execute:
        return [
            {
                "status": "planned",
                "bucket": bucket,
                "prefix": prefix,
                "include_globs": include_globs,
            }
        ]

    ensure_dir(out_dir)
    catalog_dir = out_dir / "_catalogs"
    ensure_dir(catalog_dir)

    s3 = boto3.client("s3", config=config_cls(signature_version=unsigned))

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
                    objects.append(
                        {
                            "key": key,
                            "size": obj.get("Size"),
                            "etag": obj.get("ETag", "").strip('"'),
                            "last_modified": obj.get("LastModified").isoformat()
                            if obj.get("LastModified")
                            else None,
                        }
                    )
            if not resp.get("IsTruncated"):
                break
            token = resp.get("NextContinuationToken")
        return objects

    results: list[dict[str, Any]] = []
    objects = list_objects()
    write_json(
        catalog_dir / "listing.json", {"bucket": bucket, "prefix": prefix, "objects": objects}
    )

    for obj in objects:
        key = obj["key"]
        rel = key[len(prefix) :].lstrip("/") if prefix else key
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


def handle_web_crawl(
    ctx: AcquireContext, row: dict[str, Any], out_dir: Path
) -> list[dict[str, Any]]:
    missing = requires("requests", requests, install="pip install requests")
    if missing:
        return [{"status": "error", "error": missing}]

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
        except Exception as e:
            logger.debug("Failed to parse robots.txt for %s: %s", robots_url, e)
            return True

    def snapshot_tos(url: str) -> None:
        try:
            resp = requests.get(url, timeout=15)
            fname = catalog_dir / f"tos_{safe_name(urlparse(url).netloc)}.html"
            tmp_path = fname.with_suffix(".tmp")
            tmp_path.write_text(resp.text, encoding="utf-8")
            tmp_path.replace(fname)
        except Exception as e:
            logger.debug("Failed to snapshot ToS from %s: %s", url, e)

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
            resp = requests.get(
                url, timeout=20, headers={"User-Agent": f"3d-modeling-crawler/{VERSION}"}
            )
            if resp.status_code >= 400:
                results.append(
                    {"status": "error", "url": url, "error": f"status {resp.status_code}"}
                )
                continue
            content_type = resp.headers.get("Content-Type", "")
            if "text/html" in content_type:
                import re

                links = re.findall(r'href=["\'](.*?)["\']', resp.text, flags=re.IGNORECASE)
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
                tmp_path = dest.with_suffix(dest.suffix + ".tmp")
                tmp_path.write_bytes(resp.content)
                tmp_path.replace(dest)
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

    write_json(
        catalog_dir / "crawl_run.json",
        {"seeds": seeds, "visited": list(visited), "results_count": len(results)},
    )
    return results


def index_mesh_assets(
    row: dict[str, Any], out_dir: Path, results: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    mesh_exts = {
        ".stl",
        ".obj",
        ".fbx",
        ".ply",
        ".glb",
        ".gltf",
        ".stp",
        ".step",
        ".iges",
        ".igs",
        ".3mf",
    }
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
                "source": {
                    "target_id": row.get("id"),
                    "source_url": row.get("license_evidence_url"),
                },
                "routing": {k: v for k, v in routing.items() if v is not None},
                "hash": {"content_sha256": sha256_file(asset_path)},
            }
        )
    return index_rows


def modeling_postprocess(
    ctx: AcquireContext,
    row: dict[str, Any],
    out_dir: Path,
    bucket: str,
    manifest: dict[str, Any],
) -> dict[str, Any] | None:
    download = normalize_download(row.get("download", {}) or {})
    if ctx.mode.execute and (download.get("emit_index") or download.get("index_assets")):
        index_rows = index_mesh_assets(row, out_dir, manifest.get("results", []))
        if index_rows:
            write_jsonl(out_dir / "records.index.jsonl", index_rows)
    return None


STRATEGY_HANDLERS = {
    **DEFAULT_STRATEGY_HANDLERS,
    "http": handle_http_single,
    "api": handle_api,
    "s3_public": handle_s3_public,
    "web_crawl": handle_web_crawl,
}

DEFAULTS = RootsDefaults(
    raw_root="/data/3d/raw",
    manifests_root="/data/3d/_manifests",
    ledger_root="/data/3d/_ledger",
    logs_root="/data/3d/_logs",
)


def main() -> None:
    run_acquire_worker(
        defaults=DEFAULTS,
        targets_yaml_label="targets_3d_modeling.yaml",
        strategy_handlers=STRATEGY_HANDLERS,
        postprocess=modeling_postprocess,
    )


if __name__ == "__main__":
    main()
