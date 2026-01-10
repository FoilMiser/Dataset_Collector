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

import sys

if __package__ in (None, ""):
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
else:
    from pathlib import Path

import dataclasses
import gzip
import hashlib
import json
from collections.abc import Callable
from typing import Any

from collector_core.__version__ import __version__ as VERSION
from collector_core.acquire_strategies import (
    AcquireContext,
    DEFAULT_STRATEGY_HANDLERS,
    RootsDefaults,
    ensure_dir,
    handle_http_single,
    normalize_download,
    run_acquire_worker,
    utc_now,
)

__all__ = ["main", "VERSION"]
from collector_core.dependencies import _try_import

PdfReader = _try_import("pypdf", "PdfReader")
pdfminer_extract_text = _try_import("pdfminer.high_level", "extract_text")
BeautifulSoup = _try_import("bs4", "BeautifulSoup")
trafilatura = _try_import("trafilatura")

STRATEGY_HANDLERS = {
    **DEFAULT_STRATEGY_HANDLERS,
    "http": handle_http_single,
}

DEFAULTS = RootsDefaults(
    raw_root="/data/metrology/raw",
    manifests_root="/data/metrology/_manifests",
    logs_root="/data/metrology/_logs",
)


@dataclasses.dataclass
class ChunkingConfig:
    max_chars: int = 7000
    min_chars: int = 600


def resolve_chunking(ctx: AcquireContext) -> ChunkingConfig:
    cfg = ctx.cfg or {}
    globals_cfg = cfg.get("globals", {}) if isinstance(cfg, dict) else {}
    text_defaults = globals_cfg.get("text_processing_defaults", {}) or {}
    return ChunkingConfig(
        max_chars=int(text_defaults.get("max_chunk_chars", 7000)),
        min_chars=int(text_defaults.get("min_chunk_chars", 600)),
    )


def normalize_text(text: str) -> str:
    if not text:
        return ""
    lines = [line.rstrip() for line in text.replace("\r\n", "\n").replace("\r", "\n").split("\n")]
    return "\n".join(lines).strip()


def chunk_text(text: str, max_chars: int, min_chars: int) -> list[str]:
    if not text:
        return []
    paragraphs = [p.strip() for p in text.split("\n\n") if p.strip()]
    chunks: list[str] = []
    buf: list[str] = []
    buf_len = 0
    for p in paragraphs:
        if buf_len + len(p) + 2 <= max_chars:
            buf.append(p)
            buf_len += len(p) + 2
        else:
            if buf:
                chunks.append("\n\n".join(buf))
            buf = [p]
            buf_len = len(p)
    if buf:
        chunks.append("\n\n".join(buf))
    trimmed = []
    for c in chunks:
        if len(c) < min_chars:
            continue
        trimmed.append(c)
    return trimmed


def hash_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def extract_pdf_text(path: Path) -> str:
    if PdfReader is not None:
        try:
            reader = PdfReader(str(path))
            pages = []
            for page in reader.pages:
                text = page.extract_text() or ""
                if text:
                    pages.append(text)
            return "\n\n".join(pages)
        except Exception:
            pass
    if pdfminer_extract_text is not None:
        try:
            return pdfminer_extract_text(str(path)) or ""
        except Exception:
            return ""
    return ""


def extract_html_text(path: Path) -> str:
    html = path.read_text(encoding="utf-8", errors="ignore")
    if trafilatura is not None:
        extracted = trafilatura.extract(html, include_tables=True)
        if extracted:
            return extracted
    if BeautifulSoup is None:
        return ""
    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    return soup.get_text(separator="\n")


def resolve_routing(row: dict[str, Any]) -> dict[str, Any]:
    routing = row.get("routing")
    if isinstance(routing, dict) and routing:
        return routing
    return {
        "subject": row.get("routing_subject") or row.get("subject") or "metrology",
        "domain": row.get("routing_domain"),
        "category": row.get("routing_category"),
        "level": row.get("routing_level"),
        "granularity": row.get("routing_granularity"),
        "confidence": row.get("routing_confidence"),
        "reason": row.get("routing_reason"),
    }


def resolve_source_url(row: dict[str, Any]) -> str | None:
    download = normalize_download(row.get("download", {}) or {})
    url = download.get("url")
    if url:
        return url
    urls = download.get("urls") or []
    return urls[0] if urls else None


def write_jsonl_gz(path: Path, rows: list[dict[str, Any]], overwrite: bool) -> dict[str, Any]:
    if path.exists() and not overwrite:
        return {"status": "cached", "path": str(path)}
    ensure_dir(path.parent)
    with gzip.open(path, "wt", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
    return {"status": "ok", "path": str(path), "records": len(rows)}


def extract_artifacts(
    ctx: AcquireContext,
    row: dict[str, Any],
    out_dir: Path,
    chunking: ChunkingConfig,
) -> dict[str, Any]:
    artifacts_dir = out_dir / "artifacts"
    if not artifacts_dir.exists():
        return {"status": "noop", "reason": "no artifacts dir"}
    routing = resolve_routing(row)
    source_url = resolve_source_url(row)
    publisher = row.get("publisher")
    target_id = row.get("id")
    chunks_dir = out_dir / "chunks"
    results: list[dict[str, Any]] = []
    files = [p for p in artifacts_dir.rglob("*") if p.is_file()]
    files = [p for p in files if p.suffix.lower() in {".pdf", ".html", ".htm"}]
    for idx, path in enumerate(files):
        suffix = path.suffix.lower()
        if suffix == ".pdf":
            text = extract_pdf_text(path)
            content_type = "application/pdf"
        else:
            text = extract_html_text(path)
            content_type = "text/html"
        text = normalize_text(text)
        if not text:
            results.append({"status": "skipped", "path": str(path), "reason": "no text extracted"})
            continue
        chunks = chunk_text(text, chunking.max_chars, chunking.min_chars)
        if not chunks:
            results.append({"status": "skipped", "path": str(path), "reason": "no chunks"})
            continue
        records: list[dict[str, Any]] = []
        for chunk_id, chunk in enumerate(chunks):
            normalized = normalize_text(chunk)
            if not normalized:
                continue
            record = {
                "text": normalized,
                "chunk_id": chunk_id,
                "source": {
                    "target_id": target_id,
                    "source_url": source_url,
                    "retrieved_at_utc": utc_now(),
                    "content_type": content_type,
                    "publisher": publisher,
                    "artifact_path": str(path),
                },
                "routing": routing,
                "hash": {"content_sha256": hash_text(normalized)},
            }
            records.append(record)
        if not records:
            results.append({"status": "skipped", "path": str(path), "reason": "empty records"})
            continue
        shard_path = chunks_dir / f"chunk_{idx:05d}.jsonl.gz"
        results.append(write_jsonl_gz(shard_path, records, ctx.mode.overwrite))
    return {"status": "ok", "artifacts": len(files), "results": results}


def apply_artifacts_dir(handler: Callable[[AcquireContext, dict[str, Any], Path], list[dict[str, Any]]]):
    def wrapped(ctx: AcquireContext, row: dict[str, Any], out_dir: Path) -> list[dict[str, Any]]:
        return handler(ctx, row, out_dir / "artifacts")

    return wrapped


def metrology_postprocess(
    ctx: AcquireContext,
    row: dict[str, Any],
    out_dir: Path,
    bucket: str,
    manifest: dict[str, Any],
) -> dict[str, Any] | None:
    if not ctx.mode.execute:
        return None
    manifest["extraction"] = extract_artifacts(ctx, row, out_dir, resolve_chunking(ctx))
    return None


def main() -> None:
    strategy_handlers = {key: apply_artifacts_dir(handler) for key, handler in STRATEGY_HANDLERS.items()}
    run_acquire_worker(
        defaults=DEFAULTS,
        targets_yaml_label="targets_metrology.yaml",
        strategy_handlers=strategy_handlers,
        postprocess=metrology_postprocess,
    )


if __name__ == "__main__":
    main()
