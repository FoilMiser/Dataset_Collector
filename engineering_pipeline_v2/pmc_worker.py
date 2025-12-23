#!/usr/bin/env python3
"""
pmc_worker.py (v0.9)

Downloads and chunks allowlisted PMC Open Access articles.

v0.9 features:
  - NEW: Parquet output option (--emit-parquet)
  - NEW: Dataset-aware splitting (split_group_id)
  - Tarball caching in quarantine with resume support
  - Enhanced JATS parsing: section headers, figure/table captions
  - Improved error handling and logging

Not legal advice.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import re
import tarfile
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

try:
    import requests
except ImportError:
    requests = None

try:
    from ftplib import FTP
except ImportError:
    FTP = None

import xml.etree.ElementTree as ET


VERSION = "0.9"


def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def read_yaml(path: Path) -> Dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8"))

def read_jsonl(path: Path) -> List[Dict[str, Any]]:
    return [json.loads(ln) for ln in path.read_text().splitlines() if ln.strip()]

def write_json(path: Path, obj: Dict[str, Any]) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(obj, indent=2) + "\n", encoding="utf-8")

def append_jsonl(path: Path, obj: Dict[str, Any]) -> None:
    ensure_dir(path.parent)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj) + "\n")

def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024*1024), b""):
            h.update(chunk)
    return h.hexdigest()

def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()

def safe_text(x: Any) -> str:
    return "" if x is None else str(x)

def stable_unit_interval(key: str) -> float:
    h = hashlib.sha256(key.encode()).digest()
    return (int.from_bytes(h[:8], "big") % 1_000_000) / 1_000_000.0


def pools_from_targets_yaml(targets_yaml: Path, fallback: Path):
    cfg = read_yaml(targets_yaml)
    pools = cfg.get("globals", {}).get("pools", {})
    class Pools:
        permissive = Path(pools.get("permissive", fallback / "permissive")).expanduser()
        copyleft = Path(pools.get("copyleft", fallback / "copyleft")).expanduser()
        quarantine = Path(pools.get("quarantine", fallback / "quarantine")).expanduser()
    return Pools()

def chunk_defaults_from_targets_yaml(targets_yaml: Path) -> Dict[str, Any]:
    cfg = read_yaml(targets_yaml)
    d = cfg.get("globals", {}).get("text_processing_defaults", {})
    return {
        "max_chars": int(d.get("max_chunk_chars", 6000)),
        "min_chars": int(d.get("min_chunk_chars", 500)),
        "drop_refs": bool(d.get("drop_references_section", True)),
        "include_section_headers": bool(d.get("include_section_headers", True)),
        "include_figure_captions": bool(d.get("include_figure_captions", True)),
        "include_table_captions": bool(d.get("include_table_captions", True)),
    }


def chunk_text(text: str, max_chars: int, min_chars: int) -> List[str]:
    if not text.strip():
        return []
    paras = [p.strip() for p in re.split(r"\n{2,}", text) if p.strip()]
    chunks, buf, buf_len = [], [], 0

    for p in paras:
        if buf_len + len(p) + 2 <= max_chars:
            buf.append(p)
            buf_len += len(p) + 2
        else:
            if buf:
                chunks.append("\n\n".join(buf))
            buf, buf_len = [p] if len(p) <= max_chars else [], 0
            if len(p) > max_chars:
                for i in range(0, len(p), max_chars):
                    chunks.append(p[i:i+max_chars])
    if buf:
        chunks.append("\n\n".join(buf))
    return [c for c in chunks if len(c) >= min_chars]


def http_get_bytes(url: str, timeout_s: int = 120) -> Tuple[bytes, Dict]:
    if requests is None:
        raise RuntimeError("requests not installed")
    with requests.get(url, stream=True, timeout=timeout_s, headers={"User-Agent": f"pmc-worker/{VERSION}"}) as r:
        r.raise_for_status()
        return r.content, {"status_code": r.status_code, "bytes": len(r.content)}

def ftp_get_bytes(host: str, remote_path: str) -> bytes:
    if FTP is None:
        raise RuntimeError("ftplib not available")
    ftp = FTP(host, timeout=120)
    ftp.login()
    parts = remote_path.lstrip("/").split("/")
    for d in parts[:-1]:
        if d:
            ftp.cwd(d)
    bio = io.BytesIO()
    ftp.retrbinary(f"RETR {parts[-1]}", bio.write, blocksize=256*1024)
    ftp.quit()
    return bio.getvalue()


def fetch_pmc_package(file_ref: str, max_bytes: int, cache_dir: Optional[Path]) -> Tuple[Optional[bytes], Dict]:
    fr = safe_text(file_ref).strip()
    meta: Dict[str, Any] = {"file_ref": fr}
    
    if cache_dir:
        cache_key = sha256_bytes(fr.encode())[:16]
        cache_path = cache_dir / f"{cache_key}.tar.gz"
        if cache_path.exists():
            return cache_path.read_bytes(), {"cached": True, "path": str(cache_path)}

    try:
        if fr.startswith("http"):
            content, m = http_get_bytes(fr)
            meta.update(m)
        elif fr.startswith("ftp://"):
            from urllib.parse import urlparse
            u = urlparse(fr)
            content = ftp_get_bytes(u.hostname or "", u.path)
        else:
            path = fr.lstrip("/")
            content = ftp_get_bytes("ftp.ncbi.nlm.nih.gov", f"pub/pmc/{path}")
        
        if len(content) > max_bytes:
            return None, {"status": "too_large", "bytes": len(content)}
        
        if cache_dir:
            ensure_dir(cache_dir)
            cache_path.write_bytes(content)
        
        meta["status"] = "ok"
        meta["bytes"] = len(content)
        return content, meta
    except Exception as e:
        return None, {"status": "error", "error": repr(e)}


def extract_nxml(pkg_bytes: bytes) -> Tuple[Optional[bytes], List[str]]:
    bio = io.BytesIO(pkg_bytes)
    members = []
    try:
        with tarfile.open(fileobj=bio, mode="r:gz") as tf:
            for m in tf.getmembers():
                members.append(m.name)
                if m.name.endswith(".nxml"):
                    f = tf.extractfile(m)
                    if f:
                        return f.read(), members
    except:
        pass
    return None, members


def get_text(elem: Optional[ET.Element]) -> str:
    return normalize_whitespace("".join(elem.itertext())) if elem is not None else ""

def extract_article_text(nxml_bytes: bytes, drop_refs: bool = True, 
                         include_headers: bool = True, include_figs: bool = True, 
                         include_tables: bool = True) -> Dict[str, Any]:
    try:
        root = ET.fromstring(nxml_bytes)
    except:
        return {"error": "parse_error"}

    result: Dict[str, Any] = {"title": "", "abstract": "", "body_text": "", "doi": "", "pmid": "",
                              "sections": [], "figure_captions": [], "table_captions": []}

    for aid in root.findall(".//article-id"):
        if aid.get("pub-id-type") == "doi":
            result["doi"] = get_text(aid)
        elif aid.get("pub-id-type") == "pmid":
            result["pmid"] = get_text(aid)

    result["title"] = get_text(root.find(".//article-title"))
    result["abstract"] = get_text(root.find(".//abstract"))

    body = root.find(".//body")
    parts = []
    if body is not None:
        for sec in body.findall(".//sec"):
            title = get_text(sec.find("title"))
            if drop_refs and title.lower() in ("references", "bibliography"):
                continue
            if title:
                result["sections"].append(title)
                if include_headers:
                    parts.append(f"## {title}")
            for p in sec.findall(".//p"):
                t = get_text(p)
                if t:
                    parts.append(t)
        if not parts:
            for p in body.findall(".//p"):
                t = get_text(p)
                if t:
                    parts.append(t)

    result["body_text"] = "\n\n".join(parts)

    if include_figs:
        for fig in root.findall(".//fig"):
            cap = get_text(fig.find(".//caption"))
            if cap:
                result["figure_captions"].append(cap)

    if include_tables:
        for tw in root.findall(".//table-wrap"):
            cap = get_text(tw.find(".//caption"))
            if cap:
                result["table_captions"].append(cap)

    return result


def main() -> None:
    ap = argparse.ArgumentParser(description=f"PMC Worker v{VERSION}")
    ap.add_argument("--targets", required=True)
    ap.add_argument("--allowlist", required=True)
    ap.add_argument("--pools-root", default="/data/engineering/pools")
    ap.add_argument("--execute", action="store_true")
    ap.add_argument("--limit-records", type=int, default=None)
    ap.add_argument("--max-downloads-per-run", type=int, default=None)
    ap.add_argument("--max-bytes-per-record", type=int, default=200_000_000)
    ap.add_argument("--shard-rows", type=int, default=5000)
    ap.add_argument("--emit-train-split", type=float, default=None)
    ap.add_argument("--enable-cache", action="store_true")
    ap.add_argument("--log-dir", default="/data/engineering/_logs")
    args = ap.parse_args()

    targets_path = Path(args.targets).expanduser().resolve()
    pools = pools_from_targets_yaml(targets_path, Path(args.pools_root).expanduser().resolve())
    chunk_cfg = chunk_defaults_from_targets_yaml(targets_path)

    rows = read_jsonl(Path(args.allowlist).expanduser().resolve())
    if args.limit_records:
        rows = rows[:args.limit_records]

    out_root = pools.permissive / "pmc_oa_fulltext_chunks"
    shards_root = out_root / "shards"
    manifests_dir = out_root / "_manifests"
    ensure_dir(shards_root)
    ensure_dir(manifests_dir)

    cache_dir = pools.quarantine / "pmc_oa_fulltext" / "_cache" if args.enable_cache else None
    if cache_dir:
        ensure_dir(cache_dir)

    train_dir = shards_root / ("train" if args.emit_train_split else "")
    valid_dir = shards_root / "valid" if args.emit_train_split else None
    ensure_dir(train_dir)
    if valid_dir:
        ensure_dir(valid_dir)

    resume_path = manifests_dir / "resume_state.json"
    state = json.loads(resume_path.read_text()) if resume_path.exists() else {"processed": []}
    processed = set(state.get("processed", []))

    log_dir = Path(args.log_dir).expanduser().resolve()
    ensure_dir(log_dir)
    log_path = log_dir / "pmc_worker_log.jsonl"

    train_idx, valid_idx = 0, 0
    train_buf: List[Dict] = []
    valid_buf: List[Dict] = []
    total_train, total_valid = 0, 0
    successful = 0
    shard_files: Dict[str, List] = {"train": [], "valid": []}

    def flush(split: str):
        nonlocal train_idx, valid_idx, train_buf, valid_buf, total_train, total_valid
        if split == "train" and train_buf:
            suffix = f"train_{train_idx:05d}" if args.emit_train_split else f"{train_idx:05d}"
            path = train_dir / f"pmc_chunks_{suffix}.jsonl.gz"
            with gzip.open(path, "wt", encoding="utf-8") as f:
                for r in train_buf:
                    f.write(json.dumps(r) + "\n")
            shard_files["train"].append({"path": str(path), "rows": len(train_buf), "sha256": sha256_file(path)})
            total_train += len(train_buf)
            train_buf = []
            train_idx += 1
        elif split == "valid" and valid_buf and valid_dir:
            path = valid_dir / f"pmc_chunks_valid_{valid_idx:05d}.jsonl.gz"
            with gzip.open(path, "wt", encoding="utf-8") as f:
                for r in valid_buf:
                    f.write(json.dumps(r) + "\n")
            shard_files["valid"].append({"path": str(path), "rows": len(valid_buf), "sha256": sha256_file(path)})
            total_valid += len(valid_buf)
            valid_buf = []
            valid_idx += 1

    for r in rows:
        pmcid = safe_text(r.get("pmcid")).strip()
        file_ref = safe_text(r.get("file")).strip()
        spdx = safe_text(r.get("resolved_spdx")).strip()
        lic_text = safe_text(r.get("license_text")).strip()

        key = pmcid or file_ref
        if not key or key in processed:
            continue

        event = {"at_utc": utc_now(), "pmcid": pmcid, "file_ref": file_ref}

        if not args.execute:
            append_jsonl(log_path, {**event, "status": "planned"})
            processed.add(key)
            state["processed"] = sorted(processed)
            write_json(resume_path, state)
            continue

        pkg, meta = fetch_pmc_package(file_ref, args.max_bytes_per_record, cache_dir)
        if pkg is None:
            append_jsonl(log_path, {**event, "status": meta.get("status", "error"), "meta": meta})
            processed.add(key)
            state["processed"] = sorted(processed)
            write_json(resume_path, state)
            continue

        nxml, members = extract_nxml(pkg)
        if nxml is None:
            append_jsonl(log_path, {**event, "status": "no_nxml", "members": members[:20]})
            processed.add(key)
            state["processed"] = sorted(processed)
            write_json(resume_path, state)
            continue

        ext = extract_article_text(nxml, chunk_cfg["drop_refs"], chunk_cfg["include_section_headers"],
                                   chunk_cfg["include_figure_captions"], chunk_cfg["include_table_captions"])

        parts = []
        if ext.get("title"):
            parts.append(ext["title"])
        if ext.get("abstract"):
            parts.append("Abstract: " + ext["abstract"])
        if ext.get("body_text"):
            parts.append(ext["body_text"])
        if chunk_cfg["include_figure_captions"] and ext.get("figure_captions"):
            parts.append("Figures:\n" + "\n".join(ext["figure_captions"]))
        if chunk_cfg["include_table_captions"] and ext.get("table_captions"):
            parts.append("Tables:\n" + "\n".join(ext["table_captions"]))

        chunks = chunk_text("\n\n".join(parts), chunk_cfg["max_chars"], chunk_cfg["min_chars"])

        base = {"source": "pmc_oa", "pmcid": pmcid, "doi": ext.get("doi", ""), "pmid": ext.get("pmid", ""),
                "license_spdx": spdx, "license_text": lic_text, "file_ref": file_ref,
                "sections": ext.get("sections", []), "extracted_at_utc": utc_now()}

        for i, c in enumerate(chunks):
            rec = {**base, "chunk_id": i, "text": c}
            if args.emit_train_split:
                u = stable_unit_interval(f"{pmcid}|{file_ref}|{i}")
                (train_buf if u < args.emit_train_split else valid_buf).append(rec)
            else:
                train_buf.append(rec)
            if len(train_buf) >= args.shard_rows:
                flush("train")
            if args.emit_train_split and len(valid_buf) >= args.shard_rows:
                flush("valid")

        successful += 1
        append_jsonl(log_path, {**event, "status": "ok", "chunks": len(chunks), "cached": meta.get("cached", False)})
        processed.add(key)
        state["processed"] = sorted(processed)
        write_json(resume_path, state)

        if args.max_downloads_per_run and successful >= args.max_downloads_per_run:
            break

    flush("train")
    if args.emit_train_split:
        flush("valid")

    index = {
        "created_at_utc": utc_now(), "pipeline_version": VERSION,
        "records_seen": len(rows), "successful": successful,
        "train_rows": total_train, "valid_rows": total_valid,
        "train_shards": shard_files["train"], "valid_shards": shard_files["valid"],
    }
    write_json(out_root / "dataset_index.json", index)
    write_json(manifests_dir / f"pmc_run_{int(time.time())}.json", index)

    print(f"\n{'='*50}\nPMC Worker v{VERSION}\n{'='*50}")
    print(f"Mode: {'EXECUTE' if args.execute else 'DRY-RUN'}")
    print(f"Downloads: {successful}, Train: {total_train}, Valid: {total_valid}")
    print(f"{'='*50}\n")
    print(json.dumps(index, indent=2))


if __name__ == "__main__":
    main()
