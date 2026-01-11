#!/usr/bin/env python3
"""
pmc_worker.py

Downloads and chunks allowlisted PMC Open Access articles.

Tool/version metadata comes from collector_core.__version__.__version__ and
collector_core.__version__.__schema_version__ (source of truth).

Features:
  - Parquet output option (--emit-parquet)
  - Dataset-aware splitting (split_group_id)
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
import logging
import re
import tarfile
import time
import xml.etree.ElementTree as ET
from collections.abc import Callable
from pathlib import Path
from typing import Any

from collector_core.__version__ import __version__ as TOOL_VERSION
from collector_core.artifact_metadata import build_artifact_metadata
from collector_core.config_validator import read_yaml
from collector_core.dependencies import _try_import, requires
from collector_core.exceptions import (
    CollectorError,
    DependencyMissingError,
    OutputPathsBuilderError,
)
from collector_core.logging_config import add_logging_args, configure_logging
from collector_core.utils import (
    ensure_dir,
    sha256_file,
    utc_now,
    validate_tar_archive,
    write_json,
)
from collector_core.utils import (
    read_jsonl_list as read_jsonl,
)

requests = _try_import("requests")
FTP = _try_import("ftplib", "FTP")

logger = logging.getLogger(__name__)


def append_jsonl(path: Path, obj: dict[str, Any]) -> None:
    ensure_dir(path.parent)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj) + "\n")


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
    cfg = read_yaml(targets_yaml, schema_name="targets")
    pools = cfg.get("globals", {}).get("pools", {})

    class Pools:
        permissive = Path(pools.get("permissive", fallback / "permissive")).expanduser()
        copyleft = Path(pools.get("copyleft", fallback / "copyleft")).expanduser()
        quarantine = Path(pools.get("quarantine", fallback / "quarantine")).expanduser()

    return Pools()


def chunk_defaults_from_targets_yaml(targets_yaml: Path) -> dict[str, Any]:
    cfg = read_yaml(targets_yaml, schema_name="targets")
    d = cfg.get("globals", {}).get("text_processing_defaults", {})
    return {
        "max_chars": int(d.get("max_chunk_chars", 6000)),
        "min_chars": int(d.get("min_chunk_chars", 500)),
        "drop_refs": bool(d.get("drop_references_section", True)),
        "include_section_headers": bool(d.get("include_section_headers", True)),
        "include_figure_captions": bool(d.get("include_figure_captions", True)),
        "include_table_captions": bool(d.get("include_table_captions", True)),
    }


def chunk_text(text: str, max_chars: int, min_chars: int) -> list[str]:
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
            buf, buf_len = ([p] if len(p) <= max_chars else []), 0
            if len(p) > max_chars:
                for i in range(0, len(p), max_chars):
                    chunks.append(p[i : i + max_chars])
    if buf:
        chunks.append("\n\n".join(buf))
    return [c for c in chunks if len(c) >= min_chars]


def http_get_bytes(
    url: str, timeout_s: int = 120, user_agent_version: str = TOOL_VERSION
) -> tuple[bytes, dict]:
    missing = requires("requests", requests, install="pip install requests")
    if missing:
        raise DependencyMissingError(
            missing,
            dependency="requests",
            install="pip install requests",
        )
    with requests.get(
        url,
        stream=True,
        timeout=timeout_s,
        headers={"User-Agent": f"pmc-worker/{user_agent_version}"},
    ) as r:
        r.raise_for_status()
        return r.content, {"status_code": r.status_code, "bytes": len(r.content)}


def ftp_get_bytes(host: str, remote_path: str) -> bytes:
    missing = requires("ftplib", FTP, install="use a standard Python build that includes ftplib")
    if missing:
        raise DependencyMissingError(
            missing,
            dependency="ftplib",
            install="use a standard Python build that includes ftplib",
        )
    ftp = FTP(host, timeout=120)
    ftp.login()
    parts = remote_path.lstrip("/").split("/")
    for d in parts[:-1]:
        if d:
            ftp.cwd(d)
    bio = io.BytesIO()
    ftp.retrbinary(f"RETR {parts[-1]}", bio.write, blocksize=256 * 1024)
    ftp.quit()
    return bio.getvalue()


def fetch_pmc_package(
    file_ref: str,
    max_bytes: int,
    cache_dir: Path | None,
    *,
    user_agent_version: str = TOOL_VERSION,
) -> tuple[bytes | None, dict]:
    fr = safe_text(file_ref).strip()
    meta: dict[str, Any] = {"file_ref": fr}

    if cache_dir:
        cache_key = sha256_bytes(fr.encode())[:16]
        cache_path = cache_dir / f"{cache_key}.tar.gz"
        if cache_path.exists():
            return cache_path.read_bytes(), {"cached": True, "path": str(cache_path)}

    try:
        if fr.startswith("http"):
            content, m = http_get_bytes(fr, user_agent_version=user_agent_version)
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
        meta: dict[str, Any] = {"status": "error", "error": repr(e)}
        if isinstance(e, CollectorError):
            meta.update(e.as_log_fields())
        return None, meta


def extract_nxml(pkg_bytes: bytes) -> tuple[bytes | None, list[str]]:
    bio = io.BytesIO(pkg_bytes)
    members = []
    try:
        with tarfile.open(fileobj=bio, mode="r:gz") as tf:
            validate_tar_archive(tf)
            for m in tf.getmembers():
                members.append(m.name)
                if m.name.endswith(".nxml"):
                    f = tf.extractfile(m)
                    if f:
                        return f.read(), members
    except (tarfile.ReadError, ValueError):
        return None, members
    return None, members


def extract_nxml_v2(pkg_bytes: bytes) -> tuple[bytes | None, list[str]]:
    bio = io.BytesIO(pkg_bytes)
    members = []
    try:
        with tarfile.open(fileobj=bio, mode="r:gz") as tf:
            validate_tar_archive(tf)
            for m in tf.getmembers():
                members.append(m.name)
                if m.name.endswith(".nxml"):
                    f = tf.extractfile(m)
                    if f:
                        return f.read(), members
    except Exception:
        return None, members
    return None, members


def get_text(elem: ET.Element | None) -> str:
    if elem is None:
        return ""
    return normalize_whitespace("".join(elem.itertext()))


def extract_article_text(
    nxml: bytes,
    drop_refs: bool,
    include_section_headers: bool,
    include_figure_captions: bool,
    include_table_captions: bool,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "title": "",
        "abstract": "",
        "body_text": "",
        "sections": [],
        "figure_captions": [],
        "table_captions": [],
        "doi": "",
        "pmid": "",
    }

    root = ET.fromstring(nxml)

    title = root.find(".//article-title")
    result["title"] = get_text(title)

    abstract = root.find(".//abstract")
    result["abstract"] = get_text(abstract)

    doi = root.find(".//article-id[@pub-id-type='doi']")
    result["doi"] = get_text(doi)

    pmid = root.find(".//article-id[@pub-id-type='pmid']")
    result["pmid"] = get_text(pmid)

    body = root.find(".//body")
    if body is not None:
        for sec in body.findall(".//sec"):
            sec_title = get_text(sec.find("./title"))
            paragraphs = []
            for p in sec.findall(".//p"):
                paragraphs.append(get_text(p))
            text = "\n".join([t for t in paragraphs if t])
            if text:
                if include_section_headers and sec_title:
                    text = f"{sec_title}:\n{text}"
                result["sections"].append({"title": sec_title, "text": text})

    if result["sections"]:
        result["body_text"] = "\n\n".join([s["text"] for s in result["sections"] if s.get("text")])

    if not drop_refs:
        refs = root.find(".//ref-list")
        if refs is not None:
            refs_text = get_text(refs)
            if refs_text:
                result["body_text"] = (
                    result["body_text"] + "\n\nReferences:\n" + refs_text
                ).strip()

    if include_figure_captions:
        for fig in root.findall(".//fig"):
            cap = get_text(fig.find(".//caption"))
            if cap:
                result["figure_captions"].append(cap)

    if include_table_captions:
        for tw in root.findall(".//table-wrap"):
            cap = get_text(tw.find(".//caption"))
            if cap:
                result["table_captions"].append(cap)

    return result


def raw_root_from_targets_yaml(targets_yaml: Path, fallback: Path) -> Path:
    cfg = read_yaml(targets_yaml, schema_name="targets")
    raw_root = cfg.get("globals", {}).get("raw_root")
    return Path(raw_root or fallback).expanduser()


def chunk_defaults_from_targets_yaml_v2(targets_yaml: Path) -> dict[str, Any]:
    cfg = read_yaml(targets_yaml, schema_name="targets")
    d = cfg.get("globals", {}).get("text_processing_defaults", {})
    screening = cfg.get("globals", {}).get("screening", {})
    return {
        "max_chars": int(d.get("max_chunk_chars", screening.get("max_chars", 6000))),
        "min_chars": int(d.get("min_chunk_chars", screening.get("min_chars", 500))),
        "drop_refs": bool(d.get("drop_references_section", True)),
        "include_section_headers": bool(d.get("include_section_headers", True)),
        "include_figure_captions": bool(d.get("include_figure_captions", True)),
        "include_table_captions": bool(d.get("include_table_captions", True)),
    }


def extract_article_text_v2(
    nxml_bytes: bytes,
    drop_refs: bool = True,
    include_headers: bool = True,
    include_figs: bool = True,
    include_tables: bool = True,
) -> dict[str, Any]:
    try:
        root = ET.fromstring(nxml_bytes)
    except Exception:
        return {"error": "parse_error"}

    result: dict[str, Any] = {
        "title": "",
        "abstract": "",
        "body_text": "",
        "doi": "",
        "pmid": "",
        "sections": [],
        "figure_captions": [],
        "table_captions": [],
    }

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


def run_pmc_worker(
    *,
    pipeline_id: str,
    pools_root_default: str,
    log_dir_default: str,
    output_subdir: str = "pmc_oa_fulltext_chunks",
    version: str = TOOL_VERSION,
    args: list[str] | None = None,
    configure_parser: Callable[[argparse.ArgumentParser], None] | None = None,
    log_path_builder: Callable[[Path], Path] | None = None,
    output_paths_builder: Callable[[argparse.Namespace, Path], tuple[Path, Path | None]]
    | None = None,
    chunk_defaults_loader: Callable[[Path], dict[str, Any]] = chunk_defaults_from_targets_yaml,
    extract_article_text_fn: Callable[
        [bytes, bool, bool, bool, bool], dict[str, Any]
    ] = extract_article_text,
    extract_nxml_fn: Callable[[bytes], tuple[bytes | None, list[str]]] = extract_nxml,
    include_pools_root_arg: bool = True,
) -> None:
    ap = argparse.ArgumentParser(description=f"PMC Worker v{version} ({pipeline_id})")
    ap.add_argument("--targets", required=True)
    ap.add_argument("--allowlist", required=True)
    if include_pools_root_arg:
        ap.add_argument("--pools-root", default=pools_root_default)
    ap.add_argument("--execute", action="store_true")
    ap.add_argument("--limit-records", type=int, default=None)
    ap.add_argument("--max-downloads-per-run", type=int, default=None)
    ap.add_argument("--max-bytes-per-record", type=int, default=200_000_000)
    ap.add_argument("--shard-rows", type=int, default=5000)
    ap.add_argument("--emit-train-split", type=float, default=None)
    ap.add_argument("--enable-cache", action="store_true")
    ap.add_argument("--log-dir", default=log_dir_default)
    add_logging_args(ap)
    if configure_parser:
        configure_parser(ap)
    parsed = ap.parse_args(args=args)
    configure_logging(level=parsed.log_level, fmt=parsed.log_format)

    targets_path = Path(parsed.targets).expanduser().resolve()
    chunk_cfg = chunk_defaults_loader(targets_path)

    rows = read_jsonl(Path(parsed.allowlist).expanduser().resolve())
    if parsed.limit_records:
        rows = rows[: parsed.limit_records]

    if output_paths_builder is None:
        if not include_pools_root_arg:
            raise OutputPathsBuilderError(
                "output_paths_builder is required when --pools-root is disabled.",
                context={"include_pools_root_arg": False},
            )
        pools_root = Path(parsed.pools_root).expanduser().resolve()

        def output_paths_builder(
            parsed_args: argparse.Namespace, target_path: Path
        ) -> tuple[Path, Path | None]:
            pools = pools_from_targets_yaml(target_path, pools_root)
            out_root = pools.permissive / output_subdir
            cache_dir = (
                pools.quarantine / "pmc_oa_fulltext" / "_cache"
                if parsed_args.enable_cache
                else None
            )
            return out_root, cache_dir

    out_root, cache_dir = output_paths_builder(parsed, targets_path)
    shards_root = out_root / "shards"
    manifests_dir = out_root / "_manifests"
    ensure_dir(shards_root)
    ensure_dir(manifests_dir)

    if cache_dir:
        ensure_dir(cache_dir)

    train_dir = shards_root / ("train" if parsed.emit_train_split else "")
    valid_dir = shards_root / "valid" if parsed.emit_train_split else None
    ensure_dir(train_dir)
    if valid_dir:
        ensure_dir(valid_dir)

    resume_path = manifests_dir / "resume_state.json"
    state = json.loads(resume_path.read_text()) if resume_path.exists() else {"processed": []}
    processed = set(state.get("processed", []))

    log_dir = Path(parsed.log_dir).expanduser().resolve()
    ensure_dir(log_dir)
    log_path = log_path_builder(log_dir) if log_path_builder else log_dir / "pmc_worker_log.jsonl"

    train_idx, valid_idx = 0, 0
    train_buf: list[dict] = []
    valid_buf: list[dict] = []
    total_train, total_valid = 0, 0
    successful = 0
    shard_files: dict[str, list] = {"train": [], "valid": []}

    def flush(split: str):
        nonlocal train_idx, valid_idx, train_buf, valid_buf, total_train, total_valid
        if split == "train" and train_buf:
            suffix = f"train_{train_idx:05d}" if parsed.emit_train_split else f"{train_idx:05d}"
            path = train_dir / f"pmc_chunks_{suffix}.jsonl.gz"
            with gzip.open(path, "wt", encoding="utf-8") as f:
                for r in train_buf:
                    f.write(json.dumps(r) + "\n")
            shard_files["train"].append(
                {"path": str(path), "rows": len(train_buf), "sha256": sha256_file(path) or ""}
            )
            total_train += len(train_buf)
            train_buf = []
            train_idx += 1
        elif split == "valid" and valid_buf and valid_dir:
            path = valid_dir / f"pmc_chunks_valid_{valid_idx:05d}.jsonl.gz"
            with gzip.open(path, "wt", encoding="utf-8") as f:
                for r in valid_buf:
                    f.write(json.dumps(r) + "\n")
            shard_files["valid"].append(
                {"path": str(path), "rows": len(valid_buf), "sha256": sha256_file(path) or ""}
            )
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

        if not parsed.execute:
            append_jsonl(log_path, {**event, "status": "planned"})
            processed.add(key)
            state["processed"] = sorted(processed)
            write_json(resume_path, state)
            continue

        pkg, meta = fetch_pmc_package(
            file_ref,
            parsed.max_bytes_per_record,
            cache_dir,
            user_agent_version=version,
        )
        if pkg is None:
            append_jsonl(log_path, {**event, "status": meta.get("status", "error"), "meta": meta})
            processed.add(key)
            state["processed"] = sorted(processed)
            write_json(resume_path, state)
            continue

        nxml, members = extract_nxml_fn(pkg)
        if nxml is None:
            append_jsonl(log_path, {**event, "status": "no_nxml", "members": members[:20]})
            processed.add(key)
            state["processed"] = sorted(processed)
            write_json(resume_path, state)
            continue

        ext = extract_article_text_fn(
            nxml,
            chunk_cfg["drop_refs"],
            chunk_cfg["include_section_headers"],
            chunk_cfg["include_figure_captions"],
            chunk_cfg["include_table_captions"],
        )

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

        base = {
            "source": "pmc_oa",
            "pmcid": pmcid,
            "doi": ext.get("doi", ""),
            "pmid": ext.get("pmid", ""),
            "license_spdx": spdx,
            "license_text": lic_text,
            "file_ref": file_ref,
            "sections": ext.get("sections", []),
            "extracted_at_utc": utc_now(),
        }

        for i, c in enumerate(chunks):
            rec = {**base, "chunk_id": i, "text": c}
            if parsed.emit_train_split:
                u = stable_unit_interval(f"{pmcid}|{file_ref}|{i}")
                (train_buf if u < parsed.emit_train_split else valid_buf).append(rec)
            else:
                train_buf.append(rec)
            if len(train_buf) >= parsed.shard_rows:
                flush("train")
            if parsed.emit_train_split and len(valid_buf) >= parsed.shard_rows:
                flush("valid")

        successful += 1
        append_jsonl(
            log_path,
            {**event, "status": "ok", "chunks": len(chunks), "cached": meta.get("cached", False)},
        )
        processed.add(key)
        state["processed"] = sorted(processed)
        write_json(resume_path, state)

        if parsed.max_downloads_per_run and successful >= parsed.max_downloads_per_run:
            break

    flush("train")
    if parsed.emit_train_split:
        flush("valid")

    index = {
        "created_at_utc": utc_now(),
        "records_seen": len(rows),
        "successful": successful,
        "train_rows": total_train,
        "valid_rows": total_valid,
        "train_shards": shard_files["train"],
        "valid_shards": shard_files["valid"],
    }
    index.update(
        build_artifact_metadata(pipeline_version=version, written_at_utc=index["created_at_utc"])
    )
    write_json(out_root / "dataset_index.json", index)
    write_json(manifests_dir / f"pmc_run_{int(time.time())}.json", index)

    logger.info("%s", "=" * 50)
    logger.info("PMC Worker v%s", version)
    logger.info("Mode: %s", "EXECUTE" if parsed.execute else "DRY-RUN")
    logger.info("Downloads: %s, Train: %s, Valid: %s", successful, total_train, total_valid)
    logger.info("%s", "=" * 50)
    logger.info(json.dumps(index, indent=2))
