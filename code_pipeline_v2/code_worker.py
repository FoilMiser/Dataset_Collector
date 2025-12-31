#!/usr/bin/env python3
"""
code_worker.py (v0.2)

Lightweight code extraction worker for the v2 code pipeline. Converts raw
downloaded repositories or datasets into canonical JSONL shards with:
  - vendored/build/minified/binary filtering
  - simple language detection (extension + shebang)
  - secrets scanning (pitch for YELLOW, redact for GREEN)
  - line-window chunking with provenance + hashes

Outputs live under the v2 raw layout:
  raw/{green|yellow}/{license_pool}/{target_id}/shards/code_00000.jsonl.gz

Not legal advice.
"""

from __future__ import annotations

import argparse
import dataclasses
import fnmatch
import gzip
import hashlib
import json
import re
import time
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

import yaml

VERSION = "0.2"


def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def sha256_text(text: str) -> str:
    norm = re.sub(r"\s+", " ", (text or "").strip())
    return hashlib.sha256(norm.encode("utf-8")).hexdigest()


def read_yaml(path: Path) -> Dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def load_targets_cfg(path: Optional[Path]) -> Dict[str, Any]:
    if not path:
        return {}
    if not path.exists():
        raise FileNotFoundError(f"targets file not found: {path}")
    return read_yaml(path)


@dataclasses.dataclass
class ShardingConfig:
    max_records_per_shard: int
    compression: str
    prefix: str = "code"


@dataclasses.dataclass
class CodeProcessingConfig:
    max_file_bytes: int
    min_file_bytes: int
    max_chunk_chars: int
    min_chunk_chars: int
    include_comments: bool
    strip_trailing_whitespace: bool
    normalize_newlines: bool
    languages_allowlist: List[str]
    path_deny_globs: List[str]


@dataclasses.dataclass
class WorkerContext:
    target_id: str
    bucket: str
    license_profile: str
    license_spdx: Optional[str]
    routing: Dict[str, Any]
    source_url: Optional[str]
    processing: CodeProcessingConfig
    sharding: ShardingConfig
    input_dir: Path
    output_dir: Path
    pitches_root: Optional[Path] = None


class Sharder:
    def __init__(self, cfg: ShardingConfig, base_dir: Path):
        self.cfg = cfg
        self.base_dir = base_dir
        self.rows: List[Dict[str, Any]] = []
        self.idx = 0

    def _path(self) -> Path:
        suffix = "jsonl.gz" if self.cfg.compression == "gzip" else "jsonl"
        return self.base_dir / f"{self.cfg.prefix}_{self.idx:05d}.{suffix}"

    def add(self, row: Dict[str, Any]) -> Optional[Path]:
        self.rows.append(row)
        if len(self.rows) >= self.cfg.max_records_per_shard:
            path = self.flush()
            self.idx += 1
            return path
        return None

    def flush(self) -> Optional[Path]:
        if not self.rows:
            return None
        path = self._path()
        ensure_dir(path.parent)
        opener = gzip.open if path.suffix.endswith("gz") else open
        mode = "ab" if path.suffix.endswith("gz") else "at"
        if path.suffix.endswith("gz"):
            with opener(path, mode) as f:  # type: ignore
                for row in self.rows:
                    f.write((json.dumps(row, ensure_ascii=False) + "\n").encode("utf-8"))
        else:
            with opener(path, mode, encoding="utf-8") as f:  # type: ignore
                for row in self.rows:
                    f.write(json.dumps(row, ensure_ascii=False) + "\n")
        self.rows = []
        return path


SECRET_PATTERNS: List[Tuple[str, re.Pattern[str]]] = [
    ("aws_access_key", re.compile(r"AKIA[0-9A-Z]{16}")),
    ("aws_secret_key", re.compile(r"(?i)aws(.{0,20})?(secret|access)_?key")),
    ("github_token", re.compile(r"gh[pousr]_[A-Za-z0-9_]{30,}")),
    ("jwt", re.compile(r"eyJ[a-zA-Z0-9_-]{10,}\.[a-zA-Z0-9._-]{10,}\.[a-zA-Z0-9._-]{10,}")),
    ("pem_block", re.compile(r"-----BEGIN [A-Z ]+PRIVATE KEY-----")),
]

LANG_BY_EXT = {
    ".py": "python",
    ".js": "javascript",
    ".ts": "typescript",
    ".java": "java",
    ".go": "go",
    ".rs": "rust",
    ".c": "c",
    ".cpp": "cpp",
    ".cc": "cpp",
    ".h": "cpp",
    ".hpp": "cpp",
    ".cs": "csharp",
    ".php": "php",
    ".rb": "ruby",
    ".kt": "kotlin",
    ".swift": "swift",
    ".scala": "scala",
    ".sql": "sql",
    ".sh": "bash",
    ".bash": "bash",
    ".ps1": "powershell",
    ".html": "html",
    ".css": "css",
}


def normalize_newlines(text: str) -> str:
    return text.replace("\r\n", "\n").replace("\r", "\n")


def should_skip(path: Path, deny_globs: List[str]) -> bool:
    return any(fnmatch.fnmatch(str(path), pattern) for pattern in deny_globs)


def detect_language(path: Path, text: str) -> Optional[str]:
    ext = path.suffix.lower()
    if ext in LANG_BY_EXT:
        return LANG_BY_EXT[ext]
    if text.startswith("#!") and "python" in text[:80]:
        return "python"
    if text.startswith("#!") and ("bash" in text[:80] or "sh" in text[:80]):
        return "bash"
    return None


def scan_secrets(text: str) -> Tuple[str, List[str]]:
    hits: List[str] = []
    redacted = text
    for name, pat in SECRET_PATTERNS:
        for match in pat.finditer(redacted):
            hits.append(name)
            start, end = match.span()
            redacted = redacted[:start] + "<REDACTED_SECRET>" + redacted[end:]
    return redacted, hits


def iter_code_files(root: Path) -> Iterator[Path]:
    for fp in root.rglob("*"):
        if fp.is_file():
            yield fp


def chunk_code(text: str, min_chars: int, max_chars: int) -> List[Tuple[int, int, str]]:
    lines = text.splitlines()
    chunks: List[Tuple[int, int, str]] = []
    start_idx = 0
    buf: List[str] = []
    char_count = 0

    for idx, line in enumerate(lines, start=1):
        buf.append(line)
        char_count += len(line) + 1
        if char_count >= max_chars:
            chunk = "\n".join(buf)
            if len(chunk) >= min_chars:
                chunks.append((start_idx or 1, idx, chunk))
            start_idx = idx + 1
            buf, char_count = [], 0

    if buf:
        chunk = "\n".join(buf)
        if len(chunk) >= min_chars:
            chunks.append((start_idx or 1, start_idx + len(buf) - 1, chunk))
    return chunks


def canonical_record(
    ctx: WorkerContext,
    rel_path: Path,
    language: str,
    chunk: str,
    start_line: int,
    end_line: int,
    secrets_redacted: bool,
) -> Dict[str, Any]:
    record_id = sha256_text(f"{ctx.target_id}:{rel_path}:{start_line}:{end_line}")
    content_hash = sha256_text(chunk)
    source_block = {
        "target_id": ctx.target_id,
        "source_path": str(rel_path),
        "language": language,
        "license_profile": ctx.license_profile,
        "license_spdx": ctx.license_spdx,
        "source_url": ctx.source_url,
        "bucket": ctx.bucket,
        "retrieved_at_utc": utc_now(),
    }
    return {
        "record_id": record_id,
        "text": chunk,
        "code": chunk,
        "hash": {"content_sha256": content_hash},
        "source": source_block,
        "routing": ctx.routing,
        "code_metadata": {
            "path": str(rel_path),
            "language": language,
            "start_line": start_line,
            "end_line": end_line,
            "secrets_redacted": secrets_redacted,
        },
    }


def extract_codebase(ctx: WorkerContext) -> Dict[str, Any]:
    ensure_dir(ctx.output_dir)
    sharder = Sharder(ctx.sharding, ctx.output_dir)
    pitched: List[Dict[str, Any]] = []
    emitted = 0
    files_seen = 0
    for fp in iter_code_files(ctx.input_dir):
        if ctx.output_dir in fp.parents:
            continue
        rel = fp.relative_to(ctx.input_dir)
        if should_skip(rel, ctx.processing.path_deny_globs):
            continue
        try:
            size = fp.stat().st_size
        except OSError:
            continue
        if size < ctx.processing.min_file_bytes or size > ctx.processing.max_file_bytes:
            pitched.append({"path": str(rel), "reason": "size_bounds"})
            continue
        try:
            raw = fp.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            pitched.append({"path": str(rel), "reason": "decode_error"})
            continue
        if ctx.processing.normalize_newlines:
            raw = normalize_newlines(raw)
        if ctx.processing.strip_trailing_whitespace:
            raw = "\n".join([ln.rstrip() for ln in raw.splitlines()])

        lang = detect_language(rel, raw) or "unknown"
        if ctx.processing.languages_allowlist and lang not in ctx.processing.languages_allowlist and "multi" not in ctx.processing.languages_allowlist:
            pitched.append({"path": str(rel), "reason": "lang_not_allowlisted", "language": lang})
            continue

        redacted, secret_hits = scan_secrets(raw)
        if secret_hits and ctx.bucket.lower() == "yellow":
            pitched.append({"path": str(rel), "reason": "secrets_detected", "secrets": secret_hits})
            continue
        text = redacted
        chunks = chunk_code(text, ctx.processing.min_chunk_chars, ctx.processing.max_chunk_chars)
        if not chunks:
            pitched.append({"path": str(rel), "reason": "no_chunks"})
            continue
        files_seen += 1
        for start, end, chunk in chunks:
            rec = canonical_record(ctx, rel, lang, chunk, start, end, bool(secret_hits))
            if not ctx.sharding.max_records_per_shard:
                continue
            flushed = sharder.add(rec)
            emitted += 1
            if flushed:
                pass
    final_path = sharder.flush()
    shard_paths = [str(ctx.output_dir / p.name) for p in ctx.output_dir.glob(f"{ctx.sharding.prefix}_*.jsonl*")]
    if final_path and str(final_path) not in shard_paths:
        shard_paths.append(str(final_path))
    manifest = {
        "target_id": ctx.target_id,
        "bucket": ctx.bucket,
        "license_profile": ctx.license_profile,
        "license_spdx": ctx.license_spdx,
        "source_url": ctx.source_url,
        "routing": ctx.routing,
        "input_dir": str(ctx.input_dir),
        "output_dir": str(ctx.output_dir),
        "files_seen": files_seen,
        "records_emitted": emitted,
        "pitches": pitched,
        "shards": shard_paths,
        "written_at_utc": utc_now(),
        "version": VERSION,
    }
    manifest_path = ctx.output_dir.parent / "code_worker_manifest.json"
    ensure_dir(manifest_path.parent)
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    if ctx.pitches_root and pitched:
        ensure_dir(ctx.pitches_root)
        with (ctx.pitches_root / "code_worker_pitches.jsonl").open("a", encoding="utf-8") as f:
            for row in pitched:
                f.write(json.dumps({"target_id": ctx.target_id, **row}) + "\n")
    return manifest


def sharding_from_cfg(cfg: Dict[str, Any]) -> ShardingConfig:
    g = (cfg.get("globals", {}).get("sharding", {}) or {})
    return ShardingConfig(
        max_records_per_shard=int(g.get("max_records_per_shard", 50000)),
        compression=str(g.get("compression", "gzip")),
        prefix="code",
    )


def processing_from_cfg(cfg: Dict[str, Any], target: Dict[str, Any]) -> CodeProcessingConfig:
    g = (cfg.get("globals", {}).get("code_processing_defaults", {}) or {})
    t = (target.get("code_processing") or {})
    deny_globs = list(t.get("path_deny_globs") or g.get("path_deny_globs") or [])
    return CodeProcessingConfig(
        max_file_bytes=int(t.get("max_file_bytes", g.get("max_file_bytes", 250000))),
        min_file_bytes=int(t.get("min_file_bytes", g.get("min_file_bytes", 64))),
        max_chunk_chars=int(t.get("max_chunk_chars", g.get("max_chunk_chars", 12000))),
        min_chunk_chars=int(t.get("min_chunk_chars", g.get("min_chunk_chars", 200))),
        include_comments=bool(t.get("include_comments", g.get("include_comments", True))),
        strip_trailing_whitespace=bool(t.get("strip_trailing_whitespace", g.get("strip_trailing_whitespace", True))),
        normalize_newlines=bool(t.get("normalize_newlines", g.get("normalize_newlines", True))),
        languages_allowlist=list(t.get("languages_allowlist") or g.get("languages_allowlist") or []),
        path_deny_globs=deny_globs,
    )


def resolve_target(cfg: Dict[str, Any], target_id: str) -> Dict[str, Any]:
    for t in cfg.get("targets", []) or []:
        if t.get("id") == target_id:
            return t
    raise KeyError(f"target not found: {target_id}")


def run_extraction(
    *,
    input_dir: Path,
    target_id: str,
    license_profile: str,
    bucket: str,
    routing: Optional[Dict[str, Any]] = None,
    processing_defaults: Optional[Dict[str, Any]] = None,
    sharding: Optional[Dict[str, Any]] = None,
    source_url: Optional[str] = None,
    license_spdx: Optional[str] = None,
    output_dir: Optional[Path] = None,
    pitches_root: Optional[Path] = None,
) -> Dict[str, Any]:
    cfg_wrapper = {"globals": {"code_processing_defaults": processing_defaults or {}, "sharding": sharding or {}}}
    processing_cfg = processing_from_cfg(cfg_wrapper, {})
    sharding_cfg = ShardingConfig(
        max_records_per_shard=int((sharding or {}).get("max_records_per_shard", 50000)),
        compression=str((sharding or {}).get("compression", "gzip")),
        prefix="code",
    )
    ctx = WorkerContext(
        target_id=target_id,
        bucket=bucket,
        license_profile=license_profile,
        license_spdx=license_spdx,
        routing=routing or {},
        source_url=source_url,
        processing=processing_cfg,
        sharding=sharding_cfg,
        input_dir=input_dir,
        output_dir=output_dir or (input_dir / "shards"),
        pitches_root=pitches_root,
    )
    return extract_codebase(ctx)


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=f"Code Worker v{VERSION}")
    ap.add_argument("--targets", required=False, help="Path to targets_code.yaml")
    ap.add_argument("--target-id", required=True, help="Target id to process")
    ap.add_argument("--input-dir", required=True, help="Path to downloaded payload directory")
    ap.add_argument("--bucket", default="green", choices=["green", "yellow"], help="Bucket context (secrets policy)")
    ap.add_argument("--license-profile", default=None, help="License profile for emitted records")
    ap.add_argument("--license-spdx", default=None, help="Resolved SPDX for emitted records")
    ap.add_argument("--output-dir", default=None, help="Override output dir (default: <input>/shards)")
    args = ap.parse_args(argv)

    targets_path = Path(args.targets).expanduser().resolve() if args.targets else None
    cfg = load_targets_cfg(targets_path)
    target_cfg: Dict[str, Any] = {}
    if cfg:
        target_cfg = resolve_target(cfg, args.target_id)
    routing = (target_cfg.get("routing") or target_cfg.get("code_routing") or target_cfg.get("math_routing") or {})
    processing_cfg = processing_from_cfg(cfg, target_cfg) if cfg else processing_from_cfg({}, {})
    shard_cfg = sharding_from_cfg(cfg) if cfg else ShardingConfig(max_records_per_shard=50000, compression="gzip")

    ctx = WorkerContext(
        target_id=args.target_id,
        bucket=args.bucket,
        license_profile=args.license_profile or target_cfg.get("license_profile", "quarantine"),
        license_spdx=args.license_spdx or target_cfg.get("license_evidence", {}).get("spdx_hint"),
        routing=routing,
        source_url=(target_cfg.get("download", {}) or {}).get("repo_url") or (target_cfg.get("download", {}) or {}).get("url"),
        processing=processing_cfg,
        sharding=shard_cfg,
        input_dir=Path(args.input_dir).expanduser().resolve(),
        output_dir=Path(args.output_dir).expanduser().resolve() if args.output_dir else Path(args.input_dir).expanduser().resolve() / "shards",
        pitches_root=Path(cfg.get("globals", {}).get("pitches_root")) if cfg else None,
    )

    manifest = extract_codebase(ctx)
    print(json.dumps(manifest, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
