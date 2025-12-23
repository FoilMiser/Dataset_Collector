#!/usr/bin/env python3
"""
yellow_screen_worker.py (v2.0)

Converts raw YELLOW acquisitions into canonical JSONL shards with strict pitch
behavior. Outputs:
  - screened_yellow/{license_pool}/shards/yellow_shard_00000.jsonl.gz
  - _ledger/yellow_passed.jsonl (accepted rows)
  - _ledger/yellow_pitched.jsonl (pitched rows)
  - _manifests/{target_id}/yellow_screen_done.json
"""

from __future__ import annotations

import argparse
import dataclasses
import gzip
import hashlib
import json
import re
import time
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional

import yaml

VERSION = "2.0"


def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def sha256_text(text: str) -> str:
    norm = re.sub(r"\s+", " ", (text or "").strip())
    return hashlib.sha256(norm.encode("utf-8")).hexdigest()


def read_jsonl(path: Path) -> Iterator[Dict[str, Any]]:
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    continue


def read_json(path: Path) -> List[Dict[str, Any]]:
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8", errors="ignore") as f:
        data = json.load(f)
    if isinstance(data, list):
        return [row for row in data if isinstance(row, dict)]
    if isinstance(data, dict):
        if isinstance(data.get("data"), list):
            return [row for row in data["data"] if isinstance(row, dict)]
        return [data]
    return []


def write_json(path: Path, obj: Dict[str, Any]) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def write_jsonl(path: Path, rows: Iterable[Dict[str, Any]]) -> None:
    ensure_dir(path.parent)
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "wt", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def append_jsonl(path: Path, rows: Iterable[Dict[str, Any]]) -> None:
    ensure_dir(path.parent)
    opener = gzip.open if path.suffix == ".gz" else open
    mode = "at" if path.suffix != ".gz" else "ab"
    if path.suffix == ".gz":
        with gzip.open(path, mode) as f:  # type: ignore
            for row in rows:
                f.write((json.dumps(row, ensure_ascii=False) + "\n").encode("utf-8"))
    else:
        with open(path, mode, encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")


@dataclasses.dataclass
class Roots:
    raw_root: Path
    screened_root: Path
    manifests_root: Path
    ledger_root: Path
    pitches_root: Path


@dataclasses.dataclass
class ScreeningConfig:
    text_fields: List[str]
    license_fields: List[str]
    allow_spdx: List[str]
    deny_phrases: List[str]
    require_record_license: bool
    min_chars: int
    max_chars: int


@dataclasses.dataclass
class TextProcessingConfig:
    max_chars: int
    min_chars: int
    normalize_whitespace: bool
    force_language: Optional[str]


@dataclasses.dataclass
class ShardingConfig:
    max_records_per_shard: int
    compression: str
    prefix: str


class Sharder:
    def __init__(self, base_dir: Path, cfg: ShardingConfig):
        self.base_dir = base_dir
        self.cfg = cfg
        self.count = 0
        self.shard_idx = 0
        self.current_rows: List[Dict[str, Any]] = []

    def _next_path(self) -> Path:
        suffix = "jsonl.gz" if self.cfg.compression == "gzip" else "jsonl"
        name = f"{self.cfg.prefix}_{self.shard_idx:05d}.{suffix}"
        return self.base_dir / name

    def add(self, row: Dict[str, Any]) -> Optional[Path]:
        self.current_rows.append(row)
        self.count += 1
        if len(self.current_rows) >= self.cfg.max_records_per_shard:
            path = self.flush()
            self.shard_idx += 1
            return path
        return None

    def flush(self) -> Optional[Path]:
        if not self.current_rows:
            return None
        path = self._next_path()
        write_jsonl(path, self.current_rows)
        self.current_rows = []
        return path


def load_targets_cfg(path: Path) -> Dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def resolve_roots(cfg: Dict[str, Any]) -> Roots:
    g = (cfg.get("globals", {}) or {})
    return Roots(
        raw_root=Path(g.get("raw_root", "/data/nlp/raw")),
        screened_root=Path(g.get("screened_yellow_root", "/data/nlp/screened_yellow")),
        manifests_root=Path(g.get("manifests_root", "/data/nlp/_manifests")),
        ledger_root=Path(g.get("ledger_root", "/data/nlp/_ledger")),
        pitches_root=Path(g.get("pitches_root", "/data/nlp/_pitches")),
    )


def merge_screening_config(cfg: Dict[str, Any], target: Dict[str, Any]) -> ScreeningConfig:
    g_screen = (cfg.get("globals", {}).get("screening", {}) or {})
    t_screen = (target.get("yellow_screen", {}) or {})
    return ScreeningConfig(
        text_fields=list(t_screen.get("text_field_candidates") or g_screen.get("text_field_candidates") or ["text"]),
        license_fields=list(t_screen.get("record_license_field_candidates") or g_screen.get("record_license_field_candidates") or ["license", "license_spdx"]),
        allow_spdx=list(t_screen.get("allow_spdx") or g_screen.get("allow_spdx") or []),
        deny_phrases=[p.lower() for p in (t_screen.get("deny_phrases") or g_screen.get("deny_phrases") or [])],
        require_record_license=bool(t_screen.get("require_record_license", g_screen.get("require_record_license", False))),
        min_chars=int(t_screen.get("min_chars", g_screen.get("min_chars", 300))),
        max_chars=int(t_screen.get("max_chars", g_screen.get("max_chars", 12000))),
    )


def merge_text_processing(cfg: Dict[str, Any], target: Dict[str, Any]) -> TextProcessingConfig:
    g_text = (cfg.get("globals", {}).get("text_processing_defaults", {}) or {})
    t_text = (target.get("yellow_screen", {}) or {}).get("text_processing", {})
    max_chars = int(t_text.get("max_chunk_chars", g_text.get("max_chunk_chars", 6000)))
    min_chars = int(t_text.get("min_chunk_chars", g_text.get("min_chunk_chars", 500)))
    normalize = bool(t_text.get("normalize_whitespace", g_text.get("normalize_whitespace", True)))
    force_language = t_text.get("force_language", g_text.get("force_language"))
    return TextProcessingConfig(max_chars=max_chars, min_chars=min_chars, normalize_whitespace=normalize, force_language=force_language)


def sharding_cfg(cfg: Dict[str, Any], prefix: str) -> ShardingConfig:
    g = (cfg.get("globals", {}).get("sharding", {}) or {})
    return ShardingConfig(
        max_records_per_shard=int(g.get("max_records_per_shard", 50000)),
        compression=str(g.get("compression", "gzip")),
        prefix=prefix,
    )


def find_text(row: Dict[str, Any], candidates: List[str]) -> Optional[str]:
    for k in candidates:
        if k in row and row[k]:
            val = row[k]
            if isinstance(val, (list, tuple)):
                val = "\n".join(map(str, val))
            return str(val)
    return None


def find_license(row: Dict[str, Any], candidates: List[str]) -> Optional[str]:
    for k in candidates:
        if k in row and row[k]:
            return str(row[k])
    return None


def contains_deny(text: str, phrases: List[str]) -> bool:
    low = text.lower()
    return any(p in low for p in phrases)


def normalize_text(text: str, normalize: bool) -> str:
    return re.sub(r"\s+", " ", text).strip() if normalize else text.strip()


def chunk_text(text: str, max_chars: int, min_chars: int) -> List[str]:
    if not text.strip():
        return []
    paragraphs = [p.strip() for p in re.split(r"\n{2,}", text) if p.strip()]
    chunks: List[str] = []
    buf: List[str] = []
    buf_len = 0
    for p in paragraphs:
        if buf_len + len(p) + 2 <= max_chars:
            buf.append(p)
            buf_len += len(p) + 2
        else:
            if buf:
                chunks.append("\n\n".join(buf))
            if len(p) > max_chars:
                for idx in range(0, len(p), max_chars):
                    chunks.append(p[idx : idx + max_chars])
                buf = []
                buf_len = 0
            else:
                buf = [p]
                buf_len = len(p)
    if buf:
        chunks.append("\n\n".join(buf))
    return [c for c in chunks if len(c) >= min_chars]


def detect_language_match(raw: Dict[str, Any], force_language: Optional[str]) -> bool:
    if not force_language:
        return True
    for key in ["lang", "language", "lang_code"]:
        if key in raw and raw[key]:
            return str(raw[key]).lower().startswith(force_language.lower())
    return True


def canonical_record(
    raw: Dict[str, Any],
    text: str,
    target_id: str,
    license_profile: str,
    license_spdx: Optional[str],
    routing: Dict[str, Any],
    source_file: Optional[str],
) -> Dict[str, Any]:
    record_id = str(raw.get("record_id") or raw.get("id") or sha256_text(f"{target_id}:{text}"))
    content_hash = sha256_text(text)
    source = raw.get("source", {}) or {}
    return {
        "record_id": record_id,
        "text": text,
        "source": {
            "target_id": source.get("target_id", target_id),
            "file": source.get("file", source_file),
            "url": source.get("url", raw.get("source_url") or raw.get("url")),
            "retrieved_at_utc": source.get("retrieved_at_utc", raw.get("retrieved_at_utc")),
            "license_evidence": source.get("license_evidence", raw.get("license_evidence")),
            "license_spdx": license_spdx,
            "license_profile": license_profile,
        },
        "license": {"spdx": license_spdx, "profile": license_profile},
        "routing": routing,
        "hash": {"content_sha256": content_hash},
    }


def iter_raw_files(raw_dir: Path) -> Iterator[Path]:
    patterns = ["*.jsonl", "*.jsonl.gz", "*.json", "*.json.gz", "*.txt", "*.txt.gz"]
    for pattern in patterns:
        for fp in raw_dir.glob(pattern):
            if fp.is_file():
                yield fp


def extract_records_from_file(file_path: Path) -> Iterator[Dict[str, Any]]:
    suffix = "".join(file_path.suffixes)
    if suffix.endswith(".jsonl") or suffix.endswith(".jsonl.gz"):
        yield from read_jsonl(file_path)
        return
    if suffix.endswith(".json") or suffix.endswith(".json.gz"):
        for row in read_json(file_path):
            yield row
        return


def extract_text_from_file(file_path: Path) -> Optional[str]:
    opener = gzip.open if file_path.suffix == ".gz" else open
    with opener(file_path, "rt", encoding="utf-8", errors="ignore") as f:
        return f.read()


def process_target(cfg: Dict[str, Any], roots: Roots, queue_row: Dict[str, Any], execute: bool) -> Dict[str, Any]:
    target_id = queue_row["id"]
    target_cfg = next((t for t in cfg.get("targets", []) if t.get("id") == target_id), {})
    screen_cfg = merge_screening_config(cfg, target_cfg)
    text_cfg = merge_text_processing(cfg, target_cfg)
    shard_cfg = sharding_cfg(cfg, "yellow_shard")
    pool_dir_base = roots.raw_root / "yellow"
    license_pools = [p.name for p in pool_dir_base.iterdir() if p.is_dir()] if pool_dir_base.exists() else []
    pools = license_pools or [queue_row.get("license_profile", "quarantine")]

    passed, pitched = 0, 0
    shard_paths: List[str] = []

    base_routing = (
        queue_row.get("routing")
        or target_cfg.get("routing")
        or target_cfg.get("nlp_routing")
        or {}
    )

    for pool in pools:
        raw_dir = pool_dir_base / pool / target_id
        if not raw_dir.exists():
            continue
        sharder = Sharder(roots.screened_root / pool / "shards", shard_cfg)
        for file_path in iter_raw_files(raw_dir):
            source_file = str(file_path)
            file_suffix = "".join(file_path.suffixes)
            if file_suffix.endswith(".txt") or file_suffix.endswith(".txt.gz"):
                raw_text = extract_text_from_file(file_path)
                if raw_text is None:
                    pitched += 1
                    if execute:
                        append_jsonl(roots.ledger_root / "yellow_pitched.jsonl", [{"target_id": target_id, "reason": "unreadable_text", "file": source_file}])
                    continue
                normalized = normalize_text(raw_text, text_cfg.normalize_whitespace)
                chunks = chunk_text(normalized, text_cfg.max_chars, text_cfg.min_chars)
                if not chunks:
                    pitched += 1
                    if execute:
                        append_jsonl(roots.ledger_root / "yellow_pitched.jsonl", [{"target_id": target_id, "reason": "no_text", "file": source_file}])
                    continue
                for chunk in chunks:
                    if contains_deny(chunk, screen_cfg.deny_phrases):
                        pitched += 1
                        if execute:
                            append_jsonl(roots.ledger_root / "yellow_pitched.jsonl", [{"target_id": target_id, "reason": "deny_phrase", "file": source_file}])
                        continue
                    if len(chunk) < screen_cfg.min_chars or len(chunk) > screen_cfg.max_chars:
                        pitched += 1
                        if execute:
                            append_jsonl(roots.ledger_root / "yellow_pitched.jsonl", [{"target_id": target_id, "reason": "length_bounds", "file": source_file}])
                        continue
                    license_profile = str(queue_row.get("license_profile") or pool or "quarantine")
                    rec = canonical_record({}, chunk, target_id, license_profile, None, base_routing, source_file)
                    passed += 1
                    if execute:
                        current_shard = str(sharder._next_path())
                        path = sharder.add(rec)
                        if path:
                            current_shard = str(path)
                            shard_paths.append(current_shard)
                        ledger_row = {
                            "stage": "yellow_screen",
                            "target_id": target_id,
                            "record_id": rec["record_id"],
                            "content_sha256": rec["hash"]["content_sha256"],
                            "decision": "pass",
                            "output_shard": current_shard,
                            "seen_at_utc": utc_now(),
                        }
                        append_jsonl(roots.ledger_root / "yellow_passed.jsonl", [ledger_row])
                continue

            for raw in extract_records_from_file(file_path):
                text = find_text(raw, screen_cfg.text_fields)
                if text:
                    text = normalize_text(text, text_cfg.normalize_whitespace)
                if not text:
                    pitched += 1
                    if execute:
                        append_jsonl(roots.ledger_root / "yellow_pitched.jsonl", [{"target_id": target_id, "reason": "no_text", "sample": raw}])
                    continue
                if len(text) < screen_cfg.min_chars or len(text) > screen_cfg.max_chars:
                    pitched += 1
                    if execute:
                        append_jsonl(roots.ledger_root / "yellow_pitched.jsonl", [{"target_id": target_id, "reason": "length_bounds", "sample_id": raw.get("id")}])
                    continue
                if not detect_language_match(raw, text_cfg.force_language):
                    pitched += 1
                    if execute:
                        append_jsonl(roots.ledger_root / "yellow_pitched.jsonl", [{"target_id": target_id, "reason": "language_mismatch", "sample_id": raw.get("id")}])
                    continue
                lic = find_license(raw, screen_cfg.license_fields)
                if screen_cfg.require_record_license and not lic:
                    pitched += 1
                    if execute:
                        append_jsonl(roots.ledger_root / "yellow_pitched.jsonl", [{"target_id": target_id, "reason": "missing_record_license", "sample_id": raw.get("id")}])
                    continue
                if lic and screen_cfg.allow_spdx and lic not in screen_cfg.allow_spdx:
                    pitched += 1
                    if execute:
                        append_jsonl(roots.ledger_root / "yellow_pitched.jsonl", [{"target_id": target_id, "reason": "license_not_allowlisted", "license": lic, "sample_id": raw.get("id")}])
                    continue
                if contains_deny(text, screen_cfg.deny_phrases):
                    pitched += 1
                    if execute:
                        append_jsonl(roots.ledger_root / "yellow_pitched.jsonl", [{"target_id": target_id, "reason": "deny_phrase", "sample_id": raw.get("id")}])
                    continue
                license_profile = str(raw.get("license_profile") or queue_row.get("license_profile") or pool or "quarantine")
                routing = raw.get("routing") or raw.get("nlp_routing") or raw.get("route") or base_routing
                rec = canonical_record(raw, text, target_id, license_profile, lic, routing, source_file)
                passed += 1
                if execute:
                    current_shard = str(sharder._next_path())
                    path = sharder.add(rec)
                    if path:
                        current_shard = str(path)
                        shard_paths.append(current_shard)
                    ledger_row = {
                        "stage": "yellow_screen",
                        "target_id": target_id,
                        "record_id": rec["record_id"],
                        "content_sha256": rec["hash"]["content_sha256"],
                        "decision": "pass",
                        "output_shard": current_shard,
                        "seen_at_utc": utc_now(),
                    }
                    append_jsonl(roots.ledger_root / "yellow_passed.jsonl", [ledger_row])

        if execute:
            flushed = sharder.flush()
            if flushed:
                shard_paths.append(str(flushed))

    manifest = {
        "target_id": target_id,
        "passed": passed,
        "pitched": pitched,
        "shards": shard_paths,
        "status": "ok",
        "finished_at_utc": utc_now(),
    }
    if execute:
        ensure_dir((roots.manifests_root / target_id))
        write_json(roots.manifests_root / target_id / "yellow_screen_done.json", manifest)
    return manifest


def main() -> None:
    ap = argparse.ArgumentParser(description=f"Yellow Screen Worker v{VERSION}")
    ap.add_argument("--targets", required=True, help="Path to targets_nlp.yaml")
    ap.add_argument("--queue", required=True, help="YELLOW queue JSONL")
    ap.add_argument("--execute", action="store_true", help="Write outputs (default: dry-run)")
    args = ap.parse_args()

    targets_path = Path(args.targets).expanduser().resolve()
    cfg = load_targets_cfg(targets_path)
    roots = resolve_roots(cfg)
    ensure_dir(roots.screened_root)
    ensure_dir(roots.ledger_root)
    ensure_dir(roots.pitches_root)

    queue_rows = read_jsonl(Path(args.queue))
    queue_rows = [r for r in queue_rows if r.get("enabled", True) and r.get("id")]

    summary = {
        "run_at_utc": utc_now(),
        "pipeline_version": VERSION,
        "targets_seen": len(queue_rows),
        "execute": args.execute,
        "results": [],
    }

    for row in queue_rows:
        res = process_target(cfg, roots, row, args.execute)
        summary["results"].append(res)

    write_json(roots.ledger_root / "yellow_screen_summary.json", summary)


if __name__ == "__main__":
    main()
