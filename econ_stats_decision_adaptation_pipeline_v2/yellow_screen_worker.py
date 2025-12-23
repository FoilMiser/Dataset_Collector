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
import csv
import dataclasses
import gzip
import hashlib
import io
import json
import re
import time
import zipfile
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

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
    adapter: str
    inner_adapter: Optional[str]
    chunk_rows: int


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
        raw_root=Path(g.get("raw_root", "/data/econ/raw")),
        screened_root=Path(g.get("screened_yellow_root", "/data/econ/screened_yellow")),
        manifests_root=Path(g.get("manifests_root", "/data/econ/_manifests")),
        ledger_root=Path(g.get("ledger_root", "/data/econ/_ledger")),
        pitches_root=Path(g.get("pitches_root", "/data/econ/_pitches")),
    )


def merge_screening_config(cfg: Dict[str, Any], target: Dict[str, Any]) -> ScreeningConfig:
    g_screen = (cfg.get("globals", {}).get("screening", {}) or {})
    t_screen = (target.get("yellow_screen", {}) or {})
    adapter = str(t_screen.get("adapter") or g_screen.get("adapter") or "auto")
    inner_adapter = t_screen.get("inner_adapter")
    return ScreeningConfig(
        text_fields=list(t_screen.get("text_field_candidates") or g_screen.get("text_field_candidates") or ["text"]),
        license_fields=list(t_screen.get("record_license_field_candidates") or g_screen.get("record_license_field_candidates") or ["license", "license_spdx"]),
        allow_spdx=list(t_screen.get("allow_spdx") or g_screen.get("allow_spdx") or []),
        deny_phrases=[p.lower() for p in (t_screen.get("deny_phrases") or g_screen.get("deny_phrases") or [])],
        require_record_license=bool(t_screen.get("require_record_license", g_screen.get("require_record_license", False))),
        min_chars=int(t_screen.get("min_chars", g_screen.get("min_chars", 200))),
        max_chars=int(t_screen.get("max_chars", g_screen.get("max_chars", 12000))),
        adapter=adapter,
        inner_adapter=str(inner_adapter) if inner_adapter else None,
        chunk_rows=int(t_screen.get("chunk_rows", g_screen.get("chunk_rows", 1000))),
    )


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


def canonical_record(raw: Dict[str, Any], text: str, target_id: str, license_profile: str, license_spdx: Optional[str]) -> Dict[str, Any]:
    record_id = str(raw.get("record_id") or raw.get("id") or sha256_text(f"{target_id}:{text}"))
    content_hash = sha256_text(text)
    source = raw.get("source", {}) or {}
    return {
        "record_id": record_id,
        "text": text,
        "source": {
            "target_id": source.get("target_id", target_id),
            "origin": source.get("origin", raw.get("origin", "unknown")),
            "source_url": source.get("source_url", raw.get("source_url")),
            "artifact_path": source.get("artifact_path", raw.get("artifact_path")),
            "license_spdx": license_spdx,
            "license_profile": license_profile,
            "license_evidence": source.get("license_evidence", raw.get("license_evidence")),
            "retrieved_at_utc": source.get("retrieved_at_utc", raw.get("retrieved_at_utc")),
        },
        "routing": raw.get("routing") or raw.get("math_routing") or raw.get("route") or {},
        "hash": {"content_sha256": content_hash},
    }


def iter_raw_files(raw_dir: Path) -> Iterator[Path]:
    skip_names = {"download_manifest.json", "acquire_done.json"}
    for fp in raw_dir.rglob("*"):
        if fp.is_file() and fp.name not in skip_names:
            yield fp


def routing_from_queue(queue_row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "subject": queue_row.get("routing_subject") or queue_row.get("routing", {}).get("subject") or "econ",
        "domain": queue_row.get("routing_domain") or queue_row.get("routing", {}).get("domain"),
        "category": queue_row.get("routing_category") or queue_row.get("routing", {}).get("category"),
        "level": queue_row.get("routing_level") or queue_row.get("routing", {}).get("level"),
        "granularity": queue_row.get("routing_granularity") or queue_row.get("routing", {}).get("granularity") or "target",
    }


def primary_source_url(queue_row: Dict[str, Any]) -> Optional[str]:
    download = queue_row.get("download", {}) or {}
    urls = download.get("urls") or []
    if isinstance(urls, list) and urls:
        return str(urls[0])
    url = download.get("url") or download.get("source_url")
    return str(url) if url else None


MICRODATA_HINTS = [
    "first_name",
    "last_name",
    "full_name",
    "name",
    "address",
    "street",
    "city",
    "state",
    "zip",
    "postal",
    "email",
    "phone",
    "ssn",
    "social security",
    "patient",
    "person_id",
    "respondent",
]


def is_potential_microdata(columns: List[str]) -> bool:
    col_text = " ".join(columns).lower()
    return any(h in col_text for h in MICRODATA_HINTS)


def safe_read_text(path: Path) -> Optional[str]:
    for enc in ("utf-8", "latin-1"):
        try:
            return path.read_text(encoding=enc)
        except Exception:
            continue
    return None


def process_target(cfg: Dict[str, Any], roots: Roots, queue_row: Dict[str, Any], execute: bool) -> Dict[str, Any]:
    target_id = queue_row["id"]
    target_cfg = next((t for t in cfg.get("targets", []) if t.get("id") == target_id), {})
    screen_cfg = merge_screening_config(cfg, target_cfg)
    shard_cfg = sharding_cfg(cfg, "yellow_shard")
    pool_dir_base = roots.raw_root / "yellow"
    license_pools = [p.name for p in pool_dir_base.iterdir() if p.is_dir()] if pool_dir_base.exists() else []
    pools = license_pools or [queue_row.get("license_profile", "quarantine")]
    base_routing = routing_from_queue(queue_row)
    primary_url = primary_source_url(queue_row)
    license_spdx_hint = queue_row.get("resolved_spdx") or queue_row.get("license_spdx")

    passed, pitched = 0, 0
    shard_paths: List[str] = []

    for pool in pools:
        raw_dir = pool_dir_base / pool / target_id
        if not raw_dir.exists():
            continue
        sharder = Sharder(roots.screened_root / pool / "shards", shard_cfg)
        tabular_chunk_idx = 0

        def prepare_record(raw_record: Dict[str, Any], artifact_path: str) -> Dict[str, Any]:
            record = dict(raw_record)
            src = dict(record.get("source") or {})
            src.setdefault("target_id", target_id)
            src.setdefault("source_url", primary_url)
            src.setdefault("artifact_path", artifact_path)
            record["source"] = src
            record.setdefault("routing", base_routing)
            record.setdefault("license_profile", queue_row.get("license_profile") or pool or "quarantine")
            record.setdefault("artifact_path", artifact_path)
            return record

        def pitch(reason: str, sample_id: Optional[str] = None, sample: Any = None) -> None:
            nonlocal pitched
            pitched += 1
            if execute:
                row = {"target_id": target_id, "reason": reason, "seen_at_utc": utc_now()}
                if sample_id:
                    row["sample_id"] = sample_id
                if sample is not None:
                    row["sample"] = sample
                append_jsonl(roots.ledger_root / "yellow_pitched.jsonl", [row])

        def validate_and_add(raw_record: Dict[str, Any], text: Optional[str]) -> None:
            nonlocal passed
            if not text:
                pitch("no_text", sample_id=raw_record.get("id") or raw_record.get("record_id"))
                return
            if len(text) < screen_cfg.min_chars or len(text) > screen_cfg.max_chars:
                pitch("length_bounds", sample_id=raw_record.get("id") or raw_record.get("record_id"))
                return
            lic = find_license(raw_record, screen_cfg.license_fields)
            if screen_cfg.require_record_license and not lic:
                pitch("missing_record_license", sample_id=raw_record.get("id") or raw_record.get("record_id"))
                return
            if lic and screen_cfg.allow_spdx and lic not in screen_cfg.allow_spdx:
                pitch("license_not_allowlisted", sample_id=raw_record.get("id") or raw_record.get("record_id"))
                return
            if contains_deny(text, screen_cfg.deny_phrases):
                pitch("deny_phrase", sample_id=raw_record.get("id") or raw_record.get("record_id"))
                return
            license_profile = str(raw_record.get("license_profile") or queue_row.get("license_profile") or pool or "quarantine")
            record = canonical_record(raw_record, text, target_id, license_profile, lic or license_spdx_hint)
            passed += 1
            if execute:
                current_shard = str(sharder._next_path())
                path = sharder.add(record)
                if path:
                    current_shard = str(path)
                    shard_paths.append(current_shard)
                ledger_row = {
                    "stage": "yellow_screen",
                    "target_id": target_id,
                    "record_id": record["record_id"],
                    "content_sha256": record["hash"]["content_sha256"],
                    "decision": "pass",
                    "output_shard": current_shard,
                    "license_pool": pool,
                    "seen_at_utc": utc_now(),
                }
                append_jsonl(roots.ledger_root / "yellow_passed.jsonl", [ledger_row])

        def tabular_text(header: List[str], rows: List[Any], file_label: str) -> str:
            lines = [f"source_file: {file_label}"]
            if header:
                lines.append("columns: " + ", ".join(header))
            lines.append("rows:")
            for row in rows:
                if isinstance(row, dict):
                    lines.append(json.dumps(row, ensure_ascii=False))
                else:
                    lines.append(", ".join([str(v) for v in row]))
            return "\n".join(lines)

        def emit_tabular(reader: Iterable[Any], header: List[str], file_label: str, artifact_path: str) -> None:
            nonlocal tabular_chunk_idx
            if header and is_potential_microdata(header):
                pitch("possible_microdata", sample=header)
                return
            rows: List[Any] = []
            max_rows = max(screen_cfg.chunk_rows * 3, 3000)
            for idx, row in enumerate(reader):
                rows.append(row)
                if len(rows) >= screen_cfg.chunk_rows:
                    text = tabular_text(header, rows, file_label)
                    raw = prepare_record({"record_id": f"{target_id}:{file_label}:chunk{tabular_chunk_idx:04d}"}, artifact_path)
                    tabular_chunk_idx += 1
                    validate_and_add(raw, text)
                    rows = []
                if idx + 1 >= max_rows:
                    break
            if rows:
                text = tabular_text(header, rows, file_label)
                raw = prepare_record({"record_id": f"{target_id}:{file_label}:chunk{tabular_chunk_idx:04d}"}, artifact_path)
                tabular_chunk_idx += 1
                validate_and_add(raw, text)

        def handle_json_payload(data: Any, artifact_path: str) -> None:
            if isinstance(data, list):
                max_items = max(20, min(200, screen_cfg.chunk_rows))
                for idx, item in enumerate(data):
                    if idx >= max_items:
                        break
                    raw = item if isinstance(item, dict) else {"text": str(item)}
                    prepared = prepare_record(raw, artifact_path)
                    text = find_text(prepared, screen_cfg.text_fields) or json.dumps(item, ensure_ascii=False)
                    validate_and_add(prepared, text)
            elif isinstance(data, dict):
                prepared = prepare_record(data, artifact_path)
                text = find_text(prepared, screen_cfg.text_fields) or json.dumps(data, ensure_ascii=False)
                validate_and_add(prepared, text)
            else:
                prepared = prepare_record({"text": str(data)}, artifact_path)
                validate_and_add(prepared, str(data))

        for file_path in iter_raw_files(raw_dir):
            suffixes = [s.lower() for s in file_path.suffixes]
            suffix = suffixes[-1] if suffixes else file_path.suffix.lower()
            handler_suffix = suffix
            if suffix == ".gz" and len(suffixes) > 1:
                handler_suffix = suffixes[-2]

            if handler_suffix == ".jsonl":
                for raw in read_jsonl(file_path):
                    record = raw if isinstance(raw, dict) else {"text": str(raw)}
                    prepared = prepare_record(record, str(file_path))
                    validate_and_add(prepared, find_text(prepared, screen_cfg.text_fields))
            elif handler_suffix == ".json":
                try:
                    opener = gzip.open if suffix == ".gz" else open
                    with opener(file_path, "rt", encoding="utf-8", errors="ignore") as f:
                        data = json.load(f)
                    handle_json_payload(data, str(file_path))
                except Exception:
                    pitch("json_error", sample_id=file_path.name)
            elif handler_suffix == ".txt":
                text = safe_read_text(file_path)
                if text:
                    prepared = prepare_record({"record_id": f"{target_id}:{file_path.name}"}, str(file_path))
                    validate_and_add(prepared, text)
                else:
                    pitch("no_text", sample_id=file_path.name)
            elif handler_suffix in {".csv", ".tsv"}:
                delimiter = "," if handler_suffix == ".csv" else "\t"
                try:
                    opener = gzip.open if suffix == ".gz" else open
                    with opener(file_path, "rt", encoding="utf-8", errors="ignore", newline="") as f:
                        f.seek(0)
                        reader = csv.DictReader(f, delimiter=delimiter)
                        header = reader.fieldnames or []
                        if header:
                            emit_tabular(reader, header, file_path.name, str(file_path))
                        else:
                            f.seek(0)
                            raw_reader = csv.reader(f, delimiter=delimiter)
                            header_row = next(raw_reader, [])
                            header_list = header_row if isinstance(header_row, list) else [header_row]
                            emit_tabular(raw_reader, header_list, file_path.name, str(file_path))
                except Exception:
                    pitch("tabular_parse_error", sample_id=file_path.name)
            elif handler_suffix == ".zip":
                try:
                    with zipfile.ZipFile(file_path) as zf:
                        for info in zf.infolist():
                            if info.is_dir():
                                continue
                            if info.file_size > 50 * 1024 * 1024:
                                pitch("file_too_large", sample_id=info.filename)
                                continue
                            artifact_label = f"{file_path.name}:{info.filename}"
                            inner_suffixes = Path(info.filename).suffixes
                            inner_suffix = inner_suffixes[-1].lower() if inner_suffixes else ""
                            with zf.open(info) as f:
                                if inner_suffix in {".csv", ".tsv"} or screen_cfg.inner_adapter == "csv_chunked":
                                    delimiter = "," if inner_suffix != ".tsv" else "\t"
                                    text_stream = io.TextIOWrapper(f, encoding="utf-8", errors="ignore", newline="")
                                    reader = csv.DictReader(text_stream, delimiter=delimiter)
                                    header = reader.fieldnames or []
                                    if header:
                                        emit_tabular(reader, header, artifact_label, artifact_label)
                                    else:
                                        text_stream.seek(0)
                                        raw_reader = csv.reader(text_stream, delimiter=delimiter)
                                        header_row = next(raw_reader, [])
                                        header_list = header_row if isinstance(header_row, list) else [header_row]
                                        emit_tabular(raw_reader, header_list, artifact_label, artifact_label)
                                elif inner_suffix == ".jsonl":
                                    text_stream = io.TextIOWrapper(f, encoding="utf-8", errors="ignore")
                                    for line in text_stream:
                                        line = line.strip()
                                        if not line:
                                            continue
                                        try:
                                            raw = json.loads(line)
                                        except json.JSONDecodeError:
                                            continue
                                        prepared = prepare_record(raw if isinstance(raw, dict) else {"text": str(raw)}, artifact_label)
                                        validate_and_add(prepared, find_text(prepared, screen_cfg.text_fields))
                                elif inner_suffix == ".json":
                                    text_stream = io.TextIOWrapper(f, encoding="utf-8", errors="ignore")
                                    try:
                                        data = json.load(text_stream)
                                    except Exception:
                                        pitch("json_error", sample_id=artifact_label)
                                        continue
                                    handle_json_payload(data, artifact_label)
                                elif inner_suffix == ".txt":
                                    text_stream = io.TextIOWrapper(f, encoding="utf-8", errors="ignore")
                                    text = text_stream.read()
                                    prepared = prepare_record({"record_id": f"{target_id}:{artifact_label}"}, artifact_label)
                                    validate_and_add(prepared, text)
                                else:
                                    pitch("unsupported_inner_file", sample_id=artifact_label)
                except Exception as e:
                    pitch("zip_error", sample=str(e))
            else:
                pitch("unsupported_filetype", sample_id=file_path.name)
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
    ap.add_argument("--targets", required=True, help="Path to targets_econ_stats_decision_v2.yaml")
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
