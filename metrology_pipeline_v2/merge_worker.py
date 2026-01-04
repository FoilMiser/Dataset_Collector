#!/usr/bin/env python3
"""
merge_worker.py (v2.0)

Merges canonical GREEN records with screened YELLOW shards into combined shards
with lightweight deduplication on content_sha256.

Outputs:
  - combined/{license_pool}/shards/combined_00000.jsonl.gz
  - _ledger/combined_index.jsonl (content_sha256 -> shard mapping)
"""

from __future__ import annotations

import argparse
import dataclasses
import gzip
import hashlib
import json
import re
import sqlite3
import time
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any

import yaml
from datasets import DatasetDict, load_from_disk

from tools.output_contract import normalize_output_record, validate_output_contract

VERSION = "2.0"
PIPELINE_ID = Path(__file__).resolve().parent.name


def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def sha256_text(text: str) -> str:
    norm = re.sub(r"\s+", " ", (text or "").strip())
    return hashlib.sha256(norm.encode("utf-8")).hexdigest()


def read_jsonl(path: Path) -> Iterator[dict[str, Any]]:
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    continue


def append_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    ensure_dir(path.parent)
    opener = gzip.open if path.suffix == ".gz" else open
    mode = "ab" if path.suffix == ".gz" else "at"
    if path.suffix == ".gz":
        with opener(path, mode) as f:  # type: ignore
            for row in rows:
                f.write((json.dumps(row, ensure_ascii=False) + "\n").encode("utf-8"))
    else:
        with opener(path, mode, encoding="utf-8") as f:  # type: ignore
            for row in rows:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")


def write_json(path: Path, obj: dict[str, Any]) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


@dataclasses.dataclass
class Roots:
    raw_root: Path
    screened_root: Path
    combined_root: Path
    ledger_root: Path


@dataclasses.dataclass
class ShardingConfig:
    max_records_per_shard: int
    compression: str
    prefix: str


@dataclasses.dataclass
class GreenInput:
    raw: dict[str, Any]
    target_id: str
    pool: str
    source_path: Path
    source_kind: str


@dataclasses.dataclass
class GreenSkip:
    target_id: str
    pool: str
    source_path: Path
    source_kind: str
    reason: str
    detail: dict[str, Any] | None = None


class Sharder:
    def __init__(self, base_dir: Path, cfg: ShardingConfig):
        self.base_dir = base_dir
        self.cfg = cfg
        self.current: list[dict[str, Any]] = []
        self.shard_idx = 0

    def _path(self) -> Path:
        suffix = "jsonl.gz" if self.cfg.compression == "gzip" else "jsonl"
        return self.base_dir / f"{self.cfg.prefix}_{self.shard_idx:05d}.{suffix}"

    def add(self, row: dict[str, Any]) -> Path | None:
        self.current.append(row)
        if len(self.current) >= self.cfg.max_records_per_shard:
            path = self.flush()
            self.shard_idx += 1
            return path
        return None

    def flush(self) -> Path | None:
        if not self.current:
            return None
        path = self._path()
        append_jsonl(path, self.current)
        self.current = []
        return path


class DedupeIndex:
    def __init__(self, path: Path) -> None:
        self.path = path
        ensure_dir(path.parent)
        if path.exists():
            path.unlink()
        self.conn = sqlite3.connect(str(path))
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA synchronous=OFF;")
        self.conn.execute("CREATE TABLE IF NOT EXISTS seen (content_sha256 TEXT PRIMARY KEY)")

    def add_if_new(self, content_hash: str) -> bool:
        cursor = self.conn.execute(
            "INSERT OR IGNORE INTO seen (content_sha256) VALUES (?)",
            (content_hash,),
        )
        return cursor.rowcount == 1

    def close(self) -> None:
        self.conn.commit()
        self.conn.close()


def resolve_roots(cfg: dict[str, Any]) -> Roots:
    g = (cfg.get("globals", {}) or {})
    return Roots(
        raw_root=Path(g.get("raw_root", "/data/metrology/raw")),
        screened_root=Path(g.get("screened_yellow_root", "/data/metrology/screened_yellow")),
        combined_root=Path(g.get("combined_root", "/data/metrology/combined")),
        ledger_root=Path(g.get("ledger_root", "/data/metrology/_ledger")),
    )


def sharding_cfg(cfg: dict[str, Any]) -> ShardingConfig:
    g = (cfg.get("globals", {}).get("sharding", {}) or {})
    return ShardingConfig(
        max_records_per_shard=int(g.get("max_records_per_shard", 50000)),
        compression=str(g.get("compression", "gzip")),
        prefix="combined",
    )


def resolve_canonicalize_config(cfg: dict[str, Any], target_cfg: dict[str, Any] | None) -> tuple[list[str], int | None]:
    g = (cfg.get("globals", {}) or {})
    g_canon = (g.get("canonicalize", {}) or {})
    g_screen = (g.get("screening", {}) or {})
    t_screen = (target_cfg.get("yellow_screen", {}) or {}) if target_cfg else {}
    t_canon = (target_cfg.get("canonicalize", {}) or {}) if target_cfg else {}
    candidates = list(
        t_canon.get("text_field_candidates")
        or t_screen.get("text_field_candidates")
        or g_canon.get("text_field_candidates")
        or g_screen.get("text_field_candidates")
        or ["text"]
    )
    max_chars_value = t_canon.get("max_chars", t_screen.get("max_chars", g_canon.get("max_chars", g_screen.get("max_chars"))))
    max_chars = int(max_chars_value) if max_chars_value is not None else None
    return candidates, max_chars


def coerce_text(value: Any) -> str:
    if isinstance(value, (list, tuple)):
        return "\n".join(map(str, value))
    return str(value)


def extract_text(row: dict[str, Any], candidates: list[str]) -> str | None:
    if "text" in row and row["text"]:
        return coerce_text(row["text"])
    for key in candidates:
        if key == "text":
            continue
        if key in row and row[key]:
            return coerce_text(row[key])
    string_fields = [v for v in row.values() if isinstance(v, str) and v]
    if string_fields:
        return "\n".join(string_fields)
    try:
        return json.dumps(row, ensure_ascii=False)
    except Exception:
        return str(row)


def resolve_routing(raw: dict[str, Any]) -> dict[str, Any]:
    if raw.get("routing") or raw.get("route"):
        return raw.get("routing") or raw.get("route") or {}
    routing_keys = sorted(k for k in raw.keys() if k.endswith("_routing"))
    for key in routing_keys:
        if raw.get(key):
            return raw.get(key) or {}
    return {}


def canonicalize_row(
    raw: dict[str, Any],
    target_id: str,
    pool: str,
    candidates: list[str],
    max_chars: int | None,
    target_meta: dict[str, Any] | None,
) -> tuple[dict[str, Any] | None, str | None]:
    if not isinstance(raw, dict):
        return None, "unsupported_row_type"
    text = extract_text(raw, candidates)
    if not text:
        return None, "missing_text"
    if max_chars is not None and max_chars > 0 and len(text) > max_chars:
        text = text[:max_chars]
    record = dict(raw)
    record.setdefault("text", text)
    record_id = str(record.get("record_id") or record.get("id") or sha256_text(f"{target_id}:{text}"))
    record.setdefault("record_id", record_id)
    meta = target_meta or {}
    record = normalize_output_record(
        record,
        target_id=target_id,
        pool=pool,
        pipeline=PIPELINE_ID,
        dataset_id=meta.get("dataset_id"),
        config=meta.get("config"),
        now=utc_now(),
    )
    validate_output_contract(record, f"green/{target_id}")
    return record, None


def is_hf_dataset_dir(path: Path) -> bool:
    if not path.is_dir():
        return False
    markers = ("dataset_info.json", "state.json", "dataset_dict.json")
    return any((path / marker).exists() for marker in markers)


def iter_hf_dataset_dirs(target_dir: Path) -> list[Path]:
    candidates: list[Path] = []
    if is_hf_dataset_dir(target_dir):
        candidates.append(target_dir)
    for marker in ("dataset_info.json", "state.json", "dataset_dict.json"):
        for fp in target_dir.rglob(marker):
            candidates.append(fp.parent)
    for pattern in ("hf_dataset", "split_*"):
        candidates.extend([p for p in target_dir.rglob(pattern) if p.is_dir()])
    seen: set[Path] = set()
    ordered: list[Path] = []
    for path in sorted(candidates):
        resolved = path.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        ordered.append(path)
    return ordered


def iter_green_records(roots: Roots) -> Iterator[GreenInput | GreenSkip]:
    base = roots.raw_root / "green"
    for pool_dir in sorted(base.iterdir()) if base.exists() else []:
        if not pool_dir.is_dir():
            continue
        for target_dir in sorted(p for p in pool_dir.iterdir() if p.is_dir()):
            if not target_dir.is_dir():
                continue
            target_id = target_dir.name
            jsonl_files = sorted([fp for fp in target_dir.rglob("*.jsonl") if fp.is_file()])
            jsonl_files.extend(sorted([fp for fp in target_dir.rglob("*.jsonl.gz") if fp.is_file()]))
            jsonl_set = {fp.resolve() for fp in jsonl_files}
            for fp in jsonl_files:
                for raw in read_jsonl(fp):
                    yield GreenInput(raw, target_id, pool_dir.name, fp, "jsonl")

            hf_dirs = iter_hf_dataset_dirs(target_dir)
            hf_dir_set = {p.resolve() for p in hf_dirs}
            for ds_path in hf_dirs:
                try:
                    dataset_obj = load_from_disk(str(ds_path))
                except Exception as exc:
                    yield GreenSkip(
                        target_id,
                        pool_dir.name,
                        ds_path,
                        "hf_dataset",
                        "hf_load_failed",
                        detail={"error": str(exc)},
                    )
                    continue
                if isinstance(dataset_obj, DatasetDict):
                    for split_name in sorted(dataset_obj.keys()):
                        dataset = dataset_obj[split_name]
                        for raw in dataset:
                            row = dict(raw)
                            row.setdefault("split", split_name)
                            yield GreenInput(row, target_id, pool_dir.name, ds_path, "hf_dataset")
                else:
                    for raw in dataset_obj:
                        row = dict(raw)
                        row.setdefault("split", "train")
                        yield GreenInput(row, target_id, pool_dir.name, ds_path, "hf_dataset")

            for fp in sorted([p for p in target_dir.rglob("*") if p.is_file()]):
                resolved = fp.resolve()
                if resolved in jsonl_set:
                    continue
                if any(parent in hf_dir_set for parent in resolved.parents):
                    continue
                yield GreenSkip(
                    target_id,
                    pool_dir.name,
                    fp,
                    "file",
                    "unsupported_green_format",
                    detail={"extension": fp.suffix},
                )


def iter_screened_yellow(roots: Roots) -> Iterator[dict[str, Any]]:
    base = roots.screened_root
    for pool_dir in sorted(base.iterdir()) if base.exists() else []:
        shards_dir = pool_dir / "shards"
        if not shards_dir.exists():
            continue
        for fp in sorted(shards_dir.glob("*.jsonl*")):
            yield from read_jsonl(fp)


LICENSE_POOL_MAP = {
    "permissive": "permissive",
    "public_domain": "permissive",
    "record_level": "permissive",
    "copyleft": "copyleft",
    "unknown": "quarantine",
    "quarantine": "quarantine",
    "deny": "quarantine",
}


def route_pool(record: dict[str, Any]) -> str:
    src = record.get("source", {}) or {}
    lp = src.get("license_profile") or record.get("license_profile")
    lp = str(lp or "quarantine").lower()
    return LICENSE_POOL_MAP.get(lp, "quarantine")


def record_skip(
    roots: Roots,
    target_id: str,
    pool: str,
    reason: str,
    source_path: Path | None,
    source_kind: str,
    execute: bool,
    detail: dict[str, Any] | None = None,
) -> None:
    if not execute:
        return
    row = {
        "stage": "merge",
        "target_id": target_id,
        "license_pool": pool,
        "reason": reason,
        "source_kind": source_kind,
        "seen_at_utc": utc_now(),
    }
    if source_path is not None:
        row["source_path"] = str(source_path)
    if detail:
        row.update(detail)
    append_jsonl(roots.ledger_root / "combined_skipped.jsonl", [row])


def merge_records(cfg: dict[str, Any], roots: Roots, execute: bool) -> dict[str, Any]:
    shard_cfg = sharding_cfg(cfg)
    dedupe = DedupeIndex(roots.ledger_root / "combined_dedupe.sqlite")
    summary = {"written": 0, "deduped": 0, "skipped": 0, "shards": []}
    target_meta = {
        str(target.get("id")): {
            "dataset_id": (target.get("download", {}) or {}).get("dataset_id") or target.get("dataset_id"),
            "config": (target.get("download", {}) or {}).get("config"),
        }
        for target in (cfg.get("targets", []) or [])
        if target.get("id") is not None
    }
    target_canon = {
        str(target.get("id")): resolve_canonicalize_config(cfg, target)
        for target in (cfg.get("targets", []) or [])
        if target.get("id") is not None
    }
    default_canon = resolve_canonicalize_config(cfg, None)

    pool_sharders: dict[str, Sharder] = {}

    def get_sharder(pool: str) -> Sharder:
        if pool not in pool_sharders:
            sharder = Sharder(roots.combined_root / pool / "shards", shard_cfg)
            pool_sharders[pool] = sharder
            ensure_dir(sharder.base_dir)
        return pool_sharders[pool]

    def handle_record(
        rec: dict[str, Any],
        source_kind: str,
        source_path: Path | None,
        target_id: str | None = None,
        pool_hint: str | None = None,
    ) -> None:
        resolved_target = target_id or (rec.get("source", {}) or {}).get("target_id") or "unknown"
        pool_value = pool_hint or rec.get("pool") or route_pool(rec)
        meta = target_meta.get(resolved_target, {})
        record = normalize_output_record(
            rec,
            target_id=resolved_target,
            pool=pool_value,
            pipeline=PIPELINE_ID,
            dataset_id=meta.get("dataset_id"),
            config=meta.get("config"),
            now=utc_now(),
        )
        validate_output_contract(record, f"{source_kind}/{resolved_target}")
        content_hash = record["content_sha256"]
        if not dedupe.add_if_new(content_hash):
            summary["deduped"] += 1
            return
        pool = record.get("pool") or pool_value
        sharder = get_sharder(pool)
        shard_path = str(sharder._path())
        if execute:
            path = sharder.add(record)
            if path:
                shard_path = str(path)
                summary["shards"].append(shard_path)
            append_jsonl(roots.ledger_root / "combined_index.jsonl", [{
                "content_sha256": content_hash,
                "license_pool": pool,
                "output_shard": shard_path,
                "source": record.get("source", {}),
                "seen_at_utc": utc_now(),
            }])
        summary["written"] += 1

    for item in iter_green_records(roots):
        candidates, max_chars = target_canon.get(item.target_id, default_canon)
        meta = target_meta.get(item.target_id, {})
        if isinstance(item, GreenSkip):
            summary["skipped"] += 1
            record_skip(
                roots,
                item.target_id,
                item.pool,
                item.reason,
                item.source_path,
                item.source_kind,
                execute,
                detail=item.detail,
            )
            continue
        canonical, reason = canonicalize_row(item.raw, item.target_id, item.pool, candidates, max_chars, meta)
        if not canonical:
            summary["skipped"] += 1
            record_skip(
                roots,
                item.target_id,
                item.pool,
                reason or "canonicalize_failed",
                item.source_path,
                item.source_kind,
                execute,
            )
            continue
        handle_record(canonical, item.source_kind, item.source_path, item.target_id, item.pool)
    for rec in iter_screened_yellow(roots):
        target_id = (rec.get("source", {}) or {}).get("target_id") or "unknown"
        handle_record(rec, "screened_yellow", None, target_id)

    if execute:
        for sharder in pool_sharders.values():
            flushed = sharder.flush()
            if flushed:
                summary["shards"].append(str(flushed))

    dedupe.close()
    summary["finished_at_utc"] = utc_now()
    return summary


def main() -> None:
    ap = argparse.ArgumentParser(description=f"Merge Worker v{VERSION}")
    ap.add_argument("--targets", required=True, help="targets_metrology.yaml")
    ap.add_argument("--execute", action="store_true", help="Write combined shards")
    args = ap.parse_args()

    cfg = yaml.safe_load(Path(args.targets).read_text(encoding="utf-8")) or {}
    roots = resolve_roots(cfg)
    summary = merge_records(cfg, roots, args.execute)
    write_json(roots.ledger_root / "merge_summary.json", summary)


if __name__ == "__main__":
    main()
