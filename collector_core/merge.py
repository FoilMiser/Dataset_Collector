from __future__ import annotations

import argparse
import cProfile
import dataclasses
import gzip
import hashlib
import importlib.util
import json
import os
import pstats
import re
import sqlite3
import time
import tracemalloc
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any

from datasets import DatasetDict, load_from_disk

from collector_core.__version__ import __version__ as VERSION
from collector_core.config_validator import read_yaml
from collector_core.output_contract import normalize_output_record, validate_output_contract

if importlib.util.find_spec("tqdm"):
    from tqdm import tqdm as tqdm_progress
else:  # pragma: no cover - optional dependency
    tqdm_progress = None


@dataclasses.dataclass(frozen=True)
class RootDefaults:
    raw_root: str
    screened_root: str
    combined_root: str
    ledger_root: str


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


@dataclasses.dataclass
class MergeState:
    summary: dict[str, Any]
    dedupe: DedupeIndex | PartitionedDedupeIndex
    shard_cfg: ShardingConfig
    pool_sharders: dict[str, Sharder]
    target_meta: dict[str, dict[str, Any]]
    pipeline_id: str
    execute: bool
    progress: bool
    progress_interval: int
    inflight_records: dict[str, dict[str, Any]]
    shard_index: dict[str, str]
    pending_updates: dict[str, dict[str, Any]]
    max_source_urls: int
    max_duplicates: int


@dataclasses.dataclass
class MergeRuntimeConfig:
    progress: bool = False
    progress_interval: int = 10000
    trace_memory: bool = False
    profile: bool = False
    profile_path: Path | None = None
    profile_sort: str = "tottime"
    dedupe_partitions: int = 1


class Sharder:
    def __init__(self, base_dir: Path, cfg: ShardingConfig):
        self.base_dir = base_dir
        self.cfg = cfg
        self.current: list[dict[str, Any]] = []
        self.shard_idx = 0

    def _path(self) -> Path:
        suffix = "jsonl.gz" if self.cfg.compression == "gzip" else "jsonl"
        return self.base_dir / f"{self.cfg.prefix}_{self.shard_idx:05d}.{suffix}"

    def add(self, row: dict[str, Any]) -> tuple[Path | None, list[dict[str, Any]]]:
        self.current.append(row)
        if len(self.current) >= self.cfg.max_records_per_shard:
            path, flushed = self.flush()
            self.shard_idx += 1
            return path, flushed
        return None, []

    def flush(self) -> tuple[Path | None, list[dict[str, Any]]]:
        if not self.current:
            return None, []
        path = self._path()
        records = self.current
        append_jsonl(path, records)
        self.current = []
        return path, records


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


class PartitionedDedupeIndex:
    def __init__(self, path: Path, partitions: int) -> None:
        if partitions < 2:
            raise ValueError("PartitionedDedupeIndex requires at least 2 partitions.")
        self.partitions = partitions
        self.paths = [self._partition_path(path, idx) for idx in range(partitions)]
        self.indices = [DedupeIndex(part_path) for part_path in self.paths]

    @staticmethod
    def _partition_path(path: Path, idx: int) -> Path:
        suffix = path.suffix or ".sqlite"
        stem = path.stem
        return path.with_name(f"{stem}_part{idx:03d}{suffix}")

    def _partition_index(self, content_hash: str) -> int:
        if not content_hash:
            return 0
        return int(content_hash[:8], 16) % self.partitions

    def add_if_new(self, content_hash: str) -> bool:
        idx = self._partition_index(content_hash)
        return self.indices[idx].add_if_new(content_hash)

    def close(self) -> None:
        for index in self.indices:
            index.close()


LICENSE_POOL_MAP = {
    "permissive": "permissive",
    "public_domain": "permissive",
    "record_level": "permissive",
    "copyleft": "copyleft",
    "unknown": "quarantine",
    "quarantine": "quarantine",
    "deny": "quarantine",
}


def default_merge_roots(prefix: str) -> RootDefaults:
    return RootDefaults(
        raw_root=f"/data/{prefix}/raw",
        screened_root=f"/data/{prefix}/screened_yellow",
        combined_root=f"/data/{prefix}/combined",
        ledger_root=f"/data/{prefix}/_ledger",
    )


def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def sha256_text(text: str) -> str:
    norm = re.sub(r"\s+", " ", (text or "").strip())
    return hashlib.sha256(norm.encode("utf-8")).hexdigest()


DEFAULT_MAX_SOURCE_URLS = 10
DEFAULT_MAX_DUPLICATES = 20


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


def merge_distinct_urls(
    existing: Iterable[str],
    incoming: Iterable[str],
    limit: int,
) -> list[str]:
    merged: list[str] = []
    seen: set[str] = set()
    for url in list(existing) + list(incoming):
        if not url:
            continue
        if url in seen:
            continue
        merged.append(url)
        seen.add(url)
        if len(merged) >= limit:
            break
    return merged


def build_dedupe_update(
    record: dict[str, Any],
    *,
    source_kind: str,
    source_path: Path | None,
) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "content_sha256": record.get("content_sha256"),
        "source_urls": record.get("source_urls", []),
        "source": record.get("source", {}),
        "source_kind": source_kind,
        "seen_at_utc": utc_now(),
    }
    if source_path is not None:
        entry["source_path"] = str(source_path)
    return {
        "source_urls": record.get("source_urls", []),
        "duplicates": [entry],
    }


def merge_update_payload(
    base: dict[str, Any],
    update: dict[str, Any],
    *,
    max_source_urls: int,
    max_duplicates: int,
) -> dict[str, Any]:
    merged_urls = merge_distinct_urls(
        base.get("source_urls", []),
        update.get("source_urls", []),
        max_source_urls,
    )
    duplicates = list(base.get("duplicates", []))
    for entry in update.get("duplicates", []):
        if entry not in duplicates:
            duplicates.append(entry)
    if len(duplicates) > max_duplicates:
        duplicates = duplicates[-max_duplicates:]
    return {
        "source_urls": merged_urls,
        "duplicates": duplicates,
    }


def merge_provenance_update(
    record: dict[str, Any],
    update: dict[str, Any],
    *,
    max_source_urls: int,
    max_duplicates: int,
) -> None:
    record["source_urls"] = merge_distinct_urls(
        record.get("source_urls", []),
        update.get("source_urls", []),
        max_source_urls,
    )
    provenance = record.get("provenance") or {}
    duplicates = list(provenance.get("duplicates", []))
    for entry in update.get("duplicates", []):
        if entry not in duplicates:
            duplicates.append(entry)
    if len(duplicates) > max_duplicates:
        duplicates = duplicates[-max_duplicates:]
    if duplicates:
        provenance["duplicates"] = duplicates
        record["provenance"] = provenance
    record["timestamp_updated"] = utc_now()


def resolve_dataset_root(explicit: str | None = None) -> Path | None:
    value = explicit or os.getenv("DATASET_ROOT") or os.getenv("DATASET_COLLECTOR_ROOT")
    if not value:
        return None
    return Path(value).expanduser().resolve()


def resolve_roots(cfg: dict[str, Any], defaults: RootDefaults, dataset_root: Path | None = None) -> Roots:
    dataset_root = dataset_root or resolve_dataset_root()
    if dataset_root:
        defaults = RootDefaults(
            raw_root=str(dataset_root / "raw"),
            screened_root=str(dataset_root / "screened_yellow"),
            combined_root=str(dataset_root / "combined"),
            ledger_root=str(dataset_root / "_ledger"),
        )
    g = (cfg.get("globals", {}) or {})
    return Roots(
        raw_root=Path(g.get("raw_root", defaults.raw_root)).expanduser().resolve(),
        screened_root=Path(g.get("screened_yellow_root", defaults.screened_root)).expanduser().resolve(),
        combined_root=Path(g.get("combined_root", defaults.combined_root)).expanduser().resolve(),
        ledger_root=Path(g.get("ledger_root", defaults.ledger_root)).expanduser().resolve(),
    )


def sharding_cfg(cfg: dict[str, Any]) -> ShardingConfig:
    g = (cfg.get("globals", {}).get("sharding", {}) or {})
    return ShardingConfig(
        max_records_per_shard=int(g.get("max_records_per_shard", 50000)),
        compression=str(g.get("compression", "gzip")),
        prefix="combined",
    )


def resolve_merge_runtime(
    cfg: dict[str, Any],
    *,
    progress: bool | None = None,
    progress_interval: int | None = None,
    trace_memory: bool | None = None,
    profile: bool | None = None,
    profile_path: str | None = None,
    profile_sort: str | None = None,
    dedupe_partitions: int | None = None,
    ledger_root: Path | None = None,
) -> MergeRuntimeConfig:
    g = (cfg.get("globals", {}) or {})
    g_merge = (g.get("merge", {}) or {})
    resolved_progress = bool(progress if progress is not None else g_merge.get("progress", False))
    interval_value = progress_interval if progress_interval is not None else g_merge.get("progress_interval", 10000)
    resolved_interval = max(int(interval_value or 10000), 1)
    resolved_trace = bool(trace_memory if trace_memory is not None else g_merge.get("trace_memory", False))
    resolved_profile = bool(profile if profile is not None else g_merge.get("profile", False))
    resolved_profile_sort = str(profile_sort or g_merge.get("profile_sort", "tottime"))
    partitions_value = dedupe_partitions if dedupe_partitions is not None else g_merge.get("dedupe_partitions", 1)
    resolved_partitions = max(int(partitions_value or 1), 1)
    resolved_profile_path: Path | None = None
    if resolved_profile:
        profile_value = profile_path or g_merge.get("profile_path")
        if profile_value:
            resolved_profile_path = Path(profile_value).expanduser().resolve()
        elif ledger_root:
            resolved_profile_path = ledger_root / "merge_profile.prof"
    return MergeRuntimeConfig(
        progress=resolved_progress,
        progress_interval=resolved_interval,
        trace_memory=resolved_trace,
        profile=resolved_profile,
        profile_path=resolved_profile_path,
        profile_sort=resolved_profile_sort,
        dedupe_partitions=resolved_partitions,
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
    max_chars_value = t_canon.get(
        "max_chars",
        t_screen.get("max_chars", g_canon.get("max_chars", g_screen.get("max_chars"))),
    )
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
    *,
    pipeline_id: str,
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
        pipeline=pipeline_id,
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


def record_dedupe_event(
    roots: Roots,
    *,
    content_hash: str,
    source_kind: str,
    source_path: Path | None,
    target_id: str,
    pool: str,
    record: dict[str, Any],
    retained_shard: str | None,
) -> None:
    row: dict[str, Any] = {
        "stage": "merge",
        "content_sha256": content_hash,
        "target_id": target_id,
        "license_pool": pool,
        "source_kind": source_kind,
        "seen_at_utc": utc_now(),
        "source_urls": record.get("source_urls", []),
        "source": record.get("source", {}),
    }
    if source_path is not None:
        row["source_path"] = str(source_path)
    if retained_shard:
        row["retained_shard"] = retained_shard
    append_jsonl(roots.ledger_root / "combined_deduped.jsonl", [row])


def register_flushed_records(
    state: MergeState,
    *,
    shard_path: Path | None,
    records: list[dict[str, Any]],
) -> None:
    if not shard_path:
        return
    shard_str = str(shard_path)
    for rec in records:
        content_hash = rec.get("content_sha256")
        if not content_hash:
            continue
        state.shard_index[content_hash] = shard_str
        state.inflight_records.pop(content_hash, None)


def build_target_meta(cfg: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        str(target.get("id")): {
            "dataset_id": (target.get("download", {}) or {}).get("dataset_id") or target.get("dataset_id"),
            "config": (target.get("download", {}) or {}).get("config"),
        }
        for target in (cfg.get("targets", []) or [])
        if target.get("id") is not None
    }


def build_target_canon(cfg: dict[str, Any]) -> tuple[dict[str, tuple[list[str], int | None]], tuple[list[str], int | None]]:
    target_canon = {
        str(target.get("id")): resolve_canonicalize_config(cfg, target)
        for target in (cfg.get("targets", []) or [])
        if target.get("id") is not None
    }
    default_canon = resolve_canonicalize_config(cfg, None)
    return target_canon, default_canon


def get_sharder(pool: str, roots: Roots, state: MergeState) -> Sharder:
    if pool not in state.pool_sharders:
        sharder = Sharder(roots.combined_root / pool / "shards", state.shard_cfg)
        state.pool_sharders[pool] = sharder
        ensure_dir(sharder.base_dir)
    return state.pool_sharders[pool]


def iter_with_progress(
    iterable: Iterable[Any],
    *,
    enabled: bool,
    desc: str,
    interval: int,
) -> Iterator[Any]:
    if not enabled:
        yield from iterable
        return
    if tqdm_progress is not None:
        yield from tqdm_progress(iterable, desc=desc, unit="records")
        return
    count = 0
    for item in iterable:
        yield item
        count += 1
        if count % interval == 0:
            print(f"[merge] {desc}: {count} records processed")
    print(f"[merge] {desc}: {count} records processed")


def handle_record(
    rec: dict[str, Any],
    source_kind: str,
    source_path: Path | None,
    roots: Roots,
    state: MergeState,
    target_id: str | None = None,
    pool_hint: str | None = None,
) -> None:
    resolved_target = target_id or (rec.get("source", {}) or {}).get("target_id") or "unknown"
    pool_value = pool_hint or rec.get("pool") or route_pool(rec)
    meta = state.target_meta.get(resolved_target, {})
    record = normalize_output_record(
        rec,
        target_id=resolved_target,
        pool=pool_value,
        pipeline=state.pipeline_id,
        dataset_id=meta.get("dataset_id"),
        config=meta.get("config"),
        now=utc_now(),
    )
    validate_output_contract(record, f"{source_kind}/{resolved_target}")
    content_hash = record["content_sha256"]
    if not state.dedupe.add_if_new(content_hash):
        state.summary["deduped"] += 1
        update = build_dedupe_update(record, source_kind=source_kind, source_path=source_path)
        retained_shard = state.shard_index.get(content_hash)
        if state.execute:
            record_dedupe_event(
                roots,
                content_hash=content_hash,
                source_kind=source_kind,
                source_path=source_path,
                target_id=resolved_target,
                pool=pool_value,
                record=record,
                retained_shard=retained_shard,
            )
        retained = state.inflight_records.get(content_hash)
        if retained:
            merge_provenance_update(
                retained,
                update,
                max_source_urls=state.max_source_urls,
                max_duplicates=state.max_duplicates,
            )
        else:
            pending = state.pending_updates.get(content_hash)
            if pending:
                state.pending_updates[content_hash] = merge_update_payload(
                    pending,
                    update,
                    max_source_urls=state.max_source_urls,
                    max_duplicates=state.max_duplicates,
                )
            else:
                state.pending_updates[content_hash] = update
        return
    pool = record.get("pool") or pool_value
    sharder = get_sharder(pool, roots, state)
    shard_path = str(sharder._path())
    if state.execute:
        state.inflight_records[content_hash] = record
        path, flushed_records = sharder.add(record)
        if path:
            shard_path = str(path)
            state.summary["shards"].append(shard_path)
            register_flushed_records(state, shard_path=path, records=flushed_records)
        append_jsonl(
            roots.ledger_root / "combined_index.jsonl",
            [
                {
                    "content_sha256": content_hash,
                    "license_pool": pool,
                    "output_shard": shard_path,
                    "source": record.get("source", {}),
                    "seen_at_utc": utc_now(),
                }
            ],
        )
    state.summary["written"] += 1


def process_green_records(
    roots: Roots,
    state: MergeState,
    target_canon: dict[str, tuple[list[str], int | None]],
    default_canon: tuple[list[str], int | None],
) -> None:
    for item in iter_with_progress(
        iter_green_records(roots),
        enabled=state.progress,
        desc="GREEN merge",
        interval=state.progress_interval,
    ):
        candidates, max_chars = target_canon.get(item.target_id, default_canon)
        meta = state.target_meta.get(item.target_id, {})
        if isinstance(item, GreenSkip):
            state.summary["skipped"] += 1
            record_skip(
                roots,
                item.target_id,
                item.pool,
                item.reason,
                item.source_path,
                item.source_kind,
                state.execute,
                detail=item.detail,
            )
            continue
        canonical, reason = canonicalize_row(
            item.raw,
            item.target_id,
            item.pool,
            candidates,
            max_chars,
            meta,
            pipeline_id=state.pipeline_id,
        )
        if not canonical:
            state.summary["skipped"] += 1
            record_skip(
                roots,
                item.target_id,
                item.pool,
                reason or "canonicalize_failed",
                item.source_path,
                item.source_kind,
                state.execute,
            )
            continue
        handle_record(canonical, item.source_kind, item.source_path, roots, state, item.target_id, item.pool)


def process_screened_yellow(roots: Roots, state: MergeState) -> None:
    for rec in iter_with_progress(
        iter_screened_yellow(roots),
        enabled=state.progress,
        desc="screened YELLOW merge",
        interval=state.progress_interval,
    ):
        target_id = (rec.get("source", {}) or {}).get("target_id") or "unknown"
        handle_record(rec, "screened_yellow", None, roots, state, target_id)


def finalize_shards(state: MergeState) -> None:
    if not state.execute:
        return
    for sharder in state.pool_sharders.values():
        flushed_path, flushed_records = sharder.flush()
        if flushed_path:
            state.summary["shards"].append(str(flushed_path))
            register_flushed_records(state, shard_path=flushed_path, records=flushed_records)


def apply_pending_updates(roots: Roots, state: MergeState) -> None:
    if not state.execute or not state.pending_updates:
        return
    updates_by_shard: dict[str, dict[str, dict[str, Any]]] = {}
    for content_hash, update in state.pending_updates.items():
        shard = state.shard_index.get(content_hash)
        if not shard:
            continue
        updates_by_shard.setdefault(shard, {})[content_hash] = update
    for shard_path, updates in updates_by_shard.items():
        path = Path(shard_path)
        temp_path = path.with_suffix(path.suffix + ".tmp")
        opener = gzip.open if path.suffix == ".gz" else open
        with opener(path, "rt", encoding="utf-8", errors="ignore") as src, opener(
            temp_path,
            "wt",
            encoding="utf-8",
        ) as dst:
            for line in src:
                raw = line.strip()
                if not raw:
                    continue
                try:
                    record = json.loads(raw)
                except json.JSONDecodeError:
                    dst.write(line)
                    continue
                content_hash = record.get("content_sha256")
                update = updates.get(content_hash) if content_hash else None
                if update:
                    merge_provenance_update(
                        record,
                        update,
                        max_source_urls=state.max_source_urls,
                        max_duplicates=state.max_duplicates,
                    )
                dst.write(json.dumps(record, ensure_ascii=False) + "\n")
        temp_path.replace(path)


def build_dedupe_index(roots: Roots, partitions: int) -> DedupeIndex | PartitionedDedupeIndex:
    base_path = roots.ledger_root / "combined_dedupe.sqlite"
    if partitions > 1:
        return PartitionedDedupeIndex(base_path, partitions)
    return DedupeIndex(base_path)


def write_profile_stats(profile: cProfile.Profile, path: Path, sort: str) -> tuple[Path, Path]:
    ensure_dir(path.parent)
    profile.dump_stats(str(path))
    text_path = path.with_suffix(path.suffix + ".txt")
    with text_path.open("w", encoding="utf-8") as f:
        stats = pstats.Stats(profile, stream=f)
        stats.sort_stats(sort)
        stats.print_stats(50)
    return path, text_path


def merge_records(
    cfg: dict[str, Any],
    roots: Roots,
    execute: bool,
    *,
    pipeline_id: str,
    runtime: MergeRuntimeConfig | None = None,
) -> dict[str, Any]:
    runtime = runtime or resolve_merge_runtime(cfg, ledger_root=roots.ledger_root)
    shard_cfg = sharding_cfg(cfg)
    dedupe = build_dedupe_index(roots, runtime.dedupe_partitions)
    summary = {"written": 0, "deduped": 0, "skipped": 0, "shards": []}
    target_meta = build_target_meta(cfg)
    target_canon, default_canon = build_target_canon(cfg)
    state = MergeState(
        summary=summary,
        dedupe=dedupe,
        shard_cfg=shard_cfg,
        pool_sharders={},
        target_meta=target_meta,
        pipeline_id=pipeline_id,
        execute=execute,
        progress=runtime.progress,
        progress_interval=runtime.progress_interval,
        inflight_records={},
        shard_index={},
        pending_updates={},
        max_source_urls=DEFAULT_MAX_SOURCE_URLS,
        max_duplicates=DEFAULT_MAX_DUPLICATES,
    )
    summary["dedupe_partitions"] = runtime.dedupe_partitions
    profiler: cProfile.Profile | None = None
    if runtime.profile:
        profiler = cProfile.Profile()
        profiler.enable()
    if runtime.trace_memory:
        tracemalloc.start()
    try:
        process_green_records(roots, state, target_canon, default_canon)
        process_screened_yellow(roots, state)
        finalize_shards(state)
        apply_pending_updates(roots, state)
    finally:
        dedupe.close()
        if runtime.trace_memory:
            current, peak = tracemalloc.get_traced_memory()
            tracemalloc.stop()
            summary["memory_current_mb"] = round(current / 1024 / 1024, 2)
            summary["memory_peak_mb"] = round(peak / 1024 / 1024, 2)
        if profiler is not None:
            profiler.disable()
            if runtime.profile_path:
                profile_path, text_path = write_profile_stats(
                    profiler,
                    runtime.profile_path,
                    runtime.profile_sort,
                )
                summary["profile_path"] = str(profile_path)
                summary["profile_text_path"] = str(text_path)
    summary["counts"] = {
        "written": summary["written"],
        "deduped": summary["deduped"],
        "skipped": summary["skipped"],
    }
    summary["failed_targets"] = []
    summary["finished_at_utc"] = utc_now()
    return summary


def main(*, pipeline_id: str, defaults: RootDefaults) -> None:
    ap = argparse.ArgumentParser(description=f"Merge Worker v{VERSION}")
    ap.add_argument("--targets", required=True, help="targets.yaml")
    ap.add_argument("--execute", action="store_true", help="Write combined shards")
    ap.add_argument("--dataset-root", default=None, help="Override dataset root (raw/screened/combined/_ledger)")
    ap.add_argument("--progress", action="store_true", help="Show merge progress")
    ap.add_argument(
        "--progress-interval",
        type=int,
        default=None,
        help="Log progress every N items when tqdm is unavailable",
    )
    ap.add_argument("--trace-memory", action="store_true", help="Track memory usage with tracemalloc")
    ap.add_argument("--profile-merge", action="store_true", help="Enable cProfile for merge paths")
    ap.add_argument(
        "--profile-path",
        default=None,
        help="Write cProfile stats to path (default: <ledger_root>/merge_profile.prof)",
    )
    ap.add_argument(
        "--profile-sort",
        default=None,
        help="Sort key for profile text output (default: tottime)",
    )
    ap.add_argument(
        "--dedupe-partitions",
        type=int,
        default=None,
        help="Number of SQLite partitions for dedupe index",
    )
    args = ap.parse_args()

    cfg = read_yaml(Path(args.targets), schema_name="targets") or {}
    roots = resolve_roots(cfg, defaults, dataset_root=resolve_dataset_root(args.dataset_root))
    runtime = resolve_merge_runtime(
        cfg,
        progress=args.progress,
        progress_interval=args.progress_interval,
        trace_memory=args.trace_memory,
        profile=args.profile_merge,
        profile_path=args.profile_path,
        profile_sort=args.profile_sort,
        dedupe_partitions=args.dedupe_partitions,
        ledger_root=roots.ledger_root,
    )
    summary = merge_records(cfg, roots, args.execute, pipeline_id=pipeline_id, runtime=runtime)
    write_json(roots.ledger_root / "merge_summary.json", summary)
