from __future__ import annotations

import argparse
import cProfile
import gzip
import importlib.util
import json
import pstats
import tracemalloc
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any

from collector_core.__version__ import __version__ as VERSION
from collector_core.artifact_metadata import build_artifact_metadata
from collector_core.config_validator import read_yaml
from collector_core.dataset_root import ensure_data_root_allowed, resolve_dataset_root
from collector_core.merge.contract import (
    canonicalize_row,
    normalize_record,
    resolve_canonicalize_config,
)
from collector_core.merge.dedupe import (
    build_dedupe_index,
    build_dedupe_update,
    merge_provenance_update,
    merge_update_payload,
)
from collector_core.merge.hf import iter_hf_dataset_dirs, iter_hf_inputs
from collector_core.merge.shard import Sharder, ensure_shard_dir, sharding_cfg
from collector_core.merge.types import (
    GreenInput,
    GreenSkip,
    MergeRuntimeConfig,
    MergeState,
    RootDefaults,
    Roots,
)
from collector_core.utils.io import append_jsonl, read_jsonl, write_json
from collector_core.utils.logging import utc_now
from collector_core.utils.paths import ensure_dir

if importlib.util.find_spec("tqdm"):
    from tqdm import tqdm as tqdm_progress
else:  # pragma: no cover - optional dependency
    tqdm_progress = None


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


DEFAULT_MAX_SOURCE_URLS = 10
DEFAULT_MAX_DUPLICATES = 20


def resolve_roots(
    cfg: dict[str, Any],
    defaults: RootDefaults,
    dataset_root: Path | None = None,
    *,
    allow_data_root: bool = False,
) -> Roots:
    dataset_root = dataset_root or resolve_dataset_root()
    if dataset_root:
        defaults = RootDefaults(
            raw_root=str(dataset_root / "raw"),
            screened_root=str(dataset_root / "screened_yellow"),
            combined_root=str(dataset_root / "combined"),
            ledger_root=str(dataset_root / "_ledger"),
        )
    g = cfg.get("globals", {}) or {}
    roots = Roots(
        raw_root=Path(g.get("raw_root", defaults.raw_root)).expanduser().resolve(),
        screened_root=Path(g.get("screened_yellow_root", defaults.screened_root))
        .expanduser()
        .resolve(),
        combined_root=Path(g.get("combined_root", defaults.combined_root)).expanduser().resolve(),
        ledger_root=Path(g.get("ledger_root", defaults.ledger_root)).expanduser().resolve(),
    )
    ensure_data_root_allowed(
        [roots.raw_root, roots.screened_root, roots.combined_root, roots.ledger_root],
        allow_data_root,
    )
    return roots


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
    g = cfg.get("globals", {}) or {}
    g_merge = g.get("merge", {}) or {}
    resolved_progress = bool(progress if progress is not None else g_merge.get("progress", False))
    interval_value = (
        progress_interval
        if progress_interval is not None
        else g_merge.get("progress_interval", 10000)
    )
    resolved_interval = max(int(interval_value or 10000), 1)
    resolved_trace = bool(
        trace_memory if trace_memory is not None else g_merge.get("trace_memory", False)
    )
    resolved_profile = bool(profile if profile is not None else g_merge.get("profile", False))
    resolved_profile_sort = str(profile_sort or g_merge.get("profile_sort", "tottime"))
    partitions_value = (
        dedupe_partitions if dedupe_partitions is not None else g_merge.get("dedupe_partitions", 1)
    )
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
            jsonl_files.extend(
                sorted([fp for fp in target_dir.rglob("*.jsonl.gz") if fp.is_file()])
            )
            jsonl_set = {fp.resolve() for fp in jsonl_files}
            for fp in jsonl_files:
                for raw in read_jsonl(fp):
                    yield GreenInput(raw, target_id, pool_dir.name, fp, "jsonl")

            hf_dirs = iter_hf_dataset_dirs(target_dir)
            hf_dir_set = {p.resolve() for p in hf_dirs}
            for item in iter_hf_inputs(hf_dirs, target_id=target_id, pool=pool_dir.name):
                yield item

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
            "dataset_id": (target.get("download", {}) or {}).get("dataset_id")
            or target.get("dataset_id"),
            "config": (target.get("download", {}) or {}).get("config"),
        }
        for target in (cfg.get("targets", []) or [])
        if target.get("id") is not None
    }


def build_target_canon(
    cfg: dict[str, Any],
) -> tuple[dict[str, tuple[list[str], int | None]], tuple[list[str], int | None]]:
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
        ensure_shard_dir(sharder)
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
    record = normalize_record(
        rec,
        target_id=resolved_target,
        pool=pool_value,
        pipeline_id=state.pipeline_id,
        target_meta=state.target_meta.get(resolved_target, {}),
        context=f"{source_kind}/{resolved_target}",
    )
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
        handle_record(
            canonical, item.source_kind, item.source_path, roots, state, item.target_id, item.pool
        )


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
        with (
            opener(path, "rt", encoding="utf-8", errors="ignore") as src,
            opener(
                temp_path,
                "wt",
                encoding="utf-8",
            ) as dst,
        ):
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
    summary.update(build_artifact_metadata(written_at_utc=summary["finished_at_utc"]))
    return summary


def main(*, pipeline_id: str, defaults: RootDefaults) -> None:
    ap = argparse.ArgumentParser(description=f"Merge Worker v{VERSION}")
    ap.add_argument("--targets", required=True, help="targets.yaml")
    ap.add_argument("--execute", action="store_true", help="Write combined shards")
    ap.add_argument(
        "--dataset-root", default=None, help="Override dataset root (raw/screened/combined/_ledger)"
    )
    ap.add_argument(
        "--allow-data-root",
        action="store_true",
        help="Allow /data defaults for outputs (default: disabled).",
    )
    ap.add_argument("--progress", action="store_true", help="Show merge progress")
    ap.add_argument(
        "--progress-interval",
        type=int,
        default=None,
        help="Log progress every N items when tqdm is unavailable",
    )
    ap.add_argument(
        "--trace-memory", action="store_true", help="Track memory usage with tracemalloc"
    )
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
    roots = resolve_roots(
        cfg,
        defaults,
        dataset_root=resolve_dataset_root(args.dataset_root),
        allow_data_root=args.allow_data_root,
    )
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
