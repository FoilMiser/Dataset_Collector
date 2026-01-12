from __future__ import annotations

import argparse
import json
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from datasets import DatasetDict, load_from_disk

from collector_core.__version__ import __version__ as VERSION
from collector_core.artifact_metadata import build_artifact_metadata
from collector_core.stability import stable_api
from collector_core.utils import append_jsonl, ensure_dir, read_jsonl, utc_now, write_json
from collector_core.yellow_screen_common import (
    PitchConfig,
    Roots,
    ScreeningConfig,
    Sharder,
    YellowRootDefaults,
    contains_deny,
    extract_text,
    find_license,
    load_targets_cfg,
    merge_screening_config,
    resolve_dataset_root,
    resolve_pitch_config,
    resolve_roots,
    sharding_cfg,
    sha256_text,
)


@dataclass
class FilterDecision:
    allow: bool
    reason: str | None = None
    text: str | None = None
    license_spdx: str | None = None
    extra: dict[str, Any] | None = None
    sample_extra: dict[str, Any] | None = None


@dataclass
class DomainContext:
    cfg: dict[str, Any]
    roots: Roots
    pitch_cfg: PitchConfig
    screen_cfg: ScreeningConfig
    target_id: str
    target_cfg: dict[str, Any]
    queue_row: dict[str, Any]
    pool: str
    execute: bool
    state: dict[str, Any] = field(default_factory=dict)


@stable_api
def load_signoff(manifest_dir: Path) -> dict[str, Any] | None:
    signoff_path = manifest_dir / "review_signoff.json"
    if not signoff_path.exists():
        return None
    try:
        return json.loads(signoff_path.read_text(encoding="utf-8"))
    except Exception:
        return None


@stable_api
def record_pitch(
    roots: Roots,
    pitch_counts: dict[tuple[str, str], int],
    pitch_cfg: PitchConfig,
    target_id: str,
    reason: str,
    raw: dict[str, Any] | None = None,
    text: str | None = None,
    extra: dict[str, Any] | None = None,
    sample_extra: dict[str, Any] | None = None,
) -> None:
    row = {"target_id": target_id, "reason": reason}
    sample_id = None
    if raw:
        sample_id = raw.get("record_id") or raw.get("id")
        if sample_id:
            row["sample_id"] = sample_id
    if extra:
        row.update(extra)
    append_jsonl(roots.ledger_root / "yellow_pitched.jsonl", [row])

    key = (target_id, reason)
    if pitch_counts.get(key, 0) >= pitch_cfg.sample_limit:
        return
    sample = {"target_id": target_id, "reason": reason}
    if sample_id:
        sample["sample_id"] = sample_id
    if raw:
        source = raw.get("source", {}) or {}
        source_url = source.get("source_url") or raw.get("source_url")
        if source_url:
            sample["source_url"] = source_url
    if text:
        sample["text"] = text[: pitch_cfg.text_limit]
    if sample_extra:
        sample.update(sample_extra)
    append_jsonl(roots.pitches_root / "yellow_pitch.jsonl", [sample])
    pitch_counts[key] = pitch_counts.get(key, 0) + 1


@stable_api
def canonical_record(
    raw: dict[str, Any],
    text: str,
    target_id: str,
    license_profile: str,
    license_spdx: str | None,
) -> dict[str, Any]:
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
            "license_spdx": license_spdx,
            "license_profile": license_profile,
            "license_evidence": source.get("license_evidence", raw.get("license_evidence")),
            "retrieved_at_utc": source.get("retrieved_at_utc", raw.get("retrieved_at_utc")),
        },
        "routing": raw.get("routing") or raw.get("math_routing") or raw.get("route") or {},
        "hash": {"content_sha256": content_hash},
    }


@stable_api
def iter_raw_files(raw_dir: Path) -> list[Path]:
    files: list[Path] = []
    for ext in ("*.jsonl", "*.jsonl.gz", "*.jsonl.zst"):
        files.extend([fp for fp in raw_dir.glob(ext) if fp.is_file()])
    return files


@stable_api
def iter_hf_dataset_dirs(raw_dir: Path) -> list[Path]:
    candidates = []
    for pattern in ("hf_dataset", "split_*"):
        candidates.extend([p for p in raw_dir.rglob(pattern) if p.is_dir()])
    seen = set()
    ordered: list[Path] = []
    for path in sorted(candidates):
        resolved = path.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        ordered.append(path)
    return ordered


@stable_api
def standard_filter(raw: dict[str, Any], ctx: DomainContext) -> FilterDecision:
    text = extract_text(raw, ctx.screen_cfg.text_fields)
    if not text:
        return FilterDecision(allow=False, reason="no_text")
    if len(text) < ctx.screen_cfg.min_chars or len(text) > ctx.screen_cfg.max_chars:
        return FilterDecision(allow=False, reason="length_bounds", text=text)
    lic = find_license(raw, ctx.screen_cfg.license_fields)
    if ctx.screen_cfg.require_record_license and not lic:
        return FilterDecision(allow=False, reason="missing_record_license", text=text)
    if lic and ctx.screen_cfg.allow_spdx and lic not in ctx.screen_cfg.allow_spdx:
        return FilterDecision(
            allow=False,
            reason="license_not_allowlisted",
            text=text,
            license_spdx=lic,
            extra={"license": lic},
            sample_extra={"license": lic},
        )
    if contains_deny(text, ctx.screen_cfg.deny_phrases):
        return FilterDecision(allow=False, reason="deny_phrase", text=text)
    return FilterDecision(allow=True, text=text, license_spdx=lic)


@stable_api
def standard_transform(
    raw: dict[str, Any],
    ctx: DomainContext,
    decision: FilterDecision,
    *,
    license_profile: str,
) -> dict[str, Any] | None:
    if not decision.text:
        return None
    return canonical_record(raw, decision.text, ctx.target_id, license_profile, decision.license_spdx)


def _resolve_pools(roots: Roots, queue_row: dict[str, Any]) -> list[str]:
    pool_dir_base = roots.raw_root / "yellow"
    license_pools = (
        [p.name for p in pool_dir_base.iterdir() if p.is_dir()] if pool_dir_base.exists() else []
    )
    return license_pools or [queue_row.get("license_profile", "quarantine")]


def _resolve_target(cfg: dict[str, Any], target_id: str) -> dict[str, Any]:
    return next((t for t in cfg.get("targets", []) if t.get("id") == target_id), {})


def _domain_callable(domain: Any, name: str, fallback: Callable[..., Any]) -> Callable[..., Any]:
    value = getattr(domain, name, None)
    if value is None:
        return fallback
    return value


def process_target(
    cfg: dict[str, Any],
    roots: Roots,
    queue_row: dict[str, Any],
    execute: bool,
    pitch_cfg: PitchConfig,
    domain: Any,
) -> dict[str, Any]:
    target_id = queue_row["id"]
    target_cfg = _resolve_target(cfg, target_id)
    screen_cfg = merge_screening_config(cfg, target_cfg)
    shard_cfg = sharding_cfg(cfg, "yellow_shard")
    g = cfg.get("globals", {}) or {}
    require_signoff = bool(g.get("require_yellow_signoff", False))
    allow_without_signoff = bool(
        (target_cfg.get("yellow_screen", {}) or {}).get("allow_without_signoff", False)
    )
    manifest_dir = Path(queue_row.get("manifest_dir") or roots.manifests_root / target_id)
    signoff = load_signoff(manifest_dir) or {}
    status = str(signoff.get("status", "") or "").lower()

    passed, pitched = 0, 0
    shard_paths: list[str] = []
    pitch_counts: dict[tuple[str, str], int] = {}
    pitch_reasons: Counter[str] = Counter()

    if require_signoff and not allow_without_signoff:
        if status == "rejected":
            if execute:
                record_pitch(
                    roots,
                    pitch_counts,
                    pitch_cfg,
                    target_id,
                    "yellow_signoff_rejected",
                    sample_extra={"details": f"manifest_dir={manifest_dir}"},
                )
            manifest = {
                "target_id": target_id,
                "passed": passed,
                "pitched": pitched,
                "shards": shard_paths,
                "status": "skipped",
                "reason": "yellow_signoff_rejected",
                "finished_at_utc": utc_now(),
            }
            manifest.update(build_artifact_metadata(written_at_utc=manifest["finished_at_utc"]))
            if execute:
                ensure_dir(roots.manifests_root / target_id)
                write_json(roots.manifests_root / target_id / "yellow_screen_done.json", manifest)
            return manifest
        if status != "approved":
            if execute:
                record_pitch(
                    roots,
                    pitch_counts,
                    pitch_cfg,
                    target_id,
                    "yellow_signoff_missing",
                    sample_extra={"details": f"manifest_dir={manifest_dir}"},
                )
            manifest = {
                "target_id": target_id,
                "passed": passed,
                "pitched": pitched,
                "shards": shard_paths,
                "status": "skipped",
                "reason": "yellow_signoff_missing",
                "finished_at_utc": utc_now(),
            }
            manifest.update(build_artifact_metadata(written_at_utc=manifest["finished_at_utc"]))
            if execute:
                ensure_dir(roots.manifests_root / target_id)
                write_json(roots.manifests_root / target_id / "yellow_screen_done.json", manifest)
            return manifest

    for pool in _resolve_pools(roots, queue_row):
        raw_dir = roots.raw_root / "yellow" / pool / target_id
        if not raw_dir.exists():
            continue
        sharder = Sharder(roots.screened_root / pool / "shards", shard_cfg)
        dedupe_keys: set[str] = set()

        ctx = DomainContext(
            cfg=cfg,
            roots=roots,
            pitch_cfg=pitch_cfg,
            screen_cfg=screen_cfg,
            target_id=target_id,
            target_cfg=target_cfg,
            queue_row=queue_row,
            pool=pool,
            execute=execute,
        )

        domain_preflight = getattr(domain, "domain_preflight", None)
        if callable(domain_preflight):
            domain_preflight(ctx)

        filter_record = _domain_callable(domain, "filter_record", standard_filter)
        transform_record = _domain_callable(domain, "transform_record", standard_transform)
        dedupe_key_fn = getattr(domain, "dedupe_key", None)

        def handle_raw(raw: dict[str, Any]) -> None:
            nonlocal passed, pitched
            decision = filter_record(raw, ctx)
            if not decision.allow:
                pitched += 1
                pitch_reasons[decision.reason or "filtered"] += 1
                if execute:
                    record_pitch(
                        roots,
                        pitch_counts,
                        pitch_cfg,
                        target_id,
                        decision.reason or "filtered",
                        raw=raw,
                        text=decision.text,
                        extra=decision.extra,
                        sample_extra=decision.sample_extra,
                    )
                return
            if callable(dedupe_key_fn):
                key = dedupe_key_fn(raw, ctx, decision)
                if key and key in dedupe_keys:
                    pitched += 1
                    pitch_reasons["duplicate_record"] += 1
                    if execute:
                        record_pitch(
                            roots,
                            pitch_counts,
                            pitch_cfg,
                            target_id,
                            "duplicate_record",
                            raw=raw,
                            text=decision.text,
                        )
                    return
                if key:
                    dedupe_keys.add(key)
            license_profile = str(
                raw.get("license_profile")
                or queue_row.get("license_profile")
                or pool
                or "quarantine"
            )
            record = transform_record(raw, ctx, decision, license_profile=license_profile)
            if not record:
                pitched += 1
                pitch_reasons["transform_failed"] += 1
                if execute:
                    record_pitch(
                        roots,
                        pitch_counts,
                        pitch_cfg,
                        target_id,
                        "transform_failed",
                        raw=raw,
                        text=decision.text,
                    )
                return
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
                    "record_id": record.get("record_id"),
                    "content_sha256": (record.get("hash") or {}).get("content_sha256"),
                    "decision": "pass",
                    "output_shard": current_shard,
                    "seen_at_utc": utc_now(),
                }
                append_jsonl(roots.ledger_root / "yellow_passed.jsonl", [ledger_row])

        for file_path in iter_raw_files(raw_dir):
            for raw in read_jsonl(file_path):
                handle_raw(raw)

        for ds_path in iter_hf_dataset_dirs(raw_dir):
            try:
                dataset_obj = load_from_disk(str(ds_path))
            except Exception as exc:
                pitched += 1
                pitch_reasons["hf_load_failed"] += 1
                if execute:
                    record_pitch(
                        roots,
                        pitch_counts,
                        pitch_cfg,
                        target_id,
                        "hf_load_failed",
                        extra={"path": str(ds_path), "error": str(exc)},
                        sample_extra={"path": str(ds_path)},
                    )
                continue
            datasets = (
                list(dataset_obj.values())
                if isinstance(dataset_obj, DatasetDict)
                else [dataset_obj]
            )
            for dataset in datasets:
                for raw in dataset:
                    handle_raw(dict(raw))

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
        "metrics": {"pitch_reasons": dict(pitch_reasons)},
    }
    manifest.update(build_artifact_metadata(written_at_utc=manifest["finished_at_utc"]))
    if execute:
        ensure_dir(roots.manifests_root / target_id)
        write_json(roots.manifests_root / target_id / "yellow_screen_done.json", manifest)
    return manifest


@stable_api
def run_yellow_screen(*, defaults: YellowRootDefaults, domain: Any) -> None:
    ap = argparse.ArgumentParser(description=f"Yellow Screen Worker v{VERSION}")
    ap.add_argument("--targets", required=True, help="Path to targets.yaml")
    ap.add_argument("--queue", required=True, help="YELLOW queue JSONL")
    ap.add_argument("--execute", action="store_true", help="Write outputs (default: dry-run)")
    ap.add_argument(
        "--dataset-root",
        default=None,
        help="Override dataset root (raw/screened/_ledger/_pitches/_manifests)",
    )
    ap.add_argument(
        "--allow-data-root",
        action="store_true",
        help="Allow /data defaults for outputs (default: disabled).",
    )
    ap.add_argument(
        "--pitch-sample-limit",
        type=int,
        default=None,
        help="Max pitch samples per reason (override)",
    )
    ap.add_argument(
        "--pitch-text-limit",
        type=int,
        default=None,
        help="Max chars stored in pitch samples (override)",
    )
    args = ap.parse_args()

    targets_path = Path(args.targets).expanduser().resolve()
    cfg = load_targets_cfg(targets_path)
    pitch_cfg = resolve_pitch_config(cfg, args.pitch_sample_limit, args.pitch_text_limit)
    roots = resolve_roots(
        cfg,
        defaults,
        dataset_root=resolve_dataset_root(args.dataset_root),
        allow_data_root=args.allow_data_root,
    )
    ensure_dir(roots.screened_root)
    ensure_dir(roots.ledger_root)
    ensure_dir(roots.pitches_root)

    queue_rows = read_jsonl(Path(args.queue))
    queue_rows = [r for r in queue_rows if r.get("enabled", True) and r.get("id")]

    summary = {
        "run_at_utc": utc_now(),
        "targets_seen": len(queue_rows),
        "execute": args.execute,
        "results": [],
    }
    summary.update(build_artifact_metadata(written_at_utc=summary["run_at_utc"]))

    for row in queue_rows:
        res = process_target(cfg, roots, row, args.execute, pitch_cfg, domain)
        summary["results"].append(res)

    status_counts = Counter(result.get("status") or "unknown" for result in summary["results"])
    summary["counts"] = {"total": len(summary["results"]), **dict(status_counts)}
    summary["failed_targets"] = [
        {"id": result.get("target_id", "unknown"), "error": result.get("reason", "unknown")}
        for result in summary["results"]
        if result.get("status") != "ok"
    ]

    write_json(roots.ledger_root / "yellow_screen_summary.json", summary)


__all__ = [
    "FilterDecision",
    "DomainContext",
    "canonical_record",
    "iter_raw_files",
    "iter_hf_dataset_dirs",
    "process_target",
    "record_pitch",
    "run_yellow_screen",
    "standard_filter",
    "standard_transform",
]
