#!/usr/bin/env python3
"""
yellow_screen_worker.py (chem v2.0)

Converts raw YELLOW acquisitions into canonical JSONL shards with chemistry-aware
screening plugins. Outputs:
  - screened_yellow/{license_pool}/shards/yellow_shard_00000.jsonl.gz
  - _ledger/yellow_passed.jsonl (accepted rows)
  - _ledger/yellow_pitched.jsonl (pitched rows)
  - _pitches/yellow_pitch.jsonl (pitched samples)
  - _manifests/{target_id}/yellow_screen_done.json
"""

from __future__ import annotations

import argparse
import dataclasses
import gzip
import hashlib
import json
import re
from collections import Counter
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any

from datasets import DatasetDict, load_from_disk

from collector_core.__version__ import __version__ as VERSION
from collector_core.artifact_metadata import build_artifact_metadata
from collector_core.companion_files import read_field_schemas, resolve_companion_paths
from collector_core.config_validator import read_yaml
from collector_core.dataset_root import ensure_data_root_allowed, resolve_dataset_root
from collector_core.utils import ensure_dir, read_jsonl, utc_now, write_json, write_jsonl
from collector_core.yellow_screen_common import PitchConfig, resolve_pitch_config


def sha256_text(text: str) -> str:
    norm = re.sub(r"\s+", " ", (text or "").strip())
    return hashlib.sha256(norm.encode("utf-8")).hexdigest()


def append_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    ensure_dir(path.parent)
    mode = "at" if path.suffix != ".gz" else "ab"
    if path.suffix == ".gz":
        with gzip.open(path, mode) as f:  # type: ignore
            for row in rows:
                f.write((json.dumps(row, ensure_ascii=False) + "\n").encode("utf-8"))
    else:
        with open(path, mode, encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")


def load_yaml(path: Path, schema_name: str | None = None) -> dict[str, Any]:
    return read_yaml(path, schema_name=schema_name) or {}


# ------------------------------
# Field schemas (lightweight)
# ------------------------------


@dataclasses.dataclass
class FieldSpec:
    name: str
    field_type: str
    required: bool = False
    nullable: bool = True
    validation: dict[str, Any] = dataclasses.field(default_factory=dict)


def load_field_schemas(paths: list[Path]) -> dict[str, dict[str, FieldSpec]]:
    schemas: dict[str, dict[str, FieldSpec]] = {}
    for schema_name, schema_def in read_field_schemas(paths).items():
        fields: dict[str, FieldSpec] = {}
        for fname, fdef in (schema_def.get("fields") or {}).items():
            fields[fname] = FieldSpec(
                name=fname,
                field_type=fdef.get("type", "string"),
                required=fdef.get("required", False),
                nullable=fdef.get("nullable", True),
                validation=fdef.get("validation", {}) or {},
            )
        schemas[schema_name] = fields
    return schemas


def cast_value(value: str, field_type: str, validation: dict[str, Any]) -> Any:
    if value is None or str(value).strip() == "":
        return None
    val = str(value).strip()
    try:
        if field_type == "integer":
            res = int(float(val))
            if "min" in validation and res < validation["min"]:
                return None
            if "max" in validation and res > validation["max"]:
                return None
            return res
        if field_type == "float":
            res_f = float(val)
            if "min" in validation and res_f < validation["min"]:
                return None
            if "max" in validation and res_f > validation["max"]:
                return None
            return res_f
        if field_type == "boolean":
            return val.lower() in {"true", "1", "yes"}
        if field_type == "string":
            if "max_length" in validation and len(val) > validation["max_length"]:
                val = val[: validation["max_length"]]
            pattern = validation.get("pattern")
            if pattern and not re.match(pattern, val):
                return None
            return val
    except Exception:
        return None
    return val


def validate_record(record: dict[str, Any], schema: dict[str, FieldSpec]) -> tuple[bool, list[str]]:
    errors: list[str] = []
    for name, spec in schema.items():
        val = record.get(name)
        if val is None:
            if spec.required and not spec.nullable:
                errors.append(f"{name}:required")
            continue
        if not spec.nullable and val is None:
            errors.append(f"{name}:null")
    return (not errors, errors)


# ------------------------------
# Routing helpers
# ------------------------------


def resolve_routing(raw: dict[str, Any], queue_row: dict[str, Any]) -> dict[str, Any]:
    rt = (raw.get("routing") or raw.get("chem_routing") or raw.get("math_routing") or {}) or {}
    routing = {
        "subject": rt.get("subject") or queue_row.get("routing_subject") or "chem",
        "domain": rt.get("domain") or queue_row.get("routing_domain") or "misc",
        "category": rt.get("category") or queue_row.get("routing_category") or "misc",
        "level": rt.get("level") or queue_row.get("routing_level") or 5,
        "granularity": rt.get("granularity") or queue_row.get("routing_granularity") or "target",
        "confidence": rt.get("confidence") or queue_row.get("routing_confidence"),
        "reason": rt.get("reason") or queue_row.get("routing_reason"),
    }
    routing["level"] = int(routing["level"]) if routing.get("level") is not None else None
    return routing


def canonical_record(
    raw: dict[str, Any],
    text: str,
    target_id: str,
    license_profile: str,
    license_spdx: str | None,
    routing: dict[str, Any],
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
        "routing": routing,
        "hash": {"content_sha256": content_hash},
    }


def iter_raw_files(raw_dir: Path) -> Iterator[Path]:
    for ext in ("*.jsonl", "*.jsonl.gz"):
        yield from raw_dir.glob(ext)


def iter_hf_dataset_dirs(raw_dir: Path) -> Iterator[Path]:
    candidates = []
    for pattern in ("hf_dataset", "split_*"):
        candidates.extend([p for p in raw_dir.rglob(pattern) if p.is_dir()])
    seen = set()
    for path in sorted(candidates):
        resolved = path.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        yield path


def iter_text_files(raw_dir: Path) -> Iterator[Path]:
    for ext in ("*.txt", "*.txt.gz"):
        yield from raw_dir.glob(ext)


# ------------------------------
# PubChem helpers
# ------------------------------


def iter_sdf_records_from_gz(path: Path) -> Iterator[str]:
    buffer: list[str] = []
    opener = gzip.open if path.suffix.endswith("gz") else open
    with opener(path, "rt", encoding="utf-8", errors="ignore") as f:
        for line in f:
            if line.strip() == "$$$$":
                if buffer:
                    yield "".join(buffer)
                    buffer = []
            else:
                buffer.append(line)
        if buffer:
            yield "".join(buffer)


TAG_RE = re.compile(r"^> *<(?P<key>[^>]+)>")


def parse_sdf_tags(record: str) -> dict[str, str]:
    lines = record.splitlines()
    tags: dict[str, str] = {}
    i = 0
    while i < len(lines):
        m = TAG_RE.match(lines[i].strip())
        if m:
            key = m.group("key").strip()
            i += 1
            vals: list[str] = []
            while i < len(lines):
                if TAG_RE.match(lines[i].strip()) or lines[i].strip() == "":
                    break
                vals.append(lines[i])
                i += 1
            tags[key] = "\n".join(vals).strip()
        else:
            i += 1
    return tags


# ------------------------------
# Config dataclasses
# ------------------------------


@dataclasses.dataclass
class Roots:
    raw_root: Path
    screened_root: Path
    manifests_root: Path
    ledger_root: Path
    pitches_root: Path


@dataclasses.dataclass
class ScreeningConfig:
    text_fields: list[str]
    license_fields: list[str]
    allow_spdx: list[str]
    deny_phrases: list[str]
    require_record_license: bool
    min_chars: int
    max_chars: int


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
        self.current_rows: list[dict[str, Any]] = []

    def _next_path(self) -> Path:
        suffix = "jsonl.gz" if self.cfg.compression == "gzip" else "jsonl"
        name = f"{self.cfg.prefix}_{self.shard_idx:05d}.{suffix}"
        return self.base_dir / name

    def add(self, row: dict[str, Any]) -> Path | None:
        self.current_rows.append(row)
        self.count += 1
        if len(self.current_rows) >= self.cfg.max_records_per_shard:
            path = self.flush()
            self.shard_idx += 1
            return path
        return None

    def flush(self) -> Path | None:
        if not self.current_rows:
            return None
        path = self._next_path()
        write_jsonl(path, self.current_rows)
        self.current_rows = []
        return path


@dataclasses.dataclass
class ScreenContext:
    cfg: dict[str, Any]
    roots: Roots
    queue_row: dict[str, Any]
    target_cfg: dict[str, Any]
    screen_cfg: ScreeningConfig
    shard_cfg: ShardingConfig
    execute: bool
    field_schemas: dict[str, dict[str, FieldSpec]]
    pitch_counts: dict[tuple[str, str], int]
    pitch_cfg: PitchConfig


def load_targets_cfg(path: Path) -> dict[str, Any]:
    return read_yaml(path, schema_name="targets") or {}


def load_signoff(manifest_dir: Path) -> dict[str, Any] | None:
    signoff_path = manifest_dir / "review_signoff.json"
    if not signoff_path.exists():
        return None
    try:
        return json.loads(signoff_path.read_text(encoding="utf-8"))
    except Exception:
        return None


def resolve_roots(
    cfg: dict[str, Any], dataset_root: Path | None = None, *, allow_data_root: bool = False
) -> Roots:
    dataset_root = dataset_root or resolve_dataset_root()
    default_raw = dataset_root / "raw" if dataset_root else Path("/data/chem/raw")
    default_screened = (
        dataset_root / "screened_yellow" if dataset_root else Path("/data/chem/screened_yellow")
    )
    default_manifests = (
        dataset_root / "_manifests" if dataset_root else Path("/data/chem/_manifests")
    )
    default_ledger = dataset_root / "_ledger" if dataset_root else Path("/data/chem/_ledger")
    default_pitches = dataset_root / "_pitches" if dataset_root else Path("/data/chem/_pitches")
    g = cfg.get("globals", {}) or {}
    roots = Roots(
        raw_root=Path(g.get("raw_root", default_raw)).expanduser().resolve(),
        screened_root=Path(g.get("screened_yellow_root", default_screened)).expanduser().resolve(),
        manifests_root=Path(g.get("manifests_root", default_manifests)).expanduser().resolve(),
        ledger_root=Path(g.get("ledger_root", default_ledger)).expanduser().resolve(),
        pitches_root=Path(g.get("pitches_root", default_pitches)).expanduser().resolve(),
    )
    ensure_data_root_allowed(
        [
            roots.raw_root,
            roots.screened_root,
            roots.manifests_root,
            roots.ledger_root,
            roots.pitches_root,
        ],
        allow_data_root,
    )
    return roots


def merge_screening_config(cfg: dict[str, Any], target: dict[str, Any]) -> ScreeningConfig:
    g = cfg.get("globals", {}) or {}
    g_screen = g.get("screening", {}) or {}
    g_canon = g.get("canonicalize", {}) or {}
    t_screen = target.get("yellow_screen", {}) or {}
    t_canon = target.get("canonicalize", {}) or {}
    return ScreeningConfig(
        text_fields=list(
            t_canon.get("text_field_candidates")
            or t_screen.get("text_field_candidates")
            or g_canon.get("text_field_candidates")
            or g_screen.get("text_field_candidates")
            or ["text"]
        ),
        license_fields=list(
            t_screen.get("record_license_field_candidates")
            or g_screen.get("record_license_field_candidates")
            or ["license", "license_spdx"]
        ),
        allow_spdx=list(t_screen.get("allow_spdx") or g_screen.get("allow_spdx") or []),
        deny_phrases=[
            p.lower() for p in (t_screen.get("deny_phrases") or g_screen.get("deny_phrases") or [])
        ],
        require_record_license=bool(
            t_screen.get("require_record_license", g_screen.get("require_record_license", False))
        ),
        min_chars=int(t_screen.get("min_chars", g_screen.get("min_chars", 200))),
        max_chars=int(
            t_canon.get(
                "max_chars",
                t_screen.get(
                    "max_chars", g_canon.get("max_chars", g_screen.get("max_chars", 12000))
                ),
            )
        ),
    )


def sharding_cfg(cfg: dict[str, Any], prefix: str) -> ShardingConfig:
    g = cfg.get("globals", {}).get("sharding", {}) or {}
    return ShardingConfig(
        max_records_per_shard=int(g.get("max_records_per_shard", 50000)),
        compression=str(g.get("compression", "gzip")),
        prefix=prefix,
    )


def find_text(row: dict[str, Any], candidates: list[str]) -> str | None:
    for k in candidates:
        if k in row and row[k]:
            val = row[k]
            if isinstance(val, (list, tuple)):
                val = "\n".join(map(str, val))
            return str(val)
    return None


def extract_text(row: dict[str, Any], candidates: list[str]) -> str | None:
    if row.get("text"):
        val = row["text"]
        if isinstance(val, (list, tuple)):
            val = "\n".join(map(str, val))
        return str(val)
    remaining = [c for c in candidates if c != "text"]
    text = find_text(row, remaining)
    if text:
        return text
    string_fields = [str(v) for v in row.values() if isinstance(v, str) and v]
    if string_fields:
        return "\n".join(string_fields)
    try:
        return json.dumps(row, ensure_ascii=False)
    except Exception:
        return str(row)


def find_license(row: dict[str, Any], candidates: list[str]) -> str | None:
    for k in candidates:
        if k in row and row[k]:
            return str(row[k])
    return None


def contains_deny(text: str, phrases: list[str]) -> bool:
    low = text.lower()
    return any(p in low for p in phrases)


def log_pitch(
    ctx: ScreenContext,
    reason: str,
    sample_id: str | None = None,
    text: str | None = None,
    extra: dict[str, Any] | None = None,
    sample_extra: dict[str, Any] | None = None,
) -> None:
    if not ctx.execute:
        return
    row = {
        "stage": "yellow_screen",
        "target_id": ctx.queue_row["id"],
        "reason": reason,
        "seen_at_utc": utc_now(),
    }
    if sample_id:
        row["sample_id"] = sample_id
    if extra:
        row.update(extra)
    append_jsonl(ctx.roots.ledger_root / "yellow_pitched.jsonl", [row])

    key = (ctx.queue_row["id"], reason)
    if ctx.pitch_counts.get(key, 0) >= ctx.pitch_cfg.sample_limit:
        return
    sample = {"target_id": ctx.queue_row["id"], "reason": reason}
    if sample_id:
        sample["sample_id"] = sample_id
    if text:
        sample["text"] = text[: ctx.pitch_cfg.text_limit]
    if sample_extra:
        sample.update(sample_extra)
    append_jsonl(ctx.roots.pitches_root / "yellow_pitch.jsonl", [sample])
    ctx.pitch_counts[key] = ctx.pitch_counts.get(key, 0) + 1


def log_pass(ctx: ScreenContext, rec: dict[str, Any], shard_path: Path) -> None:
    if not ctx.execute:
        return
    append_jsonl(
        ctx.roots.ledger_root / "yellow_passed.jsonl",
        [
            {
                "stage": "yellow_screen",
                "target_id": ctx.queue_row["id"],
                "record_id": rec["record_id"],
                "content_sha256": rec["hash"]["content_sha256"],
                "decision": "pass",
                "output_shard": str(shard_path),
                "seen_at_utc": utc_now(),
            }
        ],
    )


def flush_sharder(sharder: Sharder, shard_paths: list[str]) -> None:
    flushed = sharder.flush()
    if flushed:
        shard_paths.append(str(flushed))


def screen_jsonl_mode(ctx: ScreenContext) -> dict[str, Any]:
    target_id = ctx.queue_row["id"]
    pool_dir_base = ctx.roots.raw_root / "yellow"
    license_pools = (
        [p.name for p in pool_dir_base.iterdir() if p.is_dir()] if pool_dir_base.exists() else []
    )
    pools = license_pools or [ctx.queue_row.get("license_profile", "quarantine")]

    passed, pitched = 0, 0
    shard_paths: list[str] = []

    for pool in pools:
        raw_dir = pool_dir_base / pool / target_id
        if not raw_dir.exists():
            continue
        sharder = Sharder(ctx.roots.screened_root / pool / "shards", ctx.shard_cfg)

        def handle_raw(
            raw: dict[str, Any],
            *,
            pool: str = pool,
            sharder: Sharder = sharder,
        ) -> None:
            nonlocal passed, pitched
            text = extract_text(raw, ctx.screen_cfg.text_fields)
            if not text:
                pitched += 1
                log_pitch(ctx, "no_text", raw.get("id"))
                return
            if len(text) < ctx.screen_cfg.min_chars or len(text) > ctx.screen_cfg.max_chars:
                pitched += 1
                log_pitch(ctx, "length_bounds", raw.get("id"), text=text)
                return
            lic = find_license(raw, ctx.screen_cfg.license_fields)
            if ctx.screen_cfg.require_record_license and not lic:
                pitched += 1
                log_pitch(ctx, "missing_record_license", raw.get("id"))
                return
            if lic and ctx.screen_cfg.allow_spdx and lic not in ctx.screen_cfg.allow_spdx:
                pitched += 1
                log_pitch(
                    ctx, "license_not_allowlisted", raw.get("id"), sample_extra={"license": lic}
                )
                return
            if contains_deny(text, ctx.screen_cfg.deny_phrases):
                pitched += 1
                log_pitch(ctx, "deny_phrase", raw.get("id"), text=text)
                return
            license_profile = str(
                raw.get("license_profile")
                or ctx.queue_row.get("license_profile")
                or pool
                or "quarantine"
            )
            routing = resolve_routing(raw, ctx.queue_row)
            rec = canonical_record(raw, text, target_id, license_profile, lic, routing)
            passed += 1
            if ctx.execute:
                current_shard = sharder._next_path()
                path = sharder.add(rec)
                log_pass(ctx, rec, path or current_shard)
                if path:
                    shard_paths.append(str(path))

        for file_path in iter_raw_files(raw_dir):
            for raw in read_jsonl(file_path):
                handle_raw(raw)

        for ds_path in iter_hf_dataset_dirs(raw_dir):
            try:
                dataset_obj = load_from_disk(str(ds_path))
            except Exception as exc:
                pitched += 1
                log_pitch(
                    ctx, "hf_load_failed", sample_extra={"path": str(ds_path), "error": str(exc)}
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

        if ctx.execute:
            flush_sharder(sharder, shard_paths)

    return {
        "target_id": target_id,
        "passed": passed,
        "pitched": pitched,
        "shards": shard_paths,
        "status": "ok",
        "finished_at_utc": utc_now(),
    }


def screen_pubchem_computed_only(ctx: ScreenContext) -> dict[str, Any]:
    target_id = ctx.queue_row["id"]
    ys_cfg = ctx.target_cfg.get("yellow_screen") or {}
    include_globs = ys_cfg.get("input_glob", "*.sdf.gz")
    include_fields = ys_cfg.get("include_fields") or [
        "PUBCHEM_COMPOUND_CID",
        "PUBCHEM_IUPAC_INCHIKEY",
        "PUBCHEM_CACTVS_CANONICAL_SMILES",
    ]
    output_pool = ys_cfg.get("output_pool") or "permissive"
    field_schema_version = ys_cfg.get("field_schema_version")
    schema = ctx.field_schemas.get(field_schema_version) if field_schema_version else None

    raw_pool = ys_cfg.get("input_pool") or ctx.queue_row.get("license_profile") or "quarantine"
    raw_dir = ctx.roots.raw_root / "yellow" / raw_pool / target_id
    sharder = Sharder(ctx.roots.screened_root / output_pool / "shards", ctx.shard_cfg)
    passed, pitched = 0, 0
    shard_paths: list[str] = []

    files = sorted(raw_dir.glob(include_globs))
    limit_files = ys_cfg.get("limit_files")
    if limit_files:
        files = files[: int(limit_files)]

    for path in files:
        for raw_rec in iter_sdf_records_from_gz(path):
            tags = parse_sdf_tags(raw_rec)
            record: dict[str, Any] = {}
            for f in include_fields:
                val = tags.get(f)
                if schema and f in schema:
                    record[f] = cast_value(val, schema[f].field_type, schema[f].validation)
                else:
                    record[f] = val
                if schema:
                    valid, _ = validate_record(record, schema)
                    if not valid:
                        pitched += 1
                        log_pitch(
                            ctx, "schema_validation_failed", str(record.get("PUBCHEM_COMPOUND_CID"))
                        )
                        continue
            cid = record.get("PUBCHEM_COMPOUND_CID")
            if cid is None:
                pitched += 1
                log_pitch(ctx, "missing_cid")
                continue
            text_parts = [f"{k}: {v}" for k, v in record.items() if v not in (None, "")]
            text = "\n".join(text_parts).strip()
            if not text:
                pitched += 1
                log_pitch(ctx, "empty_text", str(cid))
                continue
            base_raw = {"record_id": str(cid), "source": {"origin": "pubchem_computed_only"}}
            routing = resolve_routing(base_raw, ctx.queue_row)
            rec = canonical_record(
                base_raw, text, target_id, output_pool, ctx.queue_row.get("resolved_spdx"), routing
            )
            passed += 1
            if ctx.execute:
                current_shard = sharder._next_path()
                path_out = sharder.add(rec)
                log_pass(ctx, rec, path_out or current_shard)
                if path_out:
                    shard_paths.append(str(path_out))
    if ctx.execute:
        flush_sharder(sharder, shard_paths)

    return {
        "target_id": target_id,
        "passed": passed,
        "pitched": pitched,
        "shards": shard_paths,
        "status": "ok",
        "finished_at_utc": utc_now(),
    }


def screen_pmc_oa(ctx: ScreenContext) -> dict[str, Any]:
    target_id = ctx.queue_row["id"]
    ys_cfg = ctx.target_cfg.get("yellow_screen") or {}
    raw_pool = ys_cfg.get("input_pool") or ctx.queue_row.get("license_profile") or "quarantine"
    output_pool = ys_cfg.get("output_pool") or raw_pool
    raw_dir = ctx.roots.raw_root / "yellow" / raw_pool / target_id
    sharder = Sharder(ctx.roots.screened_root / output_pool / "shards", ctx.shard_cfg)
    passed, pitched = 0, 0
    shard_paths: list[str] = []

    for file_path in list(iter_raw_files(raw_dir)) + list(iter_text_files(raw_dir)):
        if file_path.suffix in {".jsonl", ".gz"}:
            iterator = read_jsonl(file_path)
            for raw in iterator:
                text = extract_text(raw, ctx.screen_cfg.text_fields)
                if not text:
                    pitched += 1
                    log_pitch(ctx, "no_text", raw.get("id"))
                    continue
                routing = resolve_routing(raw, ctx.queue_row)
                lic = find_license(raw, ctx.screen_cfg.license_fields) or ctx.queue_row.get(
                    "resolved_spdx"
                )
                rec = canonical_record(raw, text, target_id, output_pool, lic, routing)
                passed += 1
                if ctx.execute:
                    current_shard = sharder._next_path()
                    path_out = sharder.add(rec)
                    log_pass(ctx, rec, path_out or current_shard)
                    if path_out:
                        shard_paths.append(str(path_out))
        else:
            opener = gzip.open if file_path.suffix == ".gz" else open
            with opener(file_path, "rt", encoding="utf-8", errors="ignore") as f:
                text = f.read()
            if not text or len(text) < ctx.screen_cfg.min_chars:
                pitched += 1
                log_pitch(
                    ctx, "empty_text_file", file_path.name, sample_extra={"file": file_path.name}
                )
                continue
            raw = {
                "record_id": sha256_text(f"{target_id}:{file_path.name}"),
                "source": {"origin": "pmc_oa_file", "source_url": str(file_path)},
            }
            routing = resolve_routing(raw, ctx.queue_row)
            rec = canonical_record(
                raw, text, target_id, output_pool, ctx.queue_row.get("resolved_spdx"), routing
            )
            passed += 1
            if ctx.execute:
                current_shard = sharder._next_path()
                path_out = sharder.add(rec)
                log_pass(ctx, rec, path_out or current_shard)
                if path_out:
                    shard_paths.append(str(path_out))
    if ctx.execute:
        flush_sharder(sharder, shard_paths)

    return {
        "target_id": target_id,
        "passed": passed,
        "pitched": pitched,
        "shards": shard_paths,
        "status": "ok",
        "finished_at_utc": utc_now(),
    }


MODE_HANDLERS = {
    "jsonl": screen_jsonl_mode,
    "pubchem_computed_only": screen_pubchem_computed_only,
    "pmc_oa": screen_pmc_oa,
}


def process_target(ctx: ScreenContext) -> dict[str, Any]:
    g = ctx.cfg.get("globals", {}) or {}
    require_signoff = bool(g.get("require_yellow_signoff", False))
    allow_without_signoff = bool(
        (ctx.target_cfg.get("yellow_screen", {}) or {}).get("allow_without_signoff", False)
    )
    manifest_dir = Path(
        ctx.queue_row.get("manifest_dir") or ctx.roots.manifests_root / ctx.queue_row["id"]
    )
    signoff = load_signoff(manifest_dir) or {}
    status = str(signoff.get("status", "") or "").lower()
    if require_signoff and not allow_without_signoff:
        if status == "rejected":
            log_pitch(
                ctx,
                "yellow_signoff_rejected",
                sample_extra={"details": f"manifest_dir={manifest_dir}"},
            )
            manifest = {
                "target_id": ctx.queue_row["id"],
                "passed": 0,
                "pitched": 0,
                "shards": [],
                "status": "skipped",
                "reason": "yellow_signoff_rejected",
                "finished_at_utc": utc_now(),
            }
            manifest.update(build_artifact_metadata(written_at_utc=manifest["finished_at_utc"]))
            return manifest
        if status != "approved":
            log_pitch(
                ctx,
                "yellow_signoff_missing",
                sample_extra={"details": f"manifest_dir={manifest_dir}"},
            )
            manifest = {
                "target_id": ctx.queue_row["id"],
                "passed": 0,
                "pitched": 0,
                "shards": [],
                "status": "skipped",
                "reason": "yellow_signoff_missing",
                "finished_at_utc": utc_now(),
            }
            manifest.update(build_artifact_metadata(written_at_utc=manifest["finished_at_utc"]))
            return manifest
    mode = (ctx.target_cfg.get("yellow_screen", {}) or {}).get("mode", "jsonl")
    handler = MODE_HANDLERS.get(mode, screen_jsonl_mode)
    manifest = handler(ctx)
    manifest.update(build_artifact_metadata(written_at_utc=manifest["finished_at_utc"]))
    return manifest


def main() -> None:
    ap = argparse.ArgumentParser(description=f"Yellow Screen Worker v{VERSION}")
    ap.add_argument("--targets", required=True, help="Path to targets_chem.yaml")
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
        dataset_root=resolve_dataset_root(args.dataset_root),
        allow_data_root=args.allow_data_root,
    )
    ensure_dir(roots.screened_root)
    ensure_dir(roots.ledger_root)
    ensure_dir(roots.pitches_root)

    companion = cfg.get("companion_files") or {}
    field_schemas_paths = resolve_companion_paths(
        targets_path, companion.get("field_schemas"), "./field_schemas.yaml"
    )
    schemas: dict[str, dict[str, FieldSpec]] = {}
    if field_schemas_paths:
        schemas = load_field_schemas(field_schemas_paths)

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
        target_cfg = next((t for t in cfg.get("targets", []) if t.get("id") == row.get("id")), {})
        ctx = ScreenContext(
            cfg=cfg,
            roots=roots,
            queue_row=row,
            target_cfg=target_cfg,
            screen_cfg=merge_screening_config(cfg, target_cfg),
            shard_cfg=sharding_cfg(cfg, "yellow_shard"),
            execute=args.execute,
            field_schemas=schemas,
            pitch_counts={},
            pitch_cfg=pitch_cfg,
        )
        res = process_target(ctx)
        summary["results"].append(res)
        if args.execute:
            ensure_dir(roots.manifests_root / row["id"])
            write_json(roots.manifests_root / row["id"] / "yellow_screen_done.json", res)

    status_counts = Counter(result.get("status") or "unknown" for result in summary["results"])
    summary["counts"] = {"total": len(summary["results"]), **dict(status_counts)}
    summary["failed_targets"] = [
        {"id": result.get("target_id", "unknown"), "error": result.get("reason", "unknown")}
        for result in summary["results"]
        if result.get("status") != "ok"
    ]

    write_json(roots.ledger_root / "yellow_screen_summary.json", summary)


if __name__ == "__main__":
    main()
