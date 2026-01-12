from __future__ import annotations

import dataclasses
import json
from pathlib import Path
from typing import Any

from collector_core.__version__ import __version__ as VERSION
from collector_core.config_validator import read_yaml
from collector_core.dataset_root import ensure_data_root_allowed, resolve_dataset_root
from collector_core.stability import stable_api
from collector_core.utils.io import write_jsonl

__all__ = ["VERSION"]

PITCH_SAMPLE_LIMIT_DEFAULT = 25
PITCH_TEXT_LIMIT_DEFAULT = 400


@stable_api
@dataclasses.dataclass(frozen=True)
class YellowRootDefaults:
    raw_root: str
    screened_root: str
    manifests_root: str
    ledger_root: str
    pitches_root: str


@stable_api
@dataclasses.dataclass
class Roots:
    raw_root: Path
    screened_root: Path
    manifests_root: Path
    ledger_root: Path
    pitches_root: Path


@stable_api
@dataclasses.dataclass
class ScreeningConfig:
    text_fields: list[str]
    license_fields: list[str]
    allow_spdx: list[str]
    deny_phrases: list[str]
    require_record_license: bool
    min_chars: int
    max_chars: int


@stable_api
@dataclasses.dataclass
class ShardingConfig:
    max_records_per_shard: int
    compression: str
    prefix: str


@stable_api
@dataclasses.dataclass(frozen=True)
class PitchConfig:
    sample_limit: int
    text_limit: int


@stable_api
class Sharder:
    def __init__(self, base_dir: Path, cfg: ShardingConfig) -> None:
        self.base_dir = base_dir
        self.cfg = cfg
        self.count = 0
        self.shard_idx = 0
        self.current_rows: list[dict[str, Any]] = []

    def _next_path(self) -> Path:
        if self.cfg.compression == "gzip":
            suffix = "jsonl.gz"
        elif self.cfg.compression in {"zstd", "zst"}:
            suffix = "jsonl.zst"
        else:
            suffix = "jsonl"
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


@stable_api
def default_yellow_roots(prefix: str) -> YellowRootDefaults:
    return YellowRootDefaults(
        raw_root=f"/data/{prefix}/raw",
        screened_root=f"/data/{prefix}/screened_yellow",
        manifests_root=f"/data/{prefix}/_manifests",
        ledger_root=f"/data/{prefix}/_ledger",
        pitches_root=f"/data/{prefix}/_pitches",
    )


@stable_api
def load_targets_cfg(path: Path) -> dict[str, Any]:
    return read_yaml(path, schema_name="targets") or {}


@stable_api
def resolve_roots(
    cfg: dict[str, Any],
    defaults: YellowRootDefaults,
    dataset_root: Path | None = None,
    *,
    allow_data_root: bool = False,
) -> Roots:
    dataset_root = dataset_root or resolve_dataset_root()
    if dataset_root:
        defaults = YellowRootDefaults(
            raw_root=str(dataset_root / "raw"),
            screened_root=str(dataset_root / "screened_yellow"),
            manifests_root=str(dataset_root / "_manifests"),
            ledger_root=str(dataset_root / "_ledger"),
            pitches_root=str(dataset_root / "_pitches"),
        )
    g = cfg.get("globals", {}) or {}
    roots = Roots(
        raw_root=Path(g.get("raw_root", defaults.raw_root)).expanduser().resolve(),
        screened_root=Path(g.get("screened_yellow_root", defaults.screened_root))
        .expanduser()
        .resolve(),
        manifests_root=Path(g.get("manifests_root", defaults.manifests_root))
        .expanduser()
        .resolve(),
        ledger_root=Path(g.get("ledger_root", defaults.ledger_root)).expanduser().resolve(),
        pitches_root=Path(g.get("pitches_root", defaults.pitches_root)).expanduser().resolve(),
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


@stable_api
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


@stable_api
def sharding_cfg(cfg: dict[str, Any], prefix: str) -> ShardingConfig:
    g = cfg.get("globals", {}).get("sharding", {}) or {}
    return ShardingConfig(
        max_records_per_shard=int(g.get("max_records_per_shard", 50000)),
        compression=str(g.get("compression", "zstd")),
        prefix=prefix,
    )


@stable_api
def resolve_pitch_config(
    cfg: dict[str, Any],
    sample_limit_override: int | None = None,
    text_limit_override: int | None = None,
) -> PitchConfig:
    g = cfg.get("globals", {}) or {}
    pitch_cfg = g.get("pitch_limits", {}) or {}
    sample_limit = (
        sample_limit_override
        if sample_limit_override is not None
        else int(pitch_cfg.get("sample_limit", PITCH_SAMPLE_LIMIT_DEFAULT))
    )
    text_limit = (
        text_limit_override
        if text_limit_override is not None
        else int(pitch_cfg.get("text_limit", PITCH_TEXT_LIMIT_DEFAULT))
    )
    return PitchConfig(sample_limit=sample_limit, text_limit=text_limit)


@stable_api
def find_text(row: dict[str, Any], candidates: list[str]) -> str | None:
    for k in candidates:
        if k in row and row[k]:
            val = row[k]
            if isinstance(val, (list, tuple)):
                val = "\n".join(map(str, val))
            return str(val)
    return None


@stable_api
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


@stable_api
def find_license(row: dict[str, Any], candidates: list[str]) -> str | None:
    for k in candidates:
        if k in row and row[k]:
            return str(row[k])
    return None


@stable_api
def contains_deny(text: str, phrases: list[str]) -> bool:
    low = text.lower()
    return any(p in low for p in phrases)
