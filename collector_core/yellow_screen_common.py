from __future__ import annotations

import dataclasses
import gzip
import hashlib
import json
import os
import re
import time
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any

from collector_core.config_validator import read_yaml

from collector_core.__version__ import __version__ as VERSION

__all__ = ["VERSION"]

PITCH_SAMPLE_LIMIT = 25
PITCH_TEXT_LIMIT = 400


@dataclasses.dataclass(frozen=True)
class YellowRootDefaults:
    raw_root: str
    screened_root: str
    manifests_root: str
    ledger_root: str
    pitches_root: str


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


def default_yellow_roots(prefix: str) -> YellowRootDefaults:
    return YellowRootDefaults(
        raw_root=f"/data/{prefix}/raw",
        screened_root=f"/data/{prefix}/screened_yellow",
        manifests_root=f"/data/{prefix}/_manifests",
        ledger_root=f"/data/{prefix}/_ledger",
        pitches_root=f"/data/{prefix}/_pitches",
    )


def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def sha256_text(text: str) -> str:
    norm = re.sub(r"\s+", " ", (text or "").strip())
    return hashlib.sha256(norm.encode("utf-8")).hexdigest()


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


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


def write_json(path: Path, obj: dict[str, Any]) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    ensure_dir(path.parent)
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "wt", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


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


def load_targets_cfg(path: Path) -> dict[str, Any]:
    return read_yaml(path, schema_name="targets") or {}


def resolve_dataset_root(explicit: str | None = None) -> Path | None:
    value = explicit or os.getenv("DATASET_ROOT") or os.getenv("DATASET_COLLECTOR_ROOT")
    if not value:
        return None
    return Path(value).expanduser().resolve()


def resolve_roots(cfg: dict[str, Any], defaults: YellowRootDefaults, dataset_root: Path | None = None) -> Roots:
    dataset_root = dataset_root or resolve_dataset_root()
    if dataset_root:
        defaults = YellowRootDefaults(
            raw_root=str(dataset_root / "raw"),
            screened_root=str(dataset_root / "screened_yellow"),
            manifests_root=str(dataset_root / "_manifests"),
            ledger_root=str(dataset_root / "_ledger"),
            pitches_root=str(dataset_root / "_pitches"),
        )
    g = (cfg.get("globals", {}) or {})
    return Roots(
        raw_root=Path(g.get("raw_root", defaults.raw_root)).expanduser().resolve(),
        screened_root=Path(g.get("screened_yellow_root", defaults.screened_root)).expanduser().resolve(),
        manifests_root=Path(g.get("manifests_root", defaults.manifests_root)).expanduser().resolve(),
        ledger_root=Path(g.get("ledger_root", defaults.ledger_root)).expanduser().resolve(),
        pitches_root=Path(g.get("pitches_root", defaults.pitches_root)).expanduser().resolve(),
    )


def merge_screening_config(cfg: dict[str, Any], target: dict[str, Any]) -> ScreeningConfig:
    g = (cfg.get("globals", {}) or {})
    g_screen = (g.get("screening", {}) or {})
    g_canon = (g.get("canonicalize", {}) or {})
    t_screen = (target.get("yellow_screen", {}) or {})
    t_canon = (target.get("canonicalize", {}) or {})
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
        deny_phrases=[p.lower() for p in (t_screen.get("deny_phrases") or g_screen.get("deny_phrases") or [])],
        require_record_license=bool(t_screen.get("require_record_license", g_screen.get("require_record_license", False))),
        min_chars=int(t_screen.get("min_chars", g_screen.get("min_chars", 200))),
        max_chars=int(
            t_canon.get("max_chars", t_screen.get("max_chars", g_canon.get("max_chars", g_screen.get("max_chars", 12000))))
        ),
    )


def sharding_cfg(cfg: dict[str, Any], prefix: str) -> ShardingConfig:
    g = (cfg.get("globals", {}).get("sharding", {}) or {})
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
