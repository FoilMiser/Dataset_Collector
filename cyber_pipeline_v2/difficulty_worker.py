#!/usr/bin/env python3
"""
difficulty_worker.py (v2.0)

Final screening + difficulty assignment over combined shards.
Outputs:
  - final/{license_pool}/d01..d10/shards/*.jsonl.gz
  - _ledger/final_index.jsonl
  - _pitches/final_pitched.jsonl (optional pitched samples)
"""

from __future__ import annotations

import argparse
import dataclasses
import gzip
import json
import time
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional

import yaml

VERSION = "2.0"


def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


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


def append_jsonl(path: Path, rows: Iterable[Dict[str, Any]]) -> None:
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


def write_json(path: Path, obj: Dict[str, Any]) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def route_pool(record: Dict[str, Any]) -> str:
    lp = ((record.get("source") or {}).get("license_profile") or record.get("license_profile") or "quarantine").lower()
    if lp not in {"permissive", "copyleft", "quarantine"}:
        lp = "quarantine"
    return lp


@dataclasses.dataclass
class Roots:
    combined_root: Path
    final_root: Path
    ledger_root: Path
    pitches_root: Path


@dataclasses.dataclass
class ShardingConfig:
    max_records_per_shard: int
    compression: str
    prefix: str


@dataclasses.dataclass
class ScreeningConfig:
    min_chars: int
    max_chars: int


class Sharder:
    def __init__(self, base_dir: Path, cfg: ShardingConfig):
        self.base_dir = base_dir
        self.cfg = cfg
        self.rows: List[Dict[str, Any]] = []
        self.shard_idx = 0

    def _path(self) -> Path:
        suffix = "jsonl.gz" if self.cfg.compression == "gzip" else "jsonl"
        return self.base_dir / f"{self.cfg.prefix}_{self.shard_idx:05d}.{suffix}"

    def add(self, row: Dict[str, Any]) -> Optional[Path]:
        self.rows.append(row)
        if len(self.rows) >= self.cfg.max_records_per_shard:
            path = self.flush()
            self.shard_idx += 1
            return path
        return None

    def flush(self) -> Optional[Path]:
        if not self.rows:
            return None
        path = self._path()
        append_jsonl(path, self.rows)
        self.rows = []
        return path


def resolve_roots(cfg: Dict[str, Any]) -> Roots:
    g = (cfg.get("globals", {}) or {})
    return Roots(
        combined_root=Path(g.get("combined_root", "/data/cyber/combined")),
        final_root=Path(g.get("final_root", "/data/cyber/final")),
        ledger_root=Path(g.get("ledger_root", "/data/cyber/_ledger")),
        pitches_root=Path(g.get("pitches_root", "/data/cyber/_pitches")),
    )


def sharding_cfg(cfg: Dict[str, Any]) -> ShardingConfig:
    g = (cfg.get("globals", {}).get("sharding", {}) or {})
    return ShardingConfig(
        max_records_per_shard=int(g.get("max_records_per_shard", 50000)),
        compression=str(g.get("compression", "gzip")),
        prefix="final",
    )


def screening_cfg(cfg: Dict[str, Any]) -> ScreeningConfig:
    g = (cfg.get("globals", {}).get("screening", {}) or {})
    return ScreeningConfig(min_chars=int(g.get("min_chars", 200)), max_chars=int(g.get("max_chars", 12000)))


def load_difficulty_cfg(cfg: Dict[str, Any], targets_path: Path) -> Dict[str, Any]:
    comp = (cfg.get("companion_files", {}) or {})
    diff_path = comp.get("difficulties_map")
    if diff_path:
        p = Path(diff_path)
        if not p.is_absolute():
            p = targets_path.parent / p
        if p.exists():
            return yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    return {}


def level_from_routing(diff_cfg: Dict[str, Any], routing: Dict[str, Any]) -> Optional[int]:
    routing = routing or {}
    subj = routing.get("subject") or diff_cfg.get("globals", {}).get("subject") or "cyber"
    domain = routing.get("domain")
    category = routing.get("category")

    # v2 routing_map support (preferred)
    for rule in diff_cfg.get("routing_map", []) or []:
        match = rule.get("match", {}) or {}
        level = rule.get("level")
        if level is None:
            continue
        match_subject = match.get("subject", subj)
        match_domain = match.get("domain", domain)
        match_category = match.get("category", category)
        if (
            (match_subject is None or routing.get("subject") == match_subject)
            and (match_domain is None or domain == match_domain)
            and (match_category is None or category == match_category)
        ):
            try:
                return int(level)
            except Exception:
                continue

    # Backwards compatibility with math-style subject/domain tree
    subjects = (diff_cfg.get("subjects", {}) or {})
    subj_cfg = subjects.get(subj)
    if not subj_cfg:
        return None
    domains = subj_cfg.get("domains", {}) or {}
    dom_cfg = domains.get(domain)
    if not dom_cfg:
        return None
    if category and category in (dom_cfg.get("categories", {}) or {}):
        level_info = dom_cfg.get("categories")[category].get("level", {})
        return int(level_info.get("default")) if level_info else None
    return None


def heuristic_level(text: str) -> int:
    length = len(text)
    if length <= 400:
        return 2
    if length <= 1200:
        return 4
    if length <= 3000:
        return 6
    if length <= 8000:
        return 8
    return 9


def assign_difficulty(diff_cfg: Dict[str, Any], record: Dict[str, Any]) -> Dict[str, Any]:
    existing = record.get("difficulty", {}) or {}
    if existing.get("level"):
        return {"level": int(existing["level"]), "method": existing.get("method", "existing"), "confidence": existing.get("confidence", 0.8)}
    routing = record.get("routing") or {}
    level = level_from_routing(diff_cfg, routing)
    if level:
        return {"level": int(level), "method": "routing", "confidence": 0.7}
    level = heuristic_level(record.get("text", ""))
    return {"level": level, "method": "length", "confidence": 0.5}


def iter_combined_records(roots: Roots) -> Iterator[Dict[str, Any]]:
    base = roots.combined_root
    for pool_dir in base.iterdir() if base.exists() else []:
        shards_dir = pool_dir / "shards"
        if not shards_dir.exists():
            continue
        for fp in shards_dir.glob("*.jsonl*"):
            yield from read_jsonl(fp)


def difficulty_to_path(root: Path, pool: str, level: int) -> Path:
    return root / pool / f"d{level:02d}" / "shards"


def main() -> None:
    ap = argparse.ArgumentParser(description=f"Difficulty Worker v{VERSION}")
    ap.add_argument("--targets", required=True, help="targets_cyber.yaml")
    ap.add_argument("--execute", action="store_true", help="Write outputs")
    args = ap.parse_args()

    targets_path = Path(args.targets).expanduser().resolve()
    cfg = yaml.safe_load(targets_path.read_text(encoding="utf-8")) or {}
    diff_cfg = load_difficulty_cfg(cfg, targets_path)
    roots = resolve_roots(cfg)
    shard_cfg = sharding_cfg(cfg)
    screen_cfg = screening_cfg(cfg)

    pool_sharders: Dict[str, Dict[int, Sharder]] = {}
    summary = {"written": 0, "pitched": 0, "shards": []}

    for rec in iter_combined_records(roots):
        text = rec.get("text") or ""
        if len(text) < screen_cfg.min_chars or len(text) > screen_cfg.max_chars:
            summary["pitched"] += 1
            if args.execute:
                append_jsonl(roots.pitches_root / "final_pitched.jsonl", [{"record_id": rec.get("record_id"), "reason": "length_bounds"}])
            continue
        diff = assign_difficulty(diff_cfg, rec)
        rec["difficulty"] = diff
        pool = route_pool(rec)
        level = max(1, min(10, diff.get("level", 5)))
        if pool not in pool_sharders:
            pool_sharders[pool] = {}
        if level not in pool_sharders[pool]:
            out_dir = difficulty_to_path(roots.final_root, pool, level)
            ensure_dir(out_dir)
            pool_sharders[pool][level] = Sharder(out_dir, shard_cfg)
        sharder = pool_sharders[pool][level]
        if args.execute:
            current_shard = str(sharder._path())
            path = sharder.add(rec)
            if path:
                current_shard = str(path)
                summary["shards"].append(current_shard)
            append_jsonl(roots.ledger_root / "final_index.jsonl", [{
                "content_sha256": (rec.get("hash") or {}).get("content_sha256"),
                "difficulty": level,
                "license_pool": pool,
                "output_shard": current_shard,
                "source": rec.get("source", {}),
                "seen_at_utc": utc_now(),
            }])
        summary["written"] += 1

    if args.execute:
        for pool_map in pool_sharders.values():
            for sharder in pool_map.values():
                flushed = sharder.flush()
                if flushed:
                    summary["shards"].append(str(flushed))

    summary["finished_at_utc"] = utc_now()
    write_json(roots.ledger_root / "difficulty_summary.json", summary)


if __name__ == "__main__":
    main()
