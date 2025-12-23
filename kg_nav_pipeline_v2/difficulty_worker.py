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
        combined_root=Path(g.get("combined_root", "/data/kg_nav/combined")),
        final_root=Path(g.get("final_root", "/data/kg_nav/final")),
        ledger_root=Path(g.get("ledger_root", "/data/kg_nav/_ledger")),
        pitches_root=Path(g.get("pitches_root", "/data/kg_nav/_pitches")),
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
    subj = (routing or {}).get("subject") or "kg_nav"
    domain = (routing or {}).get("domain")
    category = (routing or {}).get("category")
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


def heuristic_level(record: Dict[str, Any]) -> Dict[str, Any]:
    meta = record.get("metadata") or {}
    if meta:
        h = int(meta.get("hop_count") or 0)
        j = int(meta.get("join_count") or 0)
        b = int(meta.get("branch_factor") or 0)
        e = int(meta.get("evidence_count") or meta.get("evidence_len") or 0)
        x = int(meta.get("crosswalk_steps") or 0)
        c = int(meta.get("constraint_count") or 0)
        k = 0
        if isinstance(meta.get("candidate_counts"), dict):
            try:
                k = max(int(v or 0) for v in meta.get("candidate_counts").values())
            except Exception:
                k = 0
        r_flag = 1 if meta.get("requires_reconciliation") else 0
        p_steps = int(meta.get("provenance_steps") or 0)

        level = 2
        level += max(0, h - 1)
        level += min(2, j)
        level += 1 if b >= 3 else 0
        level += 1 if e >= 4 else 0
        level += 1 if x >= 2 else 0
        level += 1 if c >= 2 else 0
        level += 1 if k >= 6 else 0
        level += 2 if r_flag else 0
        level += 1 if p_steps >= 4 else 0
        level = max(1, min(10, level))
        return {"level": level, "method": "structural", "confidence": 0.55}

    text = record.get("text", "")
    length = len(text) if isinstance(text, str) else 0
    if length == 0:
        return {}
    if length <= 400:
        lvl = 2
    elif length <= 1200:
        lvl = 4
    elif length <= 3000:
        lvl = 6
    elif length <= 8000:
        lvl = 8
    else:
        lvl = 9
    return {"level": lvl, "method": "length", "confidence": 0.5}


def assign_difficulty(diff_cfg: Dict[str, Any], record: Dict[str, Any]) -> Dict[str, Any]:
    existing = record.get("difficulty", {}) or {}
    if existing.get("level"):
        return {"level": int(existing["level"]), "method": existing.get("method", "existing"), "confidence": existing.get("confidence", 0.8)}

    routing = record.get("routing") or {}
    routing_level = level_from_routing(diff_cfg, routing)
    heur = heuristic_level(record)
    heur_level = heur.get("level")

    if routing_level and heur_level:
        level = max(int(routing_level), int(heur_level))
        return {"level": level, "method": "routing+structural" if heur.get("method") == "structural" else "routing+heuristic", "confidence": max(0.7, heur.get("confidence", 0.5))}
    if routing_level:
        return {"level": int(routing_level), "method": "routing", "confidence": 0.7}
    if heur_level:
        return {"level": int(heur_level), "method": heur.get("method", "heuristic"), "confidence": heur.get("confidence", 0.5)}
    return {"level": 5, "method": "default", "confidence": 0.3}


def iter_combined_records(roots: Roots) -> Iterator[Dict[str, Any]]:
    base = roots.combined_root
    for pool_dir in base.iterdir() if base.exists() else []:
        shards_dir = pool_dir / "shards"
        if not shards_dir.exists():
            continue
        for fp in shards_dir.glob("*.jsonl*"):
            yield from read_jsonl(fp)


def sanitize_segment(seg: str) -> str:
    import re
    return re.sub(r"[^a-zA-Z0-9._-]+", "_", seg or "misc")


def resolve_layout_routing(diff_cfg: Dict[str, Any], routing: Dict[str, Any]) -> Dict[str, Any]:
    g = diff_cfg.get("globals", {}) or {}
    return {
        "subject": routing.get("subject") or g.get("default_subject", "kg_nav"),
        "domain": routing.get("domain") or g.get("default_domain", "misc"),
        "category": routing.get("category") or g.get("default_category", "misc"),
    }


def difficulty_to_path(root: Path, layout: str, pool: str, level: int, routing: Dict[str, Any], diff_cfg: Dict[str, Any]) -> Path:
    resolved = resolve_layout_routing(diff_cfg, routing)
    sanitize = bool((diff_cfg.get("globals", {}) or {}).get("sanitize_path_segments", False))
    subject = sanitize_segment(resolved["subject"]) if sanitize else resolved["subject"]
    domain = sanitize_segment(resolved["domain"]) if sanitize else resolved["domain"]
    category = sanitize_segment(resolved["category"]) if sanitize else resolved["category"]
    formatted = layout.format(
        license_pool=pool,
        level=level,
        subject=subject,
        domain=domain,
        category=category,
    )
    if root.name == "final" and formatted.startswith("final/"):
        formatted = formatted.split("final/", 1)[1]
    return root / formatted / "shards"


def main() -> None:
    ap = argparse.ArgumentParser(description=f"Difficulty Worker v{VERSION}")
    ap.add_argument("--targets", required=True, help="targets_kg_nav.yaml")
    ap.add_argument("--execute", action="store_true", help="Write outputs")
    args = ap.parse_args()

    targets_path = Path(args.targets).expanduser().resolve()
    cfg = yaml.safe_load(targets_path.read_text(encoding="utf-8")) or {}
    diff_cfg = load_difficulty_cfg(cfg, targets_path)
    roots = resolve_roots(cfg)
    shard_cfg = sharding_cfg(cfg)
    screen_cfg = screening_cfg(cfg)
    layout = (diff_cfg.get("globals", {}) or {}).get("folder_layout", "final/{license_pool}/d{level:02d}/{subject}/{domain}/{category}")

    sharders: Dict[str, Sharder] = {}
    summary = {"written": 0, "pitched": 0, "shards": []}

    for rec in iter_combined_records(roots):
        text = rec.get("text")
        if isinstance(text, str) and text:
            if len(text) < screen_cfg.min_chars or len(text) > screen_cfg.max_chars:
                summary["pitched"] += 1
                if args.execute:
                    append_jsonl(roots.pitches_root / "final_pitched.jsonl", [{"record_id": rec.get("record_id"), "reason": "length_bounds"}])
                continue
        diff = assign_difficulty(diff_cfg, rec)
        rec["difficulty"] = diff
        pool = route_pool(rec)
        level = max(1, min(10, diff.get("level", 5)))
        routing = rec.get("routing") or {}
        out_dir = difficulty_to_path(roots.final_root, layout, pool, level, routing, diff_cfg)
        ensure_dir(out_dir)
        shard_key = f"{pool}-d{level:02d}-{out_dir}"
        if shard_key not in sharders:
            sharders[shard_key] = Sharder(out_dir, shard_cfg)
        sharder = sharders[shard_key]
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
                "routing": routing,
                "output_shard": current_shard,
                "source": rec.get("source", {}),
                "method": diff.get("method"),
                "seen_at_utc": utc_now(),
            }])
        summary["written"] += 1

    if args.execute:
        for sharder in sharders.values():
            flushed = sharder.flush()
            if flushed:
                summary["shards"].append(str(flushed))

    summary["finished_at_utc"] = utc_now()
    write_json(roots.ledger_root / "difficulty_summary.json", summary)


if __name__ == "__main__":
    main()
