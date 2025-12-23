#!/usr/bin/env python3
"""
difficulty_worker.py (v2.0)

Final screening + difficulty assignment over combined shards.
Outputs:
  - final/{license_pool}/d01..d10/{subject}/{domain}/{category}/shards/*.jsonl.gz
  - _ledger/final_index.jsonl
  - _pitches/final_pitched.jsonl (optional pitched samples)
"""

from __future__ import annotations

import argparse
import dataclasses
import gzip
import json
import re
import time
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

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
        combined_root=Path(g.get("combined_root", "/data/nlp/combined")),
        final_root=Path(g.get("final_root", "/data/nlp/final")),
        ledger_root=Path(g.get("ledger_root", "/data/nlp/_ledger")),
        pitches_root=Path(g.get("pitches_root", "/data/nlp/_pitches")),
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
    return ScreeningConfig(min_chars=int(g.get("min_chars", 300)), max_chars=int(g.get("max_chars", 12000)))


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
    subj = (routing or {}).get("subject") or "nlp"
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


def tokenize_words(text: str) -> List[str]:
    return re.findall(r"[A-Za-z]+", text)


def count_syllables(word: str) -> int:
    word = word.lower()
    if not word:
        return 0
    vowels = "aeiouy"
    count = 0
    prev_vowel = False
    for ch in word:
        is_vowel = ch in vowels
        if is_vowel and not prev_vowel:
            count += 1
        prev_vowel = is_vowel
    if word.endswith("e") and count > 1:
        count -= 1
    return max(count, 1)


def flesch_kincaid_grade(text: str) -> Optional[float]:
    sentences = re.split(r"[.!?]+", text)
    sentences = [s for s in sentences if s.strip()]
    words = tokenize_words(text)
    if not sentences or not words:
        return None
    syllables = sum(count_syllables(w) for w in words)
    return 0.39 * (len(words) / len(sentences)) + 11.8 * (syllables / len(words)) - 15.59


def punctuation_density(text: str) -> float:
    if not text:
        return 0.0
    count = len(re.findall(r"[;:()\[\]{}]", text))
    return count / max(len(text), 1)


def contains_legal_markers(text: str) -> bool:
    return bool(re.search(r"(\bCFR\b|\bU\.S\.C\.\b|§|\bU\.S\.\b)", text))


def apply_rule_sets(diff_cfg: Dict[str, Any], grade: Optional[float]) -> Optional[Dict[str, Any]]:
    if grade is None:
        return None
    for rule in diff_cfg.get("rule_sets", []) or []:
        when = rule.get("when", {}) or {}
        max_grade = when.get("flesch_kincaid_grade_max")
        min_grade = when.get("flesch_kincaid_grade_min")
        if max_grade is not None and grade > float(max_grade):
            continue
        if min_grade is not None and grade < float(min_grade):
            continue
        set_cfg = rule.get("set", {}) or {}
        level = set_cfg.get("level")
        if level:
            return {
                "level": int(level),
                "method": f"rule_set:{rule.get('id', 'rule')}",
                "confidence": float(set_cfg.get("confidence", 0.6)),
            }
    return None


def heuristic_level(text: str, grade: Optional[float]) -> Tuple[int, float]:
    length = len(text)
    if length <= 400:
        level = 2
    elif length <= 1200:
        level = 4
    elif length <= 3000:
        level = 6
    elif length <= 8000:
        level = 7
    else:
        level = 8

    if grade is not None:
        if grade >= 14:
            level += 2
        elif grade <= 5:
            level -= 2

    if punctuation_density(text) > 0.012:
        level += 1
    if contains_legal_markers(text) or re.search(r"\b\d+\.\d+", text):
        level += 1

    level = max(1, min(level, 10))
    confidence = 0.55 if grade is not None else 0.4
    return level, confidence


def assign_difficulty(diff_cfg: Dict[str, Any], record: Dict[str, Any]) -> Dict[str, Any]:
    existing = record.get("difficulty", {}) or {}
    if existing.get("level"):
        return {
            "level": int(existing["level"]),
            "method": existing.get("method", "existing"),
            "confidence": existing.get("confidence", 0.8),
        }
    routing = record.get("routing") or {}
    level = level_from_routing(diff_cfg, routing)
    if level:
        return {"level": int(level), "method": "routing", "confidence": 0.7}
    text = record.get("text", "")
    grade = flesch_kincaid_grade(text)
    rule = apply_rule_sets(diff_cfg, grade)
    if rule:
        return rule
    level, confidence = heuristic_level(text, grade)
    return {"level": level, "method": "heuristic", "confidence": confidence}


def resolve_output_path(diff_cfg: Dict[str, Any], roots: Roots, pool: str, record: Dict[str, Any], level: int) -> Path:
    routing = record.get("routing") or {}
    subject = routing.get("subject") or diff_cfg.get("globals", {}).get("default_subject", "nlp")
    domain = routing.get("domain") or diff_cfg.get("globals", {}).get("default_domain", "misc")
    category = routing.get("category") or diff_cfg.get("globals", {}).get("default_category", "misc")
    folder_layout = diff_cfg.get("globals", {}).get("folder_layout", "final/{license_pool}/d{level:02d}/{subject}/{domain}/{category}")
    path = folder_layout.format(
        license_pool=pool,
        level=level,
        subject=subject,
        domain=domain,
        category=category,
    )
    return roots.final_root / path / "shards"


def load_targets_cfg(path: Path) -> Dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def iter_combined_records(roots: Roots) -> Iterator[Dict[str, Any]]:
    base = roots.combined_root
    for pool_dir in base.iterdir() if base.exists() else []:
        shards_dir = pool_dir / "shards"
        if not shards_dir.exists():
            continue
        for fp in shards_dir.glob("*.jsonl*"):
            yield from read_jsonl(fp)


def run_difficulty(cfg: Dict[str, Any], diff_cfg: Dict[str, Any], roots: Roots, execute: bool) -> Dict[str, Any]:
    shard_cfg = sharding_cfg(cfg)
    screen_cfg = screening_cfg(cfg)
    summary = {"written": 0, "pitched": 0, "shards": []}

    pool_sharders: Dict[str, Sharder] = {}

    def get_sharder(out_dir: Path, pool: str) -> Sharder:
        key = f"{pool}:{out_dir}"
        if key not in pool_sharders:
            sharder = Sharder(out_dir, shard_cfg)
            pool_sharders[key] = sharder
            ensure_dir(sharder.base_dir)
        return pool_sharders[key]

    for rec in iter_combined_records(roots):
        text = rec.get("text", "")
        if len(text) < screen_cfg.min_chars or len(text) > screen_cfg.max_chars:
            summary["pitched"] += 1
            if execute:
                append_jsonl(roots.pitches_root / "final_pitched.jsonl", [{"reason": "length_bounds", "record_id": rec.get("record_id")}])
            continue

        pool = route_pool(rec)
        difficulty = assign_difficulty(diff_cfg, rec)
        level = int(difficulty.get("level", diff_cfg.get("globals", {}).get("default_level", 5)))
        rec["difficulty"] = difficulty

        out_dir = resolve_output_path(diff_cfg, roots, pool, rec, level)
        sharder = get_sharder(out_dir, pool)
        shard_path = str(sharder._path())
        if execute:
            path = sharder.add(rec)
            if path:
                shard_path = str(path)
                summary["shards"].append(shard_path)
            append_jsonl(roots.ledger_root / "final_index.jsonl", [{
                "record_id": rec.get("record_id"),
                "content_sha256": (rec.get("hash") or {}).get("content_sha256"),
                "difficulty": difficulty,
                "license_pool": pool,
                "output_shard": shard_path,
                "seen_at_utc": utc_now(),
            }])
        summary["written"] += 1

    if execute:
        for sharder in pool_sharders.values():
            path = sharder.flush()
            if path:
                summary["shards"].append(str(path))

    return summary


def main() -> None:
    ap = argparse.ArgumentParser(description=f"Difficulty Worker v{VERSION}")
    ap.add_argument("--targets", required=True, help="targets_nlp.yaml")
    ap.add_argument("--execute", action="store_true", help="Write output shards")
    args = ap.parse_args()

    targets_path = Path(args.targets).expanduser().resolve()
    cfg = load_targets_cfg(targets_path)
    diff_cfg = load_difficulty_cfg(cfg, targets_path)

    roots = resolve_roots(cfg)
    ensure_dir(roots.final_root)
    ensure_dir(roots.ledger_root)
    ensure_dir(roots.pitches_root)

    summary = {
        "run_at_utc": utc_now(),
        "pipeline_version": VERSION,
        "execute": args.execute,
        "results": run_difficulty(cfg, diff_cfg, roots, args.execute),
    }

    write_json(roots.ledger_root / "difficulty_summary.json", summary)


if __name__ == "__main__":
    main()
