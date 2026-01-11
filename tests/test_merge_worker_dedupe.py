import hashlib
import json
import tracemalloc
from pathlib import Path

from chem_pipeline_v2 import merge_worker


def write_screened_records(
    screened_root: Path,
    pool: str,
    total_records: int,
    duplicate_every: int,
) -> tuple[int, int]:
    shards_dir = screened_root / pool / "shards"
    shards_dir.mkdir(parents=True, exist_ok=True)
    shard_path = shards_dir / "screened_00000.jsonl"
    seen: set[str] = set()
    deduped = 0
    with shard_path.open("w", encoding="utf-8") as handle:
        for idx in range(total_records):
            base_idx = (
                idx - 1 if duplicate_every and idx % duplicate_every == 0 and idx > 0 else idx
            )
            text = f"record {base_idx}"
            content_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()
            if content_hash in seen:
                deduped += 1
            else:
                seen.add(content_hash)
            record = {
                "text": text,
                "hash": {"content_sha256": content_hash},
                "source": {"target_id": "target", "license_profile": pool},
            }
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")
    return len(seen), deduped


def run_merge(
    tmp_path: Path,
    total_records: int,
    duplicate_every: int,
    track_memory: bool = False,
) -> tuple[dict[str, int], list[dict[str, object]], list[dict[str, object]], int]:
    raw_root = tmp_path / "raw"
    screened_root = tmp_path / "screened_yellow"
    combined_root = tmp_path / "combined"
    ledger_root = tmp_path / "_ledger"

    expected_unique, expected_deduped = write_screened_records(
        screened_root,
        "permissive",
        total_records,
        duplicate_every,
    )

    cfg = {
        "globals": {
            "raw_root": str(raw_root),
            "screened_yellow_root": str(screened_root),
            "combined_root": str(combined_root),
            "ledger_root": str(ledger_root),
            "sharding": {"max_records_per_shard": 10000, "compression": "gzip"},
        },
        "targets": [{"id": "target"}],
    }

    roots = merge_worker.resolve_roots(cfg)
    if track_memory:
        tracemalloc.start()
    summary = merge_worker.merge_records(cfg, roots, execute=True)
    if track_memory:
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
    else:
        peak = 0

    merge_worker.write_json(ledger_root / "merge_summary.json", summary)

    shard_rows: list[dict[str, object]] = []
    for fp in sorted((combined_root / "permissive" / "shards").glob("*.jsonl*")):
        shard_rows.extend(list(merge_worker.read_jsonl(fp)))

    index_rows = list(merge_worker.read_jsonl(ledger_root / "combined_index.jsonl"))

    summary_counts = {
        "written": summary["written"],
        "deduped": summary["deduped"],
        "skipped": summary["skipped"],
        "expected_unique": expected_unique,
        "expected_deduped": expected_deduped,
    }
    return summary_counts, shard_rows, index_rows, peak


def normalize_index(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    normalized: list[dict[str, object]] = []
    for row in rows:
        normalized.append(
            {
                "content_sha256": row["content_sha256"],
                "license_pool": row["license_pool"],
                "source": row["source"],
            }
        )
    return normalized


def test_merge_dedupe_is_deterministic_and_memory_bounded(tmp_path: Path) -> None:
    total_records = 40000
    duplicate_every = 50

    summary_a, shards_a, index_a, peak = run_merge(
        tmp_path / "run_a",
        total_records,
        duplicate_every,
        track_memory=True,
    )
    summary_b, shards_b, index_b, _ = run_merge(
        tmp_path / "run_b",
        total_records,
        duplicate_every,
        track_memory=False,
    )

    assert summary_a["written"] == summary_a["expected_unique"]
    assert summary_a["deduped"] == summary_a["expected_deduped"]
    assert summary_a["skipped"] == 0
    assert summary_a["written"] == summary_b["written"]
    assert summary_a["deduped"] == summary_b["deduped"]

    assert peak < 80 * 1024 * 1024

    hashes_a = [row["hash"]["content_sha256"] for row in shards_a]
    hashes_b = [row["hash"]["content_sha256"] for row in shards_b]
    assert hashes_a == hashes_b

    assert normalize_index(index_a) == normalize_index(index_b)
