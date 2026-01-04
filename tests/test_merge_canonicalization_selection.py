import gzip
import json
import subprocess
import sys
from pathlib import Path

import yaml


def test_merge_canonicalization_prefers_target_text_candidates(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    screened_root = tmp_path / "screened_yellow"
    combined_root = tmp_path / "combined"
    ledger_root = tmp_path / "_ledger"
    manifests_root = tmp_path / "_manifests"

    target_id = "canon_target"
    pool = "permissive"
    target_dir = raw_root / "green" / pool / target_id
    target_dir.mkdir(parents=True, exist_ok=True)

    (target_dir / "records.jsonl").write_text(
        json.dumps({"title": "Title text", "body": "Body text"}) + "\n",
        encoding="utf-8",
    )

    targets_cfg = {
        "globals": {
            "raw_root": str(raw_root),
            "screened_yellow_root": str(screened_root),
            "combined_root": str(combined_root),
            "manifests_root": str(manifests_root),
            "ledger_root": str(ledger_root),
            "canonicalize": {"text_field_candidates": ["title"]},
            "screening": {"text_field_candidates": ["summary"]},
            "sharding": {"max_records_per_shard": 100, "compression": "gzip"},
        },
        "targets": [
            {
                "id": target_id,
                "canonicalize": {"text_field_candidates": ["body"]},
                "yellow_screen": {"text_field_candidates": ["summary"]},
            }
        ],
    }
    targets_path = tmp_path / "targets.yaml"
    targets_path.write_text(yaml.safe_dump(targets_cfg), encoding="utf-8")

    merge_worker = Path("kg_nav_pipeline_v2/merge_worker.py").resolve()
    subprocess.run(
        [sys.executable, str(merge_worker), "--targets", str(targets_path), "--execute"],
        check=True,
        cwd=Path(".").resolve(),
    )

    shard_paths = list(combined_root.glob("**/shards/*.jsonl.gz"))
    assert shard_paths

    with gzip.open(shard_paths[0], "rt", encoding="utf-8") as handle:
        rows = [json.loads(line) for line in handle if line.strip()]

    assert rows
    assert rows[0]["text"] == "Body text"
