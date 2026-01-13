from __future__ import annotations

import gzip
import json
from pathlib import Path
from typing import Any

import pytest
import yaml

from collector_core.output_contract import validate_output_contract


def _write_license_evidence(dataset_root: Path, target_id: str, text: str) -> None:
    manifest_dir = dataset_root / "_manifests" / target_id
    manifest_dir.mkdir(parents=True, exist_ok=True)
    (manifest_dir / "license_evidence.txt").write_text(text, encoding="utf-8")


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]


def _read_gzip_jsonl(path: Path) -> list[dict[str, Any]]:
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


@pytest.fixture
def minimal_dataset(tmp_path: Path) -> dict[str, Path]:
    dataset_root = tmp_path / "dataset"
    roots = {
        "raw_root": dataset_root / "raw",
        "screened_yellow_root": dataset_root / "screened_yellow",
        "combined_root": dataset_root / "combined",
        "manifests_root": dataset_root / "_manifests",
        "queues_root": dataset_root / "_queues",
        "ledger_root": dataset_root / "_ledger",
        "pitches_root": dataset_root / "_pitches",
        "logs_root": dataset_root / "_logs",
    }
    for path in roots.values():
        path.mkdir(parents=True, exist_ok=True)

    license_map = {
        "schema_version": "0.9",
        "updated_utc": "2024-01-01T00:00:00Z",
        "spdx": {
            "allow": ["MIT"],
            "conditional": ["GPL-3.0"],
            "deny_prefixes": ["PROPRIETARY"],
        },
        "normalization": {
            "rules": [
                {"match_any": ["MIT License", "MIT"], "spdx": "MIT"},
                {"match_any": ["GPL-3.0", "GPL License"], "spdx": "GPL-3.0"},
            ]
        },
        "profiles": {
            "permissive": {"spdx_hint": "MIT"},
            "copyleft": {"spdx_hint": "GPL-3.0"},
            "deny": {"spdx_hint": "PROPRIETARY"},
        },
    }
    license_map_path = tmp_path / "license_map.yaml"
    license_map_path.write_text(yaml.safe_dump(license_map), encoding="utf-8")

    denylist_path = tmp_path / "denylist.yaml"
    denylist_path.write_text(
        yaml.safe_dump(
            {
                "schema_version": "0.9",
                "updated_utc": "2024-01-01T00:00:00Z",
                "patterns": [],
                "domain_patterns": [],
                "publisher_patterns": [],
            }
        ),
        encoding="utf-8",
    )

    targets_config = {
        "schema_version": "0.9",
        "updated_utc": "2024-01-01T00:00:00Z",
        "companion_files": {
            "license_map": str(license_map_path),
            "denylist": str(denylist_path),
        },
        "globals": {
            "raw_root": str(roots["raw_root"]),
            "screened_yellow_root": str(roots["screened_yellow_root"]),
            "combined_root": str(roots["combined_root"]),
            "manifests_root": str(roots["manifests_root"]),
            "queues_root": str(roots["queues_root"]),
            "ledger_root": str(roots["ledger_root"]),
            "pitches_root": str(roots["pitches_root"]),
            "logs_root": str(roots["logs_root"]),
            "default_license_gates": ["snapshot_terms"],
            "default_content_checks": [],
            "sharding": {"max_records_per_shard": 50, "compression": "gzip"},
        },
        "targets": [
            {
                "id": "green-high",
                "name": "Green High Priority",
                "enabled": True,
                "priority": 10,
                "license_profile": "permissive",
                "license_evidence": {"spdx_hint": "MIT", "url": "https://example.com/mit"},
                "download": {"strategy": "none"},
            },
            {
                "id": "green-low",
                "name": "Green Low Priority",
                "enabled": True,
                "priority": 5,
                "license_profile": "permissive",
                "license_evidence": {"spdx_hint": "MIT", "url": "https://example.com/mit"},
                "download": {"strategy": "none"},
            },
            {
                "id": "yellow-target",
                "name": "Yellow Conditional Target",
                "enabled": True,
                "priority": 1,
                "license_profile": "copyleft",
                "license_evidence": {"spdx_hint": "GPL-3.0", "url": "https://example.com/gpl"},
                "download": {"strategy": "none"},
            },
            {
                "id": "red-target",
                "name": "Red Deny Target",
                "enabled": True,
                "priority": 2,
                "license_profile": "deny",
                "license_evidence": {
                    "spdx_hint": "PROPRIETARY-1.0",
                    "url": "https://example.com/proprietary",
                },
                "download": {"strategy": "none"},
            },
        ],
    }
    targets_path = tmp_path / "targets_minimal.yaml"
    targets_path.write_text(yaml.safe_dump(targets_config), encoding="utf-8")

    _write_license_evidence(dataset_root, "green-high", "MIT License")
    _write_license_evidence(dataset_root, "green-low", "MIT License")
    _write_license_evidence(dataset_root, "yellow-target", "GPL-3.0")
    _write_license_evidence(dataset_root, "red-target", "PROPRIETARY license terms")

    return {
        "dataset_root": dataset_root,
        "targets_path": targets_path,
        "queues_root": roots["queues_root"],
        "raw_root": roots["raw_root"],
        "combined_root": roots["combined_root"],
        "ledger_root": roots["ledger_root"],
    }


@pytest.mark.integration
def test_full_flow_minimal_dataset(minimal_dataset: dict[str, Path], run_dc) -> None:
    dataset_root = minimal_dataset["dataset_root"]
    targets_path = minimal_dataset["targets_path"]
    queues_root = minimal_dataset["queues_root"]

    run_dc(
        [
            "pipeline",
            "fixture",
            "--",
            "--targets",
            str(targets_path),
            "--dataset-root",
            str(dataset_root),
            "--no-fetch",
            "--quiet",
        ],
    )

    green_queue = queues_root / "green_download.jsonl"
    yellow_queue = queues_root / "yellow_pipeline.jsonl"
    red_queue = queues_root / "red_rejected.jsonl"
    assert green_queue.exists()
    assert yellow_queue.exists()
    assert red_queue.exists()

    green_rows = _read_jsonl(green_queue)
    assert [row["id"] for row in green_rows] == ["green-high", "green-low"]

    yellow_rows = _read_jsonl(yellow_queue)
    assert {row["id"] for row in yellow_rows} == {"yellow-target"}

    red_rows = _read_jsonl(red_queue)
    assert {row["id"] for row in red_rows} == {"red-target"}

    run_dc(
        [
            "run",
            "--pipeline",
            "fixture",
            "--stage",
            "acquire",
            "--dataset-root",
            str(dataset_root),
            "--",
            "--queue",
            str(green_queue),
            "--bucket",
            "green",
            "--targets-yaml",
            str(targets_path),
            "--execute",
        ],
    )

    raw_target_dir = minimal_dataset["raw_root"] / "green" / "permissive" / "green-high"
    raw_target_dir.mkdir(parents=True, exist_ok=True)
    raw_records = [
        {
            "id": "record-001",
            "text": "Minimal integration record.",
            "source_url": "https://example.com/data/record-001",
        }
    ]
    (raw_target_dir / "records.jsonl").write_text(
        "\n".join(json.dumps(row) for row in raw_records) + "\n", encoding="utf-8"
    )

    run_dc(
        [
            "run",
            "--pipeline",
            "fixture",
            "--stage",
            "merge",
            "--dataset-root",
            str(dataset_root),
            "--",
            "--targets",
            str(targets_path),
            "--execute",
        ],
    )

    shards = list((minimal_dataset["combined_root"] / "permissive" / "shards").glob("*.jsonl.gz"))
    assert shards

    merged_records = _read_gzip_jsonl(shards[0])
    assert merged_records
    validate_output_contract(merged_records[0], "integration_full_flow")

    merge_summary = minimal_dataset["ledger_root"] / "merge_summary.json"
    assert merge_summary.exists()
