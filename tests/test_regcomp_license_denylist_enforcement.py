from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import yaml


def read_jsonl(path: Path) -> list[dict[str, object]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def test_regcomp_license_map_and_denylist_enforced(tmp_path: Path) -> None:
    manifests_root = tmp_path / "_manifests"
    queues_root = tmp_path / "_queues"
    license_map_path = tmp_path / "license_map.yaml"
    denylist_path = tmp_path / "denylist.yaml"
    targets_path = tmp_path / "targets_regcomp.yaml"

    license_map = {
        "schema_version": "0.3",
        "spdx": {
            "allow": ["MIT"],
            "conditional": [],
            "deny_prefixes": [],
        },
        "normalization": {"rules": []},
        "restriction_scan": {"phrases": []},
        "gating": {
            "unknown_spdx_bucket": "YELLOW",
            "conditional_spdx_bucket": "YELLOW",
            "deny_spdx_bucket": "RED",
        },
        "profiles": {
            "permissive": {"default_bucket": "GREEN"},
        },
    }
    license_map_path.write_text(yaml.safe_dump(license_map), encoding="utf-8")

    denylist = {
        "schema_version": "0.2",
        "patterns": [
            {
                "type": "substring",
                "value": "Force Yellow",
                "fields": ["name"],
                "severity": "force_yellow",
                "reason": "manual review required",
            }
        ],
        "domain_patterns": [
            {
                "domain": "blocked.example",
                "severity": "hard_red",
                "rationale": "blocked domain",
            }
        ],
        "publisher_patterns": [],
    }
    denylist_path.write_text(yaml.safe_dump(denylist), encoding="utf-8")

    targets_cfg = {
        "schema_version": "0.8",
        "globals": {
            "manifests_root": str(manifests_root),
            "queues_root": str(queues_root),
            "default_gates": [],
        },
        "companion_files": {
            "license_map": str(license_map_path),
            "denylist": str(denylist_path),
        },
        "targets": [
            {
                "id": "force_yellow",
                "name": "Force Yellow Dataset",
                "license_profile": "permissive",
                "license_evidence": {
                    "spdx_hint": "MIT",
                    "url": "https://example.test/terms",
                },
            },
            {
                "id": "hard_red",
                "name": "Hard Red Dataset",
                "license_profile": "permissive",
                "license_evidence": {
                    "spdx_hint": "MIT",
                    "url": "https://blocked.example/terms",
                },
            },
        ],
    }
    targets_path.write_text(yaml.safe_dump(targets_cfg), encoding="utf-8")

    driver_path = Path("regcomp_pipeline_v2/pipeline_driver.py").resolve()
    subprocess.run(
        [
            sys.executable,
            str(driver_path),
            "--targets",
            str(targets_path),
            "--license-map",
            str(license_map_path),
            "--no-fetch",
            "--quiet",
        ],
        check=True,
        cwd=Path(".").resolve(),
    )

    green_rows = read_jsonl(queues_root / "green_download.jsonl")
    yellow_rows = read_jsonl(queues_root / "yellow_pipeline.jsonl")
    red_rows = read_jsonl(queues_root / "red_rejected.jsonl")

    assert not green_rows
    assert {row["id"] for row in yellow_rows} == {"force_yellow"}
    assert {row["id"] for row in red_rows} == {"hard_red"}

    yellow_hit = yellow_rows[0]["denylist_hits"][0]
    red_hit = red_rows[0]["denylist_hits"][0]
    assert yellow_hit["severity"] == "force_yellow"
    assert red_hit["severity"] == "hard_red"
    assert yellow_rows[0]["resolved_spdx"] == "MIT"
    assert red_rows[0]["resolved_spdx"] == "MIT"
