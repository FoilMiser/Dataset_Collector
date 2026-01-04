import json
import subprocess
import sys
from pathlib import Path

import yaml


def test_pipeline_driver_license_mapping_and_denylist(tmp_path: Path) -> None:
    license_map = {
        "spdx": {"allow": ["MIT"], "conditional": [], "deny_prefixes": []},
        "normalization": {"rules": []},
        "restriction_scan": {"phrases": []},
        "gating": {"restriction_phrase_bucket": "YELLOW"},
        "profiles": {"permissive": {"default_bucket": "GREEN"}},
    }
    license_map_path = tmp_path / "license_map.yaml"
    license_map_path.write_text(yaml.safe_dump(license_map), encoding="utf-8")

    denylist = {
        "patterns": [
            {
                "type": "substring",
                "value": "Bad Target",
                "fields": ["name"],
                "severity": "hard_red",
                "reason": "blocked source",
            }
        ],
        "domain_patterns": [],
        "publisher_patterns": [],
    }
    denylist_path = tmp_path / "denylist.yaml"
    denylist_path.write_text(yaml.safe_dump(denylist), encoding="utf-8")

    targets_cfg = {
        "companion_files": {"denylist": denylist_path.as_posix()},
        "globals": {"default_gates": []},
        "targets": [
            {
                "id": "deny_target",
                "name": "Bad Target",
                "license_profile": "permissive",
                "license_evidence": {
                    "spdx_hint": "MIT",
                    "url": "https://example.test/license",
                },
                "enabled": True,
            }
        ],
    }
    targets_path = tmp_path / "targets.yaml"
    targets_path.write_text(yaml.safe_dump(targets_cfg), encoding="utf-8")

    pipeline_driver = Path("kg_nav_pipeline_v2/pipeline_driver.py").resolve()
    subprocess.run(
        [
            sys.executable,
            str(pipeline_driver),
            "--targets",
            str(targets_path),
            "--license-map",
            str(license_map_path),
            "--dataset-root",
            str(tmp_path),
            "--quiet",
            "--no-fetch",
        ],
        check=True,
        cwd=Path(".").resolve(),
    )

    red_queue = tmp_path / "_queues" / "red_rejected.jsonl"
    assert red_queue.exists()

    rows = [json.loads(line) for line in red_queue.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert len(rows) == 1
    row = rows[0]
    assert row["resolved_spdx"] == "MIT"
    assert row["effective_bucket"] == "RED"
    assert row["denylist_hits"]
