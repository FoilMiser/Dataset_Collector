from __future__ import annotations

import dataclasses
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

from collector_core.__version__ import __version__ as CORE_VERSION
from collector_core.secrets import REDACTED


@dataclasses.dataclass(frozen=True)
class PipelineTestConfig:
    manifests_root: Path
    queues_root: Path
    ledger_root: Path
    license_map_path: Path
    denylist_path: Path

    def write_targets(
        self,
        tmp_path: Path,
        targets: list[dict[str, object]],
        globals_override: dict[str, object] | None = None,
        companion_override: dict[str, object] | None = None,
    ) -> Path:
        globals_cfg = {
            "manifests_root": str(self.manifests_root),
            "queues_root": str(self.queues_root),
            "ledger_root": str(self.ledger_root),
            "default_license_gates": [],
            "default_content_checks": [],
        }
        if globals_override:
            globals_cfg.update(globals_override)

        companion_cfg = {
            "license_map": str(self.license_map_path),
            "denylist": str(self.denylist_path),
        }
        if companion_override:
            companion_cfg.update(companion_override)

        cfg = {
            "schema_version": "0.9",
            "globals": globals_cfg,
            "companion_files": companion_cfg,
            "targets": targets,
        }
        targets_path = tmp_path / "targets.yaml"
        targets_path.write_text(yaml.safe_dump(cfg), encoding="utf-8")
        return targets_path


@pytest.fixture
def minimal_license_map(tmp_path: Path) -> Path:
    license_map_path = tmp_path / "license_map.yaml"
    license_map = {
        "schema_version": "0.9",
        "spdx": {
            "allow": ["MIT"],
            "conditional": ["LGPL-2.1-only"],
            "deny_prefixes": ["GPL-3"],
        },
        "normalization": {"rules": []},
        "restriction_scan": {"phrases": []},
        "gating": {
            "unknown_spdx_bucket": "YELLOW",
            "conditional_spdx_bucket": "YELLOW",
            "deny_spdx_bucket": "RED",
            "low_confidence_bucket": "YELLOW",
        },
        "profiles": {"permissive": {"default_bucket": "GREEN"}},
    }
    license_map_path.write_text(yaml.safe_dump(license_map), encoding="utf-8")
    return license_map_path


@pytest.fixture
def minimal_config(tmp_path: Path, minimal_license_map: Path) -> PipelineTestConfig:
    denylist_path = tmp_path / "denylist.yaml"
    denylist_path.write_text(
        yaml.safe_dump(
            {
                "schema_version": "0.9",
                "patterns": [],
                "domain_patterns": [],
                "publisher_patterns": [],
            }
        ),
        encoding="utf-8",
    )
    return PipelineTestConfig(
        manifests_root=tmp_path / "_manifests",
        queues_root=tmp_path / "_queues",
        ledger_root=tmp_path / "_ledger",
        license_map_path=minimal_license_map,
        denylist_path=denylist_path,
    )


def read_jsonl(path: Path) -> list[dict[str, object]]:
    if not path.exists():
        return []
    return [
        json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()
    ]


def run_driver(
    targets_path: Path, license_map_path: Path, extra_args: list[str] | None = None
) -> None:
    driver_path = Path("regcomp_pipeline_v2/pipeline_driver.py").resolve()
    env = os.environ.copy()
    env["PYTHONPATH"] = str(Path(".").resolve())
    args = [
        sys.executable,
        str(driver_path),
        "--targets",
        str(targets_path),
        "--license-map",
        str(license_map_path),
        "--no-fetch",
        "--quiet",
    ]
    if extra_args:
        args.extend(extra_args)
    subprocess.run(args, check=True, cwd=Path(".").resolve(), env=env)


def test_green_yellow_red_classification(
    tmp_path: Path, minimal_config: PipelineTestConfig
) -> None:
    targets = [
        {
            "id": "green_target",
            "name": "Green Dataset",
            "license_profile": "permissive",
            "license_evidence": {"spdx_hint": "MIT", "url": "https://example.test/terms"},
        },
        {
            "id": "yellow_target",
            "name": "Yellow Dataset",
            "license_profile": "permissive",
            "license_evidence": {"spdx_hint": "LGPL-2.1-only", "url": "https://example.test/terms"},
        },
        {
            "id": "red_target",
            "name": "Red Dataset",
            "license_profile": "permissive",
            "license_evidence": {"spdx_hint": "GPL-3.0-only", "url": "https://example.test/terms"},
        },
    ]
    targets_path = minimal_config.write_targets(tmp_path, targets)

    run_driver(targets_path, minimal_config.license_map_path)

    green_rows = read_jsonl(minimal_config.queues_root / "green_download.jsonl")
    yellow_rows = read_jsonl(minimal_config.queues_root / "yellow_pipeline.jsonl")
    red_rows = read_jsonl(minimal_config.queues_root / "red_rejected.jsonl")

    assert {row["id"] for row in green_rows} == {"green_target"}
    assert {row["id"] for row in yellow_rows} == {"yellow_target"}
    assert {row["id"] for row in red_rows} == {"red_target"}
    assert green_rows[0]["bucket_reason"] == "spdx_allow"
    assert yellow_rows[0]["bucket_reason"] == "conditional_spdx"
    assert red_rows[0]["bucket_reason"] == "spdx_deny"
    for row in green_rows + yellow_rows + red_rows:
        assert isinstance(row["signals"], dict)

    run_dirs = [path for path in minimal_config.ledger_root.iterdir() if path.is_dir()]
    assert len(run_dirs) == 1
    snapshot_path = run_dirs[0] / "policy_snapshot.json"
    assert snapshot_path.exists()
    snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
    assert snapshot["git_sha"]
    assert snapshot["git_sha"] != "unknown"
    assert snapshot["core_version"] == CORE_VERSION
    assert snapshot["license_map_hash"]
    assert snapshot["denylist_hash"]
    assert snapshot["schema_versions"]["targets"] == "0.9"
    assert "0.9" in snapshot["schema_versions"]["license_map"]
    assert "0.9" in snapshot["schema_versions"]["denylist"]
    assert snapshot["enabled_checks"] == []


def test_offline_snapshot_terms_forces_yellow(
    tmp_path: Path, minimal_config: PipelineTestConfig
) -> None:
    targets = [
        {
            "id": "offline_target",
            "name": "Offline Dataset",
            "license_profile": "permissive",
            "license_evidence": {"spdx_hint": "MIT", "url": "https://example.test/terms"},
        }
    ]
    targets_path = minimal_config.write_targets(
        tmp_path, targets, globals_override={"default_license_gates": ["snapshot_terms"]}
    )

    run_driver(targets_path, minimal_config.license_map_path)

    green_rows = read_jsonl(minimal_config.queues_root / "green_download.jsonl")
    yellow_rows = read_jsonl(minimal_config.queues_root / "yellow_pipeline.jsonl")

    assert not green_rows
    assert {row["id"] for row in yellow_rows} == {"offline_target"}
    assert yellow_rows[0]["bucket_reason"] == "snapshot_missing"
    assert yellow_rows[0]["signals"]["evidence"]["snapshot_missing"] is True
    evaluation = json.loads(
        (minimal_config.manifests_root / "offline_target" / "evaluation.json").read_text(
            encoding="utf-8"
        )
    )
    assert evaluation["no_fetch_missing_evidence"] is True


def test_evaluation_manifest_redacts_headers(
    tmp_path: Path, minimal_config: PipelineTestConfig
) -> None:
    secret = "supersecret"
    targets = [
        {
            "id": "redact_target",
            "name": "Redact Headers Dataset",
            "license_profile": "permissive",
            "license_evidence": {"spdx_hint": "MIT", "url": "https://example.test/terms"},
        }
    ]
    targets_path = minimal_config.write_targets(tmp_path, targets)

    run_driver(
        targets_path,
        minimal_config.license_map_path,
        extra_args=[
            "--evidence-header",
            f"Authorization=Bearer {secret}",
            "--evidence-header",
            f"X-Api-Key={secret}",
        ],
    )

    evaluation_path = minimal_config.manifests_root / "redact_target" / "evaluation.json"
    evaluation_text = evaluation_path.read_text(encoding="utf-8")
    assert secret not in evaluation_text
    assert REDACTED in evaluation_text
    evaluation = json.loads(evaluation_text)
    assert evaluation["evidence_headers_used"]["Authorization"] == REDACTED
    assert evaluation["evidence_headers_used"]["X-Api-Key"] == REDACTED


def _collect_strings(value: object) -> list[str]:
    if isinstance(value, dict):
        strings: list[str] = []
        for key, item in value.items():
            strings.extend(_collect_strings(key))
            strings.extend(_collect_strings(item))
        return strings
    if isinstance(value, list):
        strings = []
        for item in value:
            strings.extend(_collect_strings(item))
        return strings
    if isinstance(value, str):
        return [value]
    return []


def test_manifests_omit_or_redact_secret_headers(
    tmp_path: Path, minimal_config: PipelineTestConfig
) -> None:
    secret = "supersecret"
    targets = [
        {
            "id": "manifest_scan_target",
            "name": "Manifest Scan Dataset",
            "license_profile": "permissive",
            "license_evidence": {"spdx_hint": "MIT", "url": "https://example.test/terms"},
        }
    ]
    targets_path = minimal_config.write_targets(tmp_path, targets)

    run_driver(
        targets_path,
        minimal_config.license_map_path,
        extra_args=["--evidence-header", f"Authorization=Bearer {secret}"],
    )

    manifest_dir = minimal_config.manifests_root / "manifest_scan_target"
    manifest_paths = list(manifest_dir.rglob("*.json"))
    assert manifest_paths

    forbidden_markers = ("Authorization: Bearer", "Authorization=Bearer", secret)
    for path in manifest_paths:
        manifest_text = path.read_text(encoding="utf-8")
        assert not any(marker in manifest_text for marker in forbidden_markers)
        manifest_payload = json.loads(manifest_text)
        strings = _collect_strings(manifest_payload)
        assert not any("Bearer " in value for value in strings)
        if "Authorization" in strings:
            assert REDACTED in strings


def test_denylist_overrides_bucket(tmp_path: Path, minimal_config: PipelineTestConfig) -> None:
    denylist_path = tmp_path / "denylist.yaml"
    denylist_path.write_text(
        yaml.safe_dump(
            {
                "schema_version": "0.9",
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
        ),
        encoding="utf-8",
    )

    targets = [
        {
            "id": "force_yellow",
            "name": "Force Yellow Dataset",
            "license_profile": "permissive",
            "license_evidence": {"spdx_hint": "MIT", "url": "https://example.test/terms"},
        },
        {
            "id": "hard_red",
            "name": "Hard Red Dataset",
            "license_profile": "permissive",
            "license_evidence": {"spdx_hint": "MIT", "url": "https://blocked.example/terms"},
        },
    ]
    targets_path = minimal_config.write_targets(
        tmp_path, targets, companion_override={"denylist": str(denylist_path)}
    )

    run_driver(targets_path, minimal_config.license_map_path)

    green_rows = read_jsonl(minimal_config.queues_root / "green_download.jsonl")
    yellow_rows = read_jsonl(minimal_config.queues_root / "yellow_pipeline.jsonl")
    red_rows = read_jsonl(minimal_config.queues_root / "red_rejected.jsonl")

    assert not green_rows
    assert {row["id"] for row in yellow_rows} == {"force_yellow"}
    assert {row["id"] for row in red_rows} == {"hard_red"}
    assert yellow_rows[0]["bucket_reason"] == "denylist_force_yellow"
    assert yellow_rows[0]["signals"]["denylist"]["force_yellow"] is True
    assert red_rows[0]["bucket_reason"] == "denylist_hard_red"
    assert red_rows[0]["signals"]["denylist"]["hard_red"] is True


def test_content_check_block_action_forces_red(
    tmp_path: Path, minimal_config: PipelineTestConfig
) -> None:
    targets = [
        {
            "id": "pii_target",
            "name": "Contact me at test@example.com",
            "license_profile": "permissive",
            "license_evidence": {"spdx_hint": "MIT", "url": "https://example.test/terms"},
        }
    ]
    targets_path = minimal_config.write_targets(
        tmp_path,
        targets,
        globals_override={
            "default_content_checks": ["pii_scan"],
            "content_check_actions": {"pii_scan": "block"},
        },
    )

    run_driver(targets_path, minimal_config.license_map_path)

    red_rows = read_jsonl(minimal_config.queues_root / "red_rejected.jsonl")
    assert {row["id"] for row in red_rows} == {"pii_target"}
    assert red_rows[0]["content_check_action"] == "block"
    assert red_rows[0]["bucket_reason"] == "content_check_block"
    assert red_rows[0]["signals"]["content_check"]["action"] == "block"
    evaluation = json.loads(
        (minimal_config.manifests_root / "pii_target" / "evaluation.json").read_text(
            encoding="utf-8"
        )
    )
    assert evaluation["content_check_action"] == "block"


def test_content_check_quarantine_action_forces_yellow(
    tmp_path: Path, minimal_config: PipelineTestConfig
) -> None:
    targets = [
        {
            "id": "dual_use_target",
            "name": "Instructions for improvised explosive devices",
            "license_profile": "permissive",
            "license_evidence": {"spdx_hint": "MIT", "url": "https://example.test/terms"},
            "content_check_actions": {"dual_use_scan": "quarantine"},
        }
    ]
    targets_path = minimal_config.write_targets(
        tmp_path,
        targets,
        globals_override={"default_content_checks": ["dual_use_scan"]},
    )

    run_driver(targets_path, minimal_config.license_map_path)

    yellow_rows = read_jsonl(minimal_config.queues_root / "yellow_pipeline.jsonl")
    assert {row["id"] for row in yellow_rows} == {"dual_use_target"}
    assert yellow_rows[0]["content_check_action"] == "quarantine"
    assert yellow_rows[0]["bucket_reason"] == "content_check_quarantine"
    assert yellow_rows[0]["signals"]["content_check"]["action"] == "quarantine"
    evaluation = json.loads(
        (minimal_config.manifests_root / "dual_use_target" / "evaluation.json").read_text(
            encoding="utf-8"
        )
    )
    assert evaluation["content_check_action"] == "quarantine"


@pytest.mark.parametrize(
    "spdx_hint, extra_args, expected_bucket",
    [
        ("UNKNOWN", [], "YELLOW"),
        ("", [], "YELLOW"),
        ("MIT", ["--min-license-confidence", "0.99"], "YELLOW"),
    ],
)
def test_edge_conditions_and_low_confidence(
    tmp_path: Path,
    minimal_config: PipelineTestConfig,
    spdx_hint: str,
    extra_args: list[str],
    expected_bucket: str,
) -> None:
    targets = [
        {
            "id": "edge_target",
            "name": "Edge Dataset",
            "license_profile": "permissive",
            "license_evidence": {"spdx_hint": spdx_hint, "url": "https://example.test/terms"},
        }
    ]
    targets_path = minimal_config.write_targets(tmp_path, targets)

    run_driver(targets_path, minimal_config.license_map_path, extra_args=extra_args)

    buckets = {
        "GREEN": read_jsonl(minimal_config.queues_root / "green_download.jsonl"),
        "YELLOW": read_jsonl(minimal_config.queues_root / "yellow_pipeline.jsonl"),
        "RED": read_jsonl(minimal_config.queues_root / "red_rejected.jsonl"),
    }

    assert {row["id"] for row in buckets[expected_bucket]} == {"edge_target"}
    for bucket, rows in buckets.items():
        if bucket != expected_bucket:
            assert not rows
