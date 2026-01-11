from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

from tests.fixtures import create_sample_jsonl


def _prepare_license_evidence(dataset_root: Path, target_id: str, text: str) -> None:
    manifest_dir = dataset_root / "_manifests" / target_id
    manifest_dir.mkdir(parents=True, exist_ok=True)
    evidence_path = manifest_dir / "license_evidence.txt"
    evidence_path.write_text(text, encoding="utf-8")


def _build_dc_env(repo_root: Path) -> dict[str, str]:
    schema_root = repo_root / "src" / "schemas"
    if not schema_root.exists():
        try:
            schema_root.symlink_to(repo_root / "schemas", target_is_directory=True)
        except OSError:
            shutil.copytree(repo_root / "schemas", schema_root)
    env = os.environ.copy()
    env["PYTHONPATH"] = f"{repo_root / 'src'}{os.pathsep}{env.get('PYTHONPATH', '')}"
    return env


def _run_dc(repo_root: Path, args: list[str]) -> None:
    env = _build_dc_env(repo_root)
    subprocess.run([sys.executable, "-m", "collector_core.dc_cli", *args], check=True, cwd=repo_root, env=env)


def _run_dc_result(repo_root: Path, args: list[str]) -> subprocess.CompletedProcess[str]:
    env = _build_dc_env(repo_root)
    return subprocess.run(
        [sys.executable, "-m", "collector_core.dc_cli", *args],
        check=False,
        cwd=repo_root,
        env=env,
        text=True,
        capture_output=True,
    )


def test_fixture_pipeline_dc_cli_creates_ledger_artifacts(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[2]
    targets_path = repo_root / "tests" / "fixtures" / "targets_fixture.yaml"
    dataset_root = tmp_path / "dataset"
    dataset_root.mkdir(parents=True, exist_ok=True)

    _prepare_license_evidence(dataset_root, "fixture-green", "MIT License")
    _prepare_license_evidence(dataset_root, "fixture-red", "GPL License")

    _run_dc(
        repo_root,
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

    ledger_root = dataset_root / "_ledger"
    policy_snapshots = list(ledger_root.glob("classification_*/policy_snapshot.json"))
    assert policy_snapshots, "Expected classification policy snapshot in _ledger"

    queue_path = dataset_root / "_queues" / "green_download.jsonl"
    assert queue_path.exists(), "Expected classify to emit green queue"

    yellow_queue = dataset_root / "_queues" / "yellow_screen.jsonl"
    create_sample_jsonl(
        yellow_queue,
        [
            {
                "id": "fixture-green",
                "license_profile": "permissive",
            }
        ],
    )

    _run_dc(
        repo_root,
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
            str(queue_path),
            "--bucket",
            "green",
            "--execute",
            "--targets-yaml",
            str(targets_path),
        ],
    )

    acquire_checks = list(ledger_root.glob("acquire_*/*/checks/dual_use_scan.json"))
    assert acquire_checks, "Expected acquire checks in _ledger"

    _run_dc(
        repo_root,
        [
            "run",
            "--pipeline",
            "fixture",
            "--stage",
            "yellow_screen",
            "--dataset-root",
            str(dataset_root),
            "--",
            "--targets",
            str(targets_path),
            "--queue",
            str(yellow_queue),
        ],
    )

    yellow_summary = ledger_root / "yellow_screen_summary.json"
    assert yellow_summary.exists(), "Expected yellow screen summary in _ledger"

    _run_dc(
        repo_root,
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

    merge_summary = ledger_root / "merge_summary.json"
    assert merge_summary.exists(), "Expected merge summary in _ledger"


def test_dc_cli_merge_requires_dataset_root(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[2]
    targets_path = repo_root / "tests" / "fixtures" / "targets_fixture.yaml"

    result = _run_dc_result(
        repo_root,
        [
            "run",
            "--pipeline",
            "fixture",
            "--stage",
            "merge",
            "--",
            "--targets",
            str(targets_path),
            "--execute",
        ],
    )

    assert result.returncode != 0, "Expected merge to fail without --dataset-root"
    combined_output = (result.stderr or "") + (result.stdout or "")
    assert "Refusing to use /data" in combined_output
