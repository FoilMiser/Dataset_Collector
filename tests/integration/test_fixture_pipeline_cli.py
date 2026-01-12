from __future__ import annotations

from pathlib import Path

def _prepare_license_evidence(dataset_root: Path, target_id: str, text: str) -> None:
    manifest_dir = dataset_root / "_manifests" / target_id
    manifest_dir.mkdir(parents=True, exist_ok=True)
    evidence_path = manifest_dir / "license_evidence.txt"
    evidence_path.write_text(text, encoding="utf-8")

def test_fixture_pipeline_dc_cli_creates_ledger_artifacts(
    tmp_path: Path,
    repo_root: Path,
    run_dc,
    sample_jsonl_writer,
) -> None:
    targets_path = repo_root / "tests" / "fixtures" / "targets_fixture.yaml"
    dataset_root = tmp_path / "dataset"
    dataset_root.mkdir(parents=True, exist_ok=True)

    _prepare_license_evidence(dataset_root, "fixture-green", "MIT License")
    _prepare_license_evidence(dataset_root, "fixture-red", "GPL License")

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

    ledger_root = dataset_root / "_ledger"
    policy_snapshots = list(ledger_root.glob("classification_*/policy_snapshot.json"))
    assert policy_snapshots, "Expected classification policy snapshot in _ledger"

    queue_path = dataset_root / "_queues" / "green_download.jsonl"
    assert queue_path.exists(), "Expected classify to emit green queue"

    yellow_queue = dataset_root / "_queues" / "yellow_screen.jsonl"
    sample_jsonl_writer(
        yellow_queue,
        [
            {
                "id": "fixture-green",
                "license_profile": "permissive",
            }
        ],
    )

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

    run_dc(
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

    merge_summary = ledger_root / "merge_summary.json"
    assert merge_summary.exists(), "Expected merge summary in _ledger"


def test_dc_cli_merge_requires_dataset_root(tmp_path: Path, repo_root: Path, run_dc) -> None:
    targets_path = repo_root / "tests" / "fixtures" / "targets_fixture.yaml"

    result = run_dc(
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
        check=False,
        capture_output=True,
    )

    assert result.returncode != 0, "Expected merge to fail without --dataset-root"
    combined_output = (result.stderr or "") + (result.stdout or "")
    assert "Refusing to use /data" in combined_output
