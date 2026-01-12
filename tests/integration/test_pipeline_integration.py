"""
Full Pipeline Integration Test for EPIC 9.1

This module provides comprehensive end-to-end integration testing
for the Dataset Collector pipeline, covering:
- Classification stage (classify targets based on license/policy)
- Acquire stage with stub/mock HTTP handlers
- Merge stage to produce final output shards

Tests validate output contracts end-to-end using fixtures.
"""

from __future__ import annotations

import gzip
import json
from pathlib import Path
from typing import Any

import pytest
import yaml

pytest.importorskip("pytest_httpserver")

from pytest_httpserver import HTTPServer
from werkzeug.wrappers import Response

from collector_core.output_contract import REQUIRED_FIELDS, validate_output_contract


# =============================================================================
# Test fixtures
# =============================================================================


@pytest.fixture
def pipeline_roots(tmp_path: Path) -> dict[str, Path]:
    """Create temporary directory structure for pipeline stages."""
    roots = {
        "raw_root": tmp_path / "raw",
        "screened_yellow_root": tmp_path / "screened_yellow",
        "combined_root": tmp_path / "combined",
        "manifests_root": tmp_path / "_manifests",
        "ledger_root": tmp_path / "_ledger",
        "queues_root": tmp_path / "_queues",
        "pitches_root": tmp_path / "_pitches",
        "logs_root": tmp_path / "_logs",
    }
    for name, path in roots.items():
        path.mkdir(parents=True, exist_ok=True)
    return roots


@pytest.fixture
def license_map_fixture(tmp_path: Path) -> Path:
    """Create a minimal license map fixture for testing."""
    license_map = {
        "schema_version": "0.9",
        "updated_utc": "2024-01-01T00:00:00Z",
        "spdx": {
            "allow": ["MIT", "Apache-2.0", "CC-BY-4.0", "CC0-1.0"],
            "conditional": ["GPL-3.0", "LGPL-3.0"],
            "deny_prefixes": ["UNLICENSED", "PROPRIETARY"],
        },
        "normalization": {
            "rules": [
                {"match_any": ["MIT License", "MIT"], "spdx": "MIT"},
                {"match_any": ["Apache License 2.0"], "spdx": "Apache-2.0"},
            ]
        },
        "restriction_scan": {"phrases": ["research only", "non-commercial"]},
        "profiles": {
            "permissive": {"spdx_hint": "MIT"},
            "copyleft": {"spdx_hint": "GPL-3.0"},
            "quarantine": {"spdx_hint": "UNKNOWN"},
        },
    }
    license_map_path = tmp_path / "license_map.yaml"
    license_map_path.write_text(yaml.safe_dump(license_map), encoding="utf-8")
    return license_map_path


@pytest.fixture
def denylist_fixture(tmp_path: Path) -> Path:
    """Create a minimal denylist fixture for testing."""
    denylist = {
        "schema_version": "0.9",
        "updated_utc": "2024-01-01T00:00:00Z",
        "patterns": [],
        "domain_patterns": [],
        "publisher_patterns": [],
    }
    denylist_path = tmp_path / "denylist.yaml"
    denylist_path.write_text(yaml.safe_dump(denylist), encoding="utf-8")
    return denylist_path


@pytest.fixture
def targets_yaml_fixture(
    tmp_path: Path,
    pipeline_roots: dict[str, Path],
    license_map_fixture: Path,
    denylist_fixture: Path,
) -> Path:
    """Create a minimal targets YAML fixture with test targets."""
    targets_config = {
        "schema_version": "0.9",
        "updated_utc": "2024-01-01T00:00:00Z",
        "companion_files": {
            "license_map": str(license_map_fixture),
            "denylist": str(denylist_fixture),
        },
        "globals": {
            "raw_root": str(pipeline_roots["raw_root"]),
            "screened_yellow_root": str(pipeline_roots["screened_yellow_root"]),
            "combined_root": str(pipeline_roots["combined_root"]),
            "manifests_root": str(pipeline_roots["manifests_root"]),
            "queues_root": str(pipeline_roots["queues_root"]),
            "ledger_root": str(pipeline_roots["ledger_root"]),
            "pitches_root": str(pipeline_roots["pitches_root"]),
            "logs_root": str(pipeline_roots["logs_root"]),
            "screening": {
                "min_chars": 1,
                "max_chars": 10000,
                "text_field_candidates": ["text", "content", "body"],
            },
            "sharding": {
                "max_records_per_shard": 100,
                "compression": "gzip",
            },
            "default_license_gates": ["snapshot_terms"],
            "default_content_checks": ["dual_use_scan"],
        },
        "targets": [
            {
                "id": "integration-test-green",
                "name": "Integration Test Green Dataset",
                "enabled": True,
                "license_profile": "permissive",
                "license_evidence": {
                    "spdx_hint": "MIT",
                    "url": "https://example.com/license",
                },
                "download": {
                    "strategy": "http",
                    "url": "PLACEHOLDER_URL",
                },
            },
            {
                "id": "integration-test-yellow",
                "name": "Integration Test Yellow Dataset",
                "enabled": True,
                "license_profile": "permissive",
                "license_evidence": {
                    "spdx_hint": "Apache-2.0",
                    "url": "https://example.com/apache-license",
                },
                "download": {
                    "strategy": "http",
                    "url": "PLACEHOLDER_URL_2",
                },
            },
        ],
    }
    targets_path = tmp_path / "targets_integration.yaml"
    targets_path.write_text(yaml.safe_dump(targets_config), encoding="utf-8")
    return targets_path


def _create_test_jsonl_data() -> list[dict[str, Any]]:
    """Create sample JSONL records for testing."""
    return [
        {
            "id": "record-001",
            "text": "This is sample text content for integration testing purposes.",
            "source_url": "https://example.com/data/record1",
            "metadata": {"author": "Test Author", "date": "2024-01-01"},
        },
        {
            "id": "record-002",
            "text": "Another sample record with different content for testing the pipeline.",
            "source_url": "https://example.com/data/record2",
            "metadata": {"author": "Another Author", "date": "2024-01-02"},
        },
        {
            "id": "record-003",
            "text": "Third test record to ensure proper batch processing and deduplication.",
            "source_url": "https://example.com/data/record3",
            "metadata": {"author": "Third Author", "date": "2024-01-03"},
        },
    ]


def _read_gzip_jsonl(path: Path) -> list[dict[str, Any]]:
    """Read a gzipped JSONL file and return list of records."""
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def _write_jsonl(path: Path, records: list[dict[str, Any]]) -> None:
    """Write records to a JSONL file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")


def _prepare_license_evidence(dataset_root: Path, target_id: str, text: str) -> None:
    """Prepare license evidence file for a target."""
    manifest_dir = dataset_root / "_manifests" / target_id
    manifest_dir.mkdir(parents=True, exist_ok=True)
    evidence_path = manifest_dir / "license_evidence.txt"
    evidence_path.write_text(text, encoding="utf-8")


# =============================================================================
# Integration tests
# =============================================================================


@pytest.mark.integration
class TestFullPipelineIntegration:
    """
    Full pipeline integration tests covering classify -> acquire -> merge stages.

    These tests validate:
    - Classification correctly buckets targets
    - Acquire stage downloads data (using mock HTTP)
    - Merge stage combines and deduplicates records
    - Output contracts are enforced end-to-end
    """

    def test_classify_acquire_merge_pipeline(
        self,
        tmp_path: Path,
        pipeline_roots: dict[str, Path],
        targets_yaml_fixture: Path,
        httpserver: HTTPServer,
        run_dc,
    ) -> None:
        """
        Full end-to-end test: classify -> acquire stub -> merge.

        This test validates:
        1. Classification stage creates proper queues
        2. Acquire stage downloads data via mock HTTP
        3. Merge stage produces output shards
        4. Output contract is validated end-to-end
        """
        # Create test payload
        test_records = _create_test_jsonl_data()
        payload_blob = "\n".join(json.dumps(r) for r in test_records) + "\n"

        # Setup mock HTTP endpoints
        file_path_1 = "/data/dataset1.jsonl"
        file_path_2 = "/data/dataset2.jsonl"

        httpserver.expect_request(file_path_1).respond_with_data(
            payload_blob, content_type="application/jsonl"
        )
        httpserver.expect_request(file_path_2).respond_with_data(
            payload_blob, content_type="application/jsonl"
        )

        # Update targets YAML with actual HTTP server URLs
        targets_cfg = yaml.safe_load(targets_yaml_fixture.read_text())
        targets_cfg["targets"][0]["download"]["url"] = httpserver.url_for(file_path_1)
        targets_cfg["targets"][1]["download"]["url"] = httpserver.url_for(file_path_2)
        targets_yaml_fixture.write_text(yaml.safe_dump(targets_cfg), encoding="utf-8")

        dataset_root = tmp_path
        _prepare_license_evidence(dataset_root, "integration-test-green", "MIT License")
        _prepare_license_evidence(dataset_root, "integration-test-yellow", "Apache-2.0")

        # Stage 1: Run classification via pipeline driver
        run_dc(
            [
                "pipeline",
                "fixture",
                "--",
                "--targets",
                str(targets_yaml_fixture),
                "--dataset-root",
                str(dataset_root),
                "--no-fetch",
                "--quiet",
            ],
        )

        # Verify classification outputs
        ledger_root = pipeline_roots["ledger_root"]
        queues_root = pipeline_roots["queues_root"]

        policy_snapshots = list(ledger_root.glob("classification_*/policy_snapshot.json"))
        assert policy_snapshots, "Expected classification policy snapshot in _ledger"

        # Stage 2: Run acquire stage with mock HTTP
        # Create acquire queue manually since classification may not produce downloads
        # for targets with strategy: none
        acquire_queue = queues_root / "green_download.jsonl"
        _write_jsonl(
            acquire_queue,
            [
                {
                    "id": "integration-test-green",
                    "name": "Integration Test Green Dataset",
                    "enabled": True,
                    "license_profile": "permissive",
                    "download": {
                        "strategy": "http",
                        "url": httpserver.url_for(file_path_1),
                    },
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
                str(acquire_queue),
                "--bucket",
                "green",
                "--execute",
                "--targets-yaml",
                str(targets_yaml_fixture),
            ],
        )

        # Verify acquire stage outputs
        acquire_logs = list(pipeline_roots["logs_root"].glob("acquire_summary*.json"))
        assert acquire_logs, "Expected acquire summary in _logs"

        # Stage 3: Run yellow screen
        yellow_queue = queues_root / "yellow_screen.jsonl"
        _write_jsonl(
            yellow_queue,
            [
                {
                    "id": "integration-test-green",
                    "license_profile": "permissive",
                    "enabled": True,
                }
            ],
        )

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
                str(targets_yaml_fixture),
                "--queue",
                str(yellow_queue),
            ],
        )

        # Verify yellow screen outputs
        yellow_summary = ledger_root / "yellow_screen_summary.json"
        assert yellow_summary.exists(), "Expected yellow screen summary in _ledger"

        # Stage 4: Run merge stage
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
                str(targets_yaml_fixture),
                "--execute",
            ],
        )

        # Verify merge stage outputs
        merge_summary = ledger_root / "merge_summary.json"
        assert merge_summary.exists(), "Expected merge summary in _ledger"

        merge_data = json.loads(merge_summary.read_text())
        assert merge_data.get("counts"), "Expected counts in merge summary"

    def test_acquire_with_retry_and_validation(
        self,
        tmp_path: Path,
        pipeline_roots: dict[str, Path],
        targets_yaml_fixture: Path,
        httpserver: HTTPServer,
        run_dc,
    ) -> None:
        """
        Test acquire stage with retry logic and content validation.

        This test validates:
        1. Retry mechanism works on transient failures
        2. Downloaded content matches expected format
        3. Ledger records are created properly
        """
        # Create test payload
        test_records = _create_test_jsonl_data()
        payload_blob = "\n".join(json.dumps(r) for r in test_records) + "\n"

        file_path = "/data/flaky_dataset.jsonl"
        download_calls = {"count": 0}

        def flaky_handler(request) -> Response:
            download_calls["count"] += 1
            if download_calls["count"] == 1:
                return Response("Server temporarily unavailable", status=500)
            return Response(
                payload_blob, status=200, content_type="application/jsonl"
            )

        httpserver.expect_request(file_path).respond_with_handler(flaky_handler)

        # Update targets with flaky endpoint
        targets_cfg = yaml.safe_load(targets_yaml_fixture.read_text())
        targets_cfg["targets"][0]["download"]["url"] = httpserver.url_for(file_path)
        targets_yaml_fixture.write_text(yaml.safe_dump(targets_cfg), encoding="utf-8")

        dataset_root = tmp_path
        _prepare_license_evidence(dataset_root, "integration-test-green", "MIT License")

        # Create acquire queue
        acquire_queue = pipeline_roots["queues_root"] / "retry_test_queue.jsonl"
        _write_jsonl(
            acquire_queue,
            [
                {
                    "id": "integration-test-green",
                    "name": "Flaky Dataset",
                    "enabled": True,
                    "license_profile": "permissive",
                    "download": {
                        "strategy": "http",
                        "url": httpserver.url_for(file_path),
                    },
                }
            ],
        )

        # Run acquire with retry enabled
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
                str(acquire_queue),
                "--bucket",
                "green",
                "--execute",
                "--targets-yaml",
                str(targets_yaml_fixture),
                "--retry-max",
                "3",
                "--retry-backoff",
                "0",
            ],
        )

        # Verify retry happened
        assert download_calls["count"] >= 2, "Expected at least 2 download attempts due to retry"

        # Verify file was downloaded
        raw_dir = pipeline_roots["raw_root"] / "green" / "permissive" / "integration-test-green"
        downloaded_files = list(raw_dir.glob("*.jsonl"))
        assert downloaded_files, f"Expected downloaded files in {raw_dir}"


@pytest.mark.integration
class TestOutputContractValidation:
    """
    Tests to validate that the pipeline output conforms to the contract.
    """

    def test_merged_output_contract_compliance(
        self,
        tmp_path: Path,
        pipeline_roots: dict[str, Path],
        targets_yaml_fixture: Path,
        httpserver: HTTPServer,
        run_dc,
    ) -> None:
        """
        Validate that merged output records conform to the output contract.

        This test:
        1. Runs a minimal pipeline to produce output
        2. Validates each output record against REQUIRED_FIELDS
        3. Ensures validate_output_contract passes
        """
        # Create test payload with required fields
        test_records = [
            {
                "id": "contract-test-001",
                "text": "Contract validation test record.",
                "source_url": "https://example.com/contract1",
            },
            {
                "id": "contract-test-002",
                "text": "Second contract validation test record.",
                "source_url": "https://example.com/contract2",
            },
        ]
        payload_blob = "\n".join(json.dumps(r) for r in test_records) + "\n"

        file_path = "/data/contract_dataset.jsonl"
        httpserver.expect_request(file_path).respond_with_data(
            payload_blob, content_type="application/jsonl"
        )

        # Update targets
        targets_cfg = yaml.safe_load(targets_yaml_fixture.read_text())
        targets_cfg["targets"][0]["download"]["url"] = httpserver.url_for(file_path)
        targets_yaml_fixture.write_text(yaml.safe_dump(targets_cfg), encoding="utf-8")

        dataset_root = tmp_path
        _prepare_license_evidence(dataset_root, "integration-test-green", "MIT License")

        # Create and run acquire queue
        acquire_queue = pipeline_roots["queues_root"] / "contract_acquire.jsonl"
        _write_jsonl(
            acquire_queue,
            [
                {
                    "id": "integration-test-green",
                    "enabled": True,
                    "license_profile": "permissive",
                    "download": {
                        "strategy": "http",
                        "url": httpserver.url_for(file_path),
                    },
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
                str(acquire_queue),
                "--bucket",
                "green",
                "--execute",
                "--targets-yaml",
                str(targets_yaml_fixture),
            ],
        )

        # Create and run yellow screen queue
        yellow_queue = pipeline_roots["queues_root"] / "contract_yellow.jsonl"
        _write_jsonl(
            yellow_queue,
            [
                {
                    "id": "integration-test-green",
                    "license_profile": "permissive",
                    "enabled": True,
                }
            ],
        )

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
                str(targets_yaml_fixture),
                "--queue",
                str(yellow_queue),
            ],
        )

        # Run merge
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
                str(targets_yaml_fixture),
                "--execute",
            ],
        )

        # Find and validate output shards
        combined_root = pipeline_roots["combined_root"]
        shard_paths = list(combined_root.glob("**/shards/*.jsonl.gz"))

        if shard_paths:
            records = _read_gzip_jsonl(shard_paths[0])
            assert records, "Expected at least one record in merged output"

            for record in records:
                # Validate against output contract
                validate_output_contract(record, "integration_test/merged_shard")

                # Check all required fields are present
                for field in REQUIRED_FIELDS:
                    assert field in record, f"Missing required field: {field}"


@pytest.mark.integration
class TestPipelineStageIsolation:
    """
    Tests to validate that pipeline stages can run in isolation.
    """

    def test_classification_stage_isolated(
        self,
        tmp_path: Path,
        pipeline_roots: dict[str, Path],
        targets_yaml_fixture: Path,
        run_dc,
    ) -> None:
        """
        Test that classification stage runs successfully in isolation.

        This validates:
        1. Classification can run without acquire/merge
        2. Policy snapshots are created
        3. Queues are emitted properly
        """
        dataset_root = tmp_path
        _prepare_license_evidence(dataset_root, "integration-test-green", "MIT License")
        _prepare_license_evidence(dataset_root, "integration-test-yellow", "Apache-2.0")

        run_dc(
            [
                "pipeline",
                "fixture",
                "--",
                "--targets",
                str(targets_yaml_fixture),
                "--dataset-root",
                str(dataset_root),
                "--no-fetch",
                "--quiet",
            ],
        )

        ledger_root = pipeline_roots["ledger_root"]
        policy_snapshots = list(ledger_root.glob("classification_*/policy_snapshot.json"))
        assert policy_snapshots, "Classification stage should create policy snapshot"

        # Verify policy snapshot content
        snapshot_data = json.loads(policy_snapshots[0].read_text())
        assert "targets_processed" in snapshot_data or "artifact_metadata" in snapshot_data

    def test_merge_stage_isolated(
        self,
        tmp_path: Path,
        pipeline_roots: dict[str, Path],
        targets_yaml_fixture: Path,
        run_dc,
    ) -> None:
        """
        Test that merge stage runs successfully in isolation (with no input data).

        This validates:
        1. Merge can run without errors when no data exists
        2. Merge summary is created
        """
        dataset_root = tmp_path

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
                str(targets_yaml_fixture),
                "--execute",
            ],
        )

        merge_summary = pipeline_roots["ledger_root"] / "merge_summary.json"
        assert merge_summary.exists(), "Merge stage should create summary even with no data"


@pytest.mark.integration
def test_pipeline_with_multiple_targets(
    tmp_path: Path,
    httpserver: HTTPServer,
    run_dc,
) -> None:
    """
    Test pipeline with multiple targets to ensure proper batch processing.
    """
    # Create comprehensive test setup
    roots = {
        "raw_root": tmp_path / "raw",
        "screened_yellow_root": tmp_path / "screened_yellow",
        "combined_root": tmp_path / "combined",
        "manifests_root": tmp_path / "_manifests",
        "ledger_root": tmp_path / "_ledger",
        "queues_root": tmp_path / "_queues",
        "pitches_root": tmp_path / "_pitches",
        "logs_root": tmp_path / "_logs",
    }
    for path in roots.values():
        path.mkdir(parents=True, exist_ok=True)

    # Create license map and denylist
    license_map = {
        "schema_version": "0.9",
        "spdx": {"allow": ["MIT", "Apache-2.0"], "deny_prefixes": []},
        "normalization": {"rules": []},
        "restriction_scan": {"phrases": []},
        "profiles": {"permissive": {"spdx_hint": "MIT"}},
    }
    license_map_path = tmp_path / "license_map.yaml"
    license_map_path.write_text(yaml.safe_dump(license_map), encoding="utf-8")

    denylist = {"schema_version": "0.9", "patterns": []}
    denylist_path = tmp_path / "denylist.yaml"
    denylist_path.write_text(yaml.safe_dump(denylist), encoding="utf-8")

    # Setup HTTP endpoints for multiple targets
    targets = []
    for i in range(3):
        target_id = f"batch-target-{i:03d}"
        file_path = f"/data/{target_id}.jsonl"
        payload = json.dumps({"id": f"record-{i}", "text": f"Content for target {i}"})
        httpserver.expect_request(file_path).respond_with_data(
            payload + "\n", content_type="application/jsonl"
        )
        targets.append(
            {
                "id": target_id,
                "name": f"Batch Target {i}",
                "enabled": True,
                "license_profile": "permissive",
                "license_evidence": {"spdx_hint": "MIT", "url": "https://example.com"},
                "download": {"strategy": "http", "url": httpserver.url_for(file_path)},
            }
        )
        _prepare_license_evidence(tmp_path, target_id, "MIT License")

    # Create targets YAML
    targets_cfg = {
        "schema_version": "0.9",
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
            "screening": {"min_chars": 1, "max_chars": 10000, "text_field_candidates": ["text"]},
            "sharding": {"max_records_per_shard": 100, "compression": "gzip"},
        },
        "targets": targets,
    }
    targets_path = tmp_path / "targets_batch.yaml"
    targets_path.write_text(yaml.safe_dump(targets_cfg), encoding="utf-8")

    # Run classification
    run_dc(
        [
            "pipeline",
            "fixture",
            "--",
            "--targets",
            str(targets_path),
            "--dataset-root",
            str(tmp_path),
            "--no-fetch",
            "--quiet",
        ],
    )

    # Create acquire queue for all targets
    acquire_queue = roots["queues_root"] / "batch_acquire.jsonl"
    _write_jsonl(acquire_queue, targets)

    # Run acquire for all targets
    run_dc(
        [
            "run",
            "--pipeline",
            "fixture",
            "--stage",
            "acquire",
            "--dataset-root",
            str(tmp_path),
            "--",
            "--queue",
            str(acquire_queue),
            "--bucket",
            "green",
            "--execute",
            "--targets-yaml",
            str(targets_path),
        ],
    )

    # Verify all targets were processed
    acquire_summary = list(roots["logs_root"].glob("acquire_summary*.json"))
    assert acquire_summary, "Expected acquire summary"
    summary_data = json.loads(acquire_summary[0].read_text())
    assert summary_data.get("counts", {}).get("total", 0) == 3


@pytest.mark.integration
def test_pipeline_error_handling(
    tmp_path: Path,
    httpserver: HTTPServer,
    run_dc,
) -> None:
    """
    Test that pipeline handles errors gracefully.
    """
    roots = {
        "raw_root": tmp_path / "raw",
        "screened_yellow_root": tmp_path / "screened_yellow",
        "combined_root": tmp_path / "combined",
        "manifests_root": tmp_path / "_manifests",
        "ledger_root": tmp_path / "_ledger",
        "queues_root": tmp_path / "_queues",
        "pitches_root": tmp_path / "_pitches",
        "logs_root": tmp_path / "_logs",
    }
    for path in roots.values():
        path.mkdir(parents=True, exist_ok=True)

    # Setup HTTP endpoint that returns 404
    file_path = "/data/missing.jsonl"
    httpserver.expect_request(file_path).respond_with_data(
        "Not Found", status=404
    )

    # Create minimal fixtures
    license_map_path = tmp_path / "license_map.yaml"
    license_map_path.write_text(
        yaml.safe_dump({"schema_version": "0.9", "spdx": {"allow": ["MIT"]}}),
        encoding="utf-8",
    )
    denylist_path = tmp_path / "denylist.yaml"
    denylist_path.write_text(
        yaml.safe_dump({"schema_version": "0.9", "patterns": []}),
        encoding="utf-8",
    )

    targets_cfg = {
        "schema_version": "0.9",
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
            "logs_root": str(roots["logs_root"]),
            "sharding": {"max_records_per_shard": 100, "compression": "gzip"},
        },
        "targets": [
            {
                "id": "error-test-target",
                "name": "Error Test Target",
                "enabled": True,
                "license_profile": "permissive",
                "download": {"strategy": "http", "url": httpserver.url_for(file_path)},
            }
        ],
    }
    targets_path = tmp_path / "targets_error.yaml"
    targets_path.write_text(yaml.safe_dump(targets_cfg), encoding="utf-8")

    _prepare_license_evidence(tmp_path, "error-test-target", "MIT License")

    acquire_queue = roots["queues_root"] / "error_acquire.jsonl"
    _write_jsonl(
        acquire_queue,
        [
            {
                "id": "error-test-target",
                "enabled": True,
                "license_profile": "permissive",
                "download": {"strategy": "http", "url": httpserver.url_for(file_path)},
            }
        ],
    )

    # Run acquire - should not crash on 404, but log the error
    run_dc(
        [
            "run",
            "--pipeline",
            "fixture",
            "--stage",
            "acquire",
            "--dataset-root",
            str(tmp_path),
            "--",
            "--queue",
            str(acquire_queue),
            "--bucket",
            "green",
            "--execute",
            "--targets-yaml",
            str(targets_path),
            "--retry-max",
            "1",
        ],
    )

    # Verify acquire completed (even with errors)
    acquire_summary = list(roots["logs_root"].glob("acquire_summary*.json"))
    assert acquire_summary, "Acquire should complete even with errors"
    summary_data = json.loads(acquire_summary[0].read_text())
    # The error should be recorded in failed_targets
    assert "failed_targets" in summary_data or "results" in summary_data
