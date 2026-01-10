from __future__ import annotations

import gzip
import json
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

pytest.importorskip("pytest_httpserver")

from pytest_httpserver import HTTPServer
from werkzeug.wrappers import Response

from kg_nav_pipeline_v2 import acquire_worker as kg_acquire

pytest.importorskip("pytest_httpserver")


def _write_targets(path: Path, roots: dict[str, Path], target_id: str) -> None:
    targets_cfg = {
        "schema_version": "0.9",
        "globals": {
            "raw_root": str(roots["raw_root"]),
            "screened_yellow_root": str(roots["screened_root"]),
            "combined_root": str(roots["combined_root"]),
            "manifests_root": str(roots["manifests_root"]),
            "ledger_root": str(roots["ledger_root"]),
            "pitches_root": str(roots["pitches_root"]),
            "logs_root": str(roots["logs_root"]),
            "screening": {
                "min_chars": 1,
                "max_chars": 1000,
                "text_field_candidates": ["text"],
            },
            "sharding": {"max_records_per_shard": 50, "compression": "gzip"},
        },
        "targets": [{"id": target_id, "name": "Zenodo test target"}],
    }
    path.write_text(yaml.safe_dump(targets_cfg), encoding="utf-8")


def _read_gzip_jsonl(path: Path) -> list[dict[str, object]]:
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def test_full_pipeline_zenodo_retry(tmp_path: Path, httpserver: HTTPServer) -> None:
    record_path = "/api/records/123"
    file_path = "/files/data.jsonl"

    payload_rows = [{"record_id": "row-1", "text": "Hello from Zenodo"}]
    payload_blob = "\n".join(json.dumps(row) for row in payload_rows) + "\n"

    httpserver.expect_request(record_path).respond_with_json(
        {
            "files": [
                {
                    "links": {"self": httpserver.url_for(file_path)},
                    "key": "data.jsonl",
                }
            ]
        }
    )

    download_calls = {"count": 0}

    def flaky_download(request) -> Response:
        download_calls["count"] += 1
        if download_calls["count"] == 1:
            return Response("transient error", status=500)
        return Response(payload_blob, status=200, content_type="application/jsonl")

    httpserver.expect_request(file_path).respond_with_handler(flaky_download)

    roots = {
        "raw_root": tmp_path / "raw",
        "screened_root": tmp_path / "screened_yellow",
        "combined_root": tmp_path / "combined",
        "manifests_root": tmp_path / "_manifests",
        "ledger_root": tmp_path / "_ledger",
        "pitches_root": tmp_path / "_pitches",
        "logs_root": tmp_path / "_logs",
    }

    targets_path = tmp_path / "targets.yaml"
    _write_targets(targets_path, roots, target_id="zenodo_target")

    acquire_queue = tmp_path / "acquire_queue.jsonl"
    acquire_queue.write_text(
        json.dumps(
            {
                "id": "zenodo_target",
                "enabled": True,
                "license_profile": "permissive",
                "download": {
                    "strategy": "zenodo",
                    "api": httpserver.url_for(record_path),
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )

    subprocess.run(
        [
            sys.executable,
            "kg_nav_pipeline_v2/acquire_worker.py",
            "--queue",
            str(acquire_queue),
            "--targets-yaml",
            str(targets_path),
            "--bucket",
            "yellow",
            "--execute",
            "--retry-max",
            "2",
            "--retry-backoff",
            "0",
        ],
        check=True,
        cwd=Path(".").resolve(),
    )

    assert download_calls["count"] == 2

    screen_queue = tmp_path / "screen_queue.jsonl"
    screen_queue.write_text(
        json.dumps({"id": "zenodo_target", "license_profile": "permissive", "enabled": True})
        + "\n",
        encoding="utf-8",
    )

    subprocess.run(
        [
            sys.executable,
            "kg_nav_pipeline_v2/yellow_screen_worker.py",
            "--targets",
            str(targets_path),
            "--queue",
            str(screen_queue),
            "--execute",
        ],
        check=True,
        cwd=Path(".").resolve(),
    )

    subprocess.run(
        [
            sys.executable,
            "kg_nav_pipeline_v2/merge_worker.py",
            "--targets",
            str(targets_path),
            "--execute",
        ],
        check=True,
        cwd=Path(".").resolve(),
    )

    shard_paths = list((roots["combined_root"] / "permissive" / "shards").glob("*.jsonl.gz"))
    assert shard_paths

    records = _read_gzip_jsonl(shard_paths[0])
    assert records
    assert records[0]["text"] == "Hello from Zenodo"


def test_resume_partial_download(tmp_path: Path, httpserver: HTTPServer) -> None:
    payload = b"hello world"
    partial = payload[:5]
    remaining = payload[5:]
    file_path = "/files/partial.bin"

    def range_handler(request) -> Response:
        expected = f"bytes={len(partial)}-"
        assert request.headers.get("Range") == expected
        return Response(
            remaining,
            status=206,
            headers={
                "Content-Range": f"bytes {len(partial)}-{len(payload) - 1}/{len(payload)}",
            },
        )

    httpserver.expect_request(
        file_path, headers={"Range": f"bytes={len(partial)}-"}
    ).respond_with_handler(range_handler)

    out_path = tmp_path / "partial.bin"
    out_path.write_bytes(partial)

    ctx = kg_acquire.AcquireContext(
        roots=kg_acquire.Roots(raw_root=tmp_path, manifests_root=tmp_path, logs_root=tmp_path),
        limits=kg_acquire.Limits(limit_targets=None, limit_files=None, max_bytes_per_target=None),
        mode=kg_acquire.RunMode(
            execute=True,
            overwrite=False,
            verify_sha256=False,
            verify_zenodo_md5=False,
            enable_resume=True,
            workers=1,
        ),
        retry=kg_acquire.RetryConfig(max_attempts=1, backoff_base=0),
    )

    result = kg_acquire._http_download_with_resume(ctx, httpserver.url_for(file_path), out_path)
    assert result["status"] == "ok"
    assert out_path.read_bytes() == payload
