"""
Shared pytest fixtures for Dataset Collector tests.

Provides common mocks and fixtures for:
- Targets YAML configurations
- HTTP responses
- File system operations
- Rate limiters
"""

from __future__ import annotations

import json
from collections.abc import Generator
from pathlib import Path
import sys
from typing import Any
from unittest.mock import MagicMock

import pytest

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if SRC_ROOT.is_dir():
    sys.path.insert(0, str(SRC_ROOT))


# =============================================================================
# Targets YAML fixtures
# =============================================================================


@pytest.fixture
def minimal_target() -> dict[str, Any]:
    """A minimal valid target configuration."""
    return {
        "id": "test-dataset",
        "name": "Test Dataset",
        "enabled": True,
        "license_profile": "CC-BY-4.0",
        "license_evidence": {"url": "https://example.com/license"},
        "download": {"strategy": "http", "url": "https://example.com/data.zip"},
    }


@pytest.fixture
def targets_yaml_content(minimal_target: dict[str, Any]) -> dict[str, Any]:
    """Complete targets YAML structure."""
    return {
        "schema_version": "0.9",
        "updated_utc": "2024-01-01T00:00:00Z",
        "companion_files": {
            "license_map": "shared/license_map.yaml",
            "field_schemas": "shared/field_schemas.yaml",
        },
        "globals": {
            "retry": {"max": 3, "backoff": 2.0},
            "pitch_limits": {"sample_limit": 25, "text_limit": 400},
        },
        "targets": [minimal_target],
    }


@pytest.fixture
def targets_yaml_file(
    tmp_path: Path, targets_yaml_content: dict[str, Any]
) -> Generator[Path, None, None]:
    """Write targets YAML to temp file and yield path."""
    import yaml

    targets_file = tmp_path / "targets_test.yaml"
    targets_file.write_text(yaml.safe_dump(targets_yaml_content))
    yield targets_file


# =============================================================================
# HTTP response fixtures
# =============================================================================


@pytest.fixture
def fake_http_response() -> Any:
    """Create a fake HTTP response with configurable attributes."""

    def _create(
        content: bytes = b"test content",
        status_code: int = 200,
        headers: dict[str, str] | None = None,
        url: str = "https://example.com/test",
    ) -> MagicMock:
        response = MagicMock()
        response.content = content
        response.status_code = status_code
        response.headers = headers or {"Content-Length": str(len(content))}
        response.url = url
        response.ok = 200 <= status_code < 400
        response.iter_content = MagicMock(return_value=iter([content]))
        response.raise_for_status = MagicMock()
        if status_code >= 400:
            response.raise_for_status.side_effect = Exception(f"HTTP {status_code}")
        return response

    return _create


@pytest.fixture
def mock_requests_get(fake_http_response: Any) -> Generator[MagicMock, None, None]:
    """Mock requests.get to return fake responses."""
    from unittest.mock import patch

    with patch("requests.get") as mock_get:
        mock_get.return_value = fake_http_response()
        yield mock_get


# =============================================================================
# File system fixtures
# =============================================================================


@pytest.fixture
def temp_output_dir(tmp_path: Path) -> Path:
    """Create a temporary output directory structure."""
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    screened_dir = tmp_path / "screened_yellow"
    screened_dir.mkdir()
    return tmp_path


@pytest.fixture
def sample_jsonl_file(tmp_path: Path) -> Path:
    """Create a sample JSONL file with test data."""
    jsonl_file = tmp_path / "sample.jsonl"
    records = [
        {"id": "1", "text": "First record", "score": 0.9},
        {"id": "2", "text": "Second record", "score": 0.8},
        {"id": "3", "text": "Third record", "score": 0.7},
    ]
    jsonl_file.write_text("\n".join(json.dumps(r) for r in records))
    return jsonl_file


# =============================================================================
# Rate limiter fixtures
# =============================================================================


@pytest.fixture(autouse=True)
def reset_rate_limiters_fixture() -> Generator[None, None, None]:
    """Reset shared rate limiters before and after each test."""
    from collector_core.rate_limit import reset_rate_limiters

    reset_rate_limiters()
    yield
    reset_rate_limiters()


@pytest.fixture
def deterministic_clock() -> Any:
    """A clock that advances only when explicitly told to."""

    class DeterministicClock:
        def __init__(self, start: float = 0.0) -> None:
            self.time = start
            self.sleep_calls: list[float] = []

        def __call__(self) -> float:
            return self.time

        def advance(self, seconds: float) -> None:
            self.time += seconds

        def sleep(self, seconds: float) -> None:
            self.sleep_calls.append(seconds)
            self.advance(seconds)

    return DeterministicClock()


# =============================================================================
# Pipeline context fixtures
# =============================================================================


@pytest.fixture
def mock_pipeline_spec() -> dict[str, Any]:
    """A mock pipeline spec for testing."""
    return {
        "domain": "test",
        "prefix": "test",
        "targets_yaml": "targets_test.yaml",
        "routing_keys": ["data_type"],
        "yellow_screen_module": None,
        "custom_workers": {},
    }


# =============================================================================
# Hypothesis strategies for property-based testing
# =============================================================================

try:
    from hypothesis import strategies as st

    # Strategy for valid target IDs (non-empty strings, alphanumeric with dashes/underscores)
    target_id_strategy = st.text(
        alphabet=st.sampled_from("abcdefghijklmnopqrstuvwxyz0123456789-_"),
        min_size=1,
        max_size=64,
    ).filter(lambda s: s and s[0].isalnum())

    # Strategy for URLs
    url_strategy = st.from_regex(
        r"https?://[a-z0-9-]+(\.[a-z0-9-]+)+(/[a-z0-9-_.]*)*", fullmatch=True
    )

    # Strategy for download strategies
    download_strategy_strategy = st.sampled_from(
        [
            "none",
            "http",
            "ftp",
            "git",
            "zenodo",
            "dataverse",
            "figshare",
            "github_release",
            "huggingface_datasets",
            "s3_public",
            "s3_sync",
            "api",
        ]
    )

except ImportError:
    # hypothesis not installed
    pass
