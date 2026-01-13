from __future__ import annotations

from pathlib import Path

import pytest

from collector_core.yellow.base import DomainContext
from collector_core.yellow_screen_common import PitchConfig, Roots, ScreeningConfig


@pytest.fixture
def mock_roots(tmp_path: Path) -> Roots:
    return Roots(
        raw_root=tmp_path / "raw",
        screened_root=tmp_path / "screened",
        ledger_root=tmp_path / "ledger",
        pitches_root=tmp_path / "pitches",
        manifests_root=tmp_path / "manifests",
    )


@pytest.fixture
def pitch_cfg() -> PitchConfig:
    return PitchConfig(sample_limit=5, text_limit=500)


@pytest.fixture
def screen_cfg() -> ScreeningConfig:
    return ScreeningConfig(
        min_chars=1,
        max_chars=10_000,
        text_fields=["text", "content"],
        license_fields=["license", "license_spdx"],
        require_record_license=False,
        allow_spdx=None,
        deny_phrases=[],
    )


@pytest.fixture
def domain_ctx(
    mock_roots: Roots,
    pitch_cfg: PitchConfig,
    screen_cfg: ScreeningConfig,
) -> DomainContext:
    return DomainContext(
        cfg={"globals": {}, "targets": []},
        roots=mock_roots,
        pitch_cfg=pitch_cfg,
        screen_cfg=screen_cfg,
        target_id="test_target",
        target_cfg={},
        queue_row={"id": "test_target"},
        pool="permissive",
        execute=False,
    )
