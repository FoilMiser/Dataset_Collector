from __future__ import annotations

from types import SimpleNamespace

import pytest

from collector_core import merge, yellow_screen_common
from collector_core.acquire_strategies import RootsDefaults, load_roots


def test_dataset_root_applies_across_stages(tmp_path, monkeypatch) -> None:
    dataset_root = tmp_path / "dataset_root"
    monkeypatch.setenv("DATASET_ROOT", str(dataset_root))

    acquire_defaults = RootsDefaults(
        raw_root="/data/example/raw",
        manifests_root="/data/example/_manifests",
        ledger_root="/data/example/_ledger",
        logs_root="/data/example/_logs",
    )
    overrides = SimpleNamespace(
        dataset_root=None,
        raw_root=None,
        manifests_root=None,
        ledger_root=None,
        logs_root=None,
        allow_data_root=False,
    )
    acquire_roots = load_roots({}, overrides, acquire_defaults)

    merge_defaults = merge.default_merge_roots("example")
    merge_roots = merge.resolve_roots({}, merge_defaults)

    yellow_defaults = yellow_screen_common.default_yellow_roots("example")
    yellow_roots = yellow_screen_common.resolve_roots({}, yellow_defaults)

    expected_raw = dataset_root / "raw"
    expected_manifests = dataset_root / "_manifests"
    expected_ledger = dataset_root / "_ledger"

    assert acquire_roots.raw_root == expected_raw
    assert acquire_roots.manifests_root == expected_manifests
    assert acquire_roots.ledger_root == expected_ledger
    assert acquire_roots.logs_root == dataset_root / "_logs"

    assert merge_roots.raw_root == expected_raw
    assert merge_roots.screened_root == dataset_root / "screened_yellow"
    assert merge_roots.combined_root == dataset_root / "combined"
    assert merge_roots.ledger_root == expected_ledger

    assert yellow_roots.raw_root == expected_raw
    assert yellow_roots.screened_root == dataset_root / "screened_yellow"
    assert yellow_roots.manifests_root == expected_manifests
    assert yellow_roots.ledger_root == expected_ledger
    assert yellow_roots.pitches_root == dataset_root / "_pitches"


def test_defaults_require_allow_data_root(monkeypatch) -> None:
    monkeypatch.delenv("DATASET_ROOT", raising=False)
    monkeypatch.delenv("DATASET_COLLECTOR_ROOT", raising=False)

    defaults = RootsDefaults(
        raw_root="/data/example/raw",
        manifests_root="/data/example/_manifests",
        ledger_root="/data/example/_ledger",
        logs_root="/data/example/_logs",
    )
    overrides = SimpleNamespace(
        dataset_root=None,
        raw_root=None,
        manifests_root=None,
        ledger_root=None,
        logs_root=None,
        allow_data_root=False,
    )

    with pytest.raises(ValueError, match="allow-data-root"):
        load_roots({}, overrides, defaults)

    with pytest.raises(ValueError, match="allow-data-root"):
        merge.resolve_roots({}, merge.default_merge_roots("example"))

    with pytest.raises(ValueError, match="allow-data-root"):
        yellow_screen_common.resolve_roots({}, yellow_screen_common.default_yellow_roots("example"))

    overrides.allow_data_root = True
    roots = load_roots({}, overrides, defaults)
    assert roots.raw_root.as_posix().startswith("/data/")
