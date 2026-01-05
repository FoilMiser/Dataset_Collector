from __future__ import annotations

from collector_core import merge


def test_build_target_meta_and_canon() -> None:
    cfg = {
        "globals": {"canonicalize": {"text_field_candidates": ["title"]}},
        "targets": [
            {"id": "a", "download": {"dataset_id": "ds1", "config": "cfg1"}},
            {"id": "b", "dataset_id": "ds2"},
        ],
    }
    meta = merge.build_target_meta(cfg)
    assert meta["a"]["dataset_id"] == "ds1"
    assert meta["a"]["config"] == "cfg1"
    assert meta["b"]["dataset_id"] == "ds2"

    target_canon, default_canon = merge.build_target_canon(cfg)
    assert target_canon["a"][0] == ["title"]
    assert default_canon[0] == ["title"]
