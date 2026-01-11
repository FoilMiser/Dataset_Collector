from __future__ import annotations

from collector_core.pipeline_discovery import available_pipelines, pick_default_targets


def test_available_pipelines_lists_sorted(tmp_path):
    (tmp_path / "beta_pipeline_v2").mkdir()
    (tmp_path / "alpha_pipeline_v2").mkdir()
    (tmp_path / "not_a_pipeline").mkdir()

    assert available_pipelines(tmp_path) == ["alpha_pipeline_v2", "beta_pipeline_v2"]


def test_pick_default_targets_resolves_yaml(tmp_path):
    targets_root = tmp_path / "pipelines" / "targets"
    targets_root.mkdir(parents=True)
    targets_path = targets_root / "targets_demo.yaml"
    targets_path.write_text("schema_version: '0.9'\ntargets: []\n", encoding="utf-8")

    assert pick_default_targets(tmp_path, "demo") == targets_path
