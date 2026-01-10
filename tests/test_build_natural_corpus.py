from __future__ import annotations

from pathlib import Path

import yaml

from tools import build_natural_corpus


def test_build_natural_corpus_defaults_to_sample_pipeline_map(monkeypatch, tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    tools_dir = repo_root / "tools"
    tools_dir.mkdir()

    pipeline_name = "sample_pipeline"
    pipeline_dir = repo_root / pipeline_name
    pipeline_dir.mkdir()
    targets_path = pipeline_dir / "targets.yaml"
    targets_path.write_text("schema_version: 1.0\ntargets: []\n", encoding="utf-8")

    pipeline_map_path = tools_dir / "pipeline_map.sample.yaml"
    pipeline_map_path.write_text(
        yaml.safe_dump(
            {
                "destination_root": str(tmp_path / "dest"),
                "pipelines": {
                    pipeline_name: {
                        "dest_folder": "sample",
                        "targets_yaml": targets_path.name,
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    captured: dict[str, Path] = {}

    def fake_run_preflight(*, repo_root: Path, pipeline_map_path: Path, **_kwargs) -> int:
        captured["pipeline_map_path"] = pipeline_map_path
        return 0

    def fake_patch_targets_yaml(_src: Path, _dest_root: Path, dest: Path) -> None:
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text("schema_version: 1.0\ntargets: []\n", encoding="utf-8")

    monkeypatch.setattr(build_natural_corpus, "run_preflight", fake_run_preflight)
    monkeypatch.setattr(build_natural_corpus, "patch_targets_yaml", fake_patch_targets_yaml)
    monkeypatch.setattr(build_natural_corpus, "_run_stage", lambda **_kwargs: None)

    result = build_natural_corpus.main(
        [
            "--repo-root",
            str(repo_root),
            "--pipelines",
            pipeline_name,
            "--stages",
            "classify",
        ]
    )

    assert result == 0
    assert captured["pipeline_map_path"] == pipeline_map_path.resolve()
