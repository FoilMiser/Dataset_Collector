from __future__ import annotations
from pathlib import Path
import yaml

from tools import preflight
from tools.preflight import run_preflight


def _write_yaml(path: Path, payload: dict) -> None:
    path.write_text(yaml.safe_dump(payload), encoding="utf-8")


def _write_pipeline(repo_root: Path, name: str, targets: list[dict]) -> None:
    pipeline_dir = repo_root / name
    pipeline_dir.mkdir(parents=True)
    core_dir = repo_root / "src" / "collector_core"
    core_dir.mkdir(parents=True, exist_ok=True)
    acquire_strategies_path = core_dir / "acquire_strategies.py"
    if not acquire_strategies_path.exists():
        acquire_strategies_path.write_text(
            "DEFAULT_STRATEGY_HANDLERS = {'http': None}\n",
            encoding="utf-8",
        )
    (pipeline_dir / "acquire_worker.py").write_text(
        "STRATEGY_HANDLERS = {'http': None, 'custom': None}\n",
        encoding="utf-8",
    )
    _write_yaml(
        pipeline_dir / "targets.yaml",
        {"schema_version": "0.9", "targets": targets},
    )


def _write_pipeline_map(path: Path, pipelines: dict[str, dict[str, str]]) -> None:
    _write_yaml(path, {"pipelines": pipelines})


def test_preflight_pipeline_filter_skips_unselected(tmp_path, capsys) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    _write_pipeline(repo_root, "good_pipeline", [{"id": "ok", "download": {"strategy": "http"}}])
    pipeline_map_path = repo_root / "pipeline_map.sample.yaml"
    _write_pipeline_map(
        pipeline_map_path,
        {
            "good_pipeline": {"targets_yaml": "targets.yaml"},
            "bad_pipeline": {"targets_yaml": "targets.yaml"},
        },
    )

    result = run_preflight(
        repo_root=repo_root,
        pipeline_map_path=pipeline_map_path,
        pipelines=["good_pipeline"],
    )

    output = capsys.readouterr().out
    assert result == 0
    assert "bad_pipeline" not in output


def test_preflight_reports_missing_pipeline_entries(tmp_path, capsys) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    _write_pipeline(repo_root, "good_pipeline", [{"id": "ok", "download": {"strategy": "http"}}])
    pipeline_map_path = repo_root / "pipeline_map.sample.yaml"
    _write_pipeline_map(
        pipeline_map_path,
        {"good_pipeline": {"targets_yaml": "targets.yaml"}},
    )

    result = run_preflight(
        repo_root=repo_root,
        pipeline_map_path=pipeline_map_path,
        pipelines=["good_pipeline", "missing_pipeline"],
    )

    output = capsys.readouterr().out
    assert result == 1
    assert "Pipeline map missing entries for: missing_pipeline" in output


def test_preflight_warn_disabled_emits_disabled_warnings(tmp_path, capsys) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    _write_pipeline(repo_root, "quiet_pipeline", [{"id": "off", "enabled": False}])
    pipeline_map_path = repo_root / "pipeline_map.sample.yaml"
    _write_pipeline_map(
        pipeline_map_path,
        {"quiet_pipeline": {"targets_yaml": "targets.yaml"}},
    )

    result = run_preflight(repo_root=repo_root, pipeline_map_path=pipeline_map_path)

    output = capsys.readouterr().out
    assert result == 0
    assert "Preflight warnings" not in output
    assert "disabled with missing/none download.strategy" not in output

    result = run_preflight(
        repo_root=repo_root, pipeline_map_path=pipeline_map_path, warn_disabled=True
    )

    output = capsys.readouterr().out
    assert result == 0
    assert "Preflight warnings" in output
    assert "disabled with missing/none download.strategy" in output


def test_preflight_main_defaults_to_sample_pipeline_map(monkeypatch, tmp_path) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()

    captured: dict[str, Path] = {}

    def fake_run_preflight(*, repo_root: Path, pipeline_map_path: Path, **_kwargs) -> int:
        captured["pipeline_map_path"] = pipeline_map_path
        return 0

    monkeypatch.setattr(preflight, "run_preflight", fake_run_preflight)

    result = preflight.main(["--repo-root", str(repo_root)])

    assert result == 0
    assert (
        captured["pipeline_map_path"]
        == (repo_root / "src" / "tools" / "pipeline_map.sample.yaml").resolve()
    )
