from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
import sys

from collector_core import dc_cli, pipeline_cli


def test_dc_cli_lists_pipelines(monkeypatch, capsys) -> None:
    monkeypatch.setattr(dc_cli, "list_pipelines", lambda: ["chem", "physics"])
    monkeypatch.setattr(sys, "argv", ["dc", "--list-pipelines"])

    assert dc_cli.main() == 0

    captured = capsys.readouterr()
    assert "Available pipelines:" in captured.out
    assert "chem" in captured.out
    assert "physics" in captured.out


def test_dc_cli_screen_yellow_alias_warns(monkeypatch, capsys) -> None:
    calls: dict[str, object] = {}

    def fake_run_yellow(slug, targets_path, args, ctx) -> int:
        calls["slug"] = slug
        calls["targets_path"] = targets_path
        calls["args"] = list(args)
        calls["ctx"] = ctx
        return 0

    ctx = SimpleNamespace(
        slug="chem",
        pipeline_id="chem_pipeline_v2",
        targets_path=None,
        overrides={},
    )
    monkeypatch.setattr(dc_cli, "resolve_pipeline_context", lambda **_: ctx)
    monkeypatch.setattr(dc_cli, "_run_yellow_screen", fake_run_yellow)
    monkeypatch.setattr(
        sys,
        "argv",
        ["dc", "run", "--pipeline", "chem", "--stage", "screen_yellow"],
    )

    assert dc_cli.main() == 0

    captured = capsys.readouterr()
    assert "deprecated" in captured.err.lower()
    assert calls["slug"] == "chem"


def test_dc_cli_injects_dataset_root_and_allow_data_root(monkeypatch) -> None:
    calls: dict[str, object] = {}

    def fake_run_merge(pipeline_id, slug, targets_path, args) -> int:
        calls["pipeline_id"] = pipeline_id
        calls["slug"] = slug
        calls["targets_path"] = targets_path
        calls["args"] = list(args)
        return 0

    ctx = SimpleNamespace(
        slug="chem",
        pipeline_id="chem_pipeline_v2",
        targets_path=None,
        overrides={},
    )
    monkeypatch.setattr(dc_cli, "resolve_pipeline_context", lambda **_: ctx)
    monkeypatch.setattr(dc_cli, "_run_merge", fake_run_merge)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "dc",
            "run",
            "--pipeline",
            "chem",
            "--stage",
            "merge",
            "--dataset-root",
            "/tmp/datasets",
            "--allow-data-root",
        ],
    )

    assert dc_cli.main() == 0
    assert calls["args"] == ["--dataset-root", "/tmp/datasets", "--allow-data-root"]


def test_pipeline_cli_catalog_builder_adds_default_targets(monkeypatch) -> None:
    calls: dict[str, object] = {}

    def fake_run_catalog_builder(argv: list[str], pipeline_id: str | None) -> int:
        calls["argv"] = list(argv)
        calls["pipeline_id"] = pipeline_id
        return 0

    repo_root = Path("/repo")
    monkeypatch.setattr(pipeline_cli, "resolve_repo_root", lambda _: repo_root)
    monkeypatch.setattr(
        pipeline_cli,
        "_resolve_pipeline_context",
        lambda **_: ("chem_pipeline_v2", repo_root / "chem_pipeline_v2"),
    )
    monkeypatch.setattr(pipeline_cli, "pipeline_slug", lambda *_: "chem")
    monkeypatch.setattr(
        pipeline_cli,
        "pick_default_targets",
        lambda *_: repo_root / "chem_pipeline_v2" / "targets.yaml",
    )
    monkeypatch.setattr(pipeline_cli, "_run_catalog_builder", fake_run_catalog_builder)
    monkeypatch.setattr(
        sys,
        "argv",
        ["pipeline", "--repo-root", "/repo", "catalog-builder"],
    )

    assert pipeline_cli.main() == 0
    assert calls["pipeline_id"] == "chem_pipeline_v2"
    assert calls["argv"][0] == "--targets"
    assert calls["argv"][1].endswith("targets.yaml")
