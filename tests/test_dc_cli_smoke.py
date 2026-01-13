"""Smoke tests for dc_cli module.

These tests verify basic CLI functionality works without errors.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

# Import registry to ensure specs are registered
import collector_core.pipeline_specs_registry  # noqa: F401


class TestDcCliSmoke:
    """Smoke tests for dc CLI command."""

    def test_dc_help_exits_cleanly(self) -> None:
        """dc --help should exit with code 0."""
        result = subprocess.run(
            [sys.executable, "-m", "collector_core.dc_cli", "--help"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert "Dataset Collector CLI" in result.stdout

    def test_dc_list_pipelines_shows_registered_pipelines(self) -> None:
        """dc --list-pipelines should show registered pipelines."""
        result = subprocess.run(
            [sys.executable, "-m", "collector_core.dc_cli", "--list-pipelines"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert "Available pipelines:" in result.stdout
        assert "chem" in result.stdout
        assert "physics" in result.stdout

    def test_dc_no_command_shows_usage_message(self) -> None:
        """dc with no command should show usage message."""
        result = subprocess.run(
            [sys.executable, "-m", "collector_core.dc_cli"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 1
        assert "No command specified" in result.stdout

    def test_dc_run_help_exits_cleanly(self) -> None:
        """dc run --help should exit with code 0."""
        result = subprocess.run(
            [sys.executable, "-m", "collector_core.dc_cli", "run", "--help"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert "--stage" in result.stdout
        assert "--pipeline" in result.stdout


class TestDcCliMain:
    """Tests for dc_cli.main function."""

    def test_main_import_succeeds(self) -> None:
        """Main function should be importable."""
        from collector_core.dc_cli import main

        assert callable(main)

    def test_main_parse_args_import_succeeds(self) -> None:
        """Parse args function should be importable."""
        from collector_core.dc_cli import _parse_args

        assert callable(_parse_args)

    def test_yellow_screen_dispatch_import_succeeds(self) -> None:
        """Yellow screen dispatch should be importable."""
        from collector_core.yellow_screen_dispatch import get_yellow_screen_main

        assert callable(get_yellow_screen_main)


class TestDcCliPipelineResolution:
    """Tests for pipeline resolution in dc_cli."""

    def test_resolves_chem_pipeline(self) -> None:
        """Should resolve chem pipeline correctly."""
        from collector_core.pipeline_spec import get_pipeline_spec

        spec = get_pipeline_spec("chem")
        assert spec is not None
        assert spec.domain == "chem"
        assert spec.name == "Chemistry Pipeline"

    def test_resolves_physics_pipeline(self) -> None:
        """Should resolve physics pipeline correctly."""
        from collector_core.pipeline_spec import get_pipeline_spec

        spec = get_pipeline_spec("physics")
        assert spec is not None
        assert spec.domain == "physics"
        assert spec.name == "Physics Pipeline"

    def test_returns_none_for_unknown_pipeline(self) -> None:
        """Should return None for unknown pipeline."""
        from collector_core.pipeline_spec import get_pipeline_spec

        spec = get_pipeline_spec("unknown_domain_xyz")
        assert spec is None


class TestDcCliStageDispatch:
    """Tests for stage dispatch logic."""

    def test_run_with_args_preserves_sys_argv(self) -> None:
        """_run_with_args should restore sys.argv after execution."""
        from collector_core.dc_cli import _run_with_args

        original_argv = sys.argv.copy()

        def dummy_func() -> None:
            pass

        _run_with_args(dummy_func, ["--dummy", "arg"])
        assert sys.argv == original_argv

    def test_has_arg_detects_exact_match(self) -> None:
        """_has_arg should detect exact argument matches."""
        from collector_core.dc_cli import _has_arg

        assert _has_arg(["--foo", "--bar"], "--foo")
        assert _has_arg(["--foo", "--bar"], "--bar")
        assert not _has_arg(["--foo", "--bar"], "--baz")

    def test_has_arg_detects_prefix_match(self) -> None:
        """_has_arg should detect argument prefix matches."""
        from collector_core.dc_cli import _has_arg

        assert _has_arg(["--foo=value", "--bar"], "--foo")
        assert _has_arg(["--bar=123"], "--bar")
        assert not _has_arg(["--foobar=value"], "--foo")
