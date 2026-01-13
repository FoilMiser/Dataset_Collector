"""Tests for collector_core.yellow_screen_dispatch module."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

# Import registry to ensure specs are registered
import collector_core.pipeline_specs_registry  # noqa: F401
from collector_core.yellow_screen_dispatch import get_yellow_screen_main

# Check if datasets module is available for tests that need it
try:
    import datasets  # noqa: F401

    HAS_DATASETS = True
except ImportError:
    HAS_DATASETS = False


class TestGetYellowScreenMain:
    """Tests for get_yellow_screen_main function."""

    def test_raises_value_error_for_unknown_domain(self) -> None:
        """Unknown domain should raise ValueError."""
        with pytest.raises(ValueError, match="Unknown domain"):
            get_yellow_screen_main("nonexistent_domain")

    def test_raises_value_error_for_unknown_yellow_screen_module(self) -> None:
        """Unknown yellow_screen module should raise ValueError with available options."""
        with pytest.raises(ValueError, match="not found in collector_core.yellow.domains"):
            get_yellow_screen_main("physics", yellow_screen="nonexistent_module")

    def test_returns_callable_for_physics_domain(self) -> None:
        """physics domain (no yellow_screen override) should return callable."""
        main_fn = get_yellow_screen_main("physics")
        assert callable(main_fn)

    def test_returns_callable_for_biology_domain(self) -> None:
        """biology domain (no yellow_screen override) should return callable."""
        main_fn = get_yellow_screen_main("biology")
        assert callable(main_fn)

    def test_returns_callable_with_yellow_screen_override(self) -> None:
        """Specifying yellow_screen should return callable for that domain."""
        main_fn = get_yellow_screen_main("physics", yellow_screen="chem")
        assert callable(main_fn)

    def test_returns_standard_for_explicit_standard_override(self) -> None:
        """Explicit 'standard' yellow_screen should return standard wrapper."""
        main_fn = get_yellow_screen_main("physics", yellow_screen="standard")
        assert callable(main_fn)
        # The function should use standard domain
        assert main_fn.__name__ == "_standard_main"

    def test_returns_domain_main_for_yellow_screen_override(self) -> None:
        """Non-standard yellow_screen should return domain main."""
        main_fn = get_yellow_screen_main("physics", yellow_screen="chem")
        assert callable(main_fn)
        assert main_fn.__name__ == "_domain_main"

    @pytest.mark.skipif(not HAS_DATASETS, reason="datasets module not installed")
    def test_chem_domain_uses_chem_yellow_module(self) -> None:
        """chem yellow_screen should call run_yellow_screen with chem domain."""
        mock_run = MagicMock()
        with patch("collector_core.yellow.base.run_yellow_screen", mock_run):
            main_fn = get_yellow_screen_main("chem", yellow_screen="chem")
            main_fn()
            mock_run.assert_called_once()
            call_kwargs = mock_run.call_args[1]
            assert "defaults" in call_kwargs
            assert "domain" in call_kwargs
            # Verify the domain module is the chem module
            from collector_core.yellow.domains import chem

            assert call_kwargs["domain"] is chem

    @pytest.mark.skipif(not HAS_DATASETS, reason="datasets module not installed")
    def test_nlp_domain_uses_nlp_yellow_module(self) -> None:
        """nlp yellow_screen should call run_yellow_screen with nlp domain."""
        mock_run = MagicMock()
        with patch("collector_core.yellow.base.run_yellow_screen", mock_run):
            main_fn = get_yellow_screen_main("nlp", yellow_screen="nlp")
            main_fn()
            mock_run.assert_called_once()
            call_kwargs = mock_run.call_args[1]
            from collector_core.yellow.domains import nlp

            assert call_kwargs["domain"] is nlp

    @pytest.mark.skipif(not HAS_DATASETS, reason="datasets module not installed")
    def test_kg_nav_domain_uses_kg_nav_yellow_module(self) -> None:
        """kg_nav yellow_screen should call run_yellow_screen with kg_nav domain."""
        mock_run = MagicMock()
        with patch("collector_core.yellow.base.run_yellow_screen", mock_run):
            main_fn = get_yellow_screen_main("kg_nav", yellow_screen="kg_nav")
            main_fn()
            mock_run.assert_called_once()
            call_kwargs = mock_run.call_args[1]
            from collector_core.yellow.domains import kg_nav

            assert call_kwargs["domain"] is kg_nav

    @pytest.mark.skipif(not HAS_DATASETS, reason="datasets module not installed")
    def test_econ_domain_uses_econ_yellow_module(self) -> None:
        """econ yellow_screen should call run_yellow_screen with econ domain."""
        mock_run = MagicMock()
        with patch("collector_core.yellow.base.run_yellow_screen", mock_run):
            main_fn = get_yellow_screen_main("econ_stats_decision_adaptation", yellow_screen="econ")
            main_fn()
            mock_run.assert_called_once()
            call_kwargs = mock_run.call_args[1]
            from collector_core.yellow.domains import econ

            assert call_kwargs["domain"] is econ

    @pytest.mark.skipif(not HAS_DATASETS, reason="datasets module not installed")
    def test_safety_domain_uses_safety_yellow_module(self) -> None:
        """safety yellow_screen should call run_yellow_screen with safety domain."""
        mock_run = MagicMock()
        with patch("collector_core.yellow.base.run_yellow_screen", mock_run):
            main_fn = get_yellow_screen_main("safety_incident", yellow_screen="safety")
            main_fn()
            mock_run.assert_called_once()
            call_kwargs = mock_run.call_args[1]
            from collector_core.yellow.domains import safety

            assert call_kwargs["domain"] is safety

    @pytest.mark.skipif(not HAS_DATASETS, reason="datasets module not installed")
    def test_standard_domain_uses_standard_yellow_module(self) -> None:
        """No yellow_screen override should use standard domain."""
        mock_run = MagicMock()
        with patch("collector_core.yellow.base.run_yellow_screen", mock_run):
            main_fn = get_yellow_screen_main("physics")
            main_fn()
            mock_run.assert_called_once()
            call_kwargs = mock_run.call_args[1]
            from collector_core.yellow.domains import standard

            assert call_kwargs["domain"] is standard

    def test_dispatch_uses_correct_defaults_for_domain(self) -> None:
        """
        Verifies dispatch passes correct defaults based on pipeline spec prefix.
        """
        from collector_core.pipeline_spec import get_pipeline_spec

        # chem should have prefix "chem"
        chem_spec = get_pipeline_spec("chem")
        assert chem_spec is not None
        assert chem_spec.prefix == "chem"

        # physics should have prefix "physics"
        physics_spec = get_pipeline_spec("physics")
        assert physics_spec is not None
        assert physics_spec.prefix == "physics"

    def test_unconfigured_pipeline_falls_back_to_standard(self) -> None:
        """
        Verifies unconfigured pipeline falls back to standard yellow screen.

        This is the acceptance criterion from the checklist:
        "An unconfigured pipeline falls back to yellow_screen_standard."
        """
        # physics should NOT have a yellow_screen configured in YAML
        # so get_yellow_screen_main without override should return standard
        main_fn = get_yellow_screen_main("physics")
        assert callable(main_fn)
        # The function name should indicate it's a standard wrapper
        assert main_fn.__name__ == "_standard_main"
