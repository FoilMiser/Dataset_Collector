"""Tests for collector_core.yellow_screen_dispatch module."""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import pytest  # noqa: E402

# Import registry to ensure specs are registered
import collector_core.pipeline_specs_registry  # noqa: F401, E402
from collector_core.yellow_screen_dispatch import get_yellow_screen_main  # noqa: E402

# Check if datasets module is available for tests that need it
try:
    import datasets  # noqa: F401

    HAS_DATASETS = True
except ImportError:
    HAS_DATASETS = False


class TestGetYellowScreenMain:
    """Tests for get_yellow_screen_main function."""

    @pytest.mark.skipif(not HAS_DATASETS, reason="datasets module not installed")
    def test_returns_chem_module_for_chem_domain(self) -> None:
        """chem domain should return yellow_screen_chem.main."""
        main_fn = get_yellow_screen_main("chem")
        assert callable(main_fn)
        # Verify it's the chem module by checking the module name
        from collector_core.yellow_screen_chem import main as chem_main

        assert main_fn is chem_main

    @pytest.mark.skipif(not HAS_DATASETS, reason="datasets module not installed")
    def test_returns_econ_module_for_econ_domain(self) -> None:
        """econ domain should return yellow_screen_econ.main."""
        main_fn = get_yellow_screen_main("econ_stats_decision_adaptation")
        assert callable(main_fn)
        from collector_core.yellow_screen_econ import main as econ_main

        assert main_fn is econ_main

    @pytest.mark.skipif(not HAS_DATASETS, reason="datasets module not installed")
    def test_returns_nlp_module_for_nlp_domain(self) -> None:
        """nlp domain should return yellow_screen_nlp.main."""
        main_fn = get_yellow_screen_main("nlp")
        assert callable(main_fn)
        from collector_core.yellow_screen_nlp import main as nlp_main

        assert main_fn is nlp_main

    @pytest.mark.skipif(not HAS_DATASETS, reason="datasets module not installed")
    def test_returns_kg_nav_module_for_kg_nav_domain(self) -> None:
        """kg_nav domain should return yellow_screen_kg_nav.main."""
        main_fn = get_yellow_screen_main("kg_nav")
        assert callable(main_fn)
        from collector_core.yellow_screen_kg_nav import main as kg_nav_main

        assert main_fn is kg_nav_main

    @pytest.mark.skipif(not HAS_DATASETS, reason="datasets module not installed")
    def test_returns_safety_module_for_safety_domain(self) -> None:
        """safety_incident domain should return yellow_screen_safety.main."""
        main_fn = get_yellow_screen_main("safety_incident")
        assert callable(main_fn)
        from collector_core.yellow_screen_safety import main as safety_main

        assert main_fn is safety_main

    def test_returns_standard_wrapper_for_physics_domain(self) -> None:
        """physics domain (no yellow_screen_module) should return standard wrapper."""
        main_fn = get_yellow_screen_main("physics")
        assert callable(main_fn)
        # physics doesn't have a yellow_screen_module, so it should be a standard wrapper

    def test_returns_standard_wrapper_for_biology_domain(self) -> None:
        """biology domain (no yellow_screen_module) should return standard wrapper."""
        main_fn = get_yellow_screen_main("biology")
        assert callable(main_fn)

    def test_raises_value_error_for_unknown_domain(self) -> None:
        """Unknown domain should raise ValueError."""
        with pytest.raises(ValueError, match="Unknown domain"):
            get_yellow_screen_main("nonexistent_domain")

    @pytest.mark.skipif(not HAS_DATASETS, reason="datasets module not installed")
    def test_dispatch_respects_pipeline_spec_yellow_screen_module(self) -> None:
        """
        Verifies dispatch uses PipelineSpec.yellow_screen_module when set.

        This is the acceptance criterion from the checklist:
        "dc run ... yellow_screen selects the correct module based on
        PipelineSpec.yellow_screen_module."
        """
        from collector_core.pipeline_spec import get_pipeline_spec

        # chem should have yellow_screen_module set
        chem_spec = get_pipeline_spec("chem")
        assert chem_spec is not None
        assert chem_spec.yellow_screen_module == "yellow_screen_chem"

        # The dispatch should return the chem module
        main_fn = get_yellow_screen_main("chem")
        from collector_core.yellow_screen_chem import main as chem_main

        assert main_fn is chem_main

    def test_unconfigured_pipeline_falls_back_to_standard(self) -> None:
        """
        Verifies unconfigured pipeline falls back to yellow_screen_standard.

        This is the acceptance criterion from the checklist:
        "An unconfigured pipeline falls back to yellow_screen_standard."
        """
        from collector_core.pipeline_spec import get_pipeline_spec

        # physics should NOT have yellow_screen_module set
        physics_spec = get_pipeline_spec("physics")
        assert physics_spec is not None
        assert physics_spec.yellow_screen_module is None

        # The dispatch should return a wrapper around standard
        main_fn = get_yellow_screen_main("physics")
        assert callable(main_fn)
        # The function name should indicate it's a standard wrapper
        assert main_fn.__name__ == "_standard_main"
