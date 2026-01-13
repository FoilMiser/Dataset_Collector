# Dataset Collector repo — A‑Grade Patch Checklist (Concrete file‑by‑file diffs)

This is a **concrete, implementable** checklist (rename/move/delete exact files; update exact config blocks; edit specific functions) to make the repo **more elegant, more robust, and "A‑grade"** while keeping the current architecture intact.

> Scope note: Items are prioritized (P0→P3). P0+P1 eliminate the biggest footguns and drift risks. P2 improves developer UX. P3 items are larger polish refactors.

---

## ✅ Completed Items

### P0 — Stop path/packaging footguns ✅ DONE
- [x] **P0.1**: Removed duplicate `tools/` package at repo root, moved shell script to `scripts/`
- [x] **P0.2**: Added 14 console scripts to `pyproject.toml`
- [x] **P0.3**: Updated CI to install package and use console scripts, removed PYTHONPATH injection
- [x] **P0.4**: Deleted root-level `schemas/`, updated fallback path, cleaned up test conftest.py

### P1.2-P1.4 — Correctness fixes ✅ DONE
- [x] **P1.2**: Fixed missing `re` import in `pmc_worker.py`
- [x] **P1.3**: Fixed `_get_default_handlers()` bug in `acquire_strategies.py`
- [x] **P1.4**: Added return type annotations to `pmc_worker.py`, `acquire_strategies.py`, `near_duplicate.py`

### P2 — Developer UX ✅ DONE
- [x] **P2.1**: Updated `README.md` and `docs/quickstart.md` to use new CLI commands
- [x] **P2.2**: Updated `.pre-commit-config.yaml` to self-install the package

### P1.1 — Yellow screen entrypoint duplication → one dispatcher path ✅ DONE
- [x] **P1.1A**: Updated `yellow_screen_dispatch.py` dispatcher signature to accept `yellow_screen` override
- [x] **P1.1B**: Updated `dc_cli.py` to use new dispatcher with config-driven yellow_screen selection
- [x] **P1.1C**: Removed `yellow_screen_module` arguments from all registered specs in `pipeline_specs_registry.py`
- [x] **P1.1D**: Deleted duplicated wrapper modules:
  - `src/collector_core/yellow_screen_chem.py`
  - `src/collector_core/yellow_screen_econ.py`
  - `src/collector_core/yellow_screen_kg_nav.py`
  - `src/collector_core/yellow_screen_nlp.py`
  - `src/collector_core/yellow_screen_safety.py`
- [x] **P1.1E**: Removed `knobs.yellow_screen_module` lines from `configs/pipelines.yaml` (kept `yellow_screen:` as source of truth)
- [x] **P1.1F**: Updated tests in `test_yellow_screen_dispatch.py` to use behavior tests with monkeypatching

### P0.2B — Optional: Remove run_all.py ✅ DONE
- [x] Removed `sys.path` hack from `run_all.py`, now requires package to be installed

### P3.2 — Make artifact schemas + runtime contracts enforced everywhere ✅ DONE
- [x] Added `main()` to `src/tools/output_contract.py`
- [x] Added console script `dc-validate-output-contract = "tools.output_contract:main"` to `pyproject.toml`
- [x] Added CI step to validate output contract in `.github/workflows/ci.yml`
- [x] Added test `tests/test_output_contract_cli.py`

### P3.3 — Raise mypy bar: type-check `src/tools` too ✅ DONE
- [x] Updated CI mypy step to include `src/tools`
- [x] Added `[[tool.mypy.overrides]]` for `tools.*` in `pyproject.toml`

### P3.4 — Test coverage: fill the highest-impact gaps first ✅ DONE
- [x] Added `tests/test_dc_cli_smoke.py` — smoke tests for CLI help and basic functionality
- [x] Added `tests/test_checkpoint_roundtrip.py` — checkpoint save/load/resume tests
- [x] Added `tests/test_catalog_builder_contract.py` — catalog builder contract validation tests

### P3.1 — Unify pipeline sources of truth ✅ DONE
- [x] **P3.1A**: Added `src/collector_core/pipeline_specs_loader.py` that reads `configs/pipelines.yaml` and returns `PipelineSpec` objects
- [x] **P3.1B**: Updated `src/collector_core/pipeline_specs_registry.py` to be a thin wrapper that imports the loader
- [x] **P3.1C**: Added `src/tools/validate_pipeline_specs.py` to validate YAML configuration and loader consistency
- [x] **P3.1D**: Added console script `dc-validate-pipeline-specs = "tools.validate_pipeline_specs:main"` to `pyproject.toml`
- [x] **P3.1E**: Added CI step to validate pipeline specs in `.github/workflows/ci.yml`
- [x] **P3.1F**: Added `tests/test_pipeline_specs_loader.py` with comprehensive tests

---

## 🔲 Pending Items

**None — All checklist items have been completed!**

---

## "Done when" checklist (definition of A‑grade)

- [x] No duplicate module namespaces that depend on `PYTHONPATH` ordering (`tools`, `schemas` resolved)
- [x] CI installs and tests the package (`pip install -e .`) and does not inject repo root into `PYTHONPATH`
- [x] Yellow screen selection is config-driven and does not require duplicated wrapper modules *(P1.1)*
- [x] Schema loading is via `importlib.resources` and works both installed and editable without symlinks
- [x] Pre-commit hooks self-install and run in a controlled environment
- [x] All P1 correctness issues are fixed (`re` import, `_get_default_handlers` bug, typing)
- [x] Docs match reality (copy/paste commands work in a fresh clone)
- [x] Pipeline config is unified in `configs/pipelines.yaml` *(P3.1)*
- [x] Output contracts are enforced in CI *(P3.2)*
- [x] `src/tools` is type-checked by mypy *(P3.3)*
- [x] High-risk modules have test coverage *(P3.4)*

---

## Priority Recommendations

| Priority | Item | Effort | Impact | Status |
|----------|------|--------|--------|--------|
| **High** | P1.1 Yellow screen refactoring | Medium | Eliminates code duplication, single source of truth | ✅ DONE |
| Medium | P3.1 Pipeline unification | Medium | Reduces config drift risk | ✅ DONE |
| Medium | P3.2 Output contracts | Low | Catches contract violations in CI | ✅ DONE |
| Low | P3.3 Mypy expansion | Low | Better type safety for tools | ✅ DONE |
| Low | P3.4 Test coverage | Medium | Reduces regression risk | ✅ DONE |
| Optional | P0.2B Remove run_all.py | Low | Cleaner repo | ✅ DONE |

---

## Summary of Changes Made

### Files Added
- `src/collector_core/pipeline_specs_loader.py` - Loads PipelineSpec objects from YAML
- `src/tools/validate_pipeline_specs.py` - Validates pipeline specs configuration
- `tests/test_output_contract_cli.py` - Tests for output contract CLI
- `tests/test_dc_cli_smoke.py` - Smoke tests for dc CLI
- `tests/test_checkpoint_roundtrip.py` - Checkpoint roundtrip tests
- `tests/test_catalog_builder_contract.py` - Catalog builder contract tests
- `tests/test_pipeline_specs_loader.py` - Tests for pipeline specs loader

### Files Deleted
- `src/collector_core/yellow_screen_chem.py`
- `src/collector_core/yellow_screen_econ.py`
- `src/collector_core/yellow_screen_kg_nav.py`
- `src/collector_core/yellow_screen_nlp.py`
- `src/collector_core/yellow_screen_safety.py`

### Files Modified
- `src/collector_core/yellow_screen_dispatch.py` - New config-driven dispatcher
- `src/collector_core/dc_cli.py` - Simplified yellow screen stage handling
- `src/collector_core/pipeline_specs_registry.py` - Now a thin wrapper using the loader
- `configs/pipelines.yaml` - Removed redundant knobs.yellow_screen_module
- `tests/test_yellow_screen_dispatch.py` - Updated to behavior tests
- `run_all.py` - Removed sys.path hack
- `src/tools/output_contract.py` - Added main() CLI function
- `pyproject.toml` - Added console scripts and mypy overrides
- `.github/workflows/ci.yml` - Added validation steps for output contract and pipeline specs

---

## Architecture Summary

The pipeline configuration now follows a clean, unified architecture:

1. **`configs/pipelines.yaml`** — The authoritative source of truth for all pipeline configuration
2. **`src/collector_core/pipeline_specs_loader.py`** — Loads YAML and converts to `PipelineSpec` objects
3. **`src/collector_core/pipeline_specs_registry.py`** — Thin wrapper that auto-registers specs on import
4. **`src/tools/validate_pipeline_specs.py`** — CI tool to validate configuration consistency

This architecture ensures:
- Single source of truth (YAML file)
- Backward compatibility (registry still works the same way)
- Early error detection (CI validates configuration)
- Clean separation of concerns (loader vs registry vs validation)
