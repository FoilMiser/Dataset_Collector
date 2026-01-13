# Dataset Collector repo — A‑Grade Patch Checklist (Concrete file‑by‑file diffs)

This is a **concrete, implementable** checklist (rename/move/delete exact files; update exact config blocks; edit specific functions) to make the repo **more elegant, more robust, and "A‑grade"** while keeping the current architecture intact.

> Scope note: Items are prioritized (P0→P2). If you do only P0+P1, you'll eliminate the biggest footguns and drift risks and improve developer UX significantly.

---

## P0 — Stop path/packaging footguns (single source of truth for `tools`, schemas, and CI)

### P0.1 — Remove duplicate top‑level `tools/` package (it shadows `src/tools`) ✅ COMPLETED

Right now you have **two** `tools` packages:
- `tools/` (repo root)
- `src/tools/` (the actual "real" tooling)

This is a **major correctness + CI drift risk**: whichever is first on `sys.path` wins, and CI currently sets `PYTHONPATH: ${{ github.workspace }}`, which preferentially loads the **root** `tools/`.

#### ✅ Concrete changes
**A) Delete the duplicated Python package at repo root**
- [x] **DELETE** `tools/__init__.py`
- [x] **DELETE** `tools/check_constraints.py`
- [x] **DELETE** `tools/preflight.py`
- [x] **DELETE** `tools/validate_repo.py`
- [x] **DELETE** `tools/validate_yaml_schemas.py`

**B) Keep the CI shell script, but move it out of the Python namespace**
- [x] **MOVE** `tools/run_minimal_dry_run.sh` → `scripts/run_minimal_dry_run.sh`
  - `mkdir -p scripts`
  - `git mv tools/run_minimal_dry_run.sh scripts/run_minimal_dry_run.sh`

**C) Update every reference that currently assumes `python -m tools.*` works without installation**
You'll replace those with console scripts (next section) and/or ensure CI/dev installs the package.

---

### P0.2 — Add console scripts for all "repo tools" (no more `PYTHONPATH` rituals) ✅ COMPLETED

Your `src/tools/*.py` modules already define `main(...)` entrypoints (for most tools). Make them **first‑class CLI commands**.

#### ✅ Concrete changes
**A) Edit `pyproject.toml` → add scripts**
In `[project.scripts]`, add:

```toml
# tooling / validation
dc-preflight = "tools.preflight:main"
dc-validate-repo = "tools.validate_repo:main"
dc-validate-yaml-schemas = "tools.validate_yaml_schemas:main"
dc-check-constraints = "tools.check_constraints:main"

# maintenance
dc-sync-wrappers = "tools.sync_pipeline_wrappers:main"
dc-clean-repo-tree = "tools.clean_repo_tree:main"
dc-touch-updated-utc = "tools.touch_updated_utc:main"
dc-make-release-zip = "tools.make_release_zip:main"
dc-init-layout = "tools.init_layout:main"
dc-generate-pipeline = "tools.generate_pipeline:main"
dc-migrate-pipeline-structure = "tools.migrate_pipeline_structure:main"
dc-update-wrapper-deprecations = "tools.update_wrapper_deprecations:main"
dc-validate-metrics-outputs = "tools.validate_metrics_outputs:main"
```

> If any of these modules don't have `main()`, either (1) omit the script for now, or (2) add a trivial `main()` wrapper in that module.

**B) Replace `run_all.py` with a console script (optional but cleaner)**
- [x] **ADD** in `[project.scripts]`:
  ```toml
  dc-build-natural-corpus = "tools.build_natural_corpus:main"
  ```
- [ ] **OPTION 1 (recommended)**: **DELETE** `run_all.py` and update docs to use `dc-build-natural-corpus`
- [ ] **OPTION 2**: Keep `run_all.py`, but remove the `sys.path` hack and just call the console script (less elegant).

---

### P0.3 — Fix CI so it tests the installed package (and not accidental repo‑root imports) ✅ COMPLETED

#### ✅ Concrete changes
**File:** `.github/workflows/ci.yml`

**A) Remove `PYTHONPATH` injection**
- [x] In both jobs (`validate`, `minimal-dry-run`), **DELETE**:
  ```yaml
  env:
    PYTHONPATH: ${{ github.workspace }}
  ```

**B) Install the package before running tools/tests**
After installing deps, **ADD**:

```yaml
- name: Install package (editable)
  run: |
    python -m pip install -e .
```

If you want to respect constraints:
```yaml
- name: Install package (editable, constrained)
  run: |
    python -m pip install -c requirements.constraints.txt -e .
```

**C) Replace `python -m tools.*` with the console scripts**
- [x] Updated all tool invocations to use console scripts

**D) Update minimal dry run path**
- [x] Changed `bash tools/run_minimal_dry_run.sh` to `bash scripts/run_minimal_dry_run.sh`

---

### P0.4 — Make JSON Schemas real package data (remove `src/schemas` symlink hacks) ✅ COMPLETED

Right now schema fallback points to `Path(__file__).parents[2]/schemas`, which is `src/schemas` in an src‑layout repo — but your schemas live at repo‑root `schemas/`.

You should make schemas **package resources** under `collector_core/schemas/` (your `config_validator.py` already prefers package resources!).

#### ✅ Concrete changes
**A) Move schemas into the package**
- [x] **CREATE DIR** `src/collector_core/schemas/` (already existed)
- [x] Schemas already in `src/collector_core/schemas/`

- [x] **DELETE** the now‑empty top‑level `schemas/` directory.

**B) Update fallback path in `src/collector_core/config_validator.py`**
- [x] Changed `_FALLBACK_SCHEMA_DIR` from `Path(__file__).resolve().parents[2] / "schemas"` to `Path(__file__).resolve().parent / "schemas"`

**C) Remove the test symlink workaround**
- [x] Removed the schema symlink/copy block from `_build_dc_env()` in `tests/conftest.py`

**D) Confirm packaging already includes schemas**
`pyproject.toml` already has:
```toml
[tool.setuptools.package-data]
collector_core = ["py.typed", "schemas/*.json"]
```
That will include `*.schema.json` fine. No change required.

---

## P1 — Eliminate known correctness and maintenance issues

### P1.1 — Yellow screen entrypoint duplication (6 files) → one dispatcher path

> **STATUS: NOT COMPLETED** - This is a larger refactoring task that was deferred. The current yellow screen wrappers continue to work via the existing dispatch mechanism.

You already have `src/collector_core/yellow_screen_dispatch.py`, but it still relies on per‑domain wrapper modules like `yellow_screen_chem.py`.

You also already have `configs/pipelines.yaml` containing `yellow_screen: chem|econ|...` which is a cleaner source of truth than `yellow_screen_module` duplication inside `pipeline_specs_registry.py`.

#### ✅ Concrete changes
**Goal:** Remove the per‑domain `yellow_screen_*.py` wrappers and derive the yellow screen domain from config.

**A) Update dispatcher signature to accept an override**
- [ ] Update `get_yellow_screen_main(domain: str)` to accept `yellow_screen` override parameter

**B) Remove the legacy wrapper imports in `dc_cli`**
- [ ] Simplify `_run_yellow_screen()` to use config-driven approach

**C) Remove duplication from registry**
- [ ] **REMOVE** the `yellow_screen_module="yellow_screen_..."` arguments from all registered specs.

**D) Remove the duplicated wrapper modules**
- [ ] **DELETE** yellow_screen_*.py wrappers (chem, econ, kg_nav, nlp, safety)

**E) Remove redundant knobs in `configs/pipelines.yaml`**
- [ ] **DELETE** the `knobs.yellow_screen_module` line(s) entirely.

**F) Update tests that asserted identity of wrapper mains**
- [ ] Update `tests/test_yellow_screen_dispatch.py` to use behavior tests

---

### P1.2 — Fix missing import: `re` in PMC worker ✅ COMPLETED

File: `src/collector_core/pmc_worker.py`

- [x] **ADD** near top imports:
  ```py
  import re
  ```

---

### P1.3 — Fix a real bug: `_get_default_handlers()` is undefined ✅ COMPLETED

File: `src/collector_core/acquire_strategies.py`

- [x] In `_LazyDict.values()` changed `_get_default_handlers()` to `_get_default_strategy_handlers()`

---

### P1.4 — Add missing return type annotations (strict mypy hygiene) ✅ COMPLETED

These were called out in `Pointers_for_Evaluation.md`.

#### A) `pmc_worker.py`
File: `src/collector_core/pmc_worker.py`
- [x] Add return type to `pools_from_targets_yaml` and refactor nested class to a typed dataclass:
  - Added `@dataclass(frozen=True) class PoolsPaths`
  - Changed function to return `PoolsPaths`
- [x] Add return type to nested `flush`:
  ```py
  def flush(split: str) -> None:
  ```

#### B) `acquire_strategies.py`
File: `src/collector_core/acquire_strategies.py`

- [x] Changed class header to `class _LazyDict(dict[str, StrategyHandler]):`
- [x] Added `from collections.abc import ItemsView, KeysView, ValuesView`
- [x] Added return types to `items()`, `keys()`, `values()` methods

#### C) `near_duplicate.py`
File: `src/collector_core/checks/near_duplicate.py`

- [x] Annotated `_build_minhash(self, tokens: list[str]) -> Any`

---

## P2 — Developer UX + consistency (docs, install, and repo automation)

### P2.1 — Update docs and README to match the new execution model (install → run CLI) ✅ COMPLETED

Once root `tools/` is removed, documentation must stop implying `python -m tools.*` works in a fresh clone.

#### ✅ Concrete changes
**A) README**
File: `README.md`

- [x] Replaced `python -m tools.preflight` with `dc-preflight`
- [x] Replaced `python -m tools.validate_repo` with `dc-validate-repo`
- [x] Replaced `python -m tools.validate_yaml_schemas` with `dc-validate-yaml-schemas`
- [x] Replaced `PYTHONPATH=src python src/tools/sync_pipeline_wrappers.py` with `dc-sync-wrappers`
- [x] Replaced `python src/tools/clean_repo_tree.py` with `dc-clean-repo-tree`

**B) Quickstart doc**
File: `docs/quickstart.md`

- [x] Replaced `python -m tools.preflight --pipelines math_pipeline_v2` with `dc-preflight --pipelines math_pipeline_v2`

---

### P2.2 — Pre-commit hook should self-install the repo (no "works on my machine") ✅ COMPLETED

File: `.pre-commit-config.yaml`

- [x] Changed the local hook from `language: system` with `python -m tools.validate_yaml_schemas` to `language: python` with `dc-validate-yaml-schemas --root .` and `additional_dependencies: ["-e ."]`

---

## P3 — "A‑grade" polish backlog (bigger refactors, still concrete)

> **STATUS: NOT COMPLETED** - These are larger refactors deferred for future work.

These are larger but high-leverage. They're still written as file‑level tasks so you can chip away via PRs.

### P3.1 — Unify pipeline sources of truth (avoid duplicated definitions)

Today, pipeline details exist in multiple places:
- `src/collector_core/pipeline_specs_registry.py` (registered `PipelineSpec`s)
- `configs/pipelines.yaml` (overrides, routing, knobs)
- `pipelines/targets/*.yaml` (targets)

**A‑grade direction:**
- Make `configs/pipelines.yaml` authoritative for pipeline config and generate or load registry from it.

#### Concrete incremental steps
- [ ] **ADD** `src/collector_core/pipeline_specs_loader.py` that reads `configs/pipelines.yaml` and returns `PipelineSpec` objects.
- [ ] **EDIT** `src/collector_core/pipeline_specs_registry.py` to become a thin wrapper that either:
  - imports loader, or
  - is auto-generated by `src/tools/generate_pipeline.py`.
- [ ] **ADD** `src/tools/validate_pipeline_specs.py` to ensure YAML ↔ registry consistency until the registry is fully removed.

---

### P3.2 — Make artifact schemas + runtime contracts enforced everywhere

You already have docs like `docs/output_contract.md` and tooling like `src/tools/output_contract.py`.

Concrete steps:
- [ ] **ADD** `main()` to `src/tools/output_contract.py` so CI can run it as a CLI (e.g. `dc-validate-output-contract`).
- [ ] **ADD** CI step:
  ```yaml
  - name: Validate output contract
    run: |
      dc-validate-output-contract --root .
  ```
- [ ] **ADD** tests:
  - `tests/test_output_contract_cli.py` to ensure non-zero exit on contract violation.

---

### P3.3 — Raise mypy bar: type-check `src/tools` too

CI currently runs:
```yaml
mypy src/collector_core
```

Concrete change:
- [ ] Update CI mypy step to:
  ```yaml
  mypy src/collector_core src/tools
  ```
- [ ] Add/adjust `[[tool.mypy.overrides]]` as needed to keep optional deps sane.

---

### P3.4 — Test coverage: fill the highest-impact gaps first

Instead of chasing "one test file per module", target the risk areas:

1) `dc_cli.py` / `pipeline_cli.py` (argument parsing + stage routing)
2) `checkpoint.py` (resume correctness)
3) `catalog_builder.py` (manifest correctness)
4) `observability.py` (no-op behavior when disabled, correct env wiring)

Concrete steps:
- [ ] **ADD** `tests/test_dc_cli_smoke.py` — run `python -m collector_core.dc_cli --help` and a minimal stage dry-run with fixture pipeline.
- [ ] **ADD** `tests/test_checkpoint_roundtrip.py` — write checkpoint, resume, verify idempotence.
- [ ] **ADD** `tests/test_catalog_builder_contract.py` — build catalog and validate against expected JSON schema / contract.

---

## "Done when" checklist (definition of A‑grade)

- [x] No duplicate module namespaces that depend on `PYTHONPATH` ordering (`tools`, `schemas` resolved).
- [x] CI installs and tests the package (`pip install -e .`) and does not inject repo root into `PYTHONPATH`.
- [ ] Yellow screen selection is config-driven and does not require duplicated wrapper modules. *(P1.1 deferred)*
- [x] Schema loading is via `importlib.resources` and works both installed and editable without symlinks.
- [x] Pre-commit hooks self-install and run in a controlled environment.
- [x] All P1 correctness issues are fixed (`re` import, `_get_default_handlers` bug, typing).
- [x] Docs match reality (copy/paste commands work in a fresh clone).

---

## Summary of Completed Work

### Completed (P0, P1.2-P1.4, P2)
- **P0.1**: Removed duplicate `tools/` package at repo root, moved shell script to `scripts/`
- **P0.2**: Added 14 console scripts to `pyproject.toml`
- **P0.3**: Updated CI to install package and use console scripts, removed PYTHONPATH injection
- **P0.4**: Deleted root-level `schemas/`, updated fallback path, cleaned up test conftest.py
- **P1.2**: Fixed missing `re` import in pmc_worker.py
- **P1.3**: Fixed `_get_default_handlers()` bug in acquire_strategies.py
- **P1.4**: Added return type annotations to pmc_worker.py, acquire_strategies.py, near_duplicate.py
- **P2.1**: Updated README.md and docs/quickstart.md to use new CLI commands
- **P2.2**: Updated pre-commit hook to self-install the package

### Deferred (P1.1, P3)
- **P1.1**: Yellow screen refactoring (larger task, existing wrappers continue to work)
- **P3.1-P3.4**: Larger polish items (pipeline unification, output contracts, mypy expansion, test coverage)
