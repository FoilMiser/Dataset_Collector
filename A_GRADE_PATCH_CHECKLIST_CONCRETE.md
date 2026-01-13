# Dataset Collector repo — A‑Grade Patch Checklist (Concrete file‑by‑file diffs)

This is a **concrete, implementable** checklist (rename/move/delete exact files; update exact config blocks; edit specific functions) to make the repo **more elegant, more robust, and “A‑grade”** while keeping the current architecture intact.

> Scope note: Items are prioritized (P0→P2). If you do only P0+P1, you’ll eliminate the biggest footguns and drift risks and improve developer UX significantly.

---

## P0 — Stop path/packaging footguns (single source of truth for `tools`, schemas, and CI)

### P0.1 — Remove duplicate top‑level `tools/` package (it shadows `src/tools`)
Right now you have **two** `tools` packages:
- `tools/` (repo root)
- `src/tools/` (the actual “real” tooling)

This is a **major correctness + CI drift risk**: whichever is first on `sys.path` wins, and CI currently sets `PYTHONPATH: ${{ github.workspace }}`, which preferentially loads the **root** `tools/`.

#### ✅ Concrete changes
**A) Delete the duplicated Python package at repo root**
- [ ] **DELETE** `tools/__init__.py`
- [ ] **DELETE** `tools/check_constraints.py`
- [ ] **DELETE** `tools/preflight.py`
- [ ] **DELETE** `tools/validate_repo.py`
- [ ] **DELETE** `tools/validate_yaml_schemas.py`

**B) Keep the CI shell script, but move it out of the Python namespace**
- [ ] **MOVE** `tools/run_minimal_dry_run.sh` → `scripts/run_minimal_dry_run.sh`
  - `mkdir -p scripts`
  - `git mv tools/run_minimal_dry_run.sh scripts/run_minimal_dry_run.sh`

**C) Update every reference that currently assumes `python -m tools.*` works without installation**
You’ll replace those with console scripts (next section) and/or ensure CI/dev installs the package.

---

### P0.2 — Add console scripts for all “repo tools” (no more `PYTHONPATH` rituals)
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

> If any of these modules don’t have `main()`, either (1) omit the script for now, or (2) add a trivial `main()` wrapper in that module.

**B) Replace `run_all.py` with a console script (optional but cleaner)**
- [ ] **ADD** in `[project.scripts]`:
  ```toml
  dc-build-natural-corpus = "tools.build_natural_corpus:main"
  ```
- [ ] **OPTION 1 (recommended)**: **DELETE** `run_all.py` and update docs to use `dc-build-natural-corpus`
- [ ] **OPTION 2**: Keep `run_all.py`, but remove the `sys.path` hack and just call the console script (less elegant).

---

### P0.3 — Fix CI so it tests the installed package (and not accidental repo‑root imports)

#### ✅ Concrete changes
**File:** `.github/workflows/ci.yml`

**A) Remove `PYTHONPATH` injection**
- [ ] In both jobs (`validate`, `minimal-dry-run`), **DELETE**:
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
Replace these steps:

```yaml
- name: Check constraints up to date
  run: |
    python -m tools.check_constraints
...
- name: Validate repo
  run: |
    python -m tools.validate_repo --root .
- name: Validate YAML schemas
  run: |
    python -m tools.validate_yaml_schemas --root .
- name: Preflight
  run: |
    python -m tools.preflight --repo-root . --quiet
```

With:

```yaml
- name: Check constraints up to date
  run: |
    dc-check-constraints

- name: Validate repo
  run: |
    dc-validate-repo --root .

- name: Validate YAML schemas
  run: |
    dc-validate-yaml-schemas --root .

- name: Preflight
  run: |
    dc-preflight --repo-root . --quiet
```

**D) Update minimal dry run path**
Replace:
```yaml
bash tools/run_minimal_dry_run.sh
```
With:
```yaml
bash scripts/run_minimal_dry_run.sh
```

---

### P0.4 — Make JSON Schemas real package data (remove `src/schemas` symlink hacks)
Right now schema fallback points to `Path(__file__).parents[2]/schemas`, which is `src/schemas` in an src‑layout repo — but your schemas live at repo‑root `schemas/`.

You should make schemas **package resources** under `collector_core/schemas/` (your `config_validator.py` already prefers package resources!).

#### ✅ Concrete changes
**A) Move schemas into the package**
- [ ] **CREATE DIR** `src/collector_core/schemas/`
- [ ] **MOVE** (git mv) all schema files:
  - `schemas/denylist.schema.json`
  - `schemas/field_schemas.schema.json`
  - `schemas/license_map.schema.json`
  - `schemas/pipeline_map.schema.json`
  - `schemas/targets.schema.json`
  → into `src/collector_core/schemas/`

- [ ] **DELETE** the now‑empty top‑level `schemas/` directory.

**B) Update fallback path in `src/collector_core/config_validator.py`**
Change:
```py
_FALLBACK_SCHEMA_DIR = Path(__file__).resolve().parents[2] / "schemas"
```
To:
```py
_FALLBACK_SCHEMA_DIR = Path(__file__).resolve().parent / "schemas"
```

**C) Remove the test symlink workaround**
File: `tests/conftest.py`

Delete the entire block in `_build_dc_env()` that symlinks/copies `schemas` into `src/schemas`:

```py
schema_root = repo_root / "src" / "schemas"
...
```

After the schema move, it’s no longer needed.

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
You already have `src/collector_core/yellow_screen_dispatch.py`, but it still relies on per‑domain wrapper modules like `yellow_screen_chem.py`.

You also already have `configs/pipelines.yaml` containing `yellow_screen: chem|econ|...` which is a cleaner source of truth than `yellow_screen_module` duplication inside `pipeline_specs_registry.py`.

#### ✅ Concrete changes
**Goal:** Remove the per‑domain `yellow_screen_*.py` wrappers and derive the yellow screen domain from config.

**A) Update dispatcher signature to accept an override**
File: `src/collector_core/yellow_screen_dispatch.py`

Change:
```py
def get_yellow_screen_main(domain: str) -> Callable[[], None]:
```
To:
```py
def get_yellow_screen_main(domain: str, *, yellow_screen: str | None = None) -> Callable[[], None]:
```

Then implement this logic:

1) Load `spec = get_pipeline_spec(domain)` (current behavior)
2) Compute `defaults = default_yellow_roots(spec.prefix)` (current behavior)
3) If `yellow_screen` is not None and not `"standard"`:
   - import `from collector_core.yellow import domains as yellow_domains`
   - resolve `domain_mod = getattr(yellow_domains, yellow_screen)` (raise clear error if missing)
   - return closure calling:
     ```py
     from collector_core.yellow.base import run_yellow_screen
     run_yellow_screen(defaults=defaults, domain=domain_mod)
     ```
4) Else fall back:
   - If `spec.yellow_screen_module` exists (legacy), keep it for backwards compatibility (but mark deprecated)
   - Otherwise use standard.

**B) Remove the legacy wrapper imports in `dc_cli`**
File: `src/collector_core/dc_cli.py`

Replace the current `_run_yellow_screen()` try/except importlib fallback with a single call that passes overrides:

```py
yellow = None
if isinstance(ctx.overrides, dict):
    yellow = ctx.overrides.get("yellow_screen")
main_fn = get_yellow_screen_main(slug, yellow_screen=yellow)
return _run_with_args(main_fn, args)
```

Remove the entire fallback block that tries to import `collector_core.yellow_screen_{module}`.

**C) Remove duplication from registry**
File: `src/collector_core/pipeline_specs_registry.py`

- [ ] **REMOVE** the `yellow_screen_module="yellow_screen_..."` arguments from all registered specs.
  - (chem, nlp, kg_nav, econ_stats_decision_adaptation, safety_incident)

This makes `configs/pipelines.yaml` the source of truth for which yellow screen domain to run.

**D) Remove the duplicated wrapper modules**
- [ ] **DELETE**:
  - `src/collector_core/yellow_screen_chem.py`
  - `src/collector_core/yellow_screen_econ.py`
  - `src/collector_core/yellow_screen_kg_nav.py`
  - `src/collector_core/yellow_screen_nlp.py`
  - `src/collector_core/yellow_screen_safety.py`

Keep `src/collector_core/yellow_screen_standard.py` for now as the canonical “standard” implementation.

**E) Remove redundant knobs in `configs/pipelines.yaml`**
File: `configs/pipelines.yaml`

For each pipeline that has:
```yaml
knobs:
  yellow_screen_module: yellow_screen_chem
yellow_screen: chem
```
- [ ] **DELETE** the `knobs.yellow_screen_module` line(s) entirely.
- [ ] Keep the `yellow_screen: ...` line, because that becomes the source of truth.

**F) Update tests that asserted identity of wrapper mains**
File: `tests/test_yellow_screen_dispatch.py`

- Replace tests like:
  ```py
  from collector_core.yellow_screen_chem import main as chem_main
  assert main_fn is chem_main
  ```
  With a behavior test that monkeypatches `collector_core.yellow.base.run_yellow_screen` and asserts it’s called with the correct domain module when you call the returned function.

You can do:
- Monkeypatch `run_yellow_screen` to record `(defaults, domain)` and then call `main_fn()`.
- Assert `domain.__name__` (or `domain` module object) matches `collector_core.yellow.domains.chem`, etc.

---

### P1.2 — Fix missing import: `re` in PMC worker
File: `src/collector_core/pmc_worker.py`

- [ ] **ADD** near top imports:
  ```py
  import re
  ```

---

### P1.3 — Fix a real bug: `_get_default_handlers()` is undefined
File: `src/collector_core/acquire_strategies.py`

In `_LazyDict.values()` change:
```py
self.update(_get_default_handlers())
```
To:
```py
self.update(_get_default_strategy_handlers())
```

---

### P1.4 — Add missing return type annotations (strict mypy hygiene)
These were called out in `Pointers_for_Evaluation.md`.

#### A) `pmc_worker.py`
File: `src/collector_core/pmc_worker.py`
- [ ] Add return type to `pools_from_targets_yaml` and refactor nested class to a typed dataclass:

Add near top:
```py
from dataclasses import dataclass
```

Add:
```py
@dataclass(frozen=True)
class PoolsPaths:
    permissive: Path
    copyleft: Path
    quarantine: Path
```

Then change:
```py
def pools_from_targets_yaml(targets_yaml: Path, fallback: Path):
    ...
    class Pools:
        permissive = ...
    return Pools()
```

To:
```py
def pools_from_targets_yaml(targets_yaml: Path, fallback: Path) -> PoolsPaths:
    cfg = read_yaml(targets_yaml, schema_name="targets")
    pools = cfg.get("globals", {}).get("pools", {})
    return PoolsPaths(
        permissive=Path(pools.get("permissive", fallback / "permissive")).expanduser(),
        copyleft=Path(pools.get("copyleft", fallback / "copyleft")).expanduser(),
        quarantine=Path(pools.get("quarantine", fallback / "quarantine")).expanduser(),
    )
```

- [ ] Add return type to nested `flush`:
  ```py
  def flush(split: str) -> None:
  ```

#### B) `acquire_strategies.py`
File: `src/collector_core/acquire_strategies.py`

Change class header:
```py
class _LazyDict(dict):
```
To:
```py
from collections.abc import ItemsView, KeysView, ValuesView

class _LazyDict(dict[str, StrategyHandler]):
```

Add return types:
```py
def items(self) -> ItemsView[str, StrategyHandler]: ...
def keys(self) -> KeysView[str]: ...
def values(self) -> ValuesView[StrategyHandler]: ...
```

#### C) `near_duplicate.py`
File: `src/collector_core/checks/near_duplicate.py`

Annotate:
```py
def _build_minhash(self, tokens: list[str]):
```
To:
```py
from typing import TYPE_CHECKING, Any
if TYPE_CHECKING:
    from datasketch import MinHash as _MinHash

def _build_minhash(self, tokens: list[str]) -> "_MinHash":  # or -> Any if you want to avoid TYPE_CHECKING
```

---

## P2 — Developer UX + consistency (docs, install, and repo automation)

### P2.1 — Update docs and README to match the new execution model (install → run CLI)
Once root `tools/` is removed, documentation must stop implying `python -m tools.*` works in a fresh clone.

#### ✅ Concrete changes
**A) README**
File: `README.md`

Replace:
```bash
python -m tools.preflight
python -m tools.validate_repo ...
python -m tools.validate_yaml_schemas ...
```

With:
```bash
pip install -e .
dc-preflight
dc-validate-repo ...
dc-validate-yaml-schemas ...
```

Also replace:
```bash
PYTHONPATH=src python src/tools/sync_pipeline_wrappers.py --write
python src/tools/sync_pipeline_wrappers.py --check
```
With:
```bash
pip install -e .
dc-sync-wrappers --write
dc-sync-wrappers --check
```

**B) Quickstart doc**
File: `docs/quickstart.md`

Replace the one occurrence of:
```bash
python -m tools.preflight --pipelines math_pipeline_v2
```
With:
```bash
pip install -e .
dc-preflight --pipelines math_pipeline_v2
```

---

### P2.2 — Pre-commit hook should self-install the repo (no “works on my machine”)
File: `.pre-commit-config.yaml`

Change the local hook:

From:
```yaml
- repo: local
  hooks:
    - id: validate-yaml-schemas
      name: Validate YAML schemas
      entry: python -m tools.validate_yaml_schemas
      language: system
      files: ^(configs/|pipelines/targets/).*\.yaml$
      pass_filenames: false
```

To:
```yaml
- repo: local
  hooks:
    - id: validate-yaml-schemas
      name: Validate YAML schemas
      entry: dc-validate-yaml-schemas --root .
      language: python
      additional_dependencies:
        - -e .
      files: ^(configs/|pipelines/targets/).*\.yaml$
      pass_filenames: false
```

This ensures the hook always runs against the installed package and doesn’t rely on `PYTHONPATH` or a global dev environment.

---

## P3 — “A‑grade” polish backlog (bigger refactors, still concrete)

These are larger but high-leverage. They’re still written as file‑level tasks so you can chip away via PRs.

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
Instead of chasing “one test file per module”, target the risk areas:

1) `dc_cli.py` / `pipeline_cli.py` (argument parsing + stage routing)
2) `checkpoint.py` (resume correctness)
3) `catalog_builder.py` (manifest correctness)
4) `observability.py` (no-op behavior when disabled, correct env wiring)

Concrete steps:
- [ ] **ADD** `tests/test_dc_cli_smoke.py` — run `python -m collector_core.dc_cli --help` and a minimal stage dry-run with fixture pipeline.
- [ ] **ADD** `tests/test_checkpoint_roundtrip.py` — write checkpoint, resume, verify idempotence.
- [ ] **ADD** `tests/test_catalog_builder_contract.py` — build catalog and validate against expected JSON schema / contract.

---

## “Done when” checklist (definition of A‑grade)
- [ ] No duplicate module namespaces that depend on `PYTHONPATH` ordering (`tools`, `schemas` resolved).
- [ ] CI installs and tests the package (`pip install -e .`) and does not inject repo root into `PYTHONPATH`.
- [ ] Yellow screen selection is config-driven and does not require duplicated wrapper modules.
- [ ] Schema loading is via `importlib.resources` and works both installed and editable without symlinks.
- [ ] Pre-commit hooks self-install and run in a controlled environment.
- [ ] All P1 correctness issues are fixed (`re` import, `_get_default_handlers` bug, typing).
- [ ] Docs match reality (copy/paste commands work in a fresh clone).
