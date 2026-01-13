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

---

## 🔲 Pending Items

### P1.1 — Yellow screen entrypoint duplication → one dispatcher path

**Goal:** Remove the per‑domain `yellow_screen_*.py` wrappers and derive the yellow screen domain from config.

#### A) Update dispatcher signature to accept an override
File: `src/collector_core/yellow_screen_dispatch.py`

- [ ] Change:
  ```py
  def get_yellow_screen_main(domain: str) -> Callable[[], None]:
  ```
  To:
  ```py
  def get_yellow_screen_main(domain: str, *, yellow_screen: str | None = None) -> Callable[[], None]:
  ```

- [ ] Implement logic:
  1. Load `spec = get_pipeline_spec(domain)` (current behavior)
  2. Compute `defaults = default_yellow_roots(spec.prefix)` (current behavior)
  3. If `yellow_screen` is not None and not `"standard"`:
     - import `from collector_core.yellow import domains as yellow_domains`
     - resolve `domain_mod = getattr(yellow_domains, yellow_screen)` (raise clear error if missing)
     - return closure calling `run_yellow_screen(defaults=defaults, domain=domain_mod)`
  4. Else fall back to legacy `spec.yellow_screen_module` or standard

#### B) Remove the legacy wrapper imports in `dc_cli`
File: `src/collector_core/dc_cli.py`

- [ ] Replace the current `_run_yellow_screen()` try/except importlib fallback with:
  ```py
  yellow = None
  if isinstance(ctx.overrides, dict):
      yellow = ctx.overrides.get("yellow_screen")
  main_fn = get_yellow_screen_main(slug, yellow_screen=yellow)
  return _run_with_args(main_fn, args)
  ```
- [ ] Remove the entire fallback block that tries to import `collector_core.yellow_screen_{module}`

#### C) Remove duplication from registry
File: `src/collector_core/pipeline_specs_registry.py`

- [ ] **REMOVE** the `yellow_screen_module="yellow_screen_..."` arguments from all registered specs:
  - chem
  - nlp
  - kg_nav
  - econ_stats_decision_adaptation
  - safety_incident

#### D) Remove the duplicated wrapper modules
- [ ] **DELETE** `src/collector_core/yellow_screen_chem.py`
- [ ] **DELETE** `src/collector_core/yellow_screen_econ.py`
- [ ] **DELETE** `src/collector_core/yellow_screen_kg_nav.py`
- [ ] **DELETE** `src/collector_core/yellow_screen_nlp.py`
- [ ] **DELETE** `src/collector_core/yellow_screen_safety.py`

Keep `src/collector_core/yellow_screen_standard.py` as the canonical "standard" implementation.

#### E) Remove redundant knobs in `configs/pipelines.yaml`
File: `configs/pipelines.yaml`

For each pipeline that has:
```yaml
knobs:
  yellow_screen_module: yellow_screen_chem
yellow_screen: chem
```
- [ ] **DELETE** the `knobs.yellow_screen_module` line(s) entirely
- [ ] Keep the `yellow_screen: ...` line (becomes source of truth)

#### F) Update tests that asserted identity of wrapper mains
File: `tests/test_yellow_screen_dispatch.py`

- [ ] Replace tests like:
  ```py
  from collector_core.yellow_screen_chem import main as chem_main
  assert main_fn is chem_main
  ```
  With behavior tests that monkeypatch `collector_core.yellow.base.run_yellow_screen` and assert it's called with the correct domain module.

---

### P0.2B — Optional: Remove run_all.py

- [ ] **DELETE** `run_all.py` and update docs to use `dc-build-natural-corpus`
- [ ] Or keep `run_all.py` but remove the `sys.path` hack and call the console script

---

### P3.1 — Unify pipeline sources of truth (avoid duplicated definitions)

Today, pipeline details exist in multiple places:
- `src/collector_core/pipeline_specs_registry.py` (registered `PipelineSpec`s)
- `configs/pipelines.yaml` (overrides, routing, knobs)
- `pipelines/targets/*.yaml` (targets)

**A‑grade direction:** Make `configs/pipelines.yaml` authoritative for pipeline config.

#### Concrete steps
- [ ] **ADD** `src/collector_core/pipeline_specs_loader.py` that reads `configs/pipelines.yaml` and returns `PipelineSpec` objects
- [ ] **EDIT** `src/collector_core/pipeline_specs_registry.py` to become a thin wrapper that either:
  - imports loader, or
  - is auto-generated by `src/tools/generate_pipeline.py`
- [ ] **ADD** `src/tools/validate_pipeline_specs.py` to ensure YAML ↔ registry consistency until the registry is fully removed

---

### P3.2 — Make artifact schemas + runtime contracts enforced everywhere

You already have docs like `docs/output_contract.md` and tooling like `src/tools/output_contract.py`.

#### Concrete steps
- [ ] **ADD** `main()` to `src/tools/output_contract.py` so CI can run it as a CLI
- [ ] **ADD** console script to `pyproject.toml`:
  ```toml
  dc-validate-output-contract = "tools.output_contract:main"
  ```
- [ ] **ADD** CI step in `.github/workflows/ci.yml`:
  ```yaml
  - name: Validate output contract
    run: |
      dc-validate-output-contract --root .
  ```
- [ ] **ADD** test `tests/test_output_contract_cli.py` to ensure non-zero exit on contract violation

---

### P3.3 — Raise mypy bar: type-check `src/tools` too

CI currently runs:
```yaml
mypy src/collector_core
```

#### Concrete steps
- [ ] Update CI mypy step in `.github/workflows/ci.yml` to:
  ```yaml
  mypy src/collector_core src/tools
  ```
- [ ] Add/adjust `[[tool.mypy.overrides]]` in `pyproject.toml` as needed to keep optional deps sane

---

### P3.4 — Test coverage: fill the highest-impact gaps first

Instead of chasing "one test file per module", target the risk areas:

1. `dc_cli.py` / `pipeline_cli.py` (argument parsing + stage routing)
2. `checkpoint.py` (resume correctness)
3. `catalog_builder.py` (manifest correctness)
4. `observability.py` (no-op behavior when disabled, correct env wiring)

#### Concrete steps
- [ ] **ADD** `tests/test_dc_cli_smoke.py` — run `python -m collector_core.dc_cli --help` and a minimal stage dry-run with fixture pipeline
- [ ] **ADD** `tests/test_checkpoint_roundtrip.py` — write checkpoint, resume, verify idempotence
- [ ] **ADD** `tests/test_catalog_builder_contract.py` — build catalog and validate against expected JSON schema / contract

---

## "Done when" checklist (definition of A‑grade)

- [x] No duplicate module namespaces that depend on `PYTHONPATH` ordering (`tools`, `schemas` resolved)
- [x] CI installs and tests the package (`pip install -e .`) and does not inject repo root into `PYTHONPATH`
- [ ] Yellow screen selection is config-driven and does not require duplicated wrapper modules *(P1.1)*
- [x] Schema loading is via `importlib.resources` and works both installed and editable without symlinks
- [x] Pre-commit hooks self-install and run in a controlled environment
- [x] All P1 correctness issues are fixed (`re` import, `_get_default_handlers` bug, typing)
- [x] Docs match reality (copy/paste commands work in a fresh clone)
- [ ] Pipeline config is unified in `configs/pipelines.yaml` *(P3.1)*
- [ ] Output contracts are enforced in CI *(P3.2)*
- [ ] `src/tools` is type-checked by mypy *(P3.3)*
- [ ] High-risk modules have test coverage *(P3.4)*

---

## Priority Recommendations

| Priority | Item | Effort | Impact |
|----------|------|--------|--------|
| **High** | P1.1 Yellow screen refactoring | Medium | Eliminates code duplication, single source of truth |
| Medium | P3.1 Pipeline unification | Medium | Reduces config drift risk |
| Medium | P3.2 Output contracts | Low | Catches contract violations in CI |
| Low | P3.3 Mypy expansion | Low | Better type safety for tools |
| Low | P3.4 Test coverage | Medium | Reduces regression risk |
| Optional | P0.2B Remove run_all.py | Low | Cleaner repo |
