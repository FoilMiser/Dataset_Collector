# Claude Code Checklist — Dataset Collector (v2)

**Goal:** Apply the fixes + refinements below while preserving the repository’s core promise: *ethical, auditable dataset acquisition with evidence-backed license screening (GREEN/YELLOW/RED), and a spec-driven architecture with minimal per-domain boilerplate.*

This checklist is written for **Claude Code** to execute as a set of concrete edits, with CI as the definition of “done”.

---

## 0) Non‑negotiables (read first)

- [ ] **Do not weaken license safety.** Never reduce restriction scanning, denylist behavior, evidence hashing, or signoff requirements unless explicitly instructed.
- [ ] **No network in tests.** All tests must run offline using mocks/fixtures.
- [ ] **Keep outputs stable.** Refactors must not change queue row shape / JSONL record contract unless explicitly versioned.
- [ ] **Prefer core modules over per‑pipeline code.** Pipeline wrappers should stay thin unless there is truly domain-specific logic.
- [ ] **Keep Windows friendliness.** Avoid POSIX-only assumptions in paths, subprocess calls, and shell scripts.

---

## 1) Establish a baseline (before edits)

- [ ] From repo root, run and record current results:
  - [ ] `python -m tools.validate_yaml_schemas --root .`
  - [ ] `python -m tools.validate_repo --root . --output /tmp/validate_repo.json` (or similar)
  - [ ] `python -m tools.preflight --repo-root . --quiet`
  - [ ] `python -m pytest -q`
  - [ ] `python -m ruff check .`
  - [ ] `python -m ruff format --check .`

**Acceptance:** You have a copy of the failing reports so you can prove the fixes.

---

## 2) CI blockers: validators don’t match the new “thin wrapper” architecture (HIGH)

### 2.1 Fix `tools/preflight.py` so it works with wrapper workers (HIGH)

**Problem:** Many pipelines have “thin wrapper” `acquire_worker.py` scripts that delegate to `collector_core.generic_workers.main_acquire(...)`. `tools/preflight.py` currently fails hard when it can’t find a literal `STRATEGY_HANDLERS` dict in those wrappers.

- [ ] Update `tools/preflight.py` to support wrapper-style workers **without requiring `STRATEGY_HANDLERS`**.
  - [ ] Recommended approach: **stop AST‑parsing `acquire_worker.py`** and instead determine supported strategies via **runtime registry logic**:
    - [ ] Use `collector_core.pipeline_registry.resolve_pipeline_context(...)` + `resolve_acquire_hooks(...)` to obtain the handler mapping keys for each pipeline.
    - [ ] Fall back to default strategies when a pipeline uses the generic worker path and/or has no overrides.
  - [ ] If you must keep AST parsing: detect wrapper pattern (`from collector_core.generic_workers import main_acquire`) and treat it as “default strategies”, not an error.

- [ ] Ensure preflight normalizes download configs the same way runtime does:
  - [ ] Merge `download.config` into the download dict (same behavior as `collector_core.acquire_strategies.normalize_download`).
  - [ ] Otherwise, registry checks will falsely complain about missing `dataset_id`, `repo_url`, etc.

**Acceptance:**
- `python -m tools.preflight --repo-root . --quiet` returns exit code 0.

---

### 2.2 Fix `tools/validate_repo.py` false failures for thin wrappers (HIGH)

**Problem:** `tools.validate_repo` currently expects many per-pipeline modules to import `collector_core.__version__` (and pipeline drivers to import VERSION). Thin wrappers intentionally don’t.

- [ ] Update `tools/validate_repo.py` so wrapper modules are valid:
  - [ ] In `validate_pipeline_driver_versions(...)`, treat wrapper drivers that call `collector_core.pipeline_factory.get_pipeline_driver(...)` as compliant even without explicit `VERSION` import.
  - [ ] In `validate_versioned_modules(...)`, allow thin wrapper scripts that delegate into core to omit `VERSION` imports (or gate the check behind a “not a wrapper” heuristic).
  - [ ] Keep the hardcoded version check (don’t allow `VERSION = "2.0"` literals) for non-wrapper modules.

**Acceptance:**
- `python -m tools.validate_repo --root .` returns exit code 0.

---

## 3) Strategy registry completeness + configuration validation (HIGH/MED)

### 3.1 Add missing strategies to `tools/strategy_registry.py` (MED)

Many targets use strategies not currently registered (even if disabled). The registry is used by `preflight` and `validate_repo` to validate required fields and external tools.

- [ ] Add entries for at least:
  - [ ] `none` (supported; no required fields; no external tools)
  - [ ] `api_tabular`
  - [ ] `faa_ac_crawl`
  - [ ] `noaa_ir_json`
  - [ ] `ntrs_openapi`
  - [ ] `pmc_oa`
  - [ ] `usgs_pubs_warehouse`
- [ ] For each new strategy, define:
  - [ ] `status: supported` (or `experimental` if truly not implemented yet)
  - [ ] sensible `required` field checks (even a minimal `base_url` / `url` rule is better than nothing)
  - [ ] `external_tools` if applicable

**Acceptance:** `python -m tools.preflight --repo-root .` emits no “registry missing entry” warnings for strategies used in `targets_*.yaml` (unless intentionally left unregistered).

---

## 4) Eliminate utility duplication (HIGH)

**Problem:** Many modules redefine common helpers (`utc_now`, `ensure_dir`, `sha256_file`, JSONL helpers, etc.), causing drift.

- [ ] Identify duplicated helpers and replace with imports from `collector_core/utils.py` (or a small set of canonical utility modules).
  - [ ] Prioritize `utc_now`, `ensure_dir`, `read_jsonl`, `write_json`, `sha256_file`, `safe_filename` / `safe_name` variants.
- [ ] Pick **one** canonical implementation per helper and delete the rest.
- [ ] Standardize types + behavior:
  - [ ] Decide whether `sha256_file` raises on missing file vs returns `None`; apply consistently.
  - [ ] Decide whether `read_jsonl` returns a list vs iterator; if both are needed, expose `iter_jsonl` + `read_jsonl_list`.

**Acceptance:**
- `ruff` passes with no unused imports.
- Tests still pass.
- No behavior regressions in queue emission and evidence hashing.

---

## 5) Tighten schema validation (MED)

**Problem:** Schemas are permissive (`additionalProperties: true` in many places), allowing typos to silently pass.

- [ ] Tighten `schemas/targets.schema.json`:
  - [ ] Prefer `additionalProperties: false` for key objects (targets, download, output, license_evidence), unless you explicitly need an extension mechanism.
  - [ ] Add an enum for `download.strategy` (at least core strategies + common pipeline ones).
  - [ ] If extension strategies are desired, allow them but validate required shapes for known ones.
- [ ] Tighten license config schemas (`license_map`, signoff schemas) cautiously—do **not** loosen requirements.
- [ ] Ensure `tools.validate_yaml_schemas` still passes.

**Acceptance:** A misspelling like `downlaod:` or `stategy:` fails schema validation.

---

## 6) Standardize error handling (MED)

**Problem:** Some code returns `None`, some raises exceptions, others return `{status:"error"}` dicts.

- [ ] Choose and document one of:
  - [ ] **Exceptions** for programmer/config errors; `{status:"error"}` only for recoverable per-target runtime issues.
  - [ ] A small `Result` dataclass/type for internal operations, serialized to dict only at boundaries.
- [ ] Apply consistently in:
  - [ ] `collector_core/acquire_strategies.py`
  - [ ] `collector_core/pipeline_driver_base.py`
  - [ ] yellow screen modules

**Acceptance:** Call sites no longer need “guessy” `if value is None` logic.

---

## 7) Break up monoliths (MED, do after CI is green)

### 7.1 Split `collector_core/pipeline_driver_base.py` (~1800 LOC)

- [ ] Extract into focused modules while keeping public behavior stable:
  - [ ] `collector_core/evidence_fetcher.py`
  - [ ] `collector_core/license_resolver.py`
  - [ ] `collector_core/classification_engine.py`
  - [ ] `collector_core/queue_emitter.py`
  - [ ] `collector_core/reporting.py`
- [ ] Keep `PipelineDriverBase` as a thin orchestrator importing these pieces.
- [ ] Add/adjust unit tests for each extracted module.

### 7.2 Split `collector_core/acquire_strategies.py` (~1500 LOC)

- [ ] Move strategy handlers into `collector_core/strategies/` (e.g., `http.py`, `git.py`, `s3.py`, `hf.py`, `zenodo.py`)
- [ ] Keep a stable import surface so pipeline workers don’t churn.

**Acceptance:** No public CLI breakage; tests pass.

---

## 8) Rate limiting (declared but not implemented) (MED)

- [ ] Implement a small, testable token bucket in core (e.g., `collector_core/rate_limit.py`).
- [ ] Wire it into resolvers that are most likely to rate-limit:
  - [ ] GitHub (releases / API calls)
  - [ ] Zenodo (API)
  - [ ] Dataverse instances
- [ ] Make the behavior configurable from `targets_*.yaml` resolver blocks (without breaking existing configs).
- [ ] Add tests using deterministic clocks (no real sleeping).

**Acceptance:** Configured rate limits actually throttle requests; tests validate no request bursts beyond the configured capacity.

---

## 9) Testing upgrades (MED)

- [ ] Add tests for “error paths” for each strategy:
  - [ ] missing required config keys
  - [ ] non-200 responses / retries / resume logic
  - [ ] checksum mismatch behavior where applicable
- [ ] Add tests for merge partitioning and dedupe correctness under large-ish inputs.
- [ ] Add focused tests for domain-specific yellow scrubbers (chem/nlp/etc.), especially anything that redacts or filters content.
- [ ] Consider 1–2 property-based tests (Hypothesis) for filename/text sanitizers if dependency policy allows.

**Acceptance:** Coverage increases specifically in high-risk areas (acquire + evidence + bucket classification).

---

## 10) Developer experience polish (LOW/MED)

- [ ] Expand `dc` CLI ergonomics:
  - [ ] `dc status <pipeline>` (queue health, last run, counts)
  - [ ] Global `--dry-run/--execute`
  - [ ] Better progress reporting (tqdm where appropriate)
- [ ] Reduce config duplication via inheritance or a shared base config.
- [ ] Add a diagram / docs page describing the classification + signoff flow.

---

## 11) Definition of done (must all be true)

- [ ] `python -m tools.validate_yaml_schemas --root .` ✅
- [ ] `python -m tools.validate_repo --root .` ✅
- [ ] `python -m tools.preflight --repo-root . --quiet` ✅
- [ ] `pytest -q` ✅
- [ ] `ruff check .` ✅
- [ ] `ruff format --check .` ✅

---

## Appendix: Key repo concepts Claude must preserve

- **Buckets:** GREEN/YELLOW/RED (with pools like permissive/copyleft/quarantine) are safety boundaries, not “tags”.
- **License evidence:** hash of evidence content + stored snapshots must remain stable and auditable.
- **Signoffs:** must remain evidence-backed; do not allow signoff without evidence URL/hash linkage.
- **Spec-driven architecture:** pipeline specs are the “single source of truth”; domain wrappers should remain thin.
