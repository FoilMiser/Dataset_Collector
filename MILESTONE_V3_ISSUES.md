# Dataset Collector v3 — GitHub Issue Tracker Master Plan (A‑Grade)

This file converts the v3 roadmap into **issue-tracker–ready** milestones, epics, and issues with **labels, dependencies, and acceptance criteria**.

---

## Recommended milestone sequence

### Milestone: `v3.0-alpha (Unblock + Stabilize)`
**Goal:** repo installs cleanly, CI green, no syntax/import blockers, minimal DX friction.

### Milestone: `v3.0-beta (Core refactors + Architecture consolidation)`
**Goal:** strategies refactor complete, wrapper dirs removed, yellow screening unified, policy/denylist real.

### Milestone: `v3.0 (A-Grade polish)`
**Goal:** profiles, async downloads, mypy/typing, pre-commit, observability, integration tests, docs.

---

## Labels to create (once)

### Priority
- `priority/p0`
- `priority/p1`
- `priority/p2`
- `priority/p3`

### Type
- `type/bug`
- `type/refactor`
- `type/feature`
- `type/chore`

### Area
- `area/ci`
- `area/acquire`
- `area/yellow`
- `area/policy`
- `area/config`
- `area/cli`
- `area/docs`
- `area/tests`
- `area/observability`

### Status
- `status/blocker`
- `status/migration`
- `status/good-first-issue`

---

# EPIC 0 — Repo must build (P0 blockers)

## Issue 0.1 — Fix syntax errors in yellow-screen entrypoints (template placeholders)
**Labels:** `priority/p0`, `type/bug`, `area/yellow`, `status/blocker`  
**Milestone:** `v3.0-alpha`

**Tasks**
- Remove `${module}` placeholder imports in:
  - `src/collector_core/yellow_screen_{chem,econ,kg_nav,nlp,safety}.py`
- Either delete these files (preferred if consolidating yellow) or replace with correct imports.

**Acceptance criteria**
- `python -m compileall src/` succeeds
- `pytest` reaches test collection without `SyntaxError`

---

## Issue 0.2 — Fix SyntaxError in `pmc_worker.py` (escaped quotes)
**Labels:** `priority/p0`, `type/bug`, `area/ci`, `status/blocker`  
**Milestone:** `v3.0-alpha`

**Tasks**
- Replace `build_user_agent(\"pmc-worker\", version)` with normal string literal.

**Acceptance criteria**
- `python -m compileall src/` succeeds
- `pytest -q` runs (even if some tests fail for other reasons)

---

## Issue 0.3 — Align dependency constraints with runtime imports (zstandard)
**Labels:** `priority/p0`, `type/chore`, `area/ci`, `status/blocker`  
**Milestone:** `v3.0-alpha`

**Tasks**
- Ensure anything imported at runtime (e.g., `zstandard`) is present in constraints/lock flow used by CI.
- Add `zstandard>=0.22.0` to `requirements.constraints.txt` **or** regenerate constraints from your lock workflow.

**Acceptance criteria**
- All CI install modes succeed (`min`, `constraints`, `dev`)
- `pip check` passes

---

## Issue 0.4 — Remove import-time deprecation warnings (no log spam on import)
**Labels:** `priority/p0`, `type/chore`, `area/acquire`  
**Milestone:** `v3.0-alpha`

**Tasks**
- Remove the logger warning that fires on import in `acquire_strategies.py`.
- If deprecation warnings are needed, emit only on **call** via `warnings.warn(..., DeprecationWarning)`.

**Acceptance criteria**
- `python -c "import collector_core"` emits no warnings/log spam by default

---

## Issue 0.5 — Make optional dependencies explicit (lazy import or extras)
**Labels:** `priority/p0`, `type/refactor`, `area/ci`  
**Milestone:** `v3.0-alpha`

**Tasks**
- Decide policy:
  - **Option A:** hard dependency everywhere (lock consistently)
  - **Option B:** optional extras + lazy import + clear error message
- Remove import-time requirement for optional libs from `__init__.py` paths.

**Acceptance criteria**
- Minimal install can run `dc pipeline … --stage classify` without optional deps
- When optional feature is used without extra installed, error tells user exactly what to install

---

# EPIC 1 — Strategy module refactor (biggest structural debt)

## Issue 1.1 — Extract HTTP strategy from deprecated monolith into `acquire/strategies/http.py`
**Labels:** `priority/p1`, `type/refactor`, `area/acquire`, `status/migration`  
**Milestone:** `v3.0-beta`

**Tasks**
- Move HTTP validation + resume download + multi/single handler into `src/collector_core/acquire/strategies/http.py`
- Ensure tests cover:
  - URL safety validation
  - resume behavior
  - hash/size verification behavior

**Acceptance criteria**
- `http.py` contains real logic (not re-export)
- No HTTP logic remains in deprecated file except temporary re-export wrappers

---

## Issue 1.2 — Extract Git strategy into `acquire/strategies/git.py`
**Labels:** `priority/p1`, `type/refactor`, `area/acquire`, `status/migration`  
**Milestone:** `v3.0-beta`  
**Dependencies:** Issue 1.1 pattern established

**Acceptance criteria**
- Real git clone logic lives in `git.py`
- Deprecated file only re-exports (temporary)

---

## Issue 1.3 — Extract remaining strategies (Zenodo/Figshare/Dataverse/GitHub Releases/HF/S3/FTP/Torrent)
**Labels:** `priority/p1`, `type/refactor`, `area/acquire`, `status/migration`  
**Milestone:** `v3.0-beta`

**Tasks**
- One issue per strategy OR one batched issue with a checklist:
  - `zenodo.py`, `figshare.py`, `dataverse.py`, `github_release.py`, `hf.py`, `s3.py`, `ftp.py`, `torrent.py`

**Acceptance criteria**
- Strategy files contain real implementations
- Deprecated monolith shrinks accordingly

---

## Issue 1.4 — Add lazy-loading strategy registry (`acquire/strategies/registry.py`)
**Labels:** `priority/p1`, `type/feature`, `area/acquire`  
**Milestone:** `v3.0-beta`

**Tasks**
- Implement `get_strategy(name)` lazy import
- Implement `build_default_handlers()`

**Acceptance criteria**
- Core import time stays low
- Only selected strategies import when invoked

---

## Issue 1.5 — Gut `acquire_strategies.py` into compat shim only (v3 deprecation plan)
**Labels:** `priority/p1`, `type/refactor`, `area/acquire`  
**Milestone:** `v3.0-beta`  
**Dependencies:** Issues 1.1–1.4

**Acceptance criteria**
- No “real” acquisition logic left in `acquire_strategies.py`
- Only re-exports + deprecation notes remain
- Clear removal date/version in docstring

---

# EPIC 2 — Remove per-pipeline wrapper directories (boilerplate reduction)

## Issue 2.1 — Update `dc` CLI to not require physical `*_pipeline_v2/` directories
**Labels:** `priority/p1`, `type/refactor`, `area/cli`  
**Milestone:** `v3.0-beta`

**Tasks**
- Make canonical targets location `pipelines/targets/targets_<domain>.yaml`
- Keep legacy fallback **temporarily**

**Acceptance criteria**
- A pipeline runs with only targets YAML present in canonical location

---

## Issue 2.2 — Create `pipelines/requirements/<domain>.txt` and migrate per-domain requirements
**Labels:** `priority/p1`, `type/refactor`, `area/config`  
**Milestone:** `v3.0-beta`

**Acceptance criteria**
- Domain requirements live in one place
- Docs explain installing domain deps

---

## Issue 2.3 — Create migration script to move real domain logic into `collector_core/domains/<domain>/`
**Labels:** `priority/p1`, `type/feature`, `area/cli`, `status/migration`  
**Milestone:** `v3.0-beta`

**Tasks**
- Script:
  - copy/move only files with real logic
  - relocate READMEs into `docs/pipelines/<domain>.md`
  - relocate requirements

**Acceptance criteria**
- Migration is repeatable + idempotent
- Produces a report of moved vs skipped files

---

## Issue 2.4 — Delete wrapper files + eventually remove `*_pipeline_v2/` directories
**Labels:** `priority/p1`, `type/chore`, `area/docs`, `status/migration`  
**Milestone:** `v3.0`  
**Dependencies:** Issues 2.1–2.3

**Acceptance criteria**
- `*_pipeline_v2/` removed (or archived) without losing functionality
- CI updated to new paths

---

# EPIC 3 — Consolidate yellow screening into one coherent subsystem

## Issue 3.1 — Implement unified yellow dispatcher + scrubber modules
**Labels:** `priority/p1`, `type/refactor`, `area/yellow`  
**Milestone:** `v3.0-beta`

**Tasks**
- Create:
  - `collector_core/yellow/dispatcher.py`
  - `collector_core/yellow/scrubber.py`
  - `collector_core/yellow/checks/*`
  - `collector_core/yellow/domains/*`
- Make one canonical entry path used by CLI/workers.

**Acceptance criteria**
- Only one dispatch path exists
- No duplicated “yellow_screen_*” runner logic remains active

---

## Issue 3.2 — Delete obsolete yellow modules after consolidation
**Labels:** `priority/p1`, `type/chore`, `area/yellow`  
**Milestone:** `v3.0-beta`  
**Dependencies:** Issue 3.1

**Acceptance criteria**
- The following are removed and no longer referenced:
  - `yellow_screen_*.py` (all of them)
  - `yellow_scrubber_base.py`
  - any duplicate dispatch glue

---

## Issue 3.3 — Implement real domain-specific screening (chem/cyber + roadmap domains)
**Labels:** `priority/p2`, `type/feature`, `area/yellow`  
**Milestone:** `v3.0`

**Tasks**
- Implement domain modules with real checks + metadata:
  - `chem.py`, `cyber.py` first (highest dual-use risk)
  - then `code.py`, `biology.py`, `nlp.py`, `physics.py`, `3d.py` as needed

**Acceptance criteria**
- Domain handlers add new signal beyond standard filter
- Tests cover at least one positive + one negative example per domain

---

# EPIC 4 — Ethics/licensing defensibility (denylist + policy audibility)

## Issue 4.1 — Populate `configs/common/denylist.yaml` with real entries + provenance
**Labels:** `priority/p1`, `type/feature`, `area/policy`  
**Milestone:** `v3.0-beta`

**Acceptance criteria**
- Denylist has:
  - domain patterns
  - publisher patterns
  - substring/regex patterns
  - severity + rationale + link fields

---

## Issue 4.2 — Rule IDs + decision explanation bundle per target (audit trail)
**Labels:** `priority/p1`, `type/feature`, `area/policy`  
**Milestone:** `v3.0`

**Tasks**
- Every routing decision (GREEN/YELLOW/RED) must store:
  - rule IDs that fired
  - evidence URLs + hash + timestamp
  - denylist matches + restriction phrase matches

**Acceptance criteria**
- Reviewer can answer “why was this target red/yellow?” from artifacts alone

---

## Issue 4.3 — Implement “license evidence changed” policy (automatic demotion + re-review)
**Labels:** `priority/p1`, `type/feature`, `area/policy`  
**Milestone:** `v3.0`

**Acceptance criteria**
- When evidence hash changes:
  - target is moved to re-review queue
  - merge blocks until re-approved (or equivalent conservative policy)
- Behavior is documented + tested

---

## Issue 4.4 — Add scoped allow/override mechanism with required rationale
**Labels:** `priority/p2`, `type/feature`, `area/policy`  
**Milestone:** `v3.0`

**Acceptance criteria**
- Overrides are:
  - target-scoped
  - require justification + link
  - recorded in decision bundle

---

# EPIC 5 — Config profiles + portability

## Issue 5.1 — Add config profiles (`development` / `production`) with inheritance
**Labels:** `priority/p2`, `type/feature`, `area/config`  
**Milestone:** `v3.0`

**Acceptance criteria**
- `configs/profiles/base.yaml`, `development.yaml`, `production.yaml` exist
- Profile selection via env var (e.g. `DC_PROFILE`)
- YAML inheritance works reliably

---

## Issue 5.2 — Remove hardcoded paths in targets YAML via `${DATASET_ROOT}` templates
**Labels:** `priority/p2`, `type/refactor`, `area/config`  
**Milestone:** `v3.0`  
**Dependencies:** Issue 5.1 (or environment fallback)

**Acceptance criteria**
- Targets YAML uses templates instead of fixed absolute paths
- Works on Windows + Linux (path handling is robust)

---

# EPIC 6 — Developer experience gates (typing, pre-commit, standards)

## Issue 6.1 — Add strict-ish typing + mypy in CI (ratchet plan)
**Labels:** `priority/p2`, `type/chore`, `area/ci`  
**Milestone:** `v3.0`

**Acceptance criteria**
- Mypy runs in CI
- Either strict for new modules or a baseline + ratchet (no regressions)

---

## Issue 6.2 — Add pre-commit hooks (ruff/format/mypy/yamllint/schema validation)
**Labels:** `priority/p2`, `type/chore`, `area/ci`  
**Milestone:** `v3.0`

**Acceptance criteria**
- `.pre-commit-config.yaml` exists and works
- Schema validation hook runs on YAML changes

---

# EPIC 7 — Throughput & resiliency (async + resumability)

## Issue 7.1 — Add async HTTP download path (opt-in)
**Labels:** `priority/p3`, `type/feature`, `area/acquire`  
**Milestone:** `v3.0`

**Acceptance criteria**
- Async mode is optional and respects safety constraints
- Concurrency is bounded + retries/backoff implemented

---

## Issue 7.2 — Deterministic sharding + resumable stages
**Labels:** `priority/p2`, `type/feature`, `area/cli`  
**Milestone:** `v3.0`

**Acceptance criteria**
- Each stage can resume after interruption without corruption
- Shard naming is stable across runs

---

# EPIC 8 — Observability (optional but clean)

## Issue 8.1 — Structured JSON logging mode
**Labels:** `priority/p3`, `type/feature`, `area/observability`  
**Milestone:** `v3.0`

**Acceptance criteria**
- `--log-format json` (or profile setting) outputs structured logs
- Stage + domain + target_id fields present where relevant

---

## Issue 8.2 — Optional OpenTelemetry tracing
**Labels:** `priority/p3`, `type/feature`, `area/observability`  
**Milestone:** `v3.0`

**Acceptance criteria**
- Zero-cost if otel deps not installed
- Tracing enabled only when configured

---

## Issue 8.3 — Optional Prometheus metrics
**Labels:** `priority/p3`, `type/feature`, `area/observability`  
**Milestone:** `v3.0`

**Acceptance criteria**
- Metrics exposed only when enabled
- Basic counters for downloads, queues, stage durations

---

# EPIC 9 — Testing + docs that make it “A-Grade”

## Issue 9.1 — Full pipeline integration test fixture (classify → acquire stub → merge)
**Labels:** `priority/p2`, `type/feature`, `area/tests`  
**Milestone:** `v3.0`

**Acceptance criteria**
- CI runs at least one end-to-end integration test using fixtures
- Output contract validated end-to-end

---

## Issue 9.2 — Docs overhaul: one canonical way to run + add pipeline + policy semantics
**Labels:** `priority/p2`, `type/chore`, `area/docs`  
**Milestone:** `v3.0`

**Acceptance criteria**
- README covers:
  - install modes + extras
  - “run a pipeline”
  - “add a target”
  - “add a pipeline/domain handler”
  - policy: GREEN/YELLOW/RED + evidence-change behavior + denylist governance

---

## Good first issues (quick wins)
- Issue 0.2 — Fix `pmc_worker.py` escaped string
- Issue 0.4 — Remove import-time warning
- Issue 0.3 — Add `zstandard` to constraints
- Issue 6.2 — Add pre-commit config (once repo is green)
