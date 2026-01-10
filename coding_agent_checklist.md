# Dataset_Collector v2 — Coding Agent Checklist (Jan 2026)

> **Purpose:** This file is an actionable, prioritized checklist for a coding agent to implement the highest‑impact fixes and polish for the Dataset_Collector v2 repository.

---

## 0) Non‑negotiables (read first)

- [ ] **Do not weaken license safety / auditability.**
  - Keep **GREEN / YELLOW / RED** triage semantics intact.
  - Preserve **license evidence storage + hashing** and any “no AI training” restriction phrase scanning.
  - If refactoring, keep output schemas stable unless explicitly noted and migrated.

- [ ] **Stay cross‑platform:** CI runs on **Ubuntu + Windows** (Python 3.10/3.11). Avoid symlink‑only approaches unless there’s a Windows-safe fallback.

- [ ] **Definition of Done for every change**
  - `ruff check .` passes
  - `ruff format --check .` passes
  - `yamllint .` passes
  - `pytest` passes (and add tests for new/changed behavior)

---

## 1) Quick repo orientation

### Key components
- `collector_core/` — shared framework: config validation, acquire strategies, pipeline factory, merge, license safety, etc.
- `collector_core/pipeline_specs_registry.py` — acknowledges all pipelines via `PipelineSpec(...)`.
- `collector_core/dc_cli.py` — unified CLI (`dc`) that runs stages.
- `tools/preflight.py` — checks config, tools, strategy support, etc.
- `*_pipeline_v2/` — domain pipeline wrappers + targets YAML.

### Most sensitive areas (refactor carefully)
- `collector_core/license_*` and evidence hashing/change-detection
- `docs/output_contract.md` + anything writing JSONL records
- Anything that changes license classification gates or allow/deny behavior

---

## 2) P0 — Must fix (highest priority)

### 2.1 Replace hardcoded yellow-screen dispatch in `dc_cli.py` with spec-driven dispatch
**Problem:** `collector_core/dc_cli.py` hardcodes an `if/elif` chain to call `yellow_screen_chem/econ/kg_nav/nlp/safety`, but a unified dispatcher already exists (`collector_core/yellow_screen_dispatch.py`) and should be the single source of truth.

- [ ] Update `collector_core/dc_cli.py`
  - [ ] Remove direct imports of domain yellow screen modules (`yellow_screen_chem`, etc.) if no longer needed.
  - [ ] Replace the `if module == "chem": ...` chain with a call into `collector_core.yellow_screen_dispatch`.
    - Prefer a clean API like: `from collector_core.yellow_screen_dispatch import get_yellow_screen_main`
    - Then: `main_fn = get_yellow_screen_main(slug)` and `_run_with_args(main_fn, args)` (or equivalent)
  - [ ] Ensure behavior stays the same:
    - Still respects `PipelineSpec.yellow_screen_module` when set
    - Defaults to `yellow_screen_standard` when unset
    - Keeps current CLI args/flags and exit codes

**Acceptance checks**
- [ ] Add/update tests:
  - [ ] New test ensuring `dc run ... yellow_screen` selects the correct module based on `PipelineSpec.yellow_screen_module`.
  - [ ] New test ensuring an unconfigured pipeline falls back to `yellow_screen_standard`.
- [ ] `pytest -k yellow_screen_dispatch` passes.

---

### 2.2 Wire rate limiting config into acquisition (GitHub + Figshare first)
**Problem:** Several `targets_*.yaml` files define resolver `rate_limit:` blocks (e.g., GitHub, Figshare), but acquisition code does not consume them. There is a `collector_core/rate_limit.py` implementation and tests, but it is not integrated.

#### Implement consumption path
- [ ] Update `collector_core/rate_limit.py`
  - [ ] Extend `RateLimiterConfig.from_dict()` to support friendly YAML keys currently used in targets files:
    - `requests_per_minute`
    - `requests_per_hour`
    - `requests_per_second` (optional future-proofing)
    - Optional `burst` (or map to `capacity`)
  - [ ] Keep existing keys working (`capacity`, `refill_rate`, `initial_tokens`).

- [ ] Update `collector_core/acquire_strategies.py`
  - [ ] Add a helper to fetch a limiter config from `ctx.cfg`, e.g.:
    - `cfg["resolvers"]["github"]["rate_limit"]`
    - `cfg["resolvers"]["figshare"]["rate_limit"]`
  - [ ] Acquire a token **before** each relevant API request:
    - GitHub API: release metadata requests (and/or asset listing)
    - Figshare API: article/files metadata requests
  - [ ] Respect retry policy in the YAML block when possible:
    - `retry_on_429` → retry on HTTP 429
    - `retry_on_403` → retry on rate-limit acknowledges (GitHub often uses 403 for rate-limits)
    - `exponential_backoff` → if present/true, backoff should grow (cap the max)

> If you need to adjust retry behavior, prefer updating `collector_core/network_utils.py` to accept a custom retry predicate/status-code allowlist rather than sprinkling retry loops per handler.

#### Documentation and schema drift
- [ ] Update `README.md` to remove/adjust the note that rate limiting is “not implemented” once it is actually wired.

**Acceptance checks**
- [ ] Add tests that prove the limiter is being used:
  - [ ] A unit test with a fake clock/sleep (RateLimiter already supports injection) that asserts acquisition blocks when limit is exceeded.
  - [ ] A test that simulates a 429/403 and ensures the retry logic is applied when configured.
- [ ] CI remains stable on Windows.

---

### 2.3 Deduplicate the large `yellow_scrubber.py` implementations (chem/materials/regcomp)
**Problem:** Three pipelines contain large, highly similar scrubbers with duplicated utilities and logic:
- `chem_pipeline_v2/yellow_scrubber.py`
- `materials_science_pipeline_v2/yellow_scrubber.py`
- `regcomp_pipeline_v2/yellow_scrubber.py`

- [ ] Create a shared implementation under `collector_core/`, e.g.:
  - `collector_core/yellow_scrubber_pubchem_pmc.py` (name is flexible)
  - Include shared helpers (JSONL read/write, ensure_dir, UTC time, etc.) and core logic.
- [ ] Convert each pipeline’s `yellow_scrubber.py` into a thin wrapper:
  - [ ] Define pipeline-specific constants only (user-agent prefix, default roots, targets help string).
  - [ ] Delegate to the shared core `main(...)` / `run(...)` function.

**Do not break compatibility**
- Keep CLI args stable (flags, defaults) unless you also update docs/tests and provide migration notes.
- Preserve output locations and run report format unless explicitly improved with backward compatibility.

**Acceptance checks**
- [ ] Update `tests/test_yellow_scrubber.py` (currently loads the regcomp scrubber module directly).
- [ ] Ensure all scrubber-related tests still pass.

---

## 3) P1 — Should fix (high value, not blocking)

### 3.1 Add missing tests for domain yellow screen modules
Currently there are limited tests for yellow screen behavior beyond helpers.

- [ ] Add tests for:
  - `collector_core/yellow_screen_chem.py`
  - `collector_core/yellow_screen_econ.py`
  - `collector_core/yellow_screen_kg_nav.py`
  - `collector_core/yellow_screen_nlp.py`
  - `collector_core/yellow_screen_safety.py`
- [ ] Keep tests fast:
  - Use tiny fixtures and tmp dirs
  - Avoid real network calls (mock requests / use local fixtures)

**Acceptance checks**
- [ ] Coverage increases for yellow screen modules without slowing CI unreasonably.

---

### 3.2 Validate `rate_limit` blocks in targets schema
Rate limit blocks exist in YAML but `schemas/targets.schema.json` does not validate them.

- [ ] Extend `schemas/targets.schema.json` to allow:
  - `resolvers.<resolver>.rate_limit` with keys:
    - `requests_per_minute`, `requests_per_hour`, `requests_per_second`
    - `burst`, `retry_on_429`, `retry_on_403`, `exponential_backoff`
- [ ] Add a schema validation test that ensures the current targets files validate cleanly.

---

## 4) P2 — Nice-to-have refactors (optional polish)

Acknowledged as valuable but not required for a “merge-ready” PR.

### 4.1 Reduce wrapper file duplication across pipelines
Many pipeline directories contain identical thin wrappers (workers, catalog builder, merge worker, etc.).

- [ ] Option A (recommended): Improve/standardize `tools/generate_pipeline.py` usage so wrappers are generated from a template and not manually edited.
- [ ] Option B: Move to a single canonical implementation referenced by minimal per-pipeline code (avoid symlink-only solutions unless Windows-safe).

**Acceptance checks**
- [ ] No runtime behavior changes; purely reduces drift.

---

### 4.2 Stronger typing for core data shapes
- [ ] Introduce TypedDict/dataclasses for:
  - Target config objects
  - Routing dict(s)
  - Output record/row extras
- [ ] Apply these types to the most frequently touched paths:
  - pipeline driver classification
  - merge/dedupe
  - yellow screen inputs/outputs

---

### 4.3 Docs polish
- [ ] Add/expand API docs for `collector_core` modules that are “public” surfaces.
- [ ] Expand `CHANGELOG.md` for key behavior changes (especially rate limiting and dispatch changes).

---

## 5) Suggested execution plan (agent-friendly)

1. [ ] Fix yellow screen dispatch in `dc_cli.py` + add tests.
2. [ ] Implement rate limit config parsing + integrate into GitHub/Figshare acquisition + tests.
3. [ ] Refactor scrubbers into shared core + update tests.
4. [ ] Add missing yellow screen tests.
5. [ ] Update targets schema for `rate_limit` blocks + tests.
6. [ ] Optional refactors (wrapper generation, typing, docs).

---

## 6) Commands you’ll run a lot

```bash
# install dev deps
python -m pip install -e ".[dev]"

# lint + format
ruff check .
ruff format --check .

# yaml lint
yamllint .

# tests
pytest
```

---

## 7) Notes for reviewers / PR description template

- Summary of behavior changes (dispatch, rate limiting).
- Risk assessment (rate limiting changes API call behavior; scrubber refactor should be “no behavior change”).
- Evidence: test output, coverage deltas, and (if possible) a small before/after diff of scrubber output.

