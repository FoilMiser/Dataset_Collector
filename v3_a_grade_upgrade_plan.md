# Dataset Collector v3 “A-Grade” Upgrade Plan (Coding Agent Checklist)

> **Goal:** Ship a v3 refactor that makes the repo **clean, consistent, testable, and auditably “ethical-by-default”** with minimal new abstraction.
>
> **North Star:** **One canonical engine (`collector_core`) + one canonical interface (`dc`) + spec/config drives everything.**
> Everything else is either:
> - **Data** (targets/specs/policy YAMLs)
> - **Small domain overrides** (only where behavior truly differs)
> - **Compatibility shims** (temporary, clearly deprecated)

---

## 0) Guiding rules (avoid overengineering)

- **Prefer consolidation + file organization** over new architecture layers.
- **Avoid** plugin registries, DI frameworks, and async refactors unless strictly necessary.
- Keep dispatch simple (dict/maps) when it’s already clear.
- Prefer stable CLI contracts + stable artifact formats.
- Allow breaking changes **only** if they delete a lot of complexity, and provide a clear migration path.

---

## 1) Make `dc` the single blessed entrypoint (remove wrapper sprawl)

### Problems to solve
- Stage naming drift: `screen_yellow` vs `yellow_screen`
- Two orchestration paths (legacy scripts + `dc`)
- Many wrapper files in `*_pipeline_v2/` that mostly forward to generic logic

### v3 tasks
1. **Canonical stage enum / constants**
   - Choose: `classify`, `acquire`, `yellow_screen`, `merge`, `catalog`
   - Provide **aliases**:
     - `screen_yellow` → `yellow_screen` (warn + deprecate)
   - Update docs, CLI help text, artifact names, and any internal routing.

2. **Make `dc` the “one true interface”**
   - Refactor any meta-orchestrators (e.g., corpus builder scripts) to call:
     - `dc run --pipeline X --stage Y`
     - (or `dc run --pipeline X --all`)
   - Remove calls like: `python <pipeline_dir>/pipeline_driver.py ...` where possible.

3. **Kill pipeline wrappers**
   - Delete (or drastically shrink) wrapper scripts in `*_pipeline_v2/`.
   - Replace with **pipeline specs** (YAML or a small python map) defining:
     - pipeline id
     - default targets path(s)
     - default pools / output names
     - domain name for yellow screen routing
     - any pipeline-specific knobs (limits, allowlists, etc.)
   - Keep only *true* per-domain code in `collector_core/yellow/domains/*`.

### Acceptance criteria
- You can run the entire pipeline via `dc` with no wrapper scripts.
- Stage names are consistent everywhere; alias works with a deprecation warning.
- Docs reference only the canonical CLI path.

---

## 2) Collapse Yellow Screen duplication into a base + tiny overrides (biggest win)

### v3 structure
Create:

- `collector_core/yellow/base.py`
  - Shared orchestration:
    - load queue items
    - streaming read
    - dedupe hooks
    - shard writing (incl. compression)
    - sidecar metadata writing
    - metrics/logging
    - done markers

- `collector_core/yellow/domains/<domain>.py`
  - Domain override functions only:
    - `filter_record(record, target) -> FilterResult`
    - optional `transform_record(record) -> record`
    - optional `domain_preflight(target)`

### Standardize outputs (across all domains)
- `screened_yellow/<pool>/shards/*.jsonl.zst` (or your chosen canonical compression)
- `screened_yellow/<pool>/shards/*.meta.json`
- Canonical done marker: `yellow_screen_done.json` (single name)

### Acceptance criteria
- ~70–85% duplicate yellow-screen code removed.
- All yellow screeners share identical CLI flags and artifact conventions.
- New domains can be added by writing only the domain override file + spec entry.

---

## 3) Split `acquire_strategies.py` by strategy (organization-only refactor)

### v3 layout
```
collector_core/acquire/
  __init__.py      # stable API re-exports
  context.py       # AcquireContext, root management, shared helpers
  http.py
  hf.py
  s3.py
  git.py
  zenodo.py        # if used
  utils.py         # retry/checksum helpers
```

### Tasks
- Move each strategy function into its own module.
- Keep strategy dispatch as a dict/map (no plugin system).
- Either:
  - Keep `collector_core/acquire_strategies.py` as a compatibility shim, or
  - Remove it in v3 as a breaking change.

### Acceptance criteria
- No functional changes/regressions.
- Each module is readable in isolation.
- Strategy dispatch remains explicit and testable.

---

## 4) Consolidate utilities (stop re-defining helpers everywhere)

### Create shared utilities
- `collector_core/utils/hash.py`
  - `sha256_text`, `sha256_file`, `stable_json_hash`
- `collector_core/utils/io.py`
  - JSON/YAML/JSONL read/write, atomic writes
- `collector_core/utils/paths.py`
  - safe join, ensure-under-root checks, extraction safety helpers
- `collector_core/utils/http.py`
  - requests session factory, UA, timeout defaults, retry/backoff
- `collector_core/utils/logging.py`
  - structured logging helpers, run_id context, consistent formatting

### Tasks
- Remove duplicated implementations from multiple modules.
- Import from shared utilities everywhere.

### Acceptance criteria
- No utility function exists in >1 place (except deprecated shims).
- Imports are consistent and obvious.

---

## 5) Fix packaging + tests so CI is trustworthy (A-Grade gating)

### Problems to solve
- Tests import pipeline dirs as if they’re packages (fragile / often broken)
- Some tests import `tests.*` without `tests` being a package
- Core is testable but tests aren’t consistently aimed at core units

### v3 approach
1. **Unit tests target `collector_core`**
   - license parsing / SPDX inference
   - evidence snapshotting + change detection
   - queue writing
   - dedupe worker
   - acquire strategy small tests with mocked HTTP/files

2. **Integration tests invoke `dc` via subprocess**
   - “mini pipeline” with tiny fixtures
   - assert artifacts exist + schema valid + deterministic counts
   - run in CI quickly

3. **pytest fixtures**
   - Use `tests/conftest.py`
   - Avoid `import tests.fixtures` patterns unless `tests/` is a package on purpose

### Acceptance criteria
- `pytest` passes in a clean env.
- CI runs lint + unit + small integration tests.
- Tests do not rely on pipeline directories being importable.

---

## 6) License detection improvements (accuracy + explainability)

### Tasks
1. **Context-aware SPDX heuristics**
   - Demote ambiguous short needles (`MIT`, `BSD`) unless near “license/licensed under”
   - Prefer structured license fields (HF metadata, SPDX IDs) when available

2. **Explainable classification signals**
   - Every classification decision emits:
     - `signals: [{type, match, source_url, snippet_hash, confidence}]`
   - Ensure operator can answer “why is this yellow/red?” quickly.

3. **Automation-safe flags**
   - `--fail-on-review-required`
   - `--fail-on-evidence-change`
   - Use these for scheduled runs so nothing risky proceeds silently.

### Acceptance criteria
- Fewer spurious yellows caused by token collisions (“MIT” in institute names).
- Every bucket decision is human-explainable from emitted artifacts.
- Automation can enforce strict safety without custom scripting.

---

## 7) Artifact + naming cleanup (remove operator confusion)

### Tasks
- Unify stage naming:
  - `yellow_screen` everywhere (code + docs + artifact names)
- Standardize run directory structure:
  - `runs/<run_id>/`
    - `classify/`
    - `evidence/`
    - `acquire/`
    - `yellow_screen/`
    - `merge/`
    - `catalog/`
- Each stage must write:
  - `<stage>_done.json` including:
    - counts
    - checksums / hashes
    - version
    - policy snapshot hash
    - elapsed time
    - warnings

### Acceptance criteria
- A single `runs/<run_id>` folder is sufficient to audit and reproduce what happened.
- Artifact names are stable across pipelines/domains.

---

## 8) Security + safety hardening (defensive-by-default)

### Tasks
- Archive extraction safety (zip/tar):
  - prevent path traversal
  - enforce max total extracted size
  - ensure all extracted paths are under allowed root
- Network safety defaults:
  - timeouts everywhere
  - bounded retries with backoff
  - stable user-agent
- Checksums:
  - if target supplies checksum, verify it
  - store verification results in manifests

### Acceptance criteria
- Crafted archives cannot escape data root or explode storage usage.
- Downloads are reproducible and verifiable when checksums exist.

---

## 9) Documentation + “happy path” UX polish

### Docs to ship for v3
- `README.md`: 10-minute quickstart
- `docs/architecture.md`: lifecycle, artifacts, policy snapshot
- `docs/review.md`: yellow queue review, approve/reject, rerun
- `docs/policy.md`: license_map / denylist / evidence rules + safety flags

### CLI UX improvements (if not already present)
- `dc pipelines list`
- `dc targets validate <file>`
- `dc run --pipeline X --all`  
  - runs stages in order  
  - stops on review-required unless explicitly overridden
- `dc review list/show/approve/reject`  
  - approval may simply write a signed YAML/JSON entry initially

### Acceptance criteria
- A new contributor can follow the quickstart and run a tiny pipeline.
- Review workflow is documented and standardized.

---

## 10) Recommended v3 execution order (PR-by-PR)

> Implement in this order to maximize deletions early and reduce merge conflicts.

1. **Stage naming unification + alias + docs updates**
2. **Yellow screen base + domain overrides**
3. **Delete pipeline wrappers; wire specs into `dc`**
4. **Split acquire strategies into modules**
5. **Utility consolidation (`utils/*`)**
6. **Tests + CI refactor (unit + integration)**
7. **License heuristic improvements + explainable signals + strict flags**
8. **Security hardening (archive safety, checksums)**
9. **Docs + UX polish**

---

## 11) Migration notes (compat + deprecation)

- Keep compatibility shims only where they reduce break pain:
  - `screen_yellow` alias to `yellow_screen`
  - `acquire_strategies.py` shim to new modules
- All shims must:
  - log a clear deprecation warning
  - have a target removal version (e.g., v4)

---

## 12) Definition of Done (A-Grade checklist)

- [ ] `dc` is the primary interface; wrappers removed or purely compat.
- [ ] Stage names are consistent repo-wide.
- [ ] Yellow screen duplication eliminated; only domain overrides remain.
- [ ] Acquire strategies modularized.
- [ ] Shared utilities centralized; no duplicates.
- [ ] Unit tests cover core policy logic (license/evidence/queue).
- [ ] Integration test runs a tiny pipeline through `dc`.
- [ ] CI passes reliably on clean install.
- [ ] Artifacts are consistent and audit-ready.
- [ ] Security hardening added for archive extraction + network defaults.
- [ ] Docs provide a clean quickstart + review workflow.
