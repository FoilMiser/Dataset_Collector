# Dataset Collector Repo — Issues & Fixes Punchlist (v2.0.0)

This is a repo-wide, **actionable** issue tracker based on the current contents of `Dataset_Collector-main` (zip review).
Each item includes: **impact**, **where**, **fix**, and **done when** acceptance criteria.

---

## P0 — Breaks installs / produces false confidence / breaks real runs

### 1) `pyproject.toml` is not a valid installable project (missing metadata + build-system)
**Impact:** `pip install -e .` / packaging workflows can fail or behave inconsistently. Console scripts in `[project.scripts]` won't be reliably usable until the project metadata is complete.

**Where:**
- `pyproject.toml` (`[project]` only has `dynamic` + `scripts`; no `name`)
- `pyproject.toml` is also missing a `[build-system]` section

**Fix:**
- Add minimal PEP 621 metadata:
  - `name`, `requires-python`, `readme`, `license` (or `license-files`), `authors` (optional), `description` (optional)
- Add `[build-system]`:
  - `requires = ["setuptools>=68", "wheel"]`
  - `build-backend = "setuptools.build_meta"`

**Done when:**
- `pip install -e .` succeeds on Windows + Ubuntu
- `dc-review --help` and `dc-catalog --help` run after install

---

### 2) YAML schema validation can “succeed” even if schemas are not actually being applied
**Impact:** `tools/validate_yaml_schemas.py` can report success even when `jsonschema` is missing, because `collector_core.config_validator.validate_config()` no-ops without `Draft7Validator`. This creates **false confidence** in CI and local checks.

**Where:**
- `collector_core/config_validator.py` (returns early if `jsonschema` import fails)
- `tools/validate_yaml_schemas.py` (calls `read_yaml(..., schema_name=...)` and treats that as validation)

**Fix (recommended):**
- In `tools/validate_yaml_schemas.py`, explicitly fail early if schema validation is unavailable, e.g.:
  - check `collector_core.config_validator.Draft7Validator is None` → print “Install jsonschema” → return nonzero
- Alternatively: make `jsonschema` a hard dependency in every pipeline install path (see item #3/#7).

**Done when:**
- Uninstalling `jsonschema` causes `python -m tools.validate_yaml_schemas --root .` to fail with a clear message
- CI can’t silently pass schema checks without schema validation

---

### 3) Pipeline installs from README are incomplete (deps marked “optional” but pipeline stages require them)
**Impact:** The README suggests installing per-pipeline `requirements.txt`, but many of those files comment out `datasets`/`pyarrow`/`jsonschema` as optional. The orchestrator’s stage plan includes `merge` + `catalog`, and core modules import `datasets` at import time, so installs can crash unexpectedly during normal runs.

**Where:**
- Per-pipeline requirements like:
  - `3d_modeling_pipeline_v2/requirements.txt` (and others)
- Core imports:
  - `collector_core/merge.py` (imports `datasets`)
  - `collector_core/yellow_screen_*.py` (imports `datasets`)
- Orchestrator stages:
  - `tools/build_natural_corpus.py` `STAGES = ["classify","acquire_green","acquire_yellow","screen_yellow","merge","catalog"]`

**Fix (choose one policy and make it consistent):**
1) **Make these deps required for normal pipeline runs (simplest):**
   - Ensure every pipeline `requirements.txt` includes:
     - `datasets`, `pyarrow`, `jsonschema`, `pyyaml`, `requests`
   - Update README to state “install base + pipeline extras”.
2) **Make them truly optional (more work):**
   - Lazy-import `datasets` only inside HF-specific codepaths and raise a clear error only when needed.
   - Allow a “no-HF merge” path if desired.

**Done when:**
- Following README instructions for a single pipeline results in a successful run through `merge` + `catalog`
- No `ModuleNotFoundError: datasets` / `pyarrow` surprises mid-run

---

### 4) Default `tools/pipeline_map.sample.yaml` is a placeholder but also the implicit default
**Impact:** New users run the orchestrator and immediately get confusing behavior because `destination_root` is `"YOUR_DATASET_ROOT_HERE"`.

**Where:**
- `tools/pipeline_map.sample.yaml`
- `tools/build_natural_corpus.py` uses pipeline maps by default

**Fix options:**
- Rename checked-in placeholder to `pipeline_map.sample.yaml` and require `--pipeline-map` or `--dest-root`
- Or set a safe default like `./_data` (gitignored) and keep `--dest-root` override
- Or hard-fail unless `destination_root` has been changed from the placeholder

**Done when:**
- Running `python tools/build_natural_corpus.py --pipelines all` either works out-of-the-box or fails with a clear “set destination_root” message

---

## P1 — CI fragility, noisy checks, version drift, maintainability risks

### 5) CI Codecov upload can fail PRs (especially forks) unnecessarily
**Impact:** `fail_ci_if_error: true` combined with `token: ${{ secrets.CODECOV_TOKEN }}` can cause CI failures when the token isn’t available (common on forked PRs).

**Where:**
- `.github/workflows/ci.yml` (Codecov action step)

**Fix:**
- Gate upload on token presence and/or on event type:
  - Only require Codecov on `push` to main; do not fail PRs without token
  - Or set `fail_ci_if_error: false` for PR contexts

**Done when:**
- Fork PRs pass CI even without Codecov token, while main branch still uploads coverage

---

### 6) Preflight produces lots of warnings for intentionally-disabled targets
**Impact:** CI logs become noisy and important preflight issues can be missed. Disabled targets that are “planned”/placeholder generate warnings by default.

**Where:**
- `tools/preflight.py` emits warnings for disabled targets unless `--quiet`
- `.github/workflows/ci.yml` runs `python -m tools.preflight --repo-root .` (no `--quiet`)

**Fix (recommended):**
- In CI: run `python -m tools.preflight --repo-root . --quiet`
- Optionally: change preflight defaults so disabled-target warnings require `--warn-disabled` (or similar)
- For placeholder disabled targets, set `download.strategy: none` (or unify placeholders) to suppress warnings.

**Done when:**
- CI preflight output is short and highlights only actionable issues for enabled targets

---

### 7) Dependency drift between root constraints and per-pipeline requirements
**Impact:** Hard-to-debug “works on my machine” issues: per-pipeline requirements pin different floors (e.g. `requests>=2.31.0`) vs root (`requests>=2.32.2,<2.33`), and pipelines vary in whether they include schema/merge deps.

**Where:**
- `requirements.in`, `requirements.constraints.txt`
- `*_pipeline_v2/requirements.txt`

**Fix (recommended structure):**
- Make a single **base install** path for all pipelines:
  - Install `-r requirements.constraints.txt` (or a real lock, see #12)
  - Then install pipeline extras file that contains ONLY additional packages
- Alternatively: include the base constraints file from each pipeline requirements using `-r ../requirements.constraints.txt`

**Done when:**
- Any pipeline can be installed using a consistent “base + extras” approach
- No pipeline depends on subtly different versions of core deps

---

### 8) Orchestrator logs are overwritten across pipelines/stages
**Impact:** Logs for earlier pipelines get overwritten, making debugging multi-pipeline runs painful.

**Where:**
- `tools/build_natural_corpus.py` uses: `orchestrator_{stage}.log`

**Fix:**
- Include pipeline name (and ideally a timestamp) in the log filename:
  - `orchestrator_{pipeline}_{stage}_{YYYYMMDD_HHMMSS}.log`

**Done when:**
- Multi-pipeline runs produce separate logs per pipeline per stage

---

### 9) Version labeling drift (`__version__ = 2.0.0` vs `__schema_version__ = 0.9` and hardcoded `VERSION = "0.9"`)
**Impact:** Artifacts and logs can look like the system is “v0.9” even though the repo is v2.0.0; confusing for auditability and debugging.

**Where:**
- `collector_core/__version__.py` (`__version__ = "2.0.0"`, `__schema_version__ = "0.9"`)
- Hardcoded:
  - `collector_core/review_queue.py` (`VERSION = "0.9"`)
  - `collector_core/pmc_worker.py` (`VERSION = "0.9"`)
- Pipeline scrubbers:
  - many `*_pipeline_v2/yellow_scrubber.py` import `__schema_version__ as VERSION`

**Fix:**
- Standardize naming:
  - Use `__version__` everywhere for tool version display
  - Use `__schema_version__` only as `SCHEMA_VERSION`
- Remove hardcoded `"0.9"` constants; import from `collector_core.__version__`
- Update scrubbers to display tool version + schema version separately if needed

**Done when:**
- Running any stage prints consistent tool version
- Artifacts clearly distinguish tool version vs schema version

---

### 10) `tools/validate_repo.py` hardcodes the number of pipelines (18)
**Impact:** Adding/removing a pipeline breaks validation unless you remember to update this script.

**Where:**
- `tools/validate_repo.py` expects exactly 18 `pipeline_driver.py`

**Fix:**
- Derive expected pipelines from `tools/pipeline_map.sample.yaml` (or from directory glob + allow any count)
- If you want strictness, validate that every pipeline in the map exists and has required files, rather than enforcing a fixed count.

**Done when:**
- Adding a new pipeline only requires adding it to the map + directory; validation adapts automatically

---

### 11) Enabled targets that require external tools (AWS CLI) are not “default runnable”
**Impact:** Running pipelines via the notebook/orchestrator can fail unexpectedly on machines without AWS CLI.

**Where:**
- `tools/preflight.py` checks for external tools via `tools/strategy_registry.py` requirements
- `kg_nav_pipeline_v2` includes enabled targets with S3 strategies

**Fix:**
- Decide policy:
  - **Option A:** Disable tool-heavy targets by default (enable once prerequisites are installed)
  - **Option B:** Treat AWS CLI as a documented prerequisite and keep them enabled
- Update README + preflight messaging accordingly

**Done when:**
- “Default” run path succeeds without surprise missing-tool failures, or fails early with clear prerequisite instructions

---

## P2 — Quality polish and long-term maintainability

### 12) “Constraints” are not a true lockfile (transitives unpinned)
**Impact:** Reproducibility across machines/over time is weaker than the naming implies.

**Where:**
- `requirements.constraints.txt` pins top-level deps but not transitive deps

**Fix:**
- Either rename CI mode/terminology from “lock” → “constraints”
- Or adopt a true lock approach (fully pinned transitive set; optionally hashes)

**Done when:**
- Either naming matches reality, or installs are fully reproducible across time

---

### 13) Strategy registry does not distinguish supported vs planned strategies
**Impact:** Placeholder strategies and future plans show up in preflight outputs and create confusion.

**Where:**
- `tools/strategy_registry.py`
- `tools/preflight.py`

**Fix:**
- Add status metadata per strategy: `supported | planned | deprecated`
- Preflight:
  - Error on enabled targets using `planned/deprecated`
  - Quietly ignore disabled targets unless `--warn-disabled`

**Done when:**
- Preflight output communicates roadmap cleanly without spamming warnings

---

### 14) Repeated per-pipeline worker code invites drift (especially acquire workers)
**Impact:** Bugfixes or improvements to strategy handling require editing many files; drift becomes likely.

**Where:**
- Many `*_pipeline_v2/acquire_worker.py` share similar handler patterns

**Fix (incremental refactor):**
- Move shared strategy handlers into `collector_core/acquire_strategies.py` (or similar)
- Make each pipeline’s `acquire_worker.py` a thin wrapper adding only pipeline-specific behavior

**Done when:**
- Adding a new strategy requires changing core code in one place, not 18 pipeline workers

---

### 15) Documentation/headers minor drift (confusing but easy to polish)
**Impact:** Small inconsistencies reduce trust (e.g., copied headers, lingering v0.9 mentions).

**Where:**
- Some pipeline `requirements.txt` / docs text

**Fix:**
- Normalize headers: pipeline name + v2.0.0
- Ensure README accurately describes the “base + extras” install flow (if adopted)

**Done when:**
- Docs match current repo versioning and install reality

---

## Suggested execution plan

1) Fix P0 items **#1–#4** first (they drive user success + prevent false-positive checks).
2) Fix CI+UX robustness items **#5–#11** next.
3) Take on maintainability polish **#12–#15** as a cleanup/refactor pass.

---

## Quick links to key files

- Orchestrator: `tools/build_natural_corpus.py`
- Preflight: `tools/preflight.py`
- Repo validation: `tools/validate_repo.py`
- Schema validation: `tools/validate_yaml_schemas.py`
- Schemas: `schemas/*.schema.json`
- Project metadata: `pyproject.toml`
