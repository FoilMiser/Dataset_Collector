# Dataset_Collector v2 — Repo-wide Fix + Update Plan (Thorough)

**Repo reviewed:** `Dataset_Collector-main (6).zip`  
**Goal:** make every pipeline **reliably runnable from JupyterLab on Windows**, while keeping the repo **license-auditable** (evidence snapshots, denylist enforcement, review signoff, promotion to GREEN).

---

## Executive summary (what’s “blocking” vs “polish”)

### Blocking (must fix first)
1. **License map schema drift across pipelines** is currently causing **silent downgrade to YELLOW** (even for allowlisted SPDX like `CC0-1.0`) in multiple pipelines.
2. **Several enabled targets are incomplete** (missing download URLs / repo fields / evidence URL), so those pipelines will error or stall as soon as you actually execute acquisition.
3. **License profile enums in targets don’t match `license_map.yaml` profiles**, and there’s no hard validation; results are silently conservative but confusing (and can wreck automation quality).
4. **All pipeline drivers still print “CHEMISTRY CORPUS PIPELINE — DRY-RUN…”** in reports (copy/paste artifact), which makes logs misleading and hard to audit.

### High-value maintainability (next)
5. Core logic is duplicated across many pipelines → drift is inevitable. Consolidate into a shared module or generate pipeline files from one template.
6. Add a repo-level validator (CI + preflight) that flags broken targets **before** a run.

---

## 1) Repo-wide fixes (apply to every pipeline)

### 1.1 Standardize `license_map.yaml` schema across all pipelines
Right now there are **two different shapes** in the wild:

- **Newer shape** (good):  
  `spdx`, `normalization`, `restriction_scan`, `gating`, `profiles`, `updated_utc`
- **Legacy shape** (problematic):  
  `spdx`, `non_spdx`, `license_profiles` (and sometimes missing `gating` / `restriction_scan` / `normalization`)

**Pipelines confirmed needing schema updates:**
- `math_pipeline_v2/license_map.yaml` (uses `license_profiles`, `non_spdx`, missing the newer structure)
- `physics_pipeline_v2/license_map.yaml` (same)
- `earth_pipeline_v2/license_map.yaml` (uses `license_profiles`; otherwise closer)
- `code_pipeline_v2/license_map.yaml` (schema v0.3 but **missing** `profiles` + `gating`)

✅ **Update requirement:** bring *all* pipelines to one consistent schema (recommend `schema_version: 0.3`), with these top-level keys:

```yaml
schema_version: "0.3"
updated_utc: "YYYY-MM-DD"
spdx:
  allow: [...]
  conditional: [...]
  deny_prefixes: [...]
profiles:
  permissive: { default_bucket: GREEN, ... }
  record_level: { default_bucket: YELLOW, ... }
  copyleft: { default_bucket: YELLOW|RED, ... }
  unknown: { default_bucket: YELLOW, ... }
  deny: { default_bucket: RED, ... }
gating:
  restriction_phrase_bucket: "YELLOW"
restriction_scan:
  phrases: [...]
normalization:
  rules: [...]
```

**What to do with `non_spdx`:**
- Either:
  - convert `non_spdx.allow/conditional/deny` into `normalization.rules` that map those strings into SPDX, or
  - keep a `non_spdx` section but update the driver to use it (see 1.2).

---

### 1.2 Make `pipeline_driver.py` backwards-compatible (for safety)
Even after you standardize YAML, you should support legacy keys so you don’t regress old pipelines.

**In every pipeline’s `load_license_map()` add:**
- `profiles = m.get("profiles", {}) or m.get("license_profiles", {})`
- `gating = m.get("gating", {}) or {}`
- `restriction_scan = m.get("restriction_scan", {}) or {}`
- `normalization = m.get("normalization", {}) or {}`
- optionally support `non_spdx` if you keep it

**Also add an explicit warning when a target uses a profile not defined in the map:**
- Example: target has `license_profile: public_domain` but map only defines `permissive/record_level/...`

Recommended behavior:
- Print a warning + include it in `queues/run_summary.json`
- Default bucket remains conservative (`YELLOW`) but it’s no longer silent.

---

### 1.3 Fix the report header / pipeline identity (repo-wide copy/paste bug)
Multiple pipeline drivers still emit:

- `"CHEMISTRY CORPUS PIPELINE — DRY-RUN SUMMARY REPORT"`

✅ Change to either:
- A generic string:
  - `DATASET COLLECTOR v2 — DRY-RUN SUMMARY REPORT`
- Or pipeline-specific:
  - `MATH PIPELINE v2 — DRY-RUN SUMMARY REPORT`

Also update:
- the top docstring that still references “chemistry” behavior in multiple pipelines
- any “chem” defaults in routing fallbacks (if present)

---

### 1.4 Add a repo-level validator (run before any stage)
Create: `tools/validate_repo.py`

It should validate **all enabled targets** across all pipelines:

- `license_evidence.url` exists
- download config satisfies its `strategy` requirements:
  - `http` → `download.url` or `download.urls[0]`
  - `ftp` → `download.url` or `download.urls[0]`
  - `git` → `download.repo` (or `download.url`)
  - `huggingface_datasets` → `download.dataset_id`
  - `zenodo` / `dataverse` → required IDs/URLs present
- `license_profile` exists in `license_map.yaml` profiles
- `review_required: true` implies `review_notes` exists
- `targets.schema_version` matches expected (currently `0.8`)

Output:
- a single markdown/JSON report with:
  - `ERROR` (blocks execution)
  - `WARN` (allowed but needs attention)

Then:
- call it from the notebook **before** `build_natural_corpus.py`
- optionally add a CI job to run it on every commit

---

### 1.5 Remove placeholder compliance links in denylist
These appear in multiple pipelines (e.g., math/earth) and include `https://noai.example/policy`.

✅ Replace with either:
- an empty string + a clear rationale in-line, or
- a real link to a real policy page, or
- a repo doc you control, e.g. `docs/denylist_rationale.md`

---

### 1.6 JupyterLab notebook cleanup (reduce confusion)
File: `dataset_collector_run_all_pipelines.ipynb`

Issues:
- It contains **duplicate runner cells** and **WSL/Git Bash references** that conflict with the “Windows-first runner” path.
- This increases the risk of running the wrong path or believing bash is required.

✅ Update notebook to:
- keep **one** canonical “Windows-first runner” cell using:
  - `tools/build_natural_corpus.py`
- move WSL/Git Bash runner to an optional appendix section, or remove it
- add a single, clear “Set DEST_ROOT” instruction at the top

---

### 1.7 Consolidate shared code (prevents drift)
Right now each pipeline has its own:
- `pipeline_driver.py`
- (often very similar) parsing, gating, evidence snapshotting, bucketing logic

✅ Recommended refactor options:

**Option A (best): shared package**
- Add `collector_core/` with modules:
  - `license_map.py` (parse + normalize + profile logic)
  - `evidence.py` (fetch/snapshot/hash)
  - `denylist.py` (pattern matching)
  - `routing.py` (resolve routing fields)
  - `io.py` (jsonl read/write)
  - `report.py` (dry-run report formatting)
- Then each pipeline driver becomes a thin wrapper with only pipeline defaults.

**Option B: generate drivers from a template**
- Keep one canonical `pipeline_driver_template.py`
- A generator writes the per-pipeline file with a few constants swapped

Either prevents the current “v1.0 vs v2.0 drift” and copy/paste leftovers.

---

### 1.8 Environment / dependencies
Add:
- `environment.yml` for conda (Windows-friendly)
- `requirements-all.txt` (optional) for “install everything”

Make sure README + notebook agree on:
- env name
- install commands
- minimum python version

---

## 2) Pipeline-specific fixes (what to change where)

### 2.1 `math_pipeline_v2`
**Fixes:**
- ✅ Convert `license_map.yaml` to the standardized schema:
  - rename `license_profiles` → `profiles`
  - incorporate `non_spdx` into `normalization.rules` or update driver to use `non_spdx`
  - add `restriction_scan`, `gating`, `updated_utc` if missing fields
- ✅ Update `pipeline_driver.py` to fallback-load legacy keys (see 1.2)
- ✅ Remove denylist placeholder links in `denylist.yaml`
- ✅ Add `review_notes` to all targets with `review_required: true`
- ✅ Gates drift:
  - If targets add gates like `record_level_filter`, `emit_attribution_bundle`, etc., either:
    - implement them, or
    - remove them from config until implemented

---

### 2.2 `physics_pipeline_v2`
Same issues as math:
- ✅ Convert `license_map.yaml` off `license_profiles/non_spdx`
- ✅ Add `profiles` + `gating` + `restriction_scan` + `normalization`
- ✅ Ensure driver supports fallback keys

---

### 2.3 `earth_pipeline_v2`
- ✅ Convert `license_map.yaml` from `license_profiles` → `profiles`
- ✅ Add `gating` section (or rely on defaults but include it for consistency)
- ✅ Remove denylist placeholder links

---

### 2.4 `code_pipeline_v2`
**Current critical issue:** `license_map.yaml` is missing `profiles` and `gating`.

- ✅ Add `profiles` (permissive / record_level / copyleft / unknown / deny)
- ✅ Add `gating` defaults
- ✅ Confirm `pipeline_driver.py` produces GREEN when:
  - resolved SPDX is allowlisted AND profile default_bucket is GREEN
- ✅ Ensure the “chemistry” strings are removed from report header (see 1.3)

---

### 2.5 `materials_science_pipeline_v2`
This pipeline has the most “enabled but incomplete” targets + profile mismatches.

**A) Enabled targets with `strategy: http` but empty `download.urls`:**
These must be fixed or disabled:
- `jarvis_dft_figshare` (enabled; urls empty)
- `open_catalyst_oc20_oc22` (enabled; urls empty)
- `matbench_discovery` (enabled; urls empty)
- (and any other `http` entries that say “Fill urls…”)

✅ Choose one:
- **Disable by default** (`enabled: false`) until URLs are filled, or
- **Populate real URLs** to the release artifacts you intend to download

**B) License profile mismatch**
Enabled targets use profiles like:
- `attribution`
- `mixed_record_level`
- `permissive_code`

…but `license_map.yaml` only defines:
- `permissive`, `record_level`, `copyleft`, `unknown`, `deny`

✅ Fix choices:
- **Preferred:** map those target profiles into the standard ones:
  - `attribution` → `record_level` (often)
  - `mixed_record_level` → `record_level`
  - `permissive_code` → `permissive`
- Or: add these profiles to the map explicitly with clear defaults.

---

### 2.6 `logic_pipeline_v2`
- `tptp_problem_library` is **enabled** but:
  - `license_evidence.url` is missing
  - download config is incomplete for strategy `http`

✅ Fix:
- either disable it (`enabled: false`) until filled, or
- populate evidence URL + download URL(s)

---

### 2.7 `3d_modeling_pipeline_v2`
Several enabled targets use `strategy: git` but are missing the required repo/url field.

Examples flagged:
- `blender_education`
- `blender_manual`
- `curaengine`
- `ultimaker_fdm_materials`

✅ Fix:
- add `download.repo: https://...` (and optionally branch/tag)
- or set `enabled: false` until you’ve confirmed licensing and exact sources

---

### 2.8 `biology_pipeline_v2`
An enabled FTP target is missing its URL:
- `pdb_rcsb_structures` (strategy `ftp`, missing url)

✅ Fix:
- add the canonical FTP URL(s) to `download.url` or `download.urls`
- ensure worker supports FTP resume/caching behavior on Windows

---

### 2.9 `regcomp_pipeline_v2` + other pipelines with `review_required: true`
Multiple targets are marked `review_required: true` but lack any notes.

✅ Fix:
- Add `review_notes` for each such target:
  - what to confirm (ToS text-mining permission, redistribution, API limits, attribution requirements)
  - what evidence snapshot should capture
  - what “approved” means for that dataset

---

## 3) Behavior correctness improvements (recommended)

### 3.1 Fail-fast on broken enabled targets (even in dry-run)
Right now some broken configurations will only show up when the worker runs.

✅ In `pipeline_driver.py`:
- detect missing required download fields for enabled targets
- bucket them as `RED` (config_invalid) or force YELLOW with explicit reason
- write the reason into the queue JSONL

### 3.2 Validate `license_profile` enums
- If profile not found in `license_map.profiles`, warn loudly.
- Provide a recommended mapping in the error message.

### 3.3 Make gate configuration real (or remove it)
There are gate strings in `targets_*.yaml` that are not implemented.
Pick one:
- implement the common ones (even as stubs that write explicit manifest outcomes), or
- remove them until they exist (less confusing and safer)

---

## 4) Documentation updates

### 4.1 Root `README.md`
Ensure it includes:
- Windows-first instructions
- “run from notebook” instructions
- what `--execute` actually changes
- where outputs land (`DEST_ROOT/_queues`, `_manifests`, `_logs`, etc.)

### 4.2 `docs/output_contract.md`
Confirm it matches what `catalog_builder.py` actually emits, including:
- required fields across pipelines
- optional routing / difficulty fields
- how attribution bundles are represented (if/when added)

### 4.3 Add “Compliance model” doc
Add `docs/compliance_model.md`:
- GREEN/YELLOW/RED meaning
- review signoff semantics
- evidence snapshot retention + hashing
- denylist precedence rules

---

## 5) Suggested implementation order (fastest path to “works reliably”)

1. **Fix license map schema drift** (math, physics, earth, code)
2. **Add validator** (`tools/validate_repo.py`) + run it in notebook
3. **Disable or complete broken enabled targets** (materials, logic, 3d, biology)
4. **Fix report header “CHEMISTRY…”** across drivers
5. Consolidate shared code to prevent drift
6. Clean notebook + docs + env files

---

## 6) Quick checklist (copy/paste into TODO)

- [ ] Standardize `license_map.yaml` schema across all pipelines (profiles + gating + normalization + restriction_scan)
- [ ] Add legacy key fallback loading (`license_profiles` → `profiles`, etc.)
- [ ] Add validation + warnings for missing profiles / missing required download fields
- [ ] Remove `noai.example` placeholder links from denylists
- [ ] Fix all “CHEMISTRY CORPUS PIPELINE…” report headers
- [ ] Fix/disable incomplete enabled targets:
  - [ ] `logic_pipeline_v2:tptp_problem_library`
  - [ ] `materials_science_pipeline_v2:jarvis_dft_figshare`, `open_catalyst_oc20_oc22`, `matbench_discovery` (empty urls)
  - [ ] `3d_modeling_pipeline_v2:blender_*`, `curaengine`, `ultimaker_fdm_materials` (git missing repo)
  - [ ] `biology_pipeline_v2:pdb_rcsb_structures` (ftp missing url)
- [ ] Add `review_notes` everywhere `review_required: true`
- [ ] Notebook: remove duplicate runner cells; make Windows-first path the default
- [ ] Add `environment.yml` (+ optional `requirements-all.txt`)
- [ ] Add CI job to run validator + minimal “dry-run” smoke tests
