# Dataset Collector v2 — Repo Update Plan

_Last reviewed: **2025-12-29** (America/Phoenix date reference)._

This document is a **thorough, repo-wide update plan** for bringing the Dataset Collector + license screening system to a more maintainable, reproducible, and “hard-to-misuse” state.

It is written as a set of **prioritized, concrete changes** you can implement incrementally. Nothing here requires redesigning the core v2 flow (classify → acquire → yellow_screen → merge → catalog), but several items reduce drift and future debugging time.

---

## 0) What’s already in good shape

- The repo layout is consistent: `*_pipeline_v2/` directories each contain a driver, workers, companion YAMLs, and a README.
- `tools/validate_repo.py` currently reports **0 errors / 0 warnings** on this snapshot.
- `tools/build_natural_corpus.py` + `tools/patch_targets.py` are the right direction for **Windows-first orchestration** (no bash required).

So this update plan focuses on: **reproducibility, drift reduction, documentation correctness, and stronger guardrails**.

---

## 1) Must-do repo hygiene and governance

### 1.1 Add missing “standard” repo files (low effort, high payoff)

Create the following at repo root (they are currently missing):

- `.gitignore`
- `LICENSE`
- `CONTRIBUTING.md`
- `SECURITY.md`
- `CODE_OF_CONDUCT.md`
- `CHANGELOG.md`

**Why**
- These files prevent accidental commits of output data, clarify legal use, and make collaboration safer.

**What to include**
- `.gitignore`: ignore outputs (`**/raw/`, `**/screened_yellow/`, `**/combined/`, `**/_logs/`, `**/_ledger/`, `**/_queues/`, `**/_manifests/`, `**/_catalogs/`, `**/_pitches/`), plus typical Python/Jupyter artifacts (`.ipynb_checkpoints/`, `__pycache__/`, `.pytest_cache/`, `.venv/`, `.mypy_cache/`).
- `LICENSE`: your chosen license for *this repository’s code* (separate from dataset licenses).
- `CONTRIBUTING.md`: how to add targets safely (see §6).
- `SECURITY.md`: how to report issues privately.
- `CHANGELOG.md`: lightweight “v2.x” entries.
- `CODE_OF_CONDUCT.md`: optional but standard.

### 1.2 Fix missing/ghost documentation references

Your pipeline drivers (and some docs) reference **`PIPELINE_V2_REWORK_PLAN.md`**, but it does not exist in this repo.

**Action (pick one):**
- **Option A (recommended):** create `docs/PIPELINE_V2_REWORK_PLAN.md` and point all references there.
- **Option B:** remove/replace references in docstrings and READMEs with the current canonical doc(s), e.g. `docs/output_contract.md`.

Files referencing this today (non-exhaustive list from scan):
- `3d_modeling_pipeline_v2/pipeline_driver.py`
- `agri_circular_pipeline_v2/pipeline_driver.py`
- `biology_pipeline_v2/pipeline_driver.py`
- `chem_pipeline_v2/pipeline_driver.py`
- `code_pipeline_v2/pipeline_driver.py`
- `cyber_pipeline_v2/pipeline_driver.py`
- `earth_pipeline_v2/pipeline_driver.py`
- `econ_stats_decision_adaptation_pipeline_v2/pipeline_driver.py`
- `engineering_pipeline_v2/pipeline_driver.py`
- `kg_nav_pipeline_v2/pipeline_driver.py`
- `logic_pipeline_v2/pipeline_driver.py`
- `logic_pipeline_v2/run_pipeline.sh`
- `materials_science_pipeline_v2/pipeline_driver.py`
- `math_pipeline_v2/README.md`
- `math_pipeline_v2/pipeline_driver.py`
- `math_pipeline_v2/run_pipeline.sh`
- `metrology_pipeline_v2/README.md`
- `metrology_pipeline_v2/pipeline_driver.py`
- `metrology_pipeline_v2/run_pipeline.sh`
- `nlp_pipeline_v2/pipeline_driver.py`
- `physics_pipeline_v2/pipeline_driver.py`
- `regcomp_pipeline_v2/pipeline_driver.py`
- `safety_incident_pipeline_v2/pipeline_driver.py`

---

## 2) Dependency + environment updates (make it “one env, works everywhere”)

### 2.1 Fix root dependency drift

Root `requirements.txt` is missing packages that several pipelines require.

**Add to root `requirements.txt`:**
- `boto3>=1.34.0`
- `datasets>=2.20.0`
- `pyarrow>=14.0.0`

**Why**
- Your `environment.yml` installs the root requirements; if they’re incomplete, you get “works on my machine” failures when switching pipelines.

### 2.2 Add a constraints/lock strategy (recommended)

Right now you use minimum versions (`>=`). That’s fine for a prototype, but for long-lived corpus building you want reproducibility.

**Add:**
- `requirements.lock` (pip-compile style) **or**
- `conda-lock.yml` (conda-lock style)

**Policy suggestion**
- Keep `requirements.txt` as human-maintained (broad pins).
- Generate `requirements.lock` for actual execution on Windows.

### 2.3 Make external tool dependencies explicit

`tools/preflight.py` currently warns about missing external tools (e.g. `aws` needed for `aws_requester_pays`).

**Update docs**
- Root README should include a “Toolchain” section:
  - Git
  - 7zip/unzip (optional)
  - AWS CLI (only if using AWS targets)
  - (Optional) `huggingface-cli` if you add HF auth flows later

**Update preflight**
- Add “how to install” hints per tool (Windows-first).
- Make warnings *actionable* (e.g. show the exact target(s) that require it).

---

## 3) Documentation updates (reduce confusion for future-you)

### 3.1 Make the root README the single source of truth for running

The root README should include, in order:
1. **What this repo does** (collector + license screening)
2. **Safety model** (GREEN vs YELLOW, why YELLOW requires human review)
3. **Quickstart (Windows + conda + Jupyter)**
4. **Outputs** (link to `docs/output_contract.md`)
5. **How to add a new target safely**

### 3.2 Standardize per-pipeline README templates

Each pipeline README should follow the same template:
- Purpose (domain + what targets it contains)
- How to run via `tools/build_natural_corpus.py` (preferred)
- Manual review flow (YELLOW)
- Where outputs go (`dest_folder` mapping)
- “Adding targets” notes + safety reminders

This reduces drift and makes pipelines feel consistent.

---

## 4) Normalize metadata: `updated_utc` and schema versions

### 4.1 Fix future-dated `updated_utc`

Several YAML files contain `updated_utc` dates **after** 2025-12-29.

**Recommended policy**
- `updated_utc` should be the *last actual edit date* in UTC and **never in the future**.

**Implementation**
- Add `tools/touch_updated_utc.py` that:
  - normalizes format to `YYYY-MM-DD`
  - optionally rewrites all `updated_utc` to “today” for files you touched
- Enhance `tools/validate_repo.py` to warn if `updated_utc` is future-dated.

**Files currently future-dated (path → value):**
- `3d_modeling_pipeline_v2/denylist.yaml` → `2026-06-03`
- `3d_modeling_pipeline_v2/field_schemas.yaml` → `2026-06-03`
- `3d_modeling_pipeline_v2/license_map.yaml` → `2026-06-03`
- `3d_modeling_pipeline_v2/targets_3d.yaml` → `2026-06-03`
- `biology_pipeline_v2/field_schemas.yaml` → `2026-06-15`
- `biology_pipeline_v2/targets_biology.yaml` → `2026-06-15`
- `chem_pipeline_v2/targets_chem.yaml` → `2026-01-01T00:00:00Z`
- `code_pipeline_v2/targets_code.yaml` → `2026-06-01`
- `cyber_pipeline_v2/denylist.yaml` → `2026-01-15`
- `cyber_pipeline_v2/field_schemas.yaml` → `2026-01-15`
- `cyber_pipeline_v2/license_map.yaml` → `2026-01-15`
- `cyber_pipeline_v2/targets_cyber.yaml` → `2026-06-01`
- `earth_pipeline_v2/denylist.yaml` → `2026-06-01`
- `earth_pipeline_v2/field_schemas.yaml` → `2026-06-10`
- `earth_pipeline_v2/targets_earth.yaml` → `2026-06-10T00:00:00Z`
- `econ_stats_decision_adaptation_pipeline_v2/field_schemas.yaml` → `2026-06-01`
- `econ_stats_decision_adaptation_pipeline_v2/targets_econ_stats_decision_v2.yaml` → `2026-06-01`
- `engineering_pipeline_v2/targets_engineering.yaml` → `2026-06-15`
- `kg_nav_pipeline_v2/targets_kg_nav.yaml` → `2026-06-15T00:00:00Z`
- `logic_pipeline_v2/targets_logic.yaml` → `2026-06-01`
- `materials_science_pipeline_v2/targets_materials.yaml` → `2026-06-01`
- `math_pipeline_v2/denylist.yaml` → `2026-06-01`
- `math_pipeline_v2/field_schemas.yaml` → `2026-06-01`
- `math_pipeline_v2/targets_math.yaml` → `2026-06-01`
- `physics_pipeline_v2/denylist.yaml` → `2026-06-01`
- `physics_pipeline_v2/field_schemas.yaml` → `2026-06-01`
- `physics_pipeline_v2/targets_physics.yaml` → `2026-02-15`
- `safety_incident_pipeline_v2/targets_safety_incident.yaml` → `2026-01-15`

### 4.2 Normalize `updated_utc` formatting

Some YAMLs use timestamps like `2026-01-01T00:00:00Z` rather than a simple date.

**Action**
- Normalize all `updated_utc` fields to `YYYY-MM-DD` for consistency.

Examples detected (path → value):
- `agri_circular_pipeline_v2/targets_agri_circular.yaml` → `2025-12-17T19:00:00Z`
- `chem_pipeline_v2/targets_chem.yaml` → `2026-01-01T00:00:00Z`
- `earth_pipeline_v2/targets_earth.yaml` → `2026-06-10T00:00:00Z`
- `kg_nav_pipeline_v2/targets_kg_nav.yaml` → `2026-06-15T00:00:00Z`

---

## 5) Reduce pipeline code drift (biggest long-term payoff)

Right now every pipeline has its own copies of:
- `pipeline_driver.py`
- `acquire_worker.py`
- `yellow_screen_worker.py`
- `merge_worker.py`
- `catalog_builder.py`
- `review_queue.py`

Even if they look similar, over time they will diverge and you’ll fix bugs N times.

### 5.1 Introduce a shared core package

Create a folder like:

```text
collector_core/
  __init__.py
  config.py
  stages/
    classify.py
    acquire.py
    yellow_screen.py
    merge.py
    catalog.py
    review.py
  licensing/
    spdx.py
    evidence.py
    denylist.py
    phrase_scan.py
  io/
    jsonl.py
    hashing.py
    sharding.py
  cli.py
```

Then each pipeline directory becomes mostly:
- `targets_*.yaml`
- `license_map.yaml`
- `field_schemas.yaml`
- `denylist.yaml`
- `README.md`
- optionally a tiny `pipeline.py` with any custom overrides

**Outcome**
- One place to fix merge bugs, evidence fetching, phrase scanning, etc.

### 5.2 Transitional approach (no big rewrite)

If you don’t want a refactor right now:
- At least standardize a **minimal shared module** in `tools/` or `collector_core/` and import it from pipelines.
- Start with the simplest shared primitives:
  - path resolution / layout
  - logging format
  - JSONL read/write helpers
  - SPDX normalization + deny-prefix checks

---

## 6) Targets and licensing governance updates

### 6.1 Make “adding a target” safer and more deterministic

Add a doc (or a section in `CONTRIBUTING.md`) that requires:

**For every new target:**
- clear `id` naming convention (stable, no spaces)
- explicit `acquire` method (repo/http/hf/aws/etc.)
- captured license evidence strategy:
  - repo: store `LICENSE` + `README` + commit hash
  - web page: snapshot HTML + resolved URLs + retrieval date
  - dataset host: record dataset revision/version

**And enforce via validator**
- `tools/validate_repo.py` should warn if:
  - a target lacks license evidence configuration
  - a target is YELLOW but has no review queue entry generation
  - a target points to a URL without a stable domain parse

### 6.2 Centralize “restriction phrase” lists

Right now phrase scanning exists, but you’ll want a single canonical list for:
- “NoAI”
- “no text and data mining / TDM”
- “no machine learning”
- “no training”
- “research only”
- “non-commercial” / “NC”
- “no derivatives” / “ND”
- “evaluation only”

Put these in one place (e.g. `collector_core/licensing/phrase_scan.py`) and reference from pipelines.

---

## 7) Improve resumability and auditability (dataset-scale practicality)

### 7.1 Add content hashing + download caching rules

For large acquisitions, you want deterministic behavior:
- store `sha256` for every downloaded artifact
- keep a “download cache” keyed by hash
- detect partial downloads and resume safely

You already have ledger concepts; extend them with:
- artifact hash
- size
- retrieval timestamp
- source URL
- (optional) HTTP headers like ETag / Last-Modified

### 7.2 Add a “manifest lock” for every compiled run

In `combined/`, add:
- `_manifests/run_manifest.json`:
  - which pipelines ran
  - which targets included/excluded
  - license decisions + evidence pointers
  - commit hash of the collector repo itself (so you can reproduce later)

---

## 8) Yellow review UX improvements (make manual review painless)

You already have a manual review queue flow; make it easier to use repeatedly:

**Add tools:**
- `review_export.py`: export all pending YELLOW items into a single Markdown/CSV report.
- `review_open.py`: open evidence folders (or print paths) for the next N items.

**Add a “decision protocol”:**
- every YELLOW target review produces:
  - a signed-off `decision.json` (allow/deny/conditional + notes)
  - a pointer to evidence snapshots
  - the reviewer + date

---

## 9) Merge and catalog improvements (quality + downstream use)

### 9.1 Catalog should be more than “counts”

Extend `catalog.json` (or add `catalog_extended.json`) to include:
- total records, total bytes
- schema summary (fields seen)
- per-target contribution counts
- license distribution summary
- (optional) language / domain tags if you have them

### 9.2 Optional: add Parquet export

JSONL is great for transparency; Parquet is great for speed + analytics.

Add a stage or tool:
- `tools/export_parquet.py` that converts `combined/**/*.jsonl` → `combined_parquet/` using `pyarrow`.

---

## 10) CI and automated checks (keep v2 stable)

Add GitHub Actions (or equivalent) to run on PRs:
- `python tools/validate_repo.py --root .`
- `python tools/preflight.py --repo-root .`
- `ruff` / `black` (optional but recommended)
- (optional) unit tests for:
  - SPDX normalization
  - denylist matching
  - phrase scanning
  - targets schema validation

---

## 11) Concrete “next commit” checklist

If you want a clean, high-impact next commit, do these in order:

1. Add `.gitignore`, `LICENSE`, `CONTRIBUTING.md`, `SECURITY.md`, `CHANGELOG.md`.
2. Create `docs/PIPELINE_V2_REWORK_PLAN.md` **or** remove all references to it.
3. Update root `requirements.txt` to include: boto3>=1.34.0, datasets>=2.20.0, pyarrow>=14.0.0.
4. Add validator warnings for:
   - future `updated_utc`
   - missing evidence config for new targets
5. Normalize `updated_utc` formats (add `tools/touch_updated_utc.py`).
6. Add CI that runs `validate_repo` + `preflight`.

---

## Appendix A — Notes on Windows-first execution

You already have a good pattern:
- Use `tools/build_natural_corpus.py` from the notebook.
- Keep bash scripts as optional convenience for Git Bash/WSL.

**One more improvement**
- Consider adding a `run_all.py` (thin wrapper) at repo root so users can run:

```bash
python run_all.py --dest-root "E:/AI-Research/datasets/Natural" --mode full --execute
```

This reduces “where do I run this from?” confusion.
