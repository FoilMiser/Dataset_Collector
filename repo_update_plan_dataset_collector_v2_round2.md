# Dataset Collector v2 — Remaining Updates & Fixes (Round 2)

_Reviewed against the newly uploaded repo snapshot on **2025-12-29** (America/Phoenix date reference)._

This file focuses on **what still needs to be updated or fixed**. It assumes you want the repo to be:
- easy to run from **Windows + Conda + Jupyter**
- hard to misuse (license screening defaults safe)
- reproducible and collaboration-ready

---

## 0) Current status

### What’s solid
- ✅ Standard repo hygiene files are present (`.gitignore`, `LICENSE`, `CONTRIBUTING.md`, etc.).
- ✅ `docs/PIPELINE_V2_REWORK_PLAN.md` exists (no more dangling references).
- ✅ `requirements.lock` exists and the dependency story is much cleaner.
- ✅ `tools/validate_repo.py` currently reports **0 errors / 0 warnings** on this snapshot.

### What’s still missing / incomplete
- ❌ No CI (`.github/workflows/*`) yet.
- ⚠️ `tools/validate_repo.py` writes a generated `validation_report.json` into repo root by default.
- ⚠️ Some YAML `updated_utc` values appear “future-dated” when compared to local Phoenix date (likely due to UTC rollover policy confusion).
- ⚠️ Per-pipeline READMEs don’t yet reflect the Windows-first orchestrator (`tools/build_natural_corpus.py`) and still act like `run_pipeline.sh` is the primary UX.

---

## 1) Critical updates (do these next)

### 1.1 Add CI to prevent regressions

Add a minimal GitHub Actions workflow that runs on every PR/push:
- `python tools/validate_repo.py --root .`
- `python tools/preflight.py --repo-root .`

**Suggested file:** `.github/workflows/ci.yml`

```yaml
name: CI

on:
  push:
  pull_request:

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - name: Install deps
        run: |
          python -m pip install --upgrade pip
          pip install -r requirements.txt
      - name: Validate repo
        run: |
          python tools/validate_repo.py --root .
      - name: Preflight
        run: |
          python tools/preflight.py --repo-root .
```

**Optional improvements**
- Add matrix for Python 3.10/3.11
- Add a “lint” job later if you add `ruff`/`black`

---

### 1.2 Stop writing generated files into repo root (`validation_report.json`)

Right now, running the validator will create `validation_report.json` at repo root by default.

**Why it matters**
- People will commit it accidentally.
- It creates noisy diffs.
- It makes “clean working tree” harder, especially when running from notebooks.

**Fix (recommended):**
- Change default output path to `_logs/validation_report.json`
- Ensure `_logs/` exists (create it if missing)
- OR make output opt-in: only write when `--output` is specified

**Minimal patch suggestion (opt-in output):**
- In `tools/validate_repo.py`:
  - set `--output` default to `None`
  - only write a file if `args.output` is not None

**If you keep writing a report by default:**
- Update `.gitignore` to include:
  - `validation_report.json`

---

### 1.3 Expand `.gitignore` for secrets + ephemeral artifacts

Your `.gitignore` is strong for pipeline outputs and Python artifacts, but you should still ignore common secrets files.

**Add:**
```gitignore
# Secrets / local env
.env
.env.*
*.pem
*.key
secrets*.json
*_secrets.json

# Tool outputs
validation_report.json
```

This is especially important because some acquisition methods prompt for API keys.

---

## 2) High-priority fixes (next wave)

### 2.1 Clarify and enforce `updated_utc` policy (UTC vs local confusion)

Your `CONTRIBUTING.md` says `updated_utc` should not be future-dated, but several YAML configs use:

- `updated_utc: 2025-12-30`

When reviewed on Phoenix date **2025-12-29**, that looks “future”, but it may be valid if you intended **UTC date** and edits occurred after UTC midnight.

**Pick one policy and enforce it consistently:**

#### Policy A (recommended): `updated_utc` is an ISO UTC timestamp
- Example: `2025-12-30T02:14:00Z`
- Pros: unambiguous; no “future” confusion
- Cons: slightly noisier diffs

#### Policy B: `updated_utc` is a UTC date (YYYY-MM-DD)
- Pros: tidy
- Cons: can look “future” locally

If you choose **Policy B**, update docs + validator to compare against **UTC date**, not local date.

**Concrete updates to make:**
- Update `CONTRIBUTING.md` line about “not future-dated” to specify **UTC**.
- Add a validator warning if `updated_utc` is > `date.today()` in UTC.

**Files currently “future” vs Phoenix date (path → value):**
- `3d_modeling_pipeline_v2/denylist.yaml` → `2025-12-30`
- `3d_modeling_pipeline_v2/field_schemas.yaml` → `2025-12-30`
- `3d_modeling_pipeline_v2/license_map.yaml` → `2025-12-30`
- `3d_modeling_pipeline_v2/targets_3d.yaml` → `2025-12-30`
- `agri_circular_pipeline_v2/targets_agri_circular.yaml` → `2025-12-30`
- `biology_pipeline_v2/field_schemas.yaml` → `2025-12-30`
- `biology_pipeline_v2/targets_biology.yaml` → `2025-12-30`
- `chem_pipeline_v2/targets_chem.yaml` → `2025-12-30`
- `code_pipeline_v2/targets_code.yaml` → `2025-12-30`
- `cyber_pipeline_v2/denylist.yaml` → `2025-12-30`
- `cyber_pipeline_v2/field_schemas.yaml` → `2025-12-30`
- `cyber_pipeline_v2/license_map.yaml` → `2025-12-30`
- `cyber_pipeline_v2/targets_cyber.yaml` → `2025-12-30`
- `earth_pipeline_v2/denylist.yaml` → `2025-12-30`
- `earth_pipeline_v2/field_schemas.yaml` → `2025-12-30`
- `earth_pipeline_v2/targets_earth.yaml` → `2025-12-30`
- `econ_stats_decision_adaptation_pipeline_v2/field_schemas.yaml` → `2025-12-30`
- `econ_stats_decision_adaptation_pipeline_v2/targets_econ_stats_decision_v2.yaml` → `2025-12-30`
- `engineering_pipeline_v2/targets_engineering.yaml` → `2025-12-30`
- `kg_nav_pipeline_v2/targets_kg_nav.yaml` → `2025-12-30`
- `logic_pipeline_v2/targets_logic.yaml` → `2025-12-30`
- `materials_science_pipeline_v2/targets_materials.yaml` → `2025-12-30`
- `math_pipeline_v2/denylist.yaml` → `2025-12-30`
- `math_pipeline_v2/field_schemas.yaml` → `2025-12-30`
- `math_pipeline_v2/targets_math.yaml` → `2025-12-30`
- `physics_pipeline_v2/denylist.yaml` → `2025-12-30`
- `physics_pipeline_v2/field_schemas.yaml` → `2025-12-30`
- `physics_pipeline_v2/targets_physics.yaml` → `2025-12-30`
- `safety_incident_pipeline_v2/targets_safety_incident.yaml` → `2025-12-30`


### 2.2 Bring per-pipeline READMEs in line with the Windows-first orchestrator

Right now, **every pipeline README** omits the preferred Windows-first entrypoint (`tools/build_natural_corpus.py`) and makes `run_pipeline.sh` feel primary.

**Recommended update:** Add a short “Recommended run method” section to each pipeline README:
- “Use the notebook or `tools/build_natural_corpus.py` on Windows”
- “Use `run_pipeline.sh` only if you have Git Bash/WSL”

Pipelines missing any mention of `build_natural_corpus.py`:
- `3d_modeling_pipeline_v2`
- `agri_circular_pipeline_v2`
- `biology_pipeline_v2`
- `chem_pipeline_v2`
- `code_pipeline_v2`
- `cyber_pipeline_v2`
- `earth_pipeline_v2`
- `econ_stats_decision_adaptation_pipeline_v2`
- `engineering_pipeline_v2`
- `kg_nav_pipeline_v2`
- `logic_pipeline_v2`
- `materials_science_pipeline_v2`
- `math_pipeline_v2`
- `metrology_pipeline_v2`
- `nlp_pipeline_v2`
- `physics_pipeline_v2`
- `regcomp_pipeline_v2`
- `safety_incident_pipeline_v2`

**Suggestion:** create `docs/pipeline_readme_template.md` and keep them uniform.

### 2.3 Standardize `field_schemas` filenames (minor inconsistency, but worth fixing)

Most pipelines use `field_schemas.yaml`, but a couple diverge:

- `materials_science_pipeline_v2`: `field_schemas_materials.yaml`
- `safety_incident_pipeline_v2`: `field_schemas_safety_incident.yaml`

**Two options:**
- **Rename** these to `field_schemas.yaml` and update any internal references (best for uniformity).
- Or: keep them, but add a tiny `pipeline_config.yaml` per pipeline that explicitly points to the schema file.

### 2.4 Preflight warnings should be target-aware and optionally strict

Preflight warns that AWS CLI is missing if you enable any AWS/S3 acquisition strategies.

**Improve UX by:**
- Printing *which target IDs* require AWS tooling
- Adding `--strict` to turn warnings into failures in CI (optional)

---

## 3) Medium-priority cleanup / maintainability

### 3.1 Consolidate scattered `todo.txt`

You currently have many per-pipeline `todo.txt` files. That’s fine internally, but it tends to drift and become invisible.

Options:
- Move them into `docs/todo/<pipeline>.md`
- Or convert them into GitHub Issues and delete the files

Current `todo.txt` files:
- `3d_modeling_pipeline_v2/todo.txt`
- `agri_circular_pipeline_v2/todo.txt`
- `biology_pipeline_v2/todo.txt`
- `chem_pipeline_v2/todo.txt`
- `code_pipeline_v2/todo.txt`
- `cyber_pipeline_v2/todo.txt`
- `earth_pipeline_v2/todo.txt`
- `engineering_pipeline_v2/todo.txt`
- `kg_nav_pipeline_v2/todo.txt`
- `logic_pipeline_v2/todo.txt`
- `materials_science_pipeline_v2/todo.txt`
- `math_pipeline_v2/todo.txt`
- `metrology_pipeline_v2/todo.txt`
- `nlp_pipeline_v2/todo.txt`
- `physics_pipeline_v2/todo.txt`

### 3.2 Add a lightweight formatter/linter config

Not required, but it pays off as soon as multiple people touch the code.

Recommended:
- Add `pyproject.toml` with `ruff` + (optional) `black`
- CI job: `ruff check .`

### 3.3 Add minimal unit tests for the most error-prone logic

Even 6–10 tests can prevent the worst regressions:
- SPDX normalization + confidence mapping
- denylist matching
- phrase restriction detection
- “merge only allowed sources” guardrails

Add `tests/` + run `pytest -q` in CI.

---

## 4) Nice-to-have improvements (big payoff, not urgent)

### 4.1 Reduce duplicated pipeline code via a shared core package

Most pipelines carry near-identical workers and drivers. Long-term, that means every bug fix is repeated N times.

Recommended direction:
- Create `collector_core/` with shared stage implementations
- Pipelines become mostly config + targets YAML
- Allow small pipeline-specific overrides via hooks

### 4.2 Add a `run_all.py` CLI wrapper (in addition to the notebook)

So users can run without opening Jupyter:

```bash
python run_all.py --dest-root "E:/AI-Research/datasets/Natural" --mode full --execute
```

---

## 5) Suggested next-commit checklist

If you want the most leverage with the least work, do this in order:

1. Add `.github/workflows/ci.yml` (validator + preflight).
2. Fix `validate_repo.py` so it doesn’t write `validation_report.json` into repo root by default (or add it to `.gitignore`).
3. Extend `.gitignore` with `.env` and secret/key patterns.
4. Clarify `updated_utc` policy (UTC date vs UTC timestamp) and update `CONTRIBUTING.md` + validator accordingly.
5. Update all pipeline READMEs to mention `tools/build_natural_corpus.py` as the recommended Windows-first run method.
6. Standardize the two nonstandard `field_schemas_*` filenames.

