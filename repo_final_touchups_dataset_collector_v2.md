# Dataset Collector v2 — Final Touch‑Ups Plan

_Reviewed: **2025-12-30** (America/Phoenix)._

This doc is the **final polish checklist** to make the repo feel “production-ready” for repeated Windows/Jupyter runs **and** safe collaboration (CI, consistent environments, no stray artifacts).

---

## 0) One-screen summary (do these first)

1. **Remove or relocate stray root docs** (keep root clean).
2. **Align Python versions** between `environment.yml` and CI (or test a matrix).
3. **Document + use `requirements.lock`** for reproducible installs.
4. **Upgrade CI** to include Windows and (optionally) both lock + min deps.
5. **Add lightweight lint/tests** so v2 stays stable.

---

## 1) Repo cleanliness (root should stay minimal)

### 1.1 Remove/move the old plan markdown from repo root
Your snapshot still includes:

- `repo_update_plan_dataset_collector_v2_round2.md`

**Action (pick one):**
- **Delete it** (recommended once completed), **or**
- Move to `docs/notes/repo_update_plan_dataset_collector_v2_round2.md`.

**Why**
- Root should remain “only what a new runner needs”: README, env, requirements, notebook, and policy docs.

---

## 2) Environment consistency (Conda + CI should match)

### 2.1 Align Python version: `environment.yml` vs CI
Right now:
- `environment.yml` pins `python=3.10`
- CI uses Python `3.11`

**Recommended approach:** test both in CI, and keep conda pinned to your preferred local version.

#### Option A (best): CI matrix (3.10 + 3.11) + keep conda at 3.10
Update `.github/workflows/ci.yml`:

```yaml
strategy:
  matrix:
    python-version: ["3.10", "3.11"]
steps:
  - uses: actions/setup-python@v5
    with:
      python-version: ${{ matrix.python-version }}
```

#### Option B (simple): make both 3.11
- Change `environment.yml` to `python=3.11`, and keep CI as-is.

**Either way:** add a small line in the root README: “Tested on Python X.Y”.

---

## 3) Reproducibility (make `requirements.lock` a first-class citizen)

You already have:
- `requirements.txt` (human-edited)
- `requirements.lock` (pinned)
- `requirements-dev.txt`

### 3.1 Add “How to install” using the lock file in `README.md`
Add a short section:

```md
### Reproducible install (recommended)
pip install -r requirements.lock

### Minimal install (looser pins)
pip install -r requirements.txt
```

### 3.2 Add a lock regeneration command (one-liner)
If you use `pip-tools`, document:

```bash
pip-compile --generate-hashes -o requirements.lock requirements.txt
```

If not using pip-tools, document **exactly** how you generated the lock so future-you can repeat it.

---

## 4) CI upgrades (small changes, big leverage)

### 4.1 Add Windows CI (since your target is Windows-first)
Extend `.github/workflows/ci.yml` to test both OSes:

```yaml
strategy:
  matrix:
    os: [ubuntu-latest, windows-latest]
    python-version: ["3.10", "3.11"]

runs-on: ${{ matrix.os }}
```

### 4.2 Add a second install mode job (optional but valuable)
Two jobs:
- **min-deps job:** `pip install -r requirements.txt`
- **locked job:** `pip install -r requirements.lock`

This catches:
- missing dependency declarations (min job)
- reproducibility breakages (lock job)

### 4.3 Add pip cache (speeds CI a lot)
Add:

```yaml
- uses: actions/cache@v4
  with:
    path: ~/.cache/pip
    key: ${{ runner.os }}-pip-${{ hashFiles('requirements*.txt','requirements.lock') }}
    restore-keys: |
      ${{ runner.os }}-pip-
```

---

## 5) `.gitignore` micro-fixes (tiny polish)

### 5.1 Add `.env` (not just `.env.*`)
Your `.gitignore` includes `.env.*` but not `.env`.

Add:

```gitignore
.env
```

### 5.2 Remove the literal `...` line
Your `.gitignore` currently contains a line that is literally `...`.

It isn’t dangerous, but it’s confusing. Remove it.

---

## 6) Add “tiny guardrails” (optional, but keeps v2 from drifting)

### 6.1 Add `pyproject.toml` with ruff (recommended)
Create `pyproject.toml`:

```toml
[tool.ruff]
line-length = 100
target-version = "py310"

[tool.ruff.lint]
select = ["E", "F", "I", "B", "UP"]
ignore = ["E501"]
```

Add to CI (after install):

```yaml
- name: Ruff
  run: |
    python -m pip install ruff
    ruff check .
```

### 6.2 Add 6–10 unit tests for the “license safety” primitives
Create `tests/` and cover:
- SPDX normalization + confidence rules
- denylist matching semantics
- restriction phrase scanning (NoAI / TDM / no-training / NC / ND)

Then CI:

```yaml
- name: Tests
  run: |
    python -m pip install pytest
    pytest -q
```

You don’t need a big suite — just enough to prevent regressions.

---

## 7) Quality-of-life for runners (Windows)

### 7.1 Add a simple CLI wrapper (`run_all.py`)
You already have the notebook. Add a thin wrapper so people can run without opening Jupyter:

```bash
python run_all.py --dest-root "E:/AI-Research/datasets/Natural" --execute
```

Internally it should call your existing orchestrator (don’t duplicate logic).

### 7.2 Add a “clean working tree” helper
Add `tools/clean_repo_tree.py` that deletes:
- `__pycache__/`, `*.pyc`
- any accidental output folders if someone ran a pipeline in-place

Then document:

```bash
python tools/clean_repo_tree.py --yes
```

---

## 8) Suggested “final touch-ups” commit plan

**Commit 1 — polish + consistency**
- Delete/move `repo_update_plan_dataset_collector_v2_round2.md`
- Fix `.gitignore`: add `.env`, remove `...`
- Align Python version (choose approach and update CI/env)

**Commit 2 — reproducibility**
- Add README section for `requirements.lock`
- Add lock regeneration instructions

**Commit 3 — CI upgrade**
- Add Windows CI + Python matrix
- Optional: add lock + min-deps jobs + pip cache

**Commit 4 — guardrails**
- Add `pyproject.toml` + `ruff`
- Add minimal `tests/` + `pytest` in CI

---

## Appendix — Quick diffs you can copy/paste

### A) `.gitignore` patch
```diff
+.env
-.env.*
+.env.*
-...
```

### B) `environment.yml` (if switching to 3.11)
```diff
-  - python=3.10
+  - python=3.11
```

### C) `ci.yml` matrix snippet
```yaml
strategy:
  matrix:
    os: [ubuntu-latest, windows-latest]
    python-version: ["3.10", "3.11"]

runs-on: ${{ matrix.os }}
```
