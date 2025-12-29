# Dataset Collector v2 — Collector‑only Patch List (Repo‑wide)

This document is a **repo-wide, collector-only** patch plan for the current `Dataset_Collector-main` snapshot.

## Goal

Make the repository unambiguously specialize in:

1. **Targeted acquisition** from the `targets_*.yaml` files in each pipeline folder
2. **License / restriction screening** (green/yellow/red decisions + evidence)
3. **Yellow screening + merge** into a canonical `combined/` corpus
4. **Catalog + ledger generation** for traceability

…and **nothing else** (no difficulty sorting, no “final” post-processing stage).

### Definition of done

- `python tools/build_natural_corpus.py ...` runs from a clean environment without crashing
- All pipelines can run at least **`classify → acquire_green → acquire_yellow → screen_yellow → merge → catalog`**
- Output structure is consistent and documented:
  - `raw/…` for direct downloads (green/yellow)
  - `screened_yellow/…` for screened shards
  - `combined/…` for merged canonical output
  - `/_ledger`, `/_catalogs`, `/_queues`, `/_pitches`, `/_manifests`, `/_logs` for metadata

---

## Patch Set 1 — Fix the preflight crash (run‑blocker)

### Problem

`tools/preflight.py` dynamically imports each pipeline’s `acquire_worker.py` to read `STRATEGY_HANDLERS`.  
The module is executed without being registered in `sys.modules`, which can crash during import (commonly via `dataclasses`).

### Fix (minimal and correct)

**File:** `tools/preflight.py`  
**Function:** `_load_strategy_handlers()`

Replace:

```py
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
```

With:

```py
module = importlib.util.module_from_spec(spec)
sys.modules[module_name] = module  # ensure dataclasses / typing can resolve module globals
spec.loader.exec_module(module)
```

That’s it. No other changes required.

### Quick smoke test

From repo root:

```bash
python tools/preflight.py --repo-root . --pipeline-map tools/pipeline_map.yaml
```

You should see: `Preflight checks passed.` (warnings about missing external tools are fine).

---

## Patch Set 2 — Remove “final stage” artifacts (collector‑only clarity)

Right now the repo still implies a downstream “final” stage exists (mostly via `final_root` in config and catalog builders).  
For a collector-only repo, remove these references so the repo communicates one clear output: **`combined/`**.

### 2.1 Stop creating a `final/` directory in the layout

**File:** `tools/init_layout.py`

Locate the `dirs = [...]` list and remove `"final"`.

**Before:**

```py
dirs = [
    "raw",
    "screened_yellow",
    "combined",
    "final",
    "_queues",
    ...
]
```

**After:**

```py
dirs = [
    "raw",
    "screened_yellow",
    "combined",
    "_queues",
    ...
]
```

> Note: if any downstream repo expects `final/`, it should create it there. This collector repo shouldn’t.

---

### 2.2 Stop injecting `final_root` into patched targets

**File:** `tools/patch_targets.py`

Remove the `final_root` injection so patched YAMLs only contain roots this repo produces.

Find:

```py
"final_root": dataset_root / "final",
```

Delete it.

Also remove any references in comments/README text inside that file if present.

---

### 2.3 Remove “final” from catalogs (repo‑wide)

Each pipeline’s `catalog_builder.py` currently includes something like:

```py
final_root = Path(g.get("final_root", "/data/<pipeline>/final"))
...
"final": collect_final_stage(final_root),
```

For collector-only, delete the “final” stage entirely.

#### Option A (recommended): Remove final stage collection

For **every** `*_pipeline_v2/catalog_builder.py`:

1. Remove the `final_root = ...` line
2. Remove the `"final": collect_final_stage(final_root),` entry
3. Delete the `collect_final_stage()` function if present

**Search helper:**

```bash
rg -n "collect_final_stage|final_root" */catalog_builder.py
```

#### Option B (minimal disruption): Keep code but mark as optional (not recommended)

You can keep the code, but you must:
- stop injecting `final_root`
- ensure `collect_final_stage()` returns an empty summary if the path does not exist
- update docs to say “final is not produced here”

This reduces churn but leaves lingering confusion.

---

### 2.4 Delete leftover duplicate targets file (minor cleanup)

Only one pipeline currently has duplicate targets configs:

- `agri_circular_pipeline_v2/targets.yaml` (legacy / confusing)
- `agri_circular_pipeline_v2/targets_agri_circular.yaml` (used by `tools/pipeline_map.yaml`)

**Action:**
- Delete `agri_circular_pipeline_v2/targets.yaml`, or rename it to `targets_agri_circular.yaml.bak`
- Ensure `tools/pipeline_map.yaml` continues to reference `targets_agri_circular.yaml` (it already does)

---

## Patch Set 3 — Strip “difficulty routing” language (without removing useful metadata)

Some pipeline drivers still describe behavior as “difficulty-aware routing,” even though acquisition paths are license/pool-based.

### 3.1 Update misleading comments in pipeline drivers

Across `*_pipeline_v2/pipeline_driver.py`, replace comment phrasing like:

- “difficulty-aware download paths”
- “difficulty routing”

With something accurate, e.g.:

- “optional difficulty estimate metadata (no filesystem routing)”
- “difficulty estimate is recorded in queue rows only”

**Search helper:**

```bash
rg -n "difficulty-aware|difficulty routing" */pipeline_driver.py
```

### 3.2 Keep `difficulty_level` only as metadata (optional)

Many pipeline drivers emit:

```py
"difficulty_level": mr.get("level"),
```

If you want this repo to be strictly “collector + license,” you have two clean choices:

- **Keep it** as a lightweight hint for downstream sorting (recommended)
- **Remove it** entirely to avoid any coupling

If you remove it, do so consistently (and update any downstream consumers).

**Search helper:**

```bash
rg -n '"difficulty_level"' *_pipeline_v2/pipeline_driver.py
```

---

## Patch Set 4 — Update documentation to match collector‑only reality

### 4.1 `docs/output_contract.md`

Replace the line mentioning difficulty routing happening outside the contract.

Suggested replacement paragraph:

- State explicitly: **no difficulty routing occurs in this repo**
- Note that optional fields like `difficulty_level` may exist in queue rows for downstream use

### 4.2 Root `README.md`

Ensure the README describes only these stages:

- classify
- acquire_green
- acquire_yellow
- screen_yellow
- merge
- catalog

…and that it describes `combined/` as the canonical output.

Also remove any wording that suggests this repo produces “final datasets,” unless you explicitly keep a `final/` stage.

---

## Patch Set 5 — Optional hardening (recommended if you want preflight to stay “safe”)

Right now, preflight executes pipeline code to read `STRATEGY_HANDLERS`. Even after the `sys.modules` fix, that still means:

- importing can have side effects
- imports can fail due to optional deps
- preflight becomes less deterministic

### Option: Parse `STRATEGY_HANDLERS` via AST instead of importing

- Parse `acquire_worker.py`
- Find the assignment to `STRATEGY_HANDLERS`
- Extract only **literal string keys**
- Fall back to import only if AST parsing fails

This keeps preflight dependency-free and side-effect-free.

(If you want, I can draft the exact AST-based implementation to drop into `tools/preflight.py`.)

---

## Smoke test plan (Windows + JupyterLab friendly)

### 1) Environment

```bash
python -m venv .venv
# Windows PowerShell:
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt -r requirements-dev.txt
```

### 2) Preflight

```bash
python tools/preflight.py --repo-root . --pipeline-map tools/pipeline_map.yaml
```

### 3) One pipeline end-to-end (dry run)

```bash
python tools/build_natural_corpus.py --repo-root . --pipeline-map tools/pipeline_map.yaml --pipelines math_pipeline_v2 --stages classify acquire_green acquire_yellow --workers 4
```

### 4) One pipeline end-to-end (execute)

```bash
python tools/build_natural_corpus.py --repo-root . --pipeline-map tools/pipeline_map.yaml --pipelines math_pipeline_v2 --stages classify acquire_green acquire_yellow screen_yellow merge catalog --workers 4 --execute
```

### 5) Validate outputs

Confirm these exist under your configured destination root and pipeline dest folder:

- `raw/`
- `screened_yellow/`
- `combined/`
- `_queues/`
- `_ledger/`
- `_catalogs/`

---

## Notes / gotchas

- **External tool warnings:** preflight warns (not errors) if strategies require tools you don’t have (e.g., `aws`, `aria2c`). That’s expected unless those strategies are enabled in your targets.
- **Windows paths:** use forward slashes in YAML (`E:/AI-Research/...`) to avoid escape issues.
- **Downstream sorting:** if you plan to sort by difficulty later, treat this repo’s optional `difficulty_level` as metadata only—do not route files by it here.

