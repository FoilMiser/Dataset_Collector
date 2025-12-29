# Dataset Collector v2 — Fixes & Updates (post difficulty-removal)

This doc is written against the repo snapshot in `Dataset_Collector-main (2).zip` (unpacked at `/mnt/data/Dataset_Collector-main/Dataset_Collector-main`).

Your stated goal for this version is **collector-only**:
- **Acquire the datasets listed in each pipeline’s `targets_*.yaml`**
- **License-screen / bucket them (GREEN/YELLOW/RED)**
- Be runnable from **JupyterLab on native Windows** (no WSL required)

Right now you’re *very close* on the **classify + acquire** pieces, but there are a handful of **hard mismatches** that will cause confusing “no-op” runs and/or silently skip enabled targets.

---

## What’s good already

- The repo is consistently organized as `*_pipeline_v2/` folders with:
  - `pipeline_driver.py` (license screening + queue emission)
  - `acquire_worker.py` (download strategies)
  - `run_pipeline.sh` wrappers (stage orchestration)
- `tools/build_natural_corpus.py` + `tools/pipeline_map.yaml` is a solid **Windows-first orchestrator** that:
  - patches each pipeline targets YAML to a unified dataset root
  - runs stages in a consistent order (already updated to exclude difficulty)
- The license screening logic in `pipeline_driver.py` is generally robust:
  - SPDX allow/conditional/deny buckets
  - restriction phrase scanning
  - denylist gating with severity
  - conservative downgrade to YELLOW when evidence is missing in `--no-fetch` mode

---

## Top issues to fix (priority order)

### 1) Notebook cell bug + stage list mismatch
File: `dataset_collector_run_all_pipelines.ipynb` (final code cell)

**Problems**
- `STAGES` still includes `difficulty` even though that stage has been removed repo-wide.
- The cell has a **broken f-string** split across lines:

```py
print(f"
=== Running {pipeline.name} ({'EXECUTE' if EXECUTE else 'DRY'}) ===")
```

That will raise a `SyntaxError` the moment you run the notebook.

**Fix**
- Remove `difficulty` from `STAGES`
- Fix the print string to a single line.

✅ See “Notebook: drop-in replacement cells” below.

---

### 2) Windows/JupyterLab reality: the notebook still assumes bash
The notebook runs:

- `bash <pipeline>/run_pipeline.sh ...`

On native Windows, that only works if you have a bash layer (WSL or Git Bash) and it behaves correctly.

**Recommendation (best)**
- Make the notebook call the **Windows-first orchestrator**:
  - `python tools/build_natural_corpus.py ...`
- Keep the bash-based notebook flow as an optional/advanced path, but not the default.

✅ See “Notebook: Windows-first runner cell” below.

---

### 3) Enabled targets that cannot be acquired (will silently NOOP)
Your `acquire_worker.py` treats missing/`none` strategy as a **noop** (not an error):

```py
handler = STRATEGY_HANDLERS.get(strat)
if not handler:
    results = [{"status": "noop", "note": f"unsupported strategy: {strat}"}]
```

That means **enabled targets** with `download.strategy: none` (or missing) will appear “processed” but **no files will be downloaded**.

These are mostly **synthetic / derived** targets that belong in another pipeline, but they are currently **enabled** in the natural collector.

**Fix options**
- **Preferred for this collector repo:** set `enabled: false` for these targets
- Alternative: implement a `build_worker.py` stage and add it to `run_pipeline.sh` (bigger scope)

**Currently enabled “noop” targets (from pipeline_map.yaml-selected targets YAMLs):**
- `math_pipeline_v2`: `synthetic_math_problems` (strategy `none`)
- `chem_pipeline_v2`: `synthetic_chemistry_problems` (strategy `none`)
- `chem_pipeline_v2`: `pubchem_derived_computed_only` (strategy missing)
- `biology_pipeline_v2`: `synthetic_biology_problems` (strategy `none`)
- `code_pipeline_v2`: `synthetic_code_problems_v1` (strategy `none`)
- `cyber_pipeline_v2`: `synthetic_cyber_threatmodeling_problems` (strategy `none`)
- `earth_pipeline_v2`: `synthetic_earth_cards` (strategy `none`)
- `logic_pipeline_v2`: `synthetic_logic_problems` (strategy `none`)
- `logic_pipeline_v2`: `sep_stanford_encyclopedia_of_philosophy` (strategy `none`)
- `regcomp_pipeline_v2`: many “commercial standards/codes” placeholders (strategy `none`)
- `3d_modeling_pipeline_v2`: `synthetic_cad_printing_problems` (strategy `none`)
- `3d_modeling_pipeline_v2`: `thingi10k_safe_cc0_ccby` (strategy missing)

If you don’t disable these, the notebook can “run successfully” while producing empty outputs for those targets.

---

### 4) Enabled targets with strategies **not implemented** in `acquire_worker.py`
These won’t download either (same “noop”), but the root cause is different: the strategy is real, but not wired.

**A) Earth pipeline**
- `earth_pipeline_v2` → target `copernicus_cmems_metadata`
  - `download.strategy: api_tabular`
  - `acquire_worker.py` does **not** implement `api_tabular`

**B) Materials pipeline**
- `materials_science_pipeline_v2` → targets:
  - `materials_project_api` (`api`)
  - `nomad_api` (`api`)
  - `oqmd_api_or_dump` (`api`)
  - `pmc_oa_materials_text` (`pmc_oa`)
- `acquire_worker.py` does **not** implement `api` or `pmc_oa` in this pipeline.

**Fix**
- Either:
  - implement these strategies in *every* pipeline’s `acquire_worker.py` (repo-wide consistency), or
  - change those targets to use a supported strategy (e.g., `http`, `zenodo`, `dataverse`, `git`), or
  - disable those targets until you add support.

---

### 5) Missing required Python deps for enabled targets (will error at runtime)
Some pipelines have enabled targets that require optional dependencies, but the dependency is commented out (or missing) in that pipeline’s `requirements.txt`.

**Fix: uncomment/add these deps per pipeline (based on enabled targets in pipeline_map.yaml):**
- Add **`datasets` + `pyarrow`** to:
  - `math_pipeline_v2`
  - `engineering_pipeline_v2`
  - `materials_science_pipeline_v2`
  - `biology_pipeline_v2`
  - `code_pipeline_v2`
  - `nlp_pipeline_v2`
  - `regcomp_pipeline_v2`
  - `safety_incident_pipeline_v2`  *(pyarrow is currently missing, not just commented)*
- Add **`boto3`** to:
  - `3d_modeling_pipeline_v2` (because `thingi10k_raw` uses `s3_public`)

If these aren’t fixed, you’ll get errors like:
- `datasets import failed: No module named 'datasets'`
- `boto3 import failed: No module named 'boto3'`

---

### 6) “screen_yellow / merge / catalog” are currently format-assumption traps
This is the biggest *behavior vs. contract* mismatch:

- `yellow_screen_worker.py` only processes `raw/yellow/<pool>/<target_id>/*.jsonl(.gz)`
- `merge_worker.py` only merges `raw/green/**/**/*.jsonl(.gz)` plus screened yellow shards

But your acquisition handlers generally download:
- archives, PDFs, HTML, code repos, or
- HuggingFace datasets saved via `save_to_disk()` (Arrow format)

So, unless a target *already* produces JSONL in the expected location, you’ll see:
- `screen_yellow` do almost nothing
- `merge` produce nearly empty combined shards
- `catalog` under-report what was actually acquired

**You have two clean paths:**

#### Path A (recommended for “collector-only”): remove downstream stages from the default flow
- Default stages become: **`classify`, `acquire_green`, `acquire_yellow`**
- Downstream normalization/canonicalization happens in a future dedicated pipeline

This matches your “specialize in acquiring + license screening” goal.

#### Path B (if you want this repo to output ready-to-train JSONL shards): add normalizers
- Add a **format normalization stage** that converts acquired sources to canonical records:
  - HF datasets → export to JSONL shards
  - archives → extract + parse
  - PDFs/HTML → text extraction
- Then `merge` becomes meaningful.

If you don’t do this, keep `screen_yellow/merge/catalog` as “optional / best-effort” and document the limitations.

---

## Notebook: drop-in replacement cells

### Option 1 (recommended): Windows-first notebook runner (no bash)
This uses `tools/build_natural_corpus.py` which already knows pipeline_map.yaml and patches targets for your dataset root.

Create a new final cell (or replace the existing stage loop cell) with:

```python
import os, sys, subprocess
from pathlib import Path

# --- EDIT THIS ---
DEST_ROOT = r"E:\AI-Research\datasets\Natural"   # your unified Natural root
EXECUTE = False   # True = downloads/writes; False = plan-only
WORKERS = 6       # acquisition parallelism
# ---------------
STAGES = ["classify", "acquire_green", "acquire_yellow"]  # collector-only

repo_root = Path.cwd()
while repo_root.name != "Dataset_Collector-main" and repo_root.parent != repo_root:
    repo_root = repo_root.parent

cmd = [
    sys.executable, str(repo_root / "tools" / "build_natural_corpus.py"),
    "--dest-root", DEST_ROOT,
    "--workers", str(WORKERS),
    "--stages", ",".join(STAGES),
]
if EXECUTE:
    cmd.append("--execute")

print(" ".join(cmd))
subprocess.run(cmd, check=True, cwd=str(repo_root))
```

**Why this is better**
- Works in native Windows JupyterLab
- Uses your existing pipeline_map.yaml
- Automatically patches targets to the chosen DEST_ROOT

---

### Option 2: Keep the per-pipeline bash runner (WSL/Git Bash only)
If you still want this mode, fix the existing cell:

```python
import os
import subprocess
from pathlib import Path
import yaml

STAGES = ["classify","acquire_green","acquire_yellow","screen_yellow","merge","catalog"]  # no difficulty
EXECUTE = False  # True = writes, False = dry-run

repo_root = Path.cwd()
while repo_root.name != "Dataset_Collector-main" and repo_root.parent != repo_root:
    repo_root = repo_root.parent

pipeline_map = yaml.safe_load((repo_root / "tools" / "pipeline_map.yaml").read_text(encoding="utf-8"))
targets_for = {k: v["targets_yaml"] for k, v in (pipeline_map.get("pipelines") or {}).items()}

pipeline_dirs = [repo_root / name for name in targets_for.keys()]

env = os.environ.copy()

for pipeline in pipeline_dirs:
    run_script = pipeline / "run_pipeline.sh"
    targets_path = pipeline / targets_for[pipeline.name]

    print(f"\n=== Running {pipeline.name} ({'EXECUTE' if EXECUTE else 'DRY'}) ===")
    for stage in STAGES:
        cmd = ["bash", str(run_script), "--targets", str(targets_path), "--stage", stage]
        if EXECUTE:
            cmd.append("--execute")
        print(" ".join(cmd))
        subprocess.run(cmd, check=True, env=env, cwd=str(pipeline))
```

---

## Docs that must be updated to match the new scope

### 1) `README.md`
Update:
- Stage list: remove `difficulty`
- Examples: remove any `difficulty` references
- Add a Windows-first “Run in JupyterLab” section that uses `tools/build_natural_corpus.py`

Also clarify external tools:
- `git` must be installed for `download.strategy: git`
- `aws` CLI must be installed for `s3_sync`/`aws_requester_pays` targets (kg_nav)

### 2) `docs/output_contract.md`
Currently still claims:

- `final/d01..d10`
- a `difficulty` stage

Update the contract to either:
- collector-only outputs (raw + ledgers + queues + manifests), or
- collector + normalizer outputs (if you choose Path B)

### 3) `DATASET_COLLECTOR_FIXES.md`
This file is now out of date (it references a patch_targets SyntaxError that is already fixed).
Replace it with the real current fix list (this document, or a trimmed version).

---

## Targets YAML hygiene checklist (collector-only version)

For each pipeline selected in `tools/pipeline_map.yaml`, enforce:

1. **If `enabled: true`, then `download.strategy` must be implemented and non-`none`.**
2. Any placeholder / future-work targets should be:
   - `enabled: false`, and
   - retained with a comment or `notes:` field.

**Concrete edits to make now (minimum):**
- Disable synthetic/derived targets (listed above in issue #3).
- For Earth + Materials:
  - either disable the API-based targets, or change them to supported strategies.

---

## Requirements fixes (per pipeline)

Where you see these lines commented out, uncomment them:

### HuggingFace-enabled pipelines
Add/uncomment in `<pipeline>/requirements.txt`:

```txt
datasets>=2.20.0
pyarrow>=14.0.0
```

Pipelines:
- math
- engineering
- materials_science
- biology
- code
- nlp
- regcomp
- safety_incident *(pyarrow is missing; add it)*

### 3D pipeline (s3_public)
Uncomment:

```txt
boto3>=1.34.0
```

---

## Strongly recommended: add a small preflight validator
Add `tools/preflight.py` that checks:

- `tools/pipeline_map.yaml` pipelines exist and their `targets_yaml` exists
- all enabled targets have:
  - a supported strategy (or are explicitly disabled)
  - required python deps installed (best-effort: try imports)
- optional: warn if a target uses a strategy that requires external tools (`git`, `aws`)

This prevents the “runs successfully but did nothing” failure mode.

---

## Quick “known-good” run recipe (JupyterLab, Windows)

1) Create/activate env and install deps:
```bash
pip install -r requirements.txt -r requirements-dev.txt
```

2) Open `dataset_collector_run_all_pipelines.ipynb`

3) Use **Option 1** runner cell (Windows-first) and start with:
- `EXECUTE = False`
- `STAGES = ["classify"]`

4) After verifying queues/manifests look correct, run:
- `STAGES = ["classify","acquire_green","acquire_yellow"]`
- `EXECUTE = True`

That will get you to a reliable “download + license-screened queues/ledgers” state without pretending you’ve already normalized everything into training shards.

---

## Summary
You successfully removed difficulty sorting from the orchestrator, but you still need to:
- fix the notebook execution cell
- clean up enabled targets that cannot be acquired
- add missing deps for enabled strategies
- decide whether this repo is **collector-only** (recommended) or also a **normalizer/sharder** (bigger scope)

Once the above is addressed, the pipeline will be correctly specialized for targeted acquisition + license screening and will run cleanly from JupyterLab on Windows.
