# Dataset Collector v2 — Collector-Only Fixes (Windows-First)

This is the **current, trimmed fix list** for **collector-only** usage on **native Windows** (JupyterLab). The goal is **license screening + acquisition** of targets in each pipeline’s `targets_*.yaml`.

---

## Scope (collector-only)
- **Stages to run by default:** `classify`, `acquire_green`, `acquire_yellow`
- Downstream stages (`screen_yellow`, `merge`, `catalog`) are **optional** and only meaningful if you add a normalization stage that outputs JSONL.

---

## Priority fixes

### 1) Notebook runner: Windows-first (no bash)
The existing notebook assumes `bash run_pipeline.sh`, which fails on native Windows.

**Use the Windows-first orchestrator instead**: `tools/build_natural_corpus.py`.

```python
import os, sys, subprocess
from pathlib import Path

# --- EDIT THIS ---
DEST_ROOT = r"E:\AI-Research\datasets\Natural"   # unified dataset root
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

---

### 2) Remove “noop” targets (enabled but not acquirable)
If a target’s `download.strategy` is `none` (or missing), it silently **no-ops** in `acquire_worker.py`.

**Preferred fix:** set `enabled: false` for these targets (collector-only repo).

Known enabled noop targets (from the current map):
- `math_pipeline_v2`: `synthetic_math_problems` (`none`)
- `chem_pipeline_v2`: `synthetic_chemistry_problems` (`none`)
- `chem_pipeline_v2`: `pubchem_derived_computed_only` (missing)
- `biology_pipeline_v2`: `synthetic_biology_problems` (`none`)
- `code_pipeline_v2`: `synthetic_code_problems_v1` (`none`)
- `cyber_pipeline_v2`: `synthetic_cyber_threatmodeling_problems` (`none`)
- `earth_pipeline_v2`: `synthetic_earth_cards` (`none`)
- `logic_pipeline_v2`: `synthetic_logic_problems` (`none`)
- `logic_pipeline_v2`: `sep_stanford_encyclopedia_of_philosophy` (`none`)
- `regcomp_pipeline_v2`: many “commercial standards/codes” placeholders (`none`)
- `3d_modeling_pipeline_v2`: `synthetic_cad_printing_problems` (`none`)
- `3d_modeling_pipeline_v2`: `thingi10k_safe_cc0_ccby` (missing)

---

### 3) Strategies referenced but not implemented
These targets have real strategies but no handler in `acquire_worker.py` (pipeline-specific).
Disable or switch strategy until support is added.

- **Earth**: `copernicus_cmems_metadata` (`api_tabular` not implemented)
- **Materials**: `materials_project_api`, `nomad_api`, `oqmd_api_or_dump` (`api` not implemented)
- **Materials**: `pmc_oa_materials_text` (`pmc_oa` not implemented)

---

### 4) Missing Python deps for enabled strategies
Ensure pipeline `requirements.txt` include deps required by enabled targets:

- Add **`datasets` + `pyarrow`** to:
  - `math_pipeline_v2`
  - `engineering_pipeline_v2`
  - `materials_science_pipeline_v2`
  - `biology_pipeline_v2`
  - `code_pipeline_v2`
  - `nlp_pipeline_v2`
  - `regcomp_pipeline_v2`
  - `safety_incident_pipeline_v2` *(pyarrow is missing entirely)*

- Add **`boto3`** to:
  - `3d_modeling_pipeline_v2` (needed for `s3_public`)

---

## Optional: preflight validator (recommended)
Add a `tools/preflight.py` to catch:
- missing targets YAMLs
- enabled targets with unsupported/`none` strategies
- missing imports for enabled strategies

This avoids “successful run with no downloads.”

---

## Known-good Windows run (collector-only)
1) Install deps:
```bash
pip install -r requirements.txt -r requirements-dev.txt
```
2) Use the Windows-first notebook cell above.
3) Start with:
- `EXECUTE = False`
- `STAGES = ["classify"]`
4) Then run:
- `STAGES = ["classify","acquire_green","acquire_yellow"]`
- `EXECUTE = True`
