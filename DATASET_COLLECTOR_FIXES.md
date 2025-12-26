# Dataset Collector – Fix List Before Running

This repo is **very close**, but there are a couple of **hard failures** that will stop the “run all pipelines” flow immediately, plus some documentation/UX mismatches that will confuse future runs.

This document lists *everything that should be fixed* so you can reliably run either:
- the **JupyterLab notebook** (`dataset_collector_run_all_pipelines.ipynb`), or
- the **Windows-first orchestrator** (`tools/build_natural_corpus.py`).

---

## 0) Summary of blockers

### Blocker A — Notebook runs the CLI wrong
File: `dataset_collector_run_all_pipelines.ipynb` (cell 6)

Problems:
- It passes `--dry-run` (your `run_pipeline.sh` wrappers **do not accept** `--dry-run`; dry-run is the default when `--execute` is absent).
- It does **not** pass `--targets ...` (your `run_pipeline.sh` wrappers **require** `--targets FILE`).

Result: the notebook fails immediately on the first pipeline.

### Blocker B — SyntaxError in `tools/patch_targets.py`
File: `tools/patch_targets.py`

Problem:
- `_parse_dataset_root()` contains invalid escaped quotes:
  ```py
  if ":" in value or "\\" in value:
  ```
Result:
- `tools/build_natural_corpus.py` cannot import `patch_targets` and will crash immediately.

---

## 1) Fix `tools/patch_targets.py` (hard required)

File: `tools/patch_targets.py`

### What to change
Replace the broken function:

```py
def _parse_dataset_root(value: str) -> PurePath:
    if ":" in value or "\\" in value:
        return PureWindowsPath(value)
    return PurePath(value)
```

with:

```py
def _parse_dataset_root(value: str) -> PurePath:
    if ":" in value or "\" in value:
        return PureWindowsPath(value)
    return PurePath(value)
```

### Why this matters
`tools/build_natural_corpus.py` depends on this module to patch each pipeline’s `targets_*.yaml` so paths land under your unified Natural corpus root.

---

## 2) Fix the Jupyter “run all pipelines” notebook (hard required)

File: `dataset_collector_run_all_pipelines.ipynb` (cell 6)

### Problems to fix
1) **Stop using `--dry-run`.**  
   Your shell wrappers are **dry-run by default**; only `--execute` triggers writes.

2) **Always provide `--targets PATH`.**  
   All pipeline `run_pipeline.sh` scripts require it.

### Recommended fix (use `tools/pipeline_map.yaml`)
Update the final cell so it:
- loads `tools/pipeline_map.yaml`
- maps each `*_pipeline_v2/` directory to its `targets_yaml`
- builds commands like:
  - dry-run: `bash run_pipeline.sh --targets targets_*.yaml --stage <stage>`
  - execute: `... --execute`

**Drop-in replacement for cell 6:**

```python
import os
import subprocess
from pathlib import Path
import yaml

STAGES = ["classify","acquire_green","acquire_yellow","screen_yellow","merge","difficulty","catalog"]
EXECUTE = False  # True = writes, False = dry-run

repo_root = Path(repo_root)

pipeline_map_path = repo_root / "tools" / "pipeline_map.yaml"
pipeline_map = yaml.safe_load(pipeline_map_path.read_text(encoding="utf-8"))
targets_for = {k: v["targets_yaml"] for k, v in (pipeline_map.get("pipelines") or {}).items()}

env = os.environ.copy()

for pipeline in pipeline_dirs:
    run_script = pipeline / "run_pipeline.sh"
    if not run_script.exists():
        print(f"Skipping {pipeline.name}: run_pipeline.sh not found.")
        continue

    targets_name = targets_for.get(pipeline.name)
    if not targets_name:
        raise RuntimeError(f"No targets_yaml entry for {pipeline.name} in tools/pipeline_map.yaml")
    targets_path = pipeline / targets_name
    if not targets_path.exists():
        raise RuntimeError(f"Targets YAML not found for {pipeline.name}: {targets_path}")

    print(f"\n=== Running {pipeline.name} ({'EXECUTE' if EXECUTE else 'DRY'}) ===")
    for stage in STAGES:
        cmd = ["bash", str(run_script), "--targets", str(targets_path), "--stage", stage]
        if EXECUTE:
            cmd.append("--execute")
        print(" ".join(cmd))
        subprocess.run(cmd, check=True, env=env, cwd=pipeline)
```

---

## 3) Fix README CLI contract (doc mismatch)

File: `README.md`

### What is currently incorrect
The repo README claims a standardized contract:

- `--dry-run` exists
- `--targets` is not mentioned

But your actual `run_pipeline.sh` wrappers (per-pipeline) behave like:

- `--targets FILE` **required**
- dry-run is the default (no `--dry-run` flag)
- `--execute` enables writes

### Two valid ways to resolve
Pick one (either is fine):

#### Option 1 (recommended): Update README + notebook to match the scripts
Change “Standard CLI contract” to something like:

```bash
./run_pipeline.sh --targets <targets.yaml> --stage <stage> [--execute] [other flags]
# dry-run is default unless --execute is present
```

…and remove `--dry-run` usage/examples.

#### Option 2: Add `--dry-run` support to every `run_pipeline.sh`
If you prefer explicit dry-run flags, add a case:

```bash
--dry-run)
  # no-op; dry-run is the default when --execute is absent
  shift
  ;;
```

…but you must add it consistently across all pipeline wrappers.

---

## 4) Windows vs WSL execution: clarify + choose a “primary” path

### Current behavior
- The notebook runs `bash run_pipeline.sh ...`
- On native Windows, this requires:
  - running Jupyter in **WSL**, or
  - having **Git Bash** available and working reliably.

### What to fix/improve
- Update `README.md` to explicitly say:
  - **Notebook path = bash required**
  - **Windows path = use `tools/build_natural_corpus.py`**

- Consider adding a dedicated “Quickstart” section:
  - WSL/Jupyter flow
  - Windows orchestrator flow

---

## 5) Environment prerequisites: make them explicit

### Jupyter is not in `requirements.txt`
File: `requirements.txt`

That file includes core pipeline deps, but **not**:
- `jupyterlab`
- `ipykernel`

If you expect the notebook to be the primary entrypoint, you should either:
- add a `requirements-dev.txt` with Jupyter bits, or
- document the conda bootstrap commands in README.

### Notebook requirement installs are off by default
File: `dataset_collector_run_all_pipelines.ipynb` (cell 5)

- `INSTALL_REQUIREMENTS = False` means many users will hit missing imports.
- Fix options:
  - default it to `True`, or
  - add a “You probably want to set this True on first run” note directly in the notebook markdown.

---

## 6) Nice-to-have consistency fixes (not strictly required, but reduces friction)

### A) Add a lightweight repo “preflight” check
Add a `tools/preflight.py` (or a Make target) to:
- `python -m compileall tools` (would have caught the SyntaxError)
- validate that each `*_pipeline_v2/` has:
  - `run_pipeline.sh`
  - `targets_*.yaml`
  - `requirements.txt`
- validate that `tools/pipeline_map.yaml` entries exist for each pipeline.

### B) Make `run_pipeline.sh` flags consistent across pipelines
Even if you keep pipeline-specific extras, make sure the shared contract is identical:
- `--targets` required
- `--stage`
- `--execute` (optional)
- optionally accept `--dry-run` as a no-op alias (if you keep it in docs)

---

## 7) Verification checklist (after applying fixes)

1) Confirm the SyntaxError is gone:
   ```bash
   python -m compileall tools
   ```

2) Dry-run one pipeline via notebook (after cell 6 fix):
   - Set `EXECUTE=False`
   - Ensure commands include `--targets ...` and **do not** include `--dry-run`

3) Try the orchestrator (Windows-first):
   ```bash
   python tools/build_natural_corpus.py --dest-root "E:/AI-Research/datasets/Natural" --pipelines math_pipeline_v2
   ```
   Then execute:
   ```bash
   python tools/build_natural_corpus.py --dest-root "E:/AI-Research/datasets/Natural" --pipelines math_pipeline_v2 --execute
   ```

---

## Appendix: Files touched

- `tools/patch_targets.py` ✅ required
- `dataset_collector_run_all_pipelines.ipynb` ✅ required
- `README.md` ✅ strongly recommended
- (optional) `run_pipeline.sh` in each `*_pipeline_v2/` if you want to support `--dry-run`
- (optional) add `requirements-dev.txt` or update README bootstrap instructions
