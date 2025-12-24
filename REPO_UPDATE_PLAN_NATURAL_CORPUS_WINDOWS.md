# Dataset Collector — Repository Update Plan (Windows + Natural Corpus Builder)

This document details **everything needed to update** the `Dataset_Collector` repository so you can:

1) Run **all v2 pipelines** from a single entrypoint,  
2) Produce a **sorted Natural corpus** under:

- `E:\AI-Research\datasets\Natural`

3) Ensure outputs are **difficulty-sharded (d01–d10)** and the needed folders are created automatically,  
4) Make the whole system **Windows-first** (no Bash required), while staying compatible with Linux/WSL.

---

## 0) Current state (as observed in repo)

- There are **18** domain pipelines, each in `<domain>_pipeline_v2/`.
- Each pipeline has the same core workers:
  - `pipeline_driver.py` (classify + emit queues)
  - `acquire_worker.py` (download into `raw/`)
  - `yellow_screen_worker.py` (canonicalize / shard yellow)
  - `merge_worker.py` (combine green + screened yellow)
  - `difficulty_worker.py` (route records into `final/<pool>/d01..d10/shards`)
  - `catalog_builder.py` (summary catalog)
- Pipelines are currently driven via **`run_pipeline.sh`** (Bash), and the `targets_*.yaml` configs default to **Linux paths** like `/data/<domain>/...`.

### Known issue to fix immediately
`/safety_incident_pipeline_v2/targets_safety_incident.yaml` is **invalid YAML** due to indentation:
- Under a target’s `output.formats`, the list items `- csv` and `- dbf` are mis-indented under `routing`.

**Fix:** move those items under `output.formats` (see Section 2.1).

---

## 1) Desired output contract under `E:\AI-Research\datasets\Natural`

### 1.1 Domain directory mapping

Your starter `Natural.zip` contains these domain folder names:

| Pipeline folder (repo) | Natural destination folder |
|---|---|
| `math_pipeline_v2` | `math` |
| `physics_pipeline_v2` | `physics` |
| `engineering_pipeline_v2` | `engineering` |
| `materials_science_pipeline_v2` | `materials_science` |
| `metrology_pipeline_v2` | `metrology` |
| `chem_pipeline_v2` | `chemistry` |
| `biology_pipeline_v2` | `biology` |
| `code_pipeline_v2` | `code` |
| `cyber_pipeline_v2` | `cybersecurity` |
| `earth_pipeline_v2` | `earth_science` |
| `nlp_pipeline_v2` | `natural_language_processing` |
| `logic_pipeline_v2` | `logic` |
| `agri_circular_pipeline_v2` | `agriculture_circular_bioeconomy` |
| `econ_stats_decision_adaptation_pipeline_v2` | `econ_decision_science` |
| `kg_nav_pipeline_v2` | `knowledge_graph_navigation` |
| `regcomp_pipeline_v2` | `regulation_compliance` |
| `safety_incident_pipeline_v2` | `safety_incidents` |
| `3d_modeling_pipeline_v2` | `3D_modeling` |

### 1.2 Standard on-disk tree per domain

Each domain should end up with this structure:

```
E:\AI-Research\datasets\Natural\<domain>\
  raw\
    green\{permissive,copyleft,quarantine}\<target_id>\...
    yellow\{permissive,copyleft,quarantine}\<target_id>\...
  screened_yellow\
    {permissive,copyleft,quarantine}\shards\*.jsonl(.gz)
  combined\
    {permissive,copyleft,quarantine}\shards\*.jsonl(.gz)
  final\
    {permissive,copyleft,quarantine}\
      d01\shards\*.jsonl(.gz)
      ...
      d10\shards\*.jsonl(.gz)
  _queues\*.jsonl
  _ledger\*.jsonl
  _pitches\*.jsonl
  _manifests\...
  _catalogs\catalog.json
  _logs\...
```

Notes:
- Pools are consistently: `permissive`, `copyleft`, `quarantine`.
- Difficulty routing is `d01 … d10` (always two digits).
- “Additional folders corresponding with difficulty” means: ensure those `dXX` directories exist **before** writing.

---

## 2) Minimal “make it work on Windows” updates (recommended first)

This phase gets you a working end-to-end run with minimal code churn.

### 2.1 Hotfix the invalid YAML (required)
File:
- `safety_incident_pipeline_v2/targets_safety_incident.yaml`

Problem snippet (current):
```yaml
output:
  pool: permissive
  formats:
  - zip
routing:
  ...
  reason: Road safety fatality event tables.
  - csv
  - dbf
```

Corrected:
```yaml
output:
  pool: permissive
  formats:
    - zip
    - csv
    - dbf
routing:
  ...
  reason: Road safety fatality event tables.
```

After fixing, verify:
- `python -c "import yaml; yaml.safe_load(open('safety_incident_pipeline_v2/targets_safety_incident.yaml','r',encoding='utf-8').read())"`

---

## 3) Add a repo-level Windows orchestrator: `build_natural_corpus.py`

### 3.1 Why
- `run_pipeline.sh` won’t run natively on Windows (without WSL/Git Bash).
- Each `targets_*.yaml` defaults to `/data/...` roots.
- You want one command that:
  - patches configs for a given destination,
  - initializes the folder skeleton,
  - runs all pipelines and stages in order.

### 3.2 Add files (new)
Create a repo folder:

```
tools/
  build_natural_corpus.py
  pipeline_map.yaml
  patch_targets.py
  init_layout.py
  __init__.py
```

#### `tools/pipeline_map.yaml`
Declarative mapping so you don’t hardcode in Python:

```yaml
destination_root: "E:/AI-Research/datasets/Natural"
pipelines:
  math_pipeline_v2:
    dest_folder: "math"
    targets_yaml: "targets_math.yaml"
  physics_pipeline_v2:
    dest_folder: "physics"
    targets_yaml: "targets_physics.yaml"
  ...
  safety_incident_pipeline_v2:
    dest_folder: "safety_incidents"
    targets_yaml: "targets_safety_incident.yaml"
```

### 3.3 Orchestrator CLI
Implement:

```
python tools/build_natural_corpus.py ^
  --repo-root . ^
  --dest-root "E:\AI-Research\datasets\Natural" ^
  --pipelines all ^
  --stages classify acquire_green acquire_yellow screen_yellow merge difficulty catalog ^
  --workers 8 ^
  --execute
```

Defaults:
- **dry-run** unless `--execute` is present.
- `--pipelines all` runs all 18.
- `--stages` default is the full canonical stage order.

### 3.4 What the orchestrator does
For each pipeline:

1) **Patch the pipeline’s targets yaml** into a generated file (do not edit originals).
   - Output location suggestion:
     - `E:/AI-Research/datasets/Natural/<domain>/_manifests/_patched_targets/targets_<domain>_patched.yaml`

2) Patch fields:
   - In `globals`: all of these roots:
     - `raw_root`, `screened_yellow_root`, `combined_root`, `final_root`,
       `ledger_root`, `pitches_root`, `manifests_root`, `queues_root`,
       `catalogs_root`, `logs_root`
   - In `queues.emit[*].path`: force them under the patched `queues_root`.

3) **Initialize folder layout** (idempotent):
   - Create `_queues`, `_logs`, `_catalogs`, `_ledger`, `_pitches`, `_manifests`
   - Create pool folders and `final/<pool>/d01..d10/shards`

4) Run pipeline stage scripts via Python `subprocess`:
   - `classify`: `python pipeline_driver.py --targets <patched> [--no-fetch]`
   - `acquire_green`: `python acquire_worker.py --queue <queues>/green_download.jsonl --targets-yaml <patched> --bucket green --workers N [--execute]`
   - `acquire_yellow`: same but `--bucket yellow`
   - `screen_yellow`: `python yellow_screen_worker.py --targets <patched> --queue <queues>/yellow_pipeline.jsonl [--execute]`
   - `merge`: `python merge_worker.py --targets <patched> [--execute]`
   - `difficulty`: `python difficulty_worker.py --targets <patched> [--execute]`
   - `catalog`: `python catalog_builder.py --targets <patched>`

5) Capture stdout/stderr into:
   - `E:/.../<domain>/_logs/orchestrator_<stage>.log`

### 3.5 Windows path handling rules
- Internally treat paths using `pathlib.Path`.
- When writing YAML, prefer forward slashes: `E:/AI-Research/...` (avoids escape issues).
- Ensure your orchestrator uses `shell=False` in subprocess calls on Windows.

---

## 4) Repository-wide improvements (second pass, but strongly recommended)

This phase reduces “patching” hacks and makes the pipelines intrinsically cross-platform.

### 4.1 Replace Bash runner with Python runner (per pipeline)
Add per pipeline:
- `run_pipeline.py` (cross-platform)
- Keep `run_pipeline.sh` but make it call `python run_pipeline.py ...` (optional)
- Add `run_pipeline.ps1` (optional convenience)

**Goal:** the canonical entrypoint becomes:

```
python run_pipeline.py --targets targets_math.yaml --stage classify --execute
```

The Python runner should:
- map `--stage` to the correct underlying scripts,
- pass through flags (`--execute`, `--dry-run`, `--workers`, etc.),
- normalize platform differences.

### 4.2 Add `globals.dataset_root` and derive all other roots
Current YAML repeats 10 absolute root paths per pipeline, e.g.:

```yaml
globals:
  raw_root: /data/math/raw
  ...
  logs_root: /data/math/_logs
```

Update schema (recommended):
```yaml
globals:
  dataset_root: E:/AI-Research/datasets/Natural/math
  # (optional) allow overrides, but default is derived:
  raw_root: ${dataset_root}/raw
  ...
```

Then update scripts to **derive** when a root is missing:
- `raw_root = dataset_root / "raw"`
- `queues_root = dataset_root / "_queues"`
- etc.

If you don’t want templating (`${...}`), then:
- keep YAML minimal (`dataset_root` only),
- compute everything in code in `resolve_roots()`.

### 4.3 Make queues.emit paths relative
Instead of:
```yaml
queues:
  emit:
    - id: green_download
      path: /data/math/_queues/green_download.jsonl
```

Prefer:
```yaml
queues:
  emit:
    - id: green_download
      filename: green_download.jsonl
```

Then code uses:
- `Path(globals.queues_root) / filename`

This removes hard-coded OS-dependent paths from YAML.

### 4.4 Standardize CLI flags across all workers
Right now, some scripts use:
- `--targets`
- others use `--targets-yaml`
- some have `--execute` only, others have `--dry-run`

Standardize:

- `--targets` always points to the targets YAML
- `--execute` to write outputs
- `--dry-run` optional; default is dry-run if no `--execute`
- add `--dataset-root` override everywhere (optional but useful)

Example:
```
python acquire_worker.py --targets <patched> --queue ... --bucket green --workers 8 --execute
```

### 4.5 Add an `init_layout` stage (per pipeline)
Add a new stage to each pipeline runner that calls shared code to create:

- pool dirs
- `_queues`, `_logs`, etc.
- `final/<pool>/d01..d10/shards`

So you can run:

```
python run_pipeline.py --stage init_layout --execute
```

and guarantee the directory contract exists even before first acquisition.

---

## 5) Suggested shared utility module (optional, best long-term)
To avoid duplicating fixes 18 times, create:

```
collector_core/
  config.py        # read yaml + resolve roots + apply overrides
  layout.py        # init directory skeleton
  subprocesses.py  # run stage subprocess with logging
  paths.py         # windows path helpers (forward slash canonicalization)
  __init__.py
```

Then gradually update pipelines to import from `collector_core`.

This reduces drift and makes future schema updates much easier.

---

## 6) Documentation updates to add

### 6.1 Root README updates
Add a **Windows Quickstart** section:

- Create venv
- Install requirements for all pipelines or a merged set
- Run orchestrator

Example:

```
py -3.11 -m venv .venv
.venv\Scripts\activate
pip install -r math_pipeline_v2\requirements.txt
...
python tools\build_natural_corpus.py --dest-root "E:\AI-Research\datasets\Natural" --pipelines all --execute
```

### 6.2 Add “Output Contract” docs
Add `/docs/output_contract.md` explaining:
- folder tree
- what each stage writes
- what “pitches” and “ledger” are
- naming conventions for shards

---

## 7) Windows operational notes (important)

### 7.1 Long paths
Windows can error when paths exceed ~260 characters.

Recommended:
- Enable Windows long paths in system policy/registry
- `git config --global core.longpaths true`

### 7.2 PowerShell quoting
Use double quotes for paths with spaces; prefer `E:/...` or quoted `E:\...`.

### 7.3 Concurrency
Acquisition uses threads; tune `--workers` based on IO and rate limits.

---

## 8) Validation / acceptance checklist

### 8.1 Config validation
- All 18 `targets_*.yaml` load with `yaml.safe_load(...)`.
- `queues.emit` paths resolve under `_queues`.

### 8.2 Dry-run smoke test
Run orchestrator without `--execute` and confirm:
- logs created
- patched targets written
- planned outputs are correct

### 8.3 First execute test (single pipeline)
Run one pipeline end-to-end on Windows:

```
python tools/build_natural_corpus.py --pipelines math_pipeline_v2 --execute
```

Verify:
- `_queues/green_download.jsonl` exists
- `raw/green/...` has output after acquisition
- `final/permissive/d01/shards/...` exists after difficulty stage

### 8.4 Full run
Run all pipelines and ensure:
- Each domain has `_catalogs/catalog.json`
- Difficulty folders exist across all pools (even if empty)

---

## 9) Implementation order (recommended)
1) Fix safety incident YAML indentation bug (required).
2) Add `tools/` orchestrator (patch-yaml approach).
3) Add layout initializer that pre-creates difficulty folders.
4) Add Python `run_pipeline.py` per pipeline (cross-platform runner).
5) Introduce `globals.dataset_root` + derived roots, then remove absolute paths.
6) Standardize CLI flags and share `collector_core` utilities.

---

## Appendix A — “init layout” folder creation details

For each domain folder:
- Create roots:
  - `raw/`, `screened_yellow/`, `combined/`, `final/`,
  - `_queues/`, `_logs/`, `_catalogs/`, `_ledger/`, `_pitches/`, `_manifests/`
- For each pool in `{permissive, copyleft, quarantine}`:
  - Create:
    - `raw/green/<pool>/`
    - `raw/yellow/<pool>/`
    - `screened_yellow/<pool>/shards/`
    - `combined/<pool>/shards/`
    - `final/<pool>/d01..d10/shards/`

This ensures “additional folders corresponding with difficulty” exist even before any records are written.

---

## Appendix B — Notes on compatibility with existing scripts

The “minimal” orchestrator approach works **without rewriting** pipeline scripts, because it:
- generates patched targets YAML using Windows destination roots,
- runs the existing scripts exactly as-is.

The “second pass” refactor reduces maintenance by:
- replacing Bash with Python entrypoints,
- eliminating absolute Linux paths from YAML,
- centralizing shared code.

