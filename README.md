# Dataset Collector

## Overview
The Dataset Collector repository organizes a family of domain-specific data collection pipelines under `*_pipeline_v2/` directories. The primary way to run the full suite is the JupyterLab notebook `dataset_collector_run_all_pipelines.ipynb`, which executes every domain pipeline sequentially in one session while prompting for required API keys and optionally installing per-pipeline requirements. The collector's canonical output is the `combined/` stage; no "final" post-processing stage is produced here. See `docs/output_contract.md` for the full output layout.

Each `*_pipeline_v2` directory represents a self-contained pipeline for a domain:

- `3d_modeling_pipeline_v2`: 3D modeling datasets and task sources.
- `agri_circular_pipeline_v2`: agriculture and circular-economy datasets.
- `biology_pipeline_v2`: biological datasets and task sources.
- `chem_pipeline_v2`: chemistry datasets and task sources.
- `code_pipeline_v2`: programming/code datasets and task sources.
- `cyber_pipeline_v2`: cybersecurity datasets and task sources.
- `earth_pipeline_v2`: earth science datasets and task sources.
- `econ_stats_decision_adaptation_pipeline_v2`: economics, statistics, decision, and adaptation datasets.
- `engineering_pipeline_v2`: engineering datasets and task sources.
- `kg_nav_pipeline_v2`: knowledge graph/navigation datasets and task sources.
- `logic_pipeline_v2`: logic/reasoning datasets and task sources.
- `materials_science_pipeline_v2`: materials science datasets and task sources.
- `math_pipeline_v2`: math datasets and task sources.
- `metrology_pipeline_v2`: metrology datasets and task sources.
- `nlp_pipeline_v2`: NLP datasets and task sources.
- `physics_pipeline_v2`: physics datasets and task sources.
- `regcomp_pipeline_v2`: regulatory/compliance datasets and task sources.
- `safety_incident_pipeline_v2`: safety incident datasets and task sources.

## Safety model

Dataset Collector v2 uses three safety buckets to control merges:

- **GREEN**: allowed to merge automatically.
- **YELLOW**: requires manual review before merging.
- **RED**: do not collect or merge.

YELLOW items must be reviewed and approved before they are eligible for the combined corpus.

## Known Limitations

- **Yellow-screen boundaries**: The `acquire_yellow` and `screen_yellow` stages only apply to targets explicitly marked as YELLOW in pipeline target YAMLs. Anything not labeled YELLOW is outside the yellow-screen workflow, and no automatic promotion to GREEN occurs without human review.
- **Excluded sources**: RED-labeled sources are excluded by design and are not collected or merged. Some targets may be disabled in `targets_*.yaml` or omitted from `tools/pipeline_map.yaml`, which means they are intentionally not part of the run.
- **Non-goals**: This repository does not perform data cleaning, deduplication across domains, or final dataset curation beyond the per-pipeline merge step. It also does not guarantee license compatibility checks outside the configured `license_map.yaml` entries.
- **Corpus constraints**: Outputs are constrained to the `combined/` stage described in `docs/output_contract.md`; there is no final, unified post-processing stage here. Downstream consumers should expect per-pipeline catalogs and manifests with varying completeness based on enabled targets and available credentials.

## Run all pipelines via JupyterLab

1. Launch JupyterLab from the repository root (Windows, macOS, or Linux; `bash` is optional unless you plan to run shell-based cells).
2. Open `dataset_collector_run_all_pipelines.ipynb`.
3. Run the notebook cells in order to execute each `*_pipeline_v2` pipeline sequentially.

The notebook prompts for missing environment variables (for example `GITHUB_TOKEN` or `CHEMSPIDER_API_KEY`) and can install each pipeline's requirements before starting its stages.

## Single-command run, outputs, and clean-room reruns

For the shortest single-command run, expected output folders, log/manifest/ledger locations,
and clean-room rerun steps (what to delete vs. keep), see
`docs/run_instructions.md`.

## Standard CLI contract (`run_pipeline.sh`)

All `*_pipeline_v2` directories expose a `run_pipeline.sh` entrypoint with a shared CLI contract. Individual pipelines may add additional flags, but the following is standardized.

```bash
./run_pipeline.sh --targets <targets.yaml> --stage <stage> [--execute] [other flags]

Flags:
  --targets <path>     Required path to the pipeline targets YAML.
  --stage <stage>      Stage name to execute (see Stage names).
  --execute            Perform the actual work (required for writes).
  --help               Show usage.
```

### Stage names
Stages are pipeline-specific, but common stage names include:

- `classify`
- `acquire_green`
- `acquire_yellow`
- `screen_yellow`
- `merge`
- `catalog`

## Example sequential execution flow

A typical end-to-end execution sequence:

```bash
./run_pipeline.sh --targets targets_math.yaml --stage classify --execute
./run_pipeline.sh --targets targets_math.yaml --stage acquire_green --execute
./run_pipeline.sh --targets targets_math.yaml --stage acquire_yellow --execute
./run_pipeline.sh --targets targets_math.yaml --stage screen_yellow --execute
./run_pipeline.sh --targets targets_math.yaml --stage merge --execute
./run_pipeline.sh --targets targets_math.yaml --stage catalog --execute
```

To preview the actions without writing data, omit `--execute` (dry-run):

```bash
./run_pipeline.sh --targets targets_math.yaml --stage classify
```

## Quickstart options

### Jupyter (Windows-first, bash optional)

The notebook runs on Windows-first Python. Use `bash run_pipeline.sh ...` cells if
you have WSL or another shell with `bash` available; otherwise use the Windows-native
orchestrator below.

### CLI wrapper (run all pipelines)

If you prefer a minimal CLI entrypoint instead of the notebook, use the thin wrapper
around the Windows-first orchestrator:

```bash
python run_all.py --dest-root "E:/AI-Research/datasets/Natural" --execute
```

### Windows-first Jupyter (Natural corpus)

If you prefer to run a smaller subset of stages from Jupyter on Windows, you can
call the Windows-native orchestrator directly from a notebook cell:

```powershell
py -3.11 -m venv .venv
.venv\Scripts\activate
pip install -r math_pipeline_v2\requirements.txt
# repeat for other pipeline requirements as needed

python tools\build_natural_corpus.py --dest-root "E:\AI-Research\datasets\Natural" --pipelines all --stages classify,acquire_green,acquire_yellow --execute
```

To preview the actions without writing data, omit `--execute` (dry-run).

#### Prereqs (Windows)

Install the following tools if you plan to use targets that rely on them:

- **Git for Windows** (required for `download.strategy: git`): https://git-scm.com/download/win
- **AWS CLI v2** (required for `s3_sync` or `aws_requester_pays`): https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html
- **aria2** (required for `download.strategy: torrent`): https://aria2.github.io/

### Windows Quickstart (Natural corpus, optional)

Use the Windows-first orchestrator to run all pipelines sequentially and emit the
Natural corpus layout under a single destination root. This is an optional alternative to the JupyterLab notebook flow.

```powershell
py -3.11 -m venv .venv
.venv\Scripts\activate
pip install -r math_pipeline_v2\requirements.txt
# repeat for other pipeline requirements as needed

python tools\build_natural_corpus.py --dest-root "E:\AI-Research\datasets\Natural" --pipelines all --execute
```

To preview the actions without writing data, omit `--execute` (dry-run). See
`docs/output_contract.md` for the expected on-disk layout.

## Toolchain (external tools)

Some targets depend on external tools. Install them as needed:

- **Git** (required for `download.strategy: git`): https://git-scm.com/download/win
- **7zip/unzip** (optional; for large archive extraction): https://www.7-zip.org/
- **AWS CLI v2** (required for `s3_sync` or `aws_requester_pays`): https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html
- **huggingface-cli** (optional; if you add Hugging Face auth flows later): https://huggingface.co/docs/huggingface_hub/quick-start#login

## Expected directory structure & configurable paths

Within each `*_pipeline_v2` directory, you should expect:

```
*_pipeline_v2/
  run_pipeline.sh
  requirements.txt
  pipeline_driver.py
  acquire_worker.py
  targets_*.yaml
  license_map.yaml
```

Two global configuration entries determine where queues and catalogs are stored:

- `globals.queues_root`: root path for pipeline queues (intermediate work items).
- `globals.catalogs_root`: root path for pipeline catalogs.

Update these in the pipeline’s configuration files (for example `targets_*.yaml` in each pipeline directory) to control where outputs are written.

## Prerequisites & execution notes

- **Python**: Each pipeline depends on Python; version and additional tools may vary by pipeline.
  Tested on Python 3.10 and 3.11.
- **Requirements**: Install per-pipeline dependencies via that pipeline’s `requirements.txt`.
- **Notebook dependencies**: `jupyterlab` and `ipykernel` are tracked in `requirements-dev.in` and installed from `requirements-dev.constraints.txt`.
- **External tools**: `git` is required when a target uses `download.strategy: git`. The AWS CLI is required for `s3_sync` or `aws_requester_pays` download modes. `aria2c` is required for `download.strategy: torrent`.
- **Dry-run vs execute**: dry-run is the default when `--execute` is absent. Use `--execute` only when you intend to modify data or produce outputs.

## Reproducible installs (recommended)

```bash
pip install -r requirements.constraints.txt
pip install -r requirements-dev.constraints.txt
```

### Regenerate the lock files

These constraints are version-pinned but do not use hash checking.

```bash
uv pip compile requirements.in -o requirements.constraints.txt
uv pip compile requirements-dev.in -o requirements-dev.constraints.txt
```

## Preflight validation

Run the preflight checker to validate pipeline map entries, verify target YAML paths, and detect enabled targets with missing or unsupported download strategies. It also warns about missing optional dependencies or external tools needed by enabled strategies.

```bash
python -m tools.preflight
```

To point at a custom pipeline map location:

```bash
python -m tools.preflight --pipeline-map tools/pipeline_map.yaml
```

For local runs, copy `tools/pipeline_map.sample.yaml` to something like `tools/pipeline_map.local.yaml`, set `destination_root` to your dataset folder, and pass it via `--pipeline-map` (or use `--dest-root` when running `tools/build_natural_corpus.py`). This keeps user-specific paths out of version control.

## Repo validation

Validate all enabled targets across pipeline configs and emit a JSON summary:

```bash
python -m tools.validate_repo --output tools/validate_report.json
```

Fail the run if warnings are present:

```bash
python -m tools.validate_repo --strict --output tools/validate_report.json
```

## Cleaning local artifacts

If you ran pipelines inside the repo and need to reset the tree:

```bash
python tools/clean_repo_tree.py --yes
```

## Adding a new target safely

Please follow the checklist in `CONTRIBUTING.md` before adding or editing target YAMLs.
