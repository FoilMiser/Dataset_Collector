# Dataset Collector

## Overview
The Dataset Collector repository organizes a family of domain-specific data collection pipelines under `*_pipeline_v2/` directories. The primary way to run the full suite is the JupyterLab notebook `dataset_collector_run_all_pipelines.ipynb`, which executes every domain pipeline sequentially in one session while prompting for required API keys and optionally installing per-pipeline requirements. The collector's canonical output is the `combined/` stage; no "final" post-processing stage is produced here.

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

## Run all pipelines via JupyterLab

1. Launch JupyterLab from the repository root (Windows, macOS, or Linux; `bash` is optional unless you plan to run shell-based cells).
2. Open `dataset_collector_run_all_pipelines.ipynb`.
3. Run the notebook cells in order to execute each `*_pipeline_v2` pipeline sequentially.

The notebook prompts for missing environment variables (for example `GITHUB_TOKEN` or `CHEMSPIDER_API_KEY`) and can install each pipeline's requirements before starting its stages.

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

## Expected directory structure & configurable paths

Within each `*_pipeline_v2` directory, you should expect:

```
*_pipeline_v2/
  run_pipeline.sh
  requirements.txt
  src/
  configs/
```

Two global configuration entries determine where queues and catalogs are stored:

- `globals.queues_root`: root path for pipeline queues (intermediate work items).
- `globals.catalogs_root`: root path for pipeline catalogs.

Update these in the pipeline’s configuration (commonly under `configs/`) to control where outputs are written.

## Prerequisites & execution notes

- **Python**: Each pipeline depends on Python; version and additional tools may vary by pipeline.
- **Requirements**: Install per-pipeline dependencies via that pipeline’s `requirements.txt`.
- **Notebook dependencies**: `jupyterlab` and `ipykernel` are not in `requirements.txt`. Install them separately (or via `requirements-dev.txt` if provided).
- **External tools**: `git` is required when a target uses `download.strategy: git`. The AWS CLI is required for `s3_sync` or `aws_requester_pays` download modes. `aria2c` is required for `download.strategy: torrent`.
- **Dry-run vs execute**: dry-run is the default when `--execute` is absent. Use `--execute` only when you intend to modify data or produce outputs.

## Preflight validation

Run the preflight checker to validate pipeline map entries, verify target YAML paths, and detect enabled targets with missing or unsupported download strategies. It also warns about missing optional dependencies or external tools needed by enabled strategies.

```bash
python tools/preflight.py
```

To point at a custom pipeline map location:

```bash
python tools/preflight.py --pipeline-map tools/pipeline_map.yaml
```
