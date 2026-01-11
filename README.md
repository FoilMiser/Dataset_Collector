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

## License profiles

Targets and catalog entries may specify a `license_profile` value. Supported values are:

- `permissive`
- `public_domain`
- `copyleft`
- `record_level`
- `unknown`
- `quarantine`
- `deny`

## Known Limitations

- **Yellow-screen boundaries**: The `acquire_yellow` and `screen_yellow` stages only apply to targets explicitly marked as YELLOW in pipeline target YAMLs. Anything not labeled YELLOW is outside the yellow-screen workflow, and no automatic promotion to GREEN occurs without human review.
- **Excluded sources**: RED-labeled sources are excluded by design and are not collected or merged. Some targets may be disabled in `targets_*.yaml` or omitted from `src/tools/pipeline_map.sample.yaml`, which means they are intentionally not part of the run.
- **Non-goals**: This repository does not perform data cleaning, deduplication across domains, or final dataset curation beyond the per-pipeline merge step. It also does not guarantee license compatibility checks outside the configured `license_map.yaml` entries.
- **Corpus constraints**: Outputs are constrained to the `combined/` stage described in `docs/output_contract.md`; there is no final, unified post-processing stage here. Downstream consumers should expect per-pipeline catalogs and manifests with varying completeness based on enabled targets and available credentials.
- **Rate limiting configuration**: `targets_*.yaml` files can include `resolvers: ... rate_limit: ...` blocks (e.g., for GitHub API) which are now consumed by the acquisition handlers for GitHub and Figshare. The schema validates rate_limit blocks with keys: `requests_per_minute`, `requests_per_hour`, `requests_per_second`, `burst`, and retry options (`retry_on_429`, `retry_on_403`).

## Documentation

- [Architecture](docs/architecture.md)
- [Environment variables](docs/environment-variables.md)
- [Troubleshooting](docs/troubleshooting.md)
- [Adding a new pipeline](docs/adding-new-pipeline.md)
- [Run instructions](docs/run_instructions.md)

## Install (CLI)

Install the shared base dependencies first, then add pipeline-specific extras as needed.
The `dc` CLI is provided by the editable install.

```bash
pip install -r requirements.constraints.txt
pip install -r requirements-dev.constraints.txt
pip install -e .
pip install -r math_pipeline_v2/requirements.txt
# repeat for other pipeline extras as needed
```

## Reproducible installs (recommended)

Use the base + extras flow: install the shared base constraints once, then layer in the
pipeline-specific extras you need. Dependency source of truth lives in
`pyproject.toml` (`[project.dependencies]` and `[project.optional-dependencies]`); the
requirements/constraints files exist to support deterministic CI installs.

```bash
pip install -r requirements.constraints.txt
pip install -r requirements-dev.constraints.txt
pip install -e .
pip install -r math_pipeline_v2/requirements.txt
# repeat for other pipeline extras as needed
```

### Regenerate the constraints files

These constraints are version-pinned but do not use hash checking, and they are not a
full lockfile that pins every transitive dependency. Update `pyproject.toml` first, then
sync `requirements*.in` and recompile the constraints.

```bash
uv pip compile requirements.in -o requirements.constraints.txt
uv pip compile requirements-dev.in -o requirements-dev.constraints.txt
```

## Run all pipelines via JupyterLab

1. Launch JupyterLab from the repository root (Windows, macOS, or Linux; `bash` is optional unless you plan to run shell-based cells).
2. Open `dataset_collector_run_all_pipelines.ipynb`.
3. Run the notebook cells in order to execute each `*_pipeline_v2` pipeline sequentially.

The notebook prompts for missing environment variables (for example `GITHUB_TOKEN` or `CHEMSPIDER_API_KEY`) and can install each pipeline's requirements before starting its stages.

## Single-command run, outputs, and clean-room reruns

For the shortest single-command run, expected output folders, log/manifest/ledger locations,
and clean-room rerun steps (what to delete vs. keep), see
`docs/run_instructions.md`.

## Dataset roots and ledger artifacts

All pipeline stages write into a per-domain dataset root. You can set it explicitly with
`--dataset-root` or with `DATASET_ROOT` / `DATASET_COLLECTOR_ROOT`, and the CLI will
derive standard subfolders (`raw/`, `combined/`, `_ledger/`, `_logs/`, etc.). Defaults
fall back to `/data/<pipeline>` unless you pass `--allow-data-root` to confirm that
path is acceptable. See `docs/pipeline_runtime_contract.md` and `docs/output_contract.md`
for the full layout and precedence rules.

Ledger artifacts live in `<dataset_root>/_ledger/` and provide the audit trail for
screening and merging, including `yellow_passed.jsonl`, `yellow_pitched.jsonl`,
`combined_index.jsonl`, and merge summaries.

## Unified CLI (`dc run`)

Use the unified CLI to run a single stage for any pipeline:

```bash
dc run --pipeline physics --stage acquire --allow-data-root -- --queue /data/physics/_queues/green_pipeline.jsonl --bucket green --execute
dc run --pipeline physics --stage merge --allow-data-root -- --queue /data/physics/_queues/green_pipeline.jsonl --execute
dc run --pipeline physics --stage yellow_screen --allow-data-root -- --queue /data/physics/_queues/yellow_pipeline.jsonl --execute
```

The `--` separator passes additional flags directly to the underlying stage worker (so you can continue to use
`--targets`, `--queue`, `--execute`, etc.). Pipeline-specific overrides live in `configs/pipelines.yaml`, and can
point to optional plugin hooks for specialized acquisition or yellow-screen logic.

Available stages are `acquire`, `merge`, and `yellow_screen`.

### Deprecated pipeline scripts
The per-pipeline worker scripts (`acquire_worker.py`, `merge_worker.py`, `yellow_screen_worker.py`) are deprecated in
favor of `dc run`. Legacy `run_pipeline.sh` wrappers have moved under `legacy/` for reference, but new usage should
prefer the unified CLI.

## Example sequential execution flow (dc CLI)

An end-to-end execution sequence with the unified CLI:

```bash
dc pipeline math -- --targets pipelines/targets/targets_math.yaml --dataset-root /data/Natural/math --stage classify
dc run --pipeline math --stage acquire --dataset-root /data/Natural/math -- --queue /data/Natural/math/_queues/green_pipeline.jsonl --bucket green --targets-yaml pipelines/targets/targets_math.yaml --execute
dc run --pipeline math --stage acquire --dataset-root /data/Natural/math -- --queue /data/Natural/math/_queues/yellow_pipeline.jsonl --bucket yellow --targets-yaml pipelines/targets/targets_math.yaml --execute
dc review-queue --pipeline math --queue /data/Natural/math/_queues/yellow_pipeline.jsonl list --limit 50
dc run --pipeline math --stage yellow_screen --dataset-root /data/Natural/math -- --queue /data/Natural/math/_queues/yellow_pipeline.jsonl --targets pipelines/targets/targets_math.yaml --execute
dc run --pipeline math --stage merge --dataset-root /data/Natural/math -- --targets pipelines/targets/targets_math.yaml --execute
dc catalog-builder --pipeline math -- --targets pipelines/targets/targets_math.yaml --output /data/Natural/math/_catalogs/catalog.json
```

To preview the actions without writing data, add `--no-fetch` during classification and omit `--execute` (dry-run):

```bash
dc pipeline math -- --targets pipelines/targets/targets_math.yaml --stage classify --no-fetch
```

## Contributor quickstart checklist

- [ ] Install base + pipeline requirements and confirm `dc --help` runs.
- [ ] Choose a per-domain dataset root (or set `DATASET_ROOT`) and ensure it is writable.
- [ ] Run `dc pipeline <domain> -- --stage classify` to emit queues and manifests.
- [ ] Run `dc run --pipeline <domain> --stage acquire` for green and yellow queues.
- [ ] Review YELLOW targets with `dc review-queue` and record signoffs.
- [ ] Run `dc run --pipeline <domain> --stage yellow_screen`.
- [ ] Run `dc run --pipeline <domain> --stage merge` followed by `dc catalog-builder`.
- [ ] Check `_ledger/` and `_logs/` under the dataset root for audit artifacts.

### Jupyter (Windows-first, bash optional)

The notebook runs on Windows-first Python. For shell-based stage runs, use `dc` commands
in Bash cells when you have WSL or another shell with `bash` available.

#### Prereqs (Windows)

Install the following tools if you plan to use targets that rely on them:

- **Git for Windows** (required for `download.strategy: git`): https://git-scm.com/download/win
- **AWS CLI v2** (required for `s3_sync` or `aws_requester_pays`): https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html
- **aria2** (required for `download.strategy: torrent`): https://aria2.github.io/

### Windows Quickstart (Natural corpus, optional)

For a Windows-first, all-pipelines run, use the JupyterLab notebook and execute the
cells in order. See `docs/output_contract.md` for the expected on-disk layout.

## Toolchain (external tools)

Some targets depend on external tools. Install them as needed:

- **Git** (required for `download.strategy: git`): https://git-scm.com/download/win
- **7zip/unzip** (optional; for large archive extraction): https://www.7-zip.org/
- **AWS CLI v2** (required for `s3_sync` or `aws_requester_pays`): https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html
- **huggingface-cli** (optional; if you add Hugging Face auth flows later): https://huggingface.co/docs/huggingface_hub/quick-start#login

## Architecture: Spec-driven pipelines

All 18 domain pipelines are now configured via `src/collector_core/pipeline_specs_registry.py`.
Each `PipelineSpec` defines:

- **domain**: The unique domain identifier (e.g., `physics`, `chem`, `math`)
- **name**: Human-readable pipeline name
- **domain_prefix**: Short prefix for path construction (defaults to domain)
- **targets_yaml**: The targets YAML file name
- **routing_keys/routing_confidence_keys**: Routing configuration
- **default_routing**: Default routing values
- **yellow_screen_module**: Domain-specific yellow screen module (if any)
- **custom_workers**: Domain-specific worker modules (if any)

### Thin wrappers

Per-pipeline worker files are now thin wrappers that delegate to `collector_core`:

```python
# pipeline_driver.py
from collector_core.pipeline_factory import get_pipeline_driver
DOMAIN = "physics"
if __name__ == "__main__":
    get_pipeline_driver(DOMAIN).main()
```

```python
# acquire_worker.py
from collector_core.generic_workers import main_acquire
DOMAIN = "physics"
if __name__ == "__main__":
    main_acquire(DOMAIN)
```

```python
# yellow_screen_worker.py
from collector_core.yellow_screen_dispatch import main_yellow_screen
DOMAIN = "physics"
if __name__ == "__main__":
    main_yellow_screen(DOMAIN)
```

### Syncing wrappers

To regenerate all thin wrappers from the registry:

```bash
python src/tools/sync_pipeline_wrappers.py
```

To check wrappers are up-to-date (CI mode):

```bash
python src/tools/sync_pipeline_wrappers.py --check
```

## Expected directory structure & configurable paths

Within each `*_pipeline_v2` directory, you should expect:

```
*_pipeline_v2/
  legacy/run_pipeline.sh  # deprecated legacy wrapper
  requirements.txt
  pipeline_driver.py
  acquire_worker.py
```

Targets YAML files now live in a shared directory:

```
pipelines/
  targets/
    targets_*.yaml
```

Shared configuration files live in `configs/common/` and are referenced from each
pipeline’s targets YAML:

```
configs/
  common/
    license_map.yaml
    field_schemas.yaml
    denylist.yaml
  pipelines.yaml
```

Targets YAMLs point at companion files using relative paths, for example in
`pipelines/targets/targets_math.yaml`:

```yaml
companion_files:
  license_map:
    - "../../configs/common/license_map.yaml"
  field_schemas:
    - "../../configs/common/field_schemas.yaml"
  denylist:
    - "../../configs/common/denylist.yaml"
```

Two global configuration entries determine where queues and catalogs are stored:

- `globals.queues_root`: root path for pipeline queues (intermediate work items).
- `globals.catalogs_root`: root path for pipeline catalogs.

Modify the shared files in `configs/common/` only when you want the change to apply
across pipelines. For pipeline-specific tweaks, add a local YAML alongside your
targets file and point to it in `companion_files` so you inherit the defaults and
override only what you need.

Update these in the pipeline’s configuration files (for example `pipelines/targets/targets_*.yaml`) to control where outputs are written.

## Prerequisites & execution notes

- **Python**: Each pipeline depends on Python; version and additional tools may vary by pipeline.
  Tested on Python 3.10 and 3.11.
- **Requirements**: Follow [Reproducible installs](#reproducible-installs-recommended) for the shared base and pipeline-specific extras.
- **Notebook dependencies**: `jupyterlab` and `ipykernel` are tracked in `requirements-dev.in` and installed via the reproducible constraints flow.
- **External tools**: `git` is required when a target uses `download.strategy: git`. The AWS CLI is required for `s3_sync` or `aws_requester_pays` download modes. `aria2c` is required for `download.strategy: torrent`.
- **Dry-run vs execute**: dry-run is the default when `--execute` is absent. Use `--execute` only when you intend to modify data or produce outputs.

## Preflight validation

Run the preflight checker to validate pipeline map entries, verify target YAML paths, and detect enabled targets with missing or unsupported download strategies. It also warns about missing optional dependencies or external tools needed by enabled strategies.

```bash
python -m tools.preflight
```

To run checks for specific pipelines:

```bash
python -m tools.preflight --pipelines chem_pipeline_v2 biology_pipeline_v2
```

To point at a custom pipeline map location:

```bash
python -m tools.preflight --pipeline-map src/tools/pipeline_map.sample.yaml
```

To emit warnings for disabled targets:

```bash
python -m tools.preflight --warn-disabled
```

For local runs, copy `src/tools/pipeline_map.sample.yaml` to something like `src/tools/pipeline_map.local.yaml`, set `destination_root` to your dataset folder, and pass it via `--pipeline-map` when running tooling. For pipeline execution, prefer `dc pipeline`/`dc run` with `--dataset-root` or `DATASET_ROOT`. This keeps user-specific paths out of version control.

## Repo validation

Validate all enabled targets across pipeline configs and emit a JSON summary:

```bash
python -m tools.validate_repo --output src/tools/validate_report.json
```

Fail the run if warnings are present:

```bash
python -m tools.validate_repo --strict --output src/tools/validate_report.json
```

## Cleaning local artifacts

If you ran pipelines inside the repo and need to reset the tree:

```bash
python src/tools/clean_repo_tree.py --yes
```

## Adding a new target safely

Please follow the checklist in `CONTRIBUTING.md` before adding or editing target YAMLs.
