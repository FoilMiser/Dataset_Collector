# Dataset Collector

## Overview
The Dataset Collector repository organizes a family of domain-specific data collection pipelines under `*_pipeline_v2/` directories. Each pipeline shares a common execution model (a staged pipeline orchestrated by `run_pipeline.sh`) while tailoring the actual stages, tools, and data sources to its domain.

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

## Standard CLI contract (`run_pipeline.sh`)

All `*_pipeline_v2` directories expose a `run_pipeline.sh` entrypoint with a shared CLI contract. Individual pipelines may add additional flags, but the following is standardized.

```bash
./run_pipeline.sh [flags] --stage <stage>

Flags:
  --stage <stage>      Stage name to execute (see Stage names).
  --dry-run            Print actions without executing.
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
- `difficulty`
- `catalog`

## Example sequential execution flow

A typical end-to-end execution sequence:

```bash
./run_pipeline.sh --stage classify --execute
./run_pipeline.sh --stage acquire_green --execute
./run_pipeline.sh --stage acquire_yellow --execute
./run_pipeline.sh --stage screen_yellow --execute
./run_pipeline.sh --stage merge --execute
./run_pipeline.sh --stage difficulty --execute
./run_pipeline.sh --stage catalog --execute
```

Use `--dry-run` for a no-op preview:

```bash
./run_pipeline.sh --stage classify --dry-run
```

## Windows Quickstart (Natural corpus)

Use the Windows-first orchestrator to run all pipelines sequentially and emit the
Natural corpus layout under a single destination root.

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
- `globals.catalogs_root`: root path for final catalogs.

Update these in the pipeline’s configuration (commonly under `configs/`) to control where outputs are written.

## Prerequisites & execution notes

- **Python**: Each pipeline depends on Python; version and additional tools may vary by pipeline.
- **Requirements**: Install per-pipeline dependencies via that pipeline’s `requirements.txt`.
- **Dry-run vs execute**: `--dry-run` prints planned actions; `--execute` performs writes. Use `--execute` only when you intend to modify data or produce outputs.
