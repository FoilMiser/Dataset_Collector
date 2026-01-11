# Environment variables

This repository can run without any environment variables set, but several optional variables unlock higher rate limits or configure where outputs are written.

## Required

None globally. Individual pipelines may require API keys depending on enabled targets.

## Optional (global)

| Variable | Purpose | Used by |
| --- | --- | --- |
| `DATASET_ROOT` | Root directory for pipeline outputs and queues. | `collector_core/pipeline_driver_base.py`, `collector_core/merge.py`, `collector_core/yellow_screen_common.py` |
| `DATASET_COLLECTOR_ROOT` | Alternate root directory for outputs (used if `DATASET_ROOT` is unset). | Same as above |
| `PIPELINE_RETRY_MAX` | Overrides the maximum number of retries for stage actions. | `collector_core/pipeline_driver_base.py` |
| `PIPELINE_RETRY_BACKOFF` | Overrides retry backoff (seconds) between failures. | `collector_core/pipeline_driver_base.py` |

## Optional (pipeline-specific)

| Variable | Purpose | Notes |
| --- | --- | --- |
| `GITHUB_TOKEN` | Higher GitHub API rate limits for targets that pull from GitHub. | Mentioned in several `pipelines/targets/targets_*.yaml` files and used in pipeline acquire workers. |
| `CHEMSPIDER_API_KEY` | Access to ChemSpider-backed sources. | Referenced in `pipelines/targets/targets_chem.yaml`. |

## How variables are resolved

1. Most pipelines check for explicit values in the target configuration first (for example `download.github_token`).
2. If not provided, the pipeline looks up the environment variable listed above.
3. If neither is present, the pipeline falls back to anonymous/unauthenticated behavior.

When running the notebook (`dataset_collector_run_all_pipelines.ipynb`), the notebook prompts for missing environment variables before launching a pipeline.
