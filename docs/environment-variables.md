# Environment variables

This repository can run without any environment variables set, but several optional variables unlock higher rate limits or configure where outputs are written.

## Required

None globally. Individual pipelines may require API keys depending on enabled targets.

## Optional (global)

| Variable | Default | Purpose | Used by |
| --- | --- | --- | --- |
| `DATASET_ROOT` | (none) | Root directory for pipeline outputs and queues. | `collector_core/pipeline_driver_base.py`, `collector_core/merge.py`, `collector_core/yellow_screen_common.py` |
| `DATASET_COLLECTOR_ROOT` | (none) | Alternate root directory for outputs (used if `DATASET_ROOT` is unset). | Same as above |
| `PIPELINE_RETRY_MAX` | `3` | Maximum number of retries for stage actions. | `collector_core/pipeline_driver_base.py` |
| `PIPELINE_RETRY_BACKOFF` | `2.0` | Retry backoff multiplier in seconds. | `collector_core/pipeline_driver_base.py` |

## Observability

| Variable | Default | Purpose |
| --- | --- | --- |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | (none) | OpenTelemetry collector endpoint for distributed tracing. |
| `OTEL_SERVICE_NAME` | `dataset-collector` | Service name for OpenTelemetry traces. |
| `DC_LOG_LEVEL` | `INFO` | Logging level (DEBUG, INFO, WARNING, ERROR). |
| `DC_LOG_FORMAT` | `text` | Log format (text or json). |

## Optional (pipeline-specific)

| Variable | Default | Purpose | Notes |
| --- | --- | --- | --- |
| `GITHUB_TOKEN` | (none) | Higher GitHub API rate limits for targets that pull from GitHub. | Mentioned in several `pipelines/targets/targets_*.yaml` files and used in pipeline acquire workers. |
| `CHEMSPIDER_API_KEY` | (none) | Access to ChemSpider-backed sources. | Referenced in `pipelines/targets/targets_chem.yaml`. |
| `HF_TOKEN` | (none) | Hugging Face Hub authentication for gated datasets. | Used by HuggingFace acquisition strategy. |
| `AWS_ACCESS_KEY_ID` | (none) | AWS credentials for S3 downloads. | Used by S3 acquisition strategy. |
| `AWS_SECRET_ACCESS_KEY` | (none) | AWS credentials for S3 downloads. | Used by S3 acquisition strategy. |

## How variables are resolved

1. Most pipelines check for explicit values in the target configuration first (for example `download.github_token`).
2. If not provided, the pipeline looks up the environment variable listed above.
3. If neither is present, the pipeline falls back to anonymous/unauthenticated behavior.

When running the notebook (`dataset_collector_run_all_pipelines.ipynb`), the notebook prompts for missing environment variables before launching a pipeline.
