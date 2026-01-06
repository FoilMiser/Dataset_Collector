# Architecture

The Dataset Collector is organized around a shared pipeline contract with domain-specific implementations. Each `*_pipeline_v2` directory supplies a `run_pipeline.sh` entrypoint that delegates to a Python driver, which then executes stage-specific workers and writes outputs to the canonical layout.

## Pipeline flow

```mermaid
flowchart LR
    A[Targets YAML\n(e.g., targets_math.yaml)] --> B[run_pipeline.sh\nCLI contract]
    B --> C[pipeline_driver.py\nPipelineDriverBase]
    C --> D{Stage selection\nclassify / acquire_* / screen_yellow / merge / catalog}
    D --> E[Worker modules\n(acquire_worker.py, merge.py, etc.)]
    E --> F[Artifacts & logs\nqueues, manifests, catalogs]
    F --> G[Combined outputs\ncombined/ stage]

    C --> H[Global config\nDATASET_ROOT / DATASET_COLLECTOR_ROOT]
    E --> I[Safety controls\nyellow screening, denylist]
```

## Key components

- **Targets YAML**: Declares the sources, download strategies, licensing expectations, and per-stage configuration for a pipeline.
- **`run_pipeline.sh`**: Standardized CLI entrypoint; forwards stage and targets arguments into the driver.
- **`pipeline_driver.py`**: Orchestrates stage execution, logging, retries, and output locations.
- **Worker modules**: Implement stage logic (classification, acquisition, screening, merge, catalog).
- **Outputs**: Stage outputs land under the configured dataset root and the `combined/` stage defined in `docs/output_contract.md`.
