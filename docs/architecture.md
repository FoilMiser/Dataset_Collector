# Architecture

The Dataset Collector is organized around a shared pipeline contract with domain-specific implementations. The preferred entrypoint is the unified `dc run --pipeline <slug> --stage <stage>` CLI, which delegates to spec-driven workers to write outputs to the canonical layout. Legacy `run_pipeline.sh` scripts remain only as deprecated compatibility shims.

## Pipeline flow

```mermaid
flowchart LR
    A[Targets YAML\n(e.g., pipelines/targets/targets_math.yaml)] --> B[dc run\nCLI contract]
    B --> C{Stage selection\nclassify / acquire / yellow_screen / merge / catalog}
    C --> D[Generic workers\n(spec-driven)]
    E --> F[Artifacts & logs\nqueues, manifests, catalogs]
    F --> G[Combined outputs\ncombined/ stage]

    C --> H[Global config\nDATASET_ROOT / DATASET_COLLECTOR_ROOT]
    D --> I[Safety controls\nyellow screening, denylist]
```

## Core modules

The `collector_core/` package contains shared functionality:

### Shared Utilities (`utils.py`)

Common utilities used across all pipelines:
- `utc_now()`, `ensure_dir()` - Basic operations
- `sha256_bytes()`, `sha256_text()`, `sha256_file()` - Hashing
- `read_json()`, `write_json()`, `read_jsonl()`, `write_jsonl()` - I/O
- `safe_filename()` - Security-conscious filename sanitization
- `normalize_whitespace()`, `lower()`, `contains_any()`, `coerce_int()` - Text helpers

### Pipeline Configuration System

The pipeline specification system enables configuration-driven pipeline creation:

- **`pipeline_spec.py`**: Defines the `PipelineSpec` dataclass for pipeline configuration
- **`pipeline_specs_registry.py`**: Registry of all domain pipeline specifications
- **`pipeline_factory.py`**: Factory for creating pipeline drivers from specifications

```python
# Example: Getting a pipeline driver dynamically
from collector_core.pipeline_factory import get_pipeline_driver

driver_class = get_pipeline_driver("chem")
driver_class.main()
```

### Yellow Review Helpers (`yellow_review_helpers.py`)

Consolidated helpers for YELLOW bucket planning and manual review prep:
- `QueueEntry` dataclass for queue records
- `load_queue()`, `summarize()`, `write_plan()` - Core operations
- `make_main()` - Factory for creating domain-specific entry points

## Key components

- **Targets YAML**: Declares the sources, download strategies, licensing expectations, and per-stage configuration for a pipeline.
- **`dc run`**: Standardized CLI entrypoint; forwards stage and targets arguments into the driver.
- **`dc --list-pipelines`**: List all registered pipeline domains.
- **`dc pipeline <domain>`**: Run a full pipeline driver for a domain.
- **`legacy/run_pipeline.sh`** (deprecated): Compatibility shim for legacy entrypoints.
- **`pipeline_driver.py`**: Internal driver used by `dc pipeline` to emit queues and manifests.
- **Worker modules**: Implement stage logic behind `dc run` (acquisition, screening, merge, catalog).
- **Outputs**: Stage outputs land under the configured dataset root and the `combined/` stage defined in `docs/output_contract.md`.
