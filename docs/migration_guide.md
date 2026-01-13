# Migration Guide: Dataset Collector Refactoring

This guide covers the changes introduced in the Dataset Collector refactoring and
how to migrate existing code.

## Summary of Changes

### 1. Shared Utilities Module

Utility functions are now centralized in `collector_core/utils.py`:

**Before:**
```python
# Duplicated in multiple files
def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
```

**After:**
```python
from collector_core.utils import utc_now, ensure_dir

# Or import from collector_core directly
from collector_core import utc_now, ensure_dir
```

### Available utilities

| Function | Description |
|----------|-------------|
| `utc_now()` | Current UTC time in ISO 8601 format |
| `ensure_dir()` | Create directory and parents if needed |
| `sha256_bytes()` | Hash bytes |
| `sha256_text()` | Hash normalized text |
| `sha256_file()` | Hash file contents |
| `read_json()` / `write_json()` | JSON I/O |
| `read_jsonl()` / `write_jsonl()` | JSONL I/O (supports .gz) |
| `append_jsonl()` | Append to JSONL file |
| `safe_filename()` | Sanitize filename for safety |
| `normalize_whitespace()` | Collapse whitespace |
| `lower()` | Lowercase with None handling |
| `contains_any()` | Find matching needles in haystack |
| `coerce_int()` | Safe int conversion |

### 2. Pipeline Configuration System

Pipelines can now be defined declaratively:

**Before:**
```python
# Each pipeline had a pipeline_driver.py with boilerplate
class ChemPipelineDriver(BasePipelineDriver):
    DOMAIN = "chem"
    TARGETS_LABEL = "targets_chem.yaml"
    # ... more config
```

**After:**
```python
# Define in pipeline_specs_registry.py
register_pipeline(PipelineSpec(
    domain="chem",
    name="Chemistry Pipeline",
    targets_yaml="targets_chem.yaml",
    routing_keys=["chem_routing"],
))

# Get driver dynamically
from collector_core.pipeline_factory import get_pipeline_driver
driver = get_pipeline_driver("chem")
```

### 3. Yellow Review Helpers

The duplicated yellow_scrubber.py files are now thin wrappers:

**Before:**
```python
# 185 lines of duplicated code in each pipeline
@dataclass
class QueueEntry:
    # ... repeated definition

def summarize(entries):
    # ... repeated implementation
```

**After:**
```python
# Thin wrapper
from collector_core.yellow_review_helpers import make_main

def main():
    make_main(
        domain_name="Physics",
        domain_prefix="physics",
        targets_yaml_name="targets_physics.yaml",
    )
```

### 4. CLI Updates

New CLI commands:

```bash
# List all available pipelines
python -m collector_core.dc_cli --list-pipelines

# Run a pipeline by domain
python -m collector_core.dc_cli pipeline chem

# Existing stage-based run still works
python -m collector_core.dc_cli run --pipeline chem --stage acquire
```

## Migration Steps

### For pipeline maintainers

1. **Remove local utility functions**
   - Delete local `utc_now()`, `ensure_dir()`, `sha256_*()`, etc.
   - Import from `collector_core.utils` instead

2. **Update yellow_scrubber.py**
   - Replace with thin wrapper using `make_main()`
   - Keep domain-specific logic in separate files if needed

3. **Consider using PipelineSpec**
   - If your pipeline follows the standard pattern, add to registry
   - Custom logic can still override methods

### For test maintainers

1. **Use new fixtures**
   - Import from `tests.fixtures` for common test data
   - Use `create_minimal_targets_yaml()` for test configs

2. **Run new tests**
   ```bash
   python -m pytest tests/test_utils.py
   python -m pytest tests/test_pipeline_spec_integration.py
   python -m pytest tests/test_yellow_review_helpers.py
   ```

## Removed in v3.0

### Per-Pipeline Worker Scripts

The following wrapper files have been removed from all `*_pipeline_v2/` directories:
- `acquire_worker.py`
- `merge_worker.py`
- `yellow_screen_worker.py`
- `pipeline_driver.py`
- `catalog_builder.py`
- `review_queue.py`
- `pmc_worker.py`

**Migration:** use the unified CLI instead.

```bash
# Old (removed):
python math_pipeline_v2/acquire_worker.py --queue /data/math/_queues/green.jsonl

# New:
dc run --pipeline math --stage acquire -- --queue /data/math/_queues/green.jsonl
```

```bash
# Old (removed):
python chem_pipeline_v2/pipeline_driver.py --targets targets_chem.yaml --no-fetch

# New:
dc pipeline chem --targets targets_chem.yaml --no-fetch
```

### Legacy Shell Scripts

All per-pipeline `legacy/` directories (including `run_pipeline.sh`) have been removed.
Replace them with `dc pipeline` or `dc run`.

```bash
# Old (removed):
./chem_pipeline_v2/legacy/run_pipeline.sh --targets targets_chem.yaml

# New:
dc pipeline chem --targets targets_chem.yaml
```

## Backwards Compatibility

- The `safe_name` alias is provided in `acquire_strategies.py`.

## Breaking Changes

- Per-pipeline wrapper scripts and legacy shell scripts have been removed. Use `dc pipeline` and `dc run` instead.
