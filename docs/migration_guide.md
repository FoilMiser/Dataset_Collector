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

## Backwards Compatibility

- Existing `pipeline_driver.py` files continue to work, but `dc pipeline`/`dc run` are the canonical entrypoints.
- The `safe_name` alias is provided in `acquire_strategies.py`
- Legacy `run_pipeline.sh` scripts have moved under each pipeline's `legacy/` directory and are deprecated (removal target: v3.0).

## Removed in v3.0

### Per-Pipeline Worker Scripts

The following files have been removed from all pipeline directories:
- `acquire_worker.py`
- `merge_worker.py`
- `yellow_screen_worker.py`
- `pipeline_driver.py`
- `catalog_builder.py`
- `review_queue.py`
- `pmc_worker.py`

**Migration:** Use the unified CLI instead:

```bash
# Old (removed):
python math_pipeline_v2/acquire_worker.py --queue /data/math/_queues/green.jsonl

# New:
dc run --pipeline math --stage acquire -- --queue /data/math/_queues/green.jsonl
```

### Legacy Shell Scripts

The `legacy/` directories have been removed from all pipelines. Use `dc run` or `dc pipeline` commands.

## New Features in v3.0

### 1. Near-Duplicate Detection

Efficient near-duplicate detection is now available:

```python
from collector_core.checks import create_detector

detector = create_detector(threshold=0.8)
detector.add("doc1", "Machine learning is a subset of AI.")

result = detector.query("Machine learning is part of AI.")
print(f"Is duplicate: {result.is_duplicate}")  # True
```

### 2. Domain-Specific Yellow Screeners

All domain screeners now have real implementations with domain-specific logic:
- Chemistry: CAS number extraction, controlled substance detection
- Biology: Gene ID extraction, biosecurity screening
- Code: License detection, secret scanning, malware patterns
- NLP: Language detection, PII detection, toxicity scanning
- Cyber: CVE extraction, exploit detection
- Safety: Incident classification, severity assessment
- Economics: Financial data sensitivity, temporal coverage
- Knowledge Graph: Entity extraction, ontology compliance

### 3. Content Checks

New content check implementations:
- `language_detect`: Language detection with confidence scoring
- `license_validate`: License validation and classification
- `toxicity_scan`: Toxicity and harmful content detection
- `schema_validate`: Schema validation for structured data
- `distribution_statement`: Export control and distribution statement extraction

### 4. Metrics Dashboard

Pipeline metrics collection and export:

```python
from collector_core.metrics import MetricsCollector

collector = MetricsCollector()
with collector.track("math", "acquire") as metrics:
    metrics.records_processed = 1000
    metrics.records_passed = 950

collector.export_json(Path("metrics.json"))
```

### 5. Checkpoint/Resume Support

Long-running operations can now checkpoint and resume:

```python
from collector_core.checkpoint import CheckpointManager

checkpoint_mgr = CheckpointManager(Path("/data/_checkpoints"))
state = checkpoint_mgr.load("my-operation")

# Process with checkpointing
for i, item in enumerate(items):
    if i < state.get("last_index", 0):
        continue
    process(item)
    if i % 100 == 0:
        checkpoint_mgr.save("my-operation", {"last_index": i})

checkpoint_mgr.cleanup("my-operation")
```

### 6. Schema Version Enforcement

Configuration files are now validated for schema version compatibility:

```python
from collector_core.schema_version import validate_schema_version

validate_schema_version(config, "targets", min_version="0.9")
```

## Breaking Changes

### v3.0 Breaking Changes

1. **Per-pipeline wrapper scripts removed**: Use `dc run` or `dc pipeline` commands instead.
2. **Legacy directories removed**: Shell scripts in `legacy/` directories have been deleted.
3. **Schemas moved to package resources**: The `src/schemas` symlink has been removed. Schemas are now included as package data in `collector_core/schemas/`.
