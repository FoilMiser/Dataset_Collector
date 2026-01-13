# Dataset Collector: Evaluation Pointers

This document identifies remaining code quality issues and areas for improvement in the Dataset Collector codebase.

---

## 1. Code Duplication

### Yellow Screen Entry Points (HIGH PRIORITY)

Six nearly-identical 16-line files exist that should be consolidated:

| File | Domain |
|------|--------|
| `src/collector_core/yellow_screen_standard.py` | standard |
| `src/collector_core/yellow_screen_chem.py` | chem |
| `src/collector_core/yellow_screen_econ.py` | econ |
| `src/collector_core/yellow_screen_kg_nav.py` | kg_nav |
| `src/collector_core/yellow_screen_nlp.py` | nlp |
| `src/collector_core/yellow_screen_safety.py` | safety |

All follow the identical pattern:
```python
from collector_core.stability import stable_api
from collector_core.yellow.base import run_yellow_screen
from collector_core.yellow.domains import <DOMAIN>
from collector_core.yellow_screen_common import YellowRootDefaults

@stable_api
def main(*, defaults: YellowRootDefaults) -> None:
    run_yellow_screen(defaults=defaults, domain=<DOMAIN>)
```

**Recommendation**: Create a factory function in `yellow_screen_dispatch.py` that dynamically loads the domain module, eliminating these redundant files.

---

## 2. Missing Type Annotations

The following functions lack return type annotations:

| File | Line | Function |
|------|------|----------|
| `src/collector_core/pmc_worker.py` | 56 | `pools_from_targets_yaml()` |
| `src/collector_core/pmc_worker.py` | 470 | `flush()` |
| `src/collector_core/acquire_strategies.py` | 362 | `items()` |
| `src/collector_core/acquire_strategies.py` | 368 | `keys()` |
| `src/collector_core/acquire_strategies.py` | 374 | `values()` |
| `src/collector_core/checks/near_duplicate.py` | 138 | `_build_minhash()` |

**Note**: Running `mypy src/collector_core --strict` will reveal additional type issues. The codebase has type annotations for most public APIs but not full strict compliance.

---

## 3. Missing `re` Import

**File**: `src/collector_core/pmc_worker.py` line 85

```python
paras = [p.strip() for p in re.split(r"\n{2,}", text) if p.strip()]
```

The `re` module is used but not imported at the top of the file.

---

## 4. Modules Without Dedicated Test Files

The following modules in `src/collector_core/` lack corresponding test files:

- `acquire_limits.py`
- `artifact_metadata.py`
- `catalog_builder.py`
- `checkpoint.py`
- `config_validator.py`
- `dc_cli.py`
- `decision_bundle.py`
- `denylist_matcher.py`
- `evidence_policy.py`
- `generic_workers.py`
- `observability.py`
- `path_templates.py`
- `pipeline_driver_base.py`
- `pipeline_factory.py`
- `pipeline_spec.py`
- `pmc_worker.py`
- `policy_override.py`
- `policy_snapshot.py`
- `review_queue.py`
- `sharding.py`
- `yaml_lite.py`
- `yellow_scrubber_base.py`

**Note**: Some of these are tested indirectly through integration tests or other test files.

---

## 5. Pipeline Directory Boilerplate

Each pipeline directory (`*_pipeline_v2/`) contains similar boilerplate files:

- `yellow_scrubber.py` - Most are thin wrappers
- `acquire_plugin.py` - Domain-specific acquisition logic

While these are intentionally domain-specific, some common patterns could be extracted.

---

## 6. Abstract Base Class Pattern

**File**: `src/collector_core/checks/base.py` line 29

```python
def check(self, record: dict[str, Any], config: dict[str, Any]) -> CheckResult:
    raise NotImplementedError
```

This is the correct abstract method pattern. Consider using `@abstractmethod` decorator from `abc` module for stricter enforcement.

---

## 7. Test Coverage

Current test coverage is below 90% target. Key areas needing more tests:

1. **CLI modules** (`dc_cli.py`, `pipeline_cli.py`)
2. **Observability** (`observability.py`)
3. **Checkpoint/Resume** (`checkpoint.py`)
4. **Catalog Builder** (`catalog_builder.py`)

---

## 8. Documentation Gaps

The following could benefit from additional documentation:

1. **Architecture overview** - How the pipeline stages connect
2. **Plugin development guide** - How to create new domain pipelines
3. **Deployment guide** - Production deployment best practices

---

## Summary

| Category | Count | Priority |
|----------|-------|----------|
| Duplicated entry point files | 6 files | High |
| Missing return type annotations | 6 functions | Medium |
| Missing import | 1 | Medium |
| Modules without dedicated tests | 20+ | Low-Medium |

The codebase is generally well-structured with good separation of concerns. The main improvement opportunities are consolidating the yellow_screen entry points and increasing test coverage for CLI and infrastructure modules.
