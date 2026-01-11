# Adding a new pipeline

This cookbook outlines the minimal steps to add a new pipeline configuration and wire it into the orchestration tools.

## 1. Generate a scaffold

Use the generator to scaffold a new pipeline directory with optional entry points,
README, and targets config:

```bash
python tools/generate_pipeline.py --domain your_domain
```

The scaffolded directory can include:

```
your_domain_pipeline_v2/
  README.md
  run_pipeline.sh  # deprecated legacy wrapper
  pipeline_driver.py
  acquire_worker.py
  yellow_screen_worker.py
  merge_worker.py
  catalog_builder.py
  review_queue.py
  requirements.txt
```

Targets YAMLs are created in the shared directory:

```
pipelines/targets/targets_your_domain.yaml
```

Only the targets YAML is required. Wrapper scripts are optional and can be regenerated
with `tools/sync_pipeline_wrappers.py` if you want legacy entry points.

## 2. Register the pipeline

Register the pipeline in `collector_core/pipeline_specs_registry.py` with the domain,
targets filename, and any routing defaults. The preferred entrypoint is `dc run`, which
uses `configs/pipelines.yaml` to register pipeline-specific hooks (custom acquisition
strategies, yellow-screen modules, etc.). If your new pipeline needs special behavior,
add a new entry under `pipelines:` for its slug.

## 3. Optional: add custom hooks

If you need custom acquisition handlers or post-processing, add a plugin module in the
pipeline directory and reference it from `configs/pipelines.yaml`. Otherwise, the
generic workers and `dc` CLI will handle the pipeline with no extra files.

## 4. Author the targets file

Define targets in `pipelines/targets/targets_your_domain.yaml`, including:

- `download` configuration (`strategy`, URLs, auth options).
- `safety_bucket` values (GREEN/YELLOW/RED).
- Any pipeline-specific metadata.

### Companion files and shared configs

Targets files declare companion files that the runtime loads alongside the target
definitions. The shared defaults live in `configs/common/` and are referenced with
relative paths, for example:

```yaml
companion_files:
  license_map:
    - "../../configs/common/license_map.yaml"
  field_schemas:
    - "../../configs/common/field_schemas.yaml"
  denylist:
    - "../../configs/common/denylist.yaml"
```

Use the shared files as-is when you want to inherit the repo defaults. When you
need pipeline-specific overrides, add a local YAML (for example
`your_domain_pipeline_v2/license_map.yaml`) and reference it under
`companion_files` (for example `../../your_domain_pipeline_v2/license_map.yaml`)
so only that pipeline changes.

## 5. Configure licensing

Update the shared license map in `configs/common/license_map.yaml` (or add a pipeline-specific file
and reference it from `pipelines/targets/targets_your_domain.yaml`) so merge stages can enforce allow/deny rules.

## 6. Validate locally

Run a dry-run classification to validate the wiring:

```bash
python -m collector_core.dc_cli pipeline your_domain -- --targets pipelines/targets/targets_your_domain.yaml
```

Then run with `--execute` once the dry-run succeeds.
