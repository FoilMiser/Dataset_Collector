# Adding a new pipeline

This cookbook outlines the minimal steps to add a new `*_pipeline_v2` directory and wire it into the orchestration tools.

## 1. Generate a scaffold

Use the generator to scaffold a new pipeline directory with standard entry points,
README, and targets config:

```bash
python tools/generate_pipeline.py --domain your_domain
```

The scaffolded directory includes:

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
  targets_your_domain.yaml
  requirements.txt
```

Use existing pipelines as references for domain-specific customization.

## 2. Wire the unified CLI configuration

The preferred entrypoint is `dc run`, which uses `configs/pipelines.yaml` to register any
pipeline-specific hooks (custom acquisition strategies, yellow-screen modules, etc.).
If your new pipeline needs special behavior, add a new entry under `pipelines:` for its slug.
Legacy `run_pipeline.sh` scripts remain for backwards compatibility but are deprecated.

## 3. Wire up the driver and workers

- Implement a `PipelineDriver` class (or equivalent) in `pipeline_driver.py` that derives from `collector_core/pipeline_driver_base.py`.
- Add stage logic in worker modules (for example, `acquire_worker.py` for acquisition, optional merge/catalog workers if needed).
- Ensure outputs align to the `combined/` stage described in `docs/output_contract.md`.

## 4. Author the targets file

Define targets in `targets_your_domain.yaml`, including:

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
    - "../configs/common/license_map.yaml"
  field_schemas:
    - "../configs/common/field_schemas.yaml"
  denylist:
    - "../configs/common/denylist.yaml"
```

Use the shared files as-is when you want to inherit the repo defaults. When you
need pipeline-specific overrides, add a local YAML (for example
`your_domain_pipeline_v2/license_map.yaml`) and reference it under
`companion_files` so only that pipeline changes.

## 5. Configure licensing

Update the shared license map in `configs/common/license_map.yaml` (or add a pipeline-specific file
and reference it from `targets_your_domain.yaml`) so merge stages can enforce allow/deny rules.

## 6. Register the pipeline

Update `tools/pipeline_map.sample.yaml` so the orchestrators and notebook can discover the new pipeline:

```yaml
pipelines:
  your_domain_pipeline_v2:
    dest_folder: "your_domain"
    targets_yaml: "targets_your_domain.yaml"
```

## 7. Validate locally

Run a dry-run classification to validate the wiring:

```bash
python your_domain_pipeline_v2/pipeline_driver.py --targets targets_your_domain.yaml
```

Then run with `--execute` once the dry-run succeeds.
