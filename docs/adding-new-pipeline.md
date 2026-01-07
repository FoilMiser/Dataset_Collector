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
  run_pipeline.sh
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

## 2. Implement the CLI entrypoint

Ensure `run_pipeline.sh` follows the standard CLI contract documented in the README:

```bash
./run_pipeline.sh --targets <targets.yaml> --stage <stage> [--execute]
```

This script should call the Python driver with the same arguments.

## 3. Wire up the driver and workers

- Implement a `PipelineDriver` class (or equivalent) in `pipeline_driver.py` that derives from `collector_core/pipeline_driver_base.py`.
- Add stage logic in worker modules (for example, `acquire_worker.py` for acquisition, optional merge/catalog workers if needed).
- Ensure outputs align to the `combined/` stage described in `docs/output_contract.md`.

## 4. Author the targets file

Define targets in `targets_your_domain.yaml`, including:

- `download` configuration (`strategy`, URLs, auth options).
- `safety_bucket` values (GREEN/YELLOW/RED).
- Any pipeline-specific metadata.

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
./run_pipeline.sh --targets targets_your_domain.yaml --stage classify
```

Then run with `--execute` once the dry-run succeeds.
