# Adding a new pipeline

This cookbook outlines the minimal steps to add a new `*_pipeline_v2` directory and wire it into the orchestration tools.

## 1. Create the pipeline directory

Create a new directory following the naming pattern `your_domain_pipeline_v2/` and include the standard files:

```
your_domain_pipeline_v2/
  run_pipeline.sh
  pipeline_driver.py
  acquire_worker.py
  targets_your_domain.yaml
  license_map.yaml
  requirements.txt
```

Use an existing pipeline as a template to keep the stage contract consistent.

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

Populate `license_map.yaml` with any required license metadata so merge stages can enforce allow/deny rules.

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
