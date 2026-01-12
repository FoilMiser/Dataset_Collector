# 3D Modeling Pipeline (v2)

A safety-first pipeline for building an ethically sourced 3D modeling + 3D printing corpus. Version 2 restructures the 3D pipeline to match the **math_pipeline_v2** stage flow:

1. Classify targets and snapshot evidence (`dc pipeline`).
2. Acquire GREEN and YELLOW targets into raw pools (`dc run --stage acquire`).
3. Screen YELLOW data into canonical records with strict pitch behavior (`dc run --stage yellow_screen`).
4. Merge GREEN + screened YELLOW into combined candidate shards (`dc run --stage merge`).
5. Build collector catalogs, ledgers, and manifests over screened shards (`dc catalog-builder`).

> Not legal advice. This tool helps you track licenses and restrictions; you are responsible for compliance.

---

## Run all pipelines via JupyterLab

This pipeline is typically executed as part of the repository-wide run in JupyterLab using `dataset_collector_run_all_pipelines.ipynb`. The notebook runs every `*_pipeline_v2` directory sequentially, prompts for required API keys, and can install each pipeline's requirements before invoking the stages.

## Recommended run method (Windows-first)

For Windows + Conda, prefer the repo-wide orchestrator:

```bash
python tools/build_natural_corpus.py --repo-root . --dest-root "E:/AI-Research/datasets/Natural" --mode full --execute
```

You can also run the Jupyter notebook, which invokes the same workflow. For direct CLI runs, use `dc` (prefer `dc pipeline` for classification and `dc run` for worker stages).

## Directory layout

Targets YAML defaults to `/data/...`; the orchestrator patches to your `--dest-root`.
For standalone runs, pass `--dataset-root` or use `tools/patch_targets.py`.


The recommended roots live in `../pipelines/targets/targets_3d.yaml -> globals`:

```
/data/3d/
  raw/
    green/{license_pool}/{target_id}/...
    yellow/{license_pool}/{target_id}/...
  screened_yellow/{license_pool}/shards/*.jsonl.gz
  combined/{license_pool}/shards/*.jsonl.gz
  screened/{license_pool}/shards/*.jsonl.gz
  _ledger/*.jsonl
  _pitches/*.jsonl
  _queues/*.jsonl
  _manifests/{target_id}/...
```

Sharding is controlled by `globals.sharding` (max records per shard, compression, naming `*_00000.jsonl.gz`).

---

## Stage overview

| Stage | Invocation | Notes |
| --- | --- | --- |
| Classify | `dc pipeline` | Emits GREEN/YELLOW/RED queues; adds `queue_bucket` and routing metadata. |
| Acquire | `dc run --stage acquire` | Downloads payloads into `raw/{green|yellow}/{license_pool}/{target_id}`. Dry-run by default; `--execute` performs downloads. |
| Screen YELLOW | `dc run --stage yellow_screen` | Converts raw YELLOW payloads into canonical records, sharding outputs and writing pass/pitch ledgers + done markers. |
| Merge | `dc run --stage merge` | Combines canonical GREEN + screened YELLOW shards with deduplication and a combined ledger. |
| Catalog | `dc catalog-builder` | Summarizes counts, bytes, manifests, and ledgers across stages. |

Use `dc pipeline` for classification, `dc run` for acquire/merge/yellow_screen, and `dc catalog-builder` for catalog outputs.

---

## Standalone quick start (optional)

```bash
pip install -r requirements.txt

# Dry-run classify only
dc pipeline 3d_modeling -- --targets ../pipelines/targets/targets_3d.yaml --stage classify --no-fetch

# Acquire GREEN and YELLOW (execute downloads)
dc run --pipeline 3d_modeling --stage acquire --allow-data-root -- --queue /data/3d_modeling/_queues/green_pipeline.jsonl --bucket green --targets-yaml ../pipelines/targets/targets_3d.yaml --execute
dc run --pipeline 3d_modeling --stage acquire --allow-data-root -- --queue /data/3d_modeling/_queues/yellow_pipeline.jsonl --bucket yellow --targets-yaml ../pipelines/targets/targets_3d.yaml --execute

# Screen, merge, catalog
dc run --pipeline 3d_modeling --stage yellow_screen --allow-data-root -- --queue /data/3d_modeling/_queues/yellow_pipeline.jsonl --targets ../pipelines/targets/targets_3d.yaml --execute
dc run --pipeline 3d_modeling --stage merge --allow-data-root -- --targets ../pipelines/targets/targets_3d.yaml --execute
dc catalog-builder --pipeline 3d_modeling --allow-data-root -- --targets ../pipelines/targets/targets_3d.yaml --output /data/3d_modeling/_catalogs/catalog.json
```

### Notes

- YELLOW screening enforces strict pitch semantics; see `../pipelines/targets/targets_3d.yaml -> globals.screening` and per-target `yellow_screen` overrides.
- Mesh metadata is computed when assets are present; enable mesh dependencies for richer metadata and hashing.
- Outputs are segregated by `license_profile` (`permissive`, `copyleft`, `quarantine`).
- Ledgers in `_ledger/` provide pass/pitch summaries and shard indexes for reproducibility.

## License

Pipeline code is provided as-is for research and development use.

**Data sources retain their own licenses** - this tool helps you track and respect them.
