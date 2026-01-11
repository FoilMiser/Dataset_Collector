# Chemistry Corpus Pipeline (v2)

A safety-first pipeline for building an ethical chemistry training corpus. Version 2 mirrors the v2 stage flow used in math_pipeline_v2 while re-homing chemistry-specific transforms into the YELLOW screening stage.

Stages:

1. Classify targets and snapshot evidence (`pipeline_driver.py`).
2. Acquire GREEN and YELLOW targets into raw pools (`acquire_worker.py`).
3. Screen YELLOW data into canonical records with chemistry-aware plugins (`yellow_screen_worker.py`).
4. Merge GREEN + screened YELLOW into combined candidate shards (`merge_worker.py`).
5. Build collector catalogs, ledgers, and manifests over screened shards (`catalog_builder.py`).

> Not legal advice. This tooling helps track licenses and restrictions; you remain responsible for compliance.

---

## Run all pipelines via JupyterLab

This pipeline is typically executed as part of the repository-wide run in JupyterLab using `dataset_collector_run_all_pipelines.ipynb`. The notebook runs every `*_pipeline_v2` directory sequentially, prompts for required API keys, and can install each pipeline's requirements before invoking the stages.

## Recommended run method (Windows-first)

For Windows + Conda, prefer the repo-wide orchestrator:

```bash
python tools/build_natural_corpus.py --repo-root . --dest-root "E:/AI-Research/datasets/Natural" --mode full --execute
```

You can also run the Jupyter notebook, which invokes the same workflow. Use `run_pipeline.sh` only if you have Git Bash/WSL on Windows or are on macOS/Linux.

## Directory layout

Targets YAML defaults to `/data/...`; the orchestrator patches to your `--dest-root`.
For standalone runs, pass `--dataset-root` or use `tools/patch_targets.py`.


Roots are defined in `../pipelines/targets/targets_chem.yaml -> globals`:

```
/data/chem/
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

| Stage | Script | Notes |
| --- | --- | --- |
| Classify | `pipeline_driver.py` | Emits GREEN/YELLOW/RED queues; adds `queue_bucket` for downstream routing with `chem_routing` support. |
| Acquire | `acquire_worker.py` | Downloads payloads into `raw/{green|yellow}/{license_pool}/{target_id}`. Dry-run by default; `--execute` performs downloads. Supports HTTP/FTP/git/Zenodo/Dataverse, HuggingFace datasets, Figshare, and GitHub releases. |
| Screen YELLOW | `yellow_screen_worker.py` | Converts raw YELLOW payloads into canonical records. Includes `jsonl` default plus chemistry plugins such as `pubchem_computed_only` and `pmc_oa`. Writes pass/pitch ledgers + done markers. |
| Merge | `merge_worker.py` | Combines canonical GREEN + screened YELLOW shards with deduplication and a combined ledger. |
| Catalog | `catalog_builder.py` | Summarizes counts, bytes, manifests, and ledgers across stages. |

`run_pipeline.sh` orchestrates these stages with `--stage classify|acquire_green|acquire_yellow|screen_yellow|merge|catalog`.

---

## Standalone quick start (optional)

```bash
pip install -r requirements.txt

# Dry-run classify only
./run_pipeline.sh --targets ../pipelines/targets/targets_chem.yaml --stage classify

# Acquire GREEN and YELLOW (execute downloads)
./run_pipeline.sh --targets ../pipelines/targets/targets_chem.yaml --stage acquire_green --execute
./run_pipeline.sh --targets ../pipelines/targets/targets_chem.yaml --stage acquire_yellow --execute

# Screen, merge, catalog
./run_pipeline.sh --targets ../pipelines/targets/targets_chem.yaml --stage screen_yellow --execute
./run_pipeline.sh --targets ../pipelines/targets/targets_chem.yaml --stage merge --execute
./run_pipeline.sh --targets ../pipelines/targets/targets_chem.yaml --stage catalog
```

### Notes

- YELLOW screening enforces "anything unclear is pitched"; see `../pipelines/targets/targets_chem.yaml -> globals.screening` and per-target `yellow_screen` overrides.
- Chemistry plugins (e.g., PubChem computed-only) run inside `yellow_screen_worker.py` rather than a standalone stage.
- Outputs are segregated by `license_profile` (`permissive`, `copyleft`, `quarantine`).
- Ledgers in `_ledger/` provide pass/pitch summaries and shard indexes for reproducibility.

## License

Pipeline code is provided as-is for research and development use.

**Data sources retain their own licenses** - this tool helps you track and respect them.
