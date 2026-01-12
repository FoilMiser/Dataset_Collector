# Code Corpus Pipeline (v2)

Version 2 aligns the code pipeline with the math_pipeline_v2 stage flow while layering in code-specific handling (secrets redaction/pitching, vendored stripping, and lightweight language detection tuned for code domains).

Stages:

1. Classify targets and snapshot license/ToU evidence (`dc pipeline`).
2. Acquire GREEN and YELLOW targets into the v2 raw layout (`dc run --stage acquire` + `code_worker.py` extraction).
3. Screen YELLOW data into canonical records with strict pitch rules (`dc run --stage yellow_screen`).
4. Merge canonical GREEN + screened YELLOW into combined shards (`dc run --stage merge`).
5. Build collector catalogs, ledgers, and manifests over screened shards (`dc catalog-builder`).

> Not legal advice. This tooling helps track licenses and restrictions; you remain responsible for compliance.

---

## Run all pipelines via JupyterLab

This pipeline is typically executed as part of the repository-wide run in JupyterLab using `dataset_collector_run_all_pipelines.ipynb`. The notebook runs every `*_pipeline_v2` directory sequentially, prompts for required API keys, and can install each pipeline's requirements before invoking the stages.

## Recommended run method (Windows-first)

For Windows + Conda, prefer the repo-wide orchestrator:

```bash
python tools/build_natural_corpus.py --repo-root . --dest-root "E:/AI-Research/datasets/Natural" --mode full --execute
```

You can also run the Jupyter notebook, which invokes the same workflow. For direct CLI runs, use `dc` (prefer `dc pipeline` for classification and `dc run` for worker stages).

---

## Directory layout

Targets YAML defaults to `/data/...`; the orchestrator patches to your `--dest-root`.
For standalone runs, pass `--dataset-root` or use `tools/patch_targets.py`.


Roots are defined in `../pipelines/targets/targets_code.yaml -> globals`:

```
/data/code/
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
| Classify | `dc pipeline` | Emits GREEN/YELLOW/RED queues with `routing` defaults tuned for code. |
| Acquire | `dc run --stage acquire` | Downloads payloads into `raw/{green|yellow}/{license_pool}/{target_id}` and optionally runs `code_worker.py` to emit canonical shards. Supports HTTP/FTP/git/Zenodo/Dataverse, HuggingFace datasets, Figshare, and GitHub releases. |
| Screen YELLOW | `dc run --stage yellow_screen` | Converts raw YELLOW payloads into canonical records, pitching anything that violates length/licensing/deny-phrase or secrets policies. Writes pass/pitch ledgers + done markers. |
| Merge | `dc run --stage merge` | Combines canonical GREEN + screened YELLOW shards with deduplication and a combined ledger. |
| Catalog | `dc catalog-builder` | Summarizes counts, bytes, language coverage, manifests, and ledgers across stages. |

Use `dc pipeline` for classification, `dc run` for acquire/merge/yellow_screen, and `dc catalog-builder` for catalog outputs.

---

## Standalone quick start (optional)

```bash
pip install -r requirements.txt

# Dry-run classify only
dc pipeline code -- --targets ../pipelines/targets/targets_code.yaml --stage classify --no-fetch

# Acquire GREEN and YELLOW (execute downloads)
dc run --pipeline code --stage acquire --allow-data-root -- --queue /data/code/_queues/green_pipeline.jsonl --bucket green --targets-yaml ../pipelines/targets/targets_code.yaml --execute
dc run --pipeline code --stage acquire --allow-data-root -- --queue /data/code/_queues/yellow_pipeline.jsonl --bucket yellow --targets-yaml ../pipelines/targets/targets_code.yaml --execute

# Screen, merge, catalog
dc run --pipeline code --stage yellow_screen --allow-data-root -- --queue /data/code/_queues/yellow_pipeline.jsonl --targets ../pipelines/targets/targets_code.yaml --execute
dc run --pipeline code --stage merge --allow-data-root -- --targets ../pipelines/targets/targets_code.yaml --execute
dc catalog-builder --pipeline code --allow-data-root -- --targets ../pipelines/targets/targets_code.yaml --output /data/code/_catalogs/catalog.json
```

### Notes

- YELLOW screening enforces "anything unclear is pitched"; see `../pipelines/targets/targets_code.yaml -> globals.screening` and per-target `yellow_screen` overrides.
- Secrets found in YELLOW are pitched by default; GREEN records redact secrets in-place.
- Outputs are segregated by `license_profile` (`permissive`, `copyleft`, `quarantine`).
- Ledgers in `_ledger/` provide pass/pitch summaries and shard indexes for reproducibility.

## License

Pipeline code is provided as-is for research and development use.

**Data sources retain their own licenses** - this tool helps you track and respect them.
