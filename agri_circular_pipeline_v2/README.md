# Agriculture + Circular Bioeconomy Pipeline (v2.0)

A staged, audit-friendly pipeline for building an agriculture + circular bioeconomy corpus. It adapts the v2 math pipeline layout so every step has explicit queues, manifests, and ledgers.

The pipeline now separates acquisition, strict screening, merging, and cataloging:

1. **Classify** targets + snapshot license evidence → GREEN / YELLOW / RED queues
2. **Acquire** GREEN + YELLOW payloads into `raw/{green|yellow}/...` with per-target manifests
3. **Screen YELLOW** strictly (anything unclear is pitched) → `screened_yellow/...` + pass/pitch ledgers
4. **Merge** canonical GREEN + screened YELLOW → `combined/...` + combined index ledger
5. **Catalog** summaries across stages → `_catalogs/catalog_v2.json`

> Not legal advice. This tool helps you *track* licenses and restrictions; you are responsible for compliance.

---

## Run all pipelines via JupyterLab

This pipeline is typically executed as part of the repository-wide run in JupyterLab using `dataset_collector_run_all_pipelines.ipynb`. The notebook runs every `*_pipeline_v2` directory sequentially, prompts for required API keys, and can install each pipeline's requirements before invoking the stages.

## Recommended run method (Windows-first)

For Windows + Conda, prefer the repo-wide orchestrator:

```bash
python tools/build_natural_corpus.py --repo-root . --dest-root "E:/AI-Research/datasets/Natural" --mode full --execute
```

You can also run the Jupyter notebook, which invokes the same workflow. Use `run_pipeline.sh` only if you have Git Bash/WSL on Windows or are on macOS/Linux.

---

## Directory layout (v2)

Targets YAML defaults to `/data/...`; the orchestrator patches to your `--dest-root`.
For standalone runs, pass `--dataset-root` or use `tools/patch_targets.py`.

```
/data/agri_circular/
  raw/
    green/{license_pool}/{target_id}/...
    yellow/{license_pool}/{target_id}/...

  screened_yellow/{license_pool}/shards/yellow_shard_00000.jsonl.gz
  combined/{license_pool}/shards/combined_00000.jsonl.gz
  screened/{license_pool}/shards/screened_00000.jsonl.gz

  _queues/{green_download,yellow_pipeline,red_rejected}.jsonl
  _manifests/{target_id}/...
  _ledger/{yellow_passed,yellow_pitched,combined_index,merge_summary,screened_index}.jsonl
  _pitches/screened_pitched.jsonl
  _catalogs/catalog_v2.json
  _logs/
```

License pools remain `permissive`, `copyleft`, and `quarantine`.

---

## Stage commands
Use `run_pipeline.sh` to orchestrate stages (dry-run by default):

```bash
# Classify only (dry-run)
./run_pipeline.sh --targets targets_agri_circular.yaml --stage classify

# Acquire
./run_pipeline.sh --targets targets_agri_circular.yaml --stage acquire_green --execute --workers 4
./run_pipeline.sh --targets targets_agri_circular.yaml --stage acquire_yellow --execute --workers 4

# Screen/merge
./run_pipeline.sh --targets targets_agri_circular.yaml --stage screen_yellow --execute
./run_pipeline.sh --targets targets_agri_circular.yaml --stage merge --execute

# Catalog
./run_pipeline.sh --targets targets_agri_circular.yaml --stage catalog
```

Additional helper stages:
- `--stage review` → list pending YELLOW items for manual signoff.
- `--stage all` → run classify → acquire → screen_yellow → merge → catalog.

---

## Key files
- `pipeline_driver.py` — classifies targets into GREEN/YELLOW/RED queues with routing metadata.
- `acquire_worker.py` — downloads GREEN/YELLOW targets into the v2 raw layout and writes manifest markers.
- `yellow_screen_worker.py` — strict YELLOW screening with pass/pitch ledgers; handles JSONL plus common CSV/TSV/TXT/HTML inputs.
- `merge_worker.py` — merges canonical GREEN + screened YELLOW into `combined/` with deduplication and combined index ledger.
- `catalog_builder.py` — summarizes counts/bytes across stages into `_catalogs/catalog_v2.json`.

Legacy/compatibility helpers:
- `yellow_scrubber.py` — lightweight planning helper for YELLOW queues (dry-run only).

---

## Targets + routing
`targets_agri_circular.yaml` now points to the v2 roots and companions. Routing can be provided via `routing` or `agri_routing` blocks; default subject is `agri_circular`.
