# Biology Corpus Pipeline (v2)

A safety-first pipeline for building an ethical biology training corpus. Version 2 mirrors the math v2 stage flow but is wired for biology-specific assets (license map, routing, PubMed/PMC strategies, etc.).

1. Classify targets and snapshot evidence (`pipeline_driver.py`).
2. Acquire GREEN and YELLOW targets into raw pools (`acquire_worker.py`).
3. Screen YELLOW data into canonical records with strict pitch behavior (`yellow_screen_worker.py`).
4. Merge GREEN + screened YELLOW into combined candidate shards (`merge_worker.py`).
5. Apply a final screen and difficulty assignment, writing difficulty shards (`difficulty_worker.py`).
6. Build catalogs over the v2 layout (`catalog_builder.py`).

> Not legal advice. This tool helps you track licenses and restrictions; you are responsible for compliance.

---

## Run all pipelines via JupyterLab

This pipeline is typically executed as part of the repository-wide run in JupyterLab using `dataset_collector_run_all_pipelines.ipynb`. The notebook runs every `*_pipeline_v2` directory sequentially, prompts for required API keys, and can install each pipeline's requirements before invoking the stages.

## Directory layout

The recommended roots live in `targets_biology.yaml -> globals`:

```
/data/bio/
  raw/
    green/{license_pool}/{target_id}/...
    yellow/{license_pool}/{target_id}/...
  screened_yellow/{license_pool}/shards/*.jsonl.gz
  combined/{license_pool}/shards/*.jsonl.gz
  final/{license_pool}/d01..d10/shards/*.jsonl.gz
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
| Classify | `pipeline_driver.py` | Emits GREEN/YELLOW/RED queues; adds `queue_bucket` for downstream routing. |
| Acquire | `acquire_worker.py` | Downloads payloads into `raw/{green|yellow}/{license_pool}/{target_id}`. Dry-run by default; `--execute` performs downloads. |
| Screen YELLOW | `yellow_screen_worker.py` | Converts raw YELLOW payloads into canonical records, sharding outputs and writing pass/pitch ledgers + done markers. |
| Merge | `merge_worker.py` | Combines canonical GREEN + screened YELLOW shards with deduplication and a combined ledger. |
| Difficulty | `difficulty_worker.py` | Final light screen + rule-based difficulty assignment; writes difficulty shards and ledger. |
| Catalog | `catalog_builder.py` | Summarizes counts, bytes, and ledgers across stages. |

`run_pipeline.sh` orchestrates these stages with sensible defaults (`--stage classify|acquire_green|acquire_yellow|screen_yellow|merge|difficulty|catalog`).

---

## Standalone quick start (optional)

```bash
pip install -r requirements.txt

# Dry-run classify only
./run_pipeline.sh --targets targets_biology.yaml --stage classify

# Acquire GREEN and YELLOW (execute downloads)
./run_pipeline.sh --targets targets_biology.yaml --stage acquire_green --execute
./run_pipeline.sh --targets targets_biology.yaml --stage acquire_yellow --execute

# Screen, merge, difficulty, catalog
./run_pipeline.sh --targets targets_biology.yaml --stage screen_yellow --execute
./run_pipeline.sh --targets targets_biology.yaml --stage merge --execute
./run_pipeline.sh --targets targets_biology.yaml --stage difficulty --execute
./run_pipeline.sh --targets targets_biology.yaml --stage catalog
```

### Notes

- YELLOW screening enforces "anything unclear is pitched"; see `targets_biology.yaml -> globals.screening` and per-target `yellow_screen` overrides.
- Difficulty assignment happens in `difficulty_worker.py` using `difficulties_biology.yaml` and routing metadata.
- Outputs are segregated by `license_profile` (`permissive`, `copyleft`, `quarantine`).
- Ledgers in `_ledger/` provide pass/pitch summaries and shard indexes for reproducibility.

## License

Pipeline code is provided as-is for research and development use.

**Data sources retain their own licenses** - this tool helps you track and respect them.
