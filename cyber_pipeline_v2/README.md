# Cyber Corpus Pipeline (v2)

Cybersecurity-focused staging of the math pipeline v2 layout. This version keeps the safety-first flow (evidence → queues → acquisition → screening → merge → catalog) while adopting cyber defaults, roots, and helper workers.

Stage order:
1) **Classify** targets and snapshot evidence (`pipeline_driver.py`).
2) **Acquire** GREEN and YELLOW payloads into `/data/cyber/raw/...` (`acquire_worker.py`).
3) **Screen YELLOW** records with strict pitch behavior (`yellow_screen_worker.py`).
4) **Merge** GREEN + screened YELLOW shards (`merge_worker.py`).
5) **Catalog** counts and shard summaries (`catalog_builder.py`).

> Not legal advice. This tool helps you track licenses and restrictions; you are responsible for compliance.

---

## Run all pipelines via JupyterLab

This pipeline is typically executed as part of the repository-wide run in JupyterLab using `dataset_collector_run_all_pipelines.ipynb`. The notebook runs every `*_pipeline_v2` directory sequentially, prompts for required API keys, and can install each pipeline's requirements before invoking the stages.

---

## Directory layout

Configured via `targets_cyber.yaml -> globals`:

```
/data/cyber/
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
  _catalogs/
  _logs/
```

Sharding is controlled by `globals.sharding` (max records per shard, compression, naming `*_00000.jsonl.gz`).

---

## Stage overview

| Stage | Script | Notes |
| --- | --- | --- |
| Classify | `pipeline_driver.py` | Emits GREEN/YELLOW/RED queues; writes per-target manifests with evidence snapshots. |
| Acquire | `acquire_worker.py` | Downloads payloads into `raw/{green|yellow}/{license_pool}/{target_id}`. Dry-run by default; `--execute` performs downloads. |
| Screen YELLOW | `yellow_screen_worker.py` | Screens JSONL records, writing pass/pitch ledgers, done markers, and sharded outputs. |
| Merge | `merge_worker.py` | Combines GREEN + screened YELLOW shards with deduplication and a combined ledger. |
| Catalog | `catalog_builder.py` | Summarizes counts, bytes, manifests, and ledgers across stages. |

`run_pipeline.sh` orchestrates these stages with sensible defaults (`--stage classify|review|acquire_green|acquire_yellow|screen_yellow|merge|catalog`).

---

## Standalone quick start (optional)

```bash
pip install -r requirements.txt

# Dry-run classify only
./run_pipeline.sh --targets targets_cyber.yaml --stage classify

# Acquire GREEN and YELLOW (execute downloads)
./run_pipeline.sh --targets targets_cyber.yaml --stage acquire_green --execute
./run_pipeline.sh --targets targets_cyber.yaml --stage acquire_yellow --execute

# Screen, merge, catalog
./run_pipeline.sh --targets targets_cyber.yaml --stage screen_yellow --execute
./run_pipeline.sh --targets targets_cyber.yaml --stage merge --execute
./run_pipeline.sh --targets targets_cyber.yaml --stage catalog
```

### Notes

- YELLOW screening enforces "anything unclear is pitched"; see `targets_cyber.yaml -> globals.screening`.
- Outputs are segregated by `license_profile` (`permissive`, `copyleft`, `quarantine`).
- Ledgers in `_ledger/` provide pass/pitch summaries and shard indexes for reproducibility.

## License

Pipeline code is provided as-is for research and development use.

**Data sources retain their own licenses** - this tool helps you track and respect them.
