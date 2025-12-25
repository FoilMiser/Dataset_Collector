# KG Navigation Pipeline (v2)

A safety-first pipeline for building a license-auditable knowledge-graph navigation corpus. Version 2 mirrors the math pipeline v2 stage order while keeping KG/Lit specifics: computed-only extraction, PII scrubbing for sensitive identifiers, and navigation-episode synthesis in the difficulty stage.

1. **Classify** targets and snapshot evidence (`pipeline_driver.py`).
2. **Acquire** GREEN/YELLOW targets into the v2 raw layout (`acquire_worker.py`).
3. **Screen YELLOW** with adapters + strict pitch (`yellow_screen_worker.py`).
4. **Merge** canonical GREEN + screened YELLOW (`merge_worker.py`).
5. **Difficulty + episodes** with routing-aware buckets (`difficulty_worker.py`).
6. **Catalog** stats and ledgers (`catalog_builder.py`).

> Not legal advice. This tooling helps you track licenses and restrictions; you remain responsible for compliance.

---

## Run all pipelines via JupyterLab

This pipeline is typically executed as part of the repository-wide run in JupyterLab using `dataset_collector_run_all_pipelines.ipynb`. The notebook runs every `*_pipeline_v2` directory sequentially, prompts for required API keys, and can install each pipeline's requirements before invoking the stages.

## Directory layout

The recommended roots live in `targets_kg_nav.yaml -> globals`:

```
/data/kg_nav/
  raw/
    green/{license_pool}/{target_id}/...
    yellow/{license_pool}/{target_id}/...
  screened_yellow/{license_pool}/shards/yellow_shard_00000.jsonl.gz
  combined/{license_pool}/shards/combined_00000.jsonl.gz
  final/{license_pool}/d01..d10/{subject}/{domain}/{category}/shards/final_00000.jsonl.gz
  _ledger/*.jsonl
  _pitches/*.jsonl
  _queues/*.jsonl
  _manifests/{target_id}/...
  _catalogs/*.json
```

Sharding is controlled by `globals.sharding` (max records per shard, compression, naming `*_00000.jsonl.gz`).

---

## Stage overview

| Stage | Script | Notes |
| --- | --- | --- |
| Classify | `pipeline_driver.py` | Emits GREEN/YELLOW/RED queues; snaps license evidence; adds routing defaults for KG navigation. |
| Acquire | `acquire_worker.py` | Downloads payloads into `raw/{green|yellow}/{license_pool}/{target_id}`. Dry-run by default; `--execute` performs downloads. Supports HTTP/FTP/Git/Zenodo/Dataverse/HF plus KG-specific `figshare`, `s3_sync`, `aws_requester_pays`, `torrent`. |
| Screen YELLOW | `yellow_screen_worker.py` | Adapter-based canonicalization (`wikidata_truthy_edges`, `openalex_minimal_graph`, `crossref_minimal_graph`, `opencitations_coci_edges`, `orcid_scrub_minimal`, `nlm_mesh_minimal`). Strict pitch ledger + done markers. |
| Merge | `merge_worker.py` | Combines canonical GREEN + screened YELLOW shards with content hash dedupe and a combined ledger. |
| Difficulty | `difficulty_worker.py` | Structural difficulty heuristics for navigation episodes + routing defaults from `difficulties_kg_nav.yaml`; writes difficulty shards and ledger index. |
| Catalog | `catalog_builder.py` | Summarizes counts, bytes, per-stage shard info, and difficulty distribution. |

`run_pipeline.sh` orchestrates these stages (`--stage classify|acquire_green|acquire_yellow|screen_yellow|merge|difficulty|catalog`).

---

## Standalone quick start (optional)

```bash
pip install -r requirements.txt

# Dry-run classify only
./run_pipeline.sh --targets targets_kg_nav.yaml --stage classify

# Acquire GREEN and YELLOW (execute downloads)
./run_pipeline.sh --targets targets_kg_nav.yaml --stage acquire_green --execute
./run_pipeline.sh --targets targets_kg_nav.yaml --stage acquire_yellow --execute

# Screen, merge, difficulty, catalog
./run_pipeline.sh --targets targets_kg_nav.yaml --stage screen_yellow --execute
./run_pipeline.sh --targets targets_kg_nav.yaml --stage merge --execute
./run_pipeline.sh --stage difficulty --targets targets_kg_nav.yaml --execute
./run_pipeline.sh --targets targets_kg_nav.yaml --stage catalog
```

### Notes

- YELLOW screening enforces "anything unclear is pitched"; tune `targets_kg_nav.yaml -> globals.screening` and per-target `yellow_screen` overrides.
- Difficulty assignment prefers existing difficulty, then routing defaults from `difficulties_kg_nav.yaml`, then structural heuristics (hop/join/branch/evidence/reconciliation).
- Outputs are segregated by `license_profile` (`permissive`, `copyleft`, `quarantine`).
- Ledgers in `_ledger/` provide pass/pitch summaries and shard indexes for reproducibility.

## License

Pipeline code is provided as-is for research and development use.

**Data sources retain their own licenses** - this tool helps you track and respect them.
