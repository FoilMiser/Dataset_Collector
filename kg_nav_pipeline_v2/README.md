# KG Navigation Pipeline (v2)

A safety-first pipeline for building a license-auditable knowledge-graph navigation corpus. Version 2 mirrors the math pipeline v2 stage order while keeping KG/Lit specifics: computed-only extraction, PII scrubbing for sensitive identifiers, and navigation-episode synthesis handled downstream by the sorter pipeline.

1. **Classify** targets and snapshot evidence (`pipeline_driver.py`).
2. **Acquire** GREEN/YELLOW targets into the v2 raw layout (`acquire_worker.py`).
3. **Screen YELLOW** with adapters + strict pitch (`yellow_screen_worker.py`).
4. **Merge** canonical GREEN + screened YELLOW (`merge_worker.py`).
5. **Catalog** stats and ledgers (`catalog_builder.py`).

> Not legal advice. This tooling helps you track licenses and restrictions; you remain responsible for compliance.

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


The recommended roots live in `../pipelines/targets/targets_kg_nav.yaml -> globals`:

```
/data/kg_nav/
  raw/
    green/{license_pool}/{target_id}/...
    yellow/{license_pool}/{target_id}/...
  screened_yellow/{license_pool}/shards/yellow_shard_00000.jsonl.gz
  combined/{license_pool}/shards/combined_00000.jsonl.gz
  screened/{license_pool}/shards/screened_00000.jsonl.gz
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
| Catalog | `catalog_builder.py` | Summarizes counts, bytes, per-stage shard info, manifests, and ledgers. |

Use `dc pipeline` for classification, `dc run` for acquire/merge/yellow_screen, and `dc catalog-builder` for catalog outputs.

---

## Standalone quick start (optional)

```bash
pip install -r requirements.txt

# Dry-run classify only
dc pipeline kg_nav -- --targets ../pipelines/targets/targets_kg_nav.yaml --stage classify --no-fetch

# Acquire GREEN and YELLOW (execute downloads)
dc run --pipeline kg_nav --stage acquire --allow-data-root -- --queue /data/kg_nav/_queues/green_pipeline.jsonl --bucket green --targets-yaml ../pipelines/targets/targets_kg_nav.yaml --execute
dc run --pipeline kg_nav --stage acquire --allow-data-root -- --queue /data/kg_nav/_queues/yellow_pipeline.jsonl --bucket yellow --targets-yaml ../pipelines/targets/targets_kg_nav.yaml --execute

# Screen, merge, catalog
dc run --pipeline kg_nav --stage yellow_screen --allow-data-root -- --queue /data/kg_nav/_queues/yellow_pipeline.jsonl --targets ../pipelines/targets/targets_kg_nav.yaml --execute
dc run --pipeline kg_nav --stage merge --allow-data-root -- --targets ../pipelines/targets/targets_kg_nav.yaml --execute
dc catalog-builder --pipeline kg_nav --allow-data-root -- --targets ../pipelines/targets/targets_kg_nav.yaml --output /data/kg_nav/_catalogs/catalog.json
```

### Notes

- YELLOW screening enforces "anything unclear is pitched"; tune `../pipelines/targets/targets_kg_nav.yaml -> globals.screening` and per-target `yellow_screen` overrides.
- Outputs are segregated by `license_profile` (`permissive`, `copyleft`, `quarantine`).
- Ledgers in `_ledger/` provide pass/pitch summaries and shard indexes for reproducibility.

## License

Pipeline code is provided as-is for research and development use.

**Data sources retain their own licenses** - this tool helps you track and respect them.
