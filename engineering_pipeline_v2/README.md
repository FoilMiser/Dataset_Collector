# Engineering Corpus Pipeline (v2.0)

A safety-first **two-pool engineering pipeline** aligned with the `math_pipeline_v2` contract. It emphasizes **license compliance, provenance tracking, and safe-by-default execution** across GREEN and YELLOW pools.

What's new in v2:
- `../pipelines/targets/targets_engineering.yaml` (schema v0.8) with explicit roots + routing metadata for downstream sorting
- v2 workers: `acquire_worker.py`, `yellow_screen_worker.py`, `merge_worker.py`, `catalog_builder.py`
- Two-pool raw layout: `raw/green` + `raw/yellow` → screened_yellow → combined → screened shards
- Unified CLI via `dc pipeline` (classification) and `dc run` (worker stages)

> Not legal advice. This tool helps you *track* licenses and restrictions; you are responsible for compliance.

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

## What This Pipeline Does

```
../pipelines/targets/targets_engineering.yaml
        |
        v
pipeline_driver.py  -> _queues/{green_download,yellow_pipeline,red_rejected}.jsonl
        |
        v
acquire_worker.py (green/yellow) -> raw/{green|yellow}/{pool}/{target}/...
        |
        v
yellow_screen_worker.py -> screened_yellow/{pool}/shards/*.jsonl.gz
        |
        v
merge_worker.py -> combined/{pool}/shards/*.jsonl.gz
        |
        v
catalog_builder.py -> _catalogs/global_catalog.json
```

---

## Standalone quick start (optional)

### Install
```bash
pip install -r requirements.txt
```

### Classify (dry-run by default)
```bash
dc pipeline engineering -- --targets ../pipelines/targets/targets_engineering.yaml --stage classify --no-fetch
```

### Review pending YELLOW items
```bash
dc review-queue --pipeline engineering -- --queue /data/engineering/_queues/yellow_pipeline.jsonl --targets ../pipelines/targets/targets_engineering.yaml list
# or:
python3 review_queue.py --queue /data/engineering/_queues/yellow_pipeline.jsonl list
```

### Execute end-to-end (recommended order)
```bash
dc pipeline engineering -- --targets ../pipelines/targets/targets_engineering.yaml --stage classify
dc run --pipeline engineering --stage acquire --allow-data-root -- \
  --queue /data/engineering/_queues/green_pipeline.jsonl \
  --bucket green \
  --targets-yaml ../pipelines/targets/targets_engineering.yaml \
  --execute
dc run --pipeline engineering --stage acquire --allow-data-root -- \
  --queue /data/engineering/_queues/yellow_pipeline.jsonl \
  --bucket yellow \
  --targets-yaml ../pipelines/targets/targets_engineering.yaml \
  --execute
dc run --pipeline engineering --stage yellow_screen --allow-data-root -- \
  --queue /data/engineering/_queues/yellow_pipeline.jsonl \
  --targets ../pipelines/targets/targets_engineering.yaml \
  --execute
dc run --pipeline engineering --stage merge --allow-data-root -- \
  --targets ../pipelines/targets/targets_engineering.yaml \
  --execute
dc catalog-builder --pipeline engineering --allow-data-root -- \
  --targets ../pipelines/targets/targets_engineering.yaml \
  --output /data/engineering/_catalogs/global_catalog.json
```

---

## Repository Layout

- `pipeline_driver.py` - classify targets, snapshot license evidence, emit queues with routing metadata
- `review_queue.py` - manual YELLOW review/signoff helper
- `acquire_worker.py` - download GREEN/YELLOW payloads into `raw/green|yellow/{pool}/{target}`
- `yellow_screen_worker.py` - canonicalize YELLOW payloads into screened JSONL shards + ledgers
- `merge_worker.py` - merge GREEN + screened YELLOW into combined shards
- `catalog_builder.py` - summarize raw/screened/combined outputs
- `legacy/run_pipeline.sh` - deprecated wrapper for the v2 stage order
- `yellow_scrubber.py` - legacy helper for bespoke YELLOW transforms (optional)
- `pmc_worker.py` - optional PMC addon (run before merge if used)

### Configuration
- `../pipelines/targets/targets_engineering.yaml` - inventory + roots/queues/routing (schema v0.8)
- `license_map.yaml` - SPDX normalization rules + gating policy
- `field_schemas.yaml` - versioned schemas for extracted/normalized records
- `denylist.yaml` - explicit denylist patterns (v0.2 with severity and provenance)

---

## Output Structure (default)

```
/data/engineering/
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
  _catalogs/global_catalog.json
  _logs/...
```

---

## Notes / Safety

- **RED items should never be included in training manifests**, even if you have local copies.
- Prefer **computed-only** and **record-level allowlisting** when possible.
- Always snapshot license/terms evidence before large downloads.
- Treat "conditional" / ambiguous licenses as YELLOW until reviewed.
- Use `split_group_id` to prevent data leakage across train/valid splits.
- For RED/YELLOW decisions, run through the legal checklist:
  - Confirm evidence snapshot hash matches the manifest and note any change detections
  - Verify restriction phrases manually even if automated scan is empty
  - Check provenance links in denylist hits and document rationale for overrides
  - Capture reviewer contact + constraints in `review_signoff.json`

---

## Dependencies

```
pyyaml>=6.0         # Core
requests>=2.31.0    # Core

# Optional
tiktoken>=0.5.0     # Token counting
pyarrow>=15.0.0     # Parquet output
datasketch>=1.6.0   # Near-duplicate detection
rdkit>=2023.9.0     # SMILES/InChIKey normalization (legacy chem helpers)
```

---

Pipeline code is provided as-is for research and development use. **Data sources retain their own licenses** — this tool helps you track and respect them.
