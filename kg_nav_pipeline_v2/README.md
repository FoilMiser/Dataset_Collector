# KG + Literature Navigation Pipeline (v2.0)

A safety-first **graph + literature navigation pipeline** aligned with the `math_pipeline_v2` contract. It keeps the KG-first ethics posture from v1 while adopting the v2 flow (classify → acquire → screen → merge → difficulty → catalog) with **computed-only defaults, provenance tracking, and safe-by-default execution** across GREEN and YELLOW pools.

What's new in v2:
- `targets_kg_nav.yaml` (schema v0.8) with explicit v2 roots plus navigation routing metadata for difficulty mapping
- v2 workers: `acquire_worker.py`, `yellow_screen_worker.py`, `merge_worker.py`, `difficulty_worker.py`, `catalog_builder.py`
- Two-pool raw layout: `raw/green` + `raw/yellow` → screened_yellow → combined → final/difficulty shards
- Wrapper `run_pipeline.sh` with stages: classify → acquire_green → acquire_yellow → screen_yellow → merge → difficulty → catalog

> Not legal advice. This tool helps you *track* licenses and restrictions; you are responsible for compliance.

---

## What This Pipeline Does

```
targets_kg_nav.yaml
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
difficulty_worker.py -> final/{pool}/d01..d10/shards/*.jsonl.gz
        |
        v
catalog_builder.py -> _catalogs/global_catalog.json
```

---

## Quick Start

### Install
```bash
pip install -r requirements.txt
```

### Classify (dry-run by default)
```bash
./run_pipeline.sh --targets targets_kg_nav.yaml --stage classify
```

### Review pending YELLOW items
```bash
./run_pipeline.sh --targets targets_kg_nav.yaml --stage review
# or:
python3 review_queue.py --queue /data/kg_nav/_queues/yellow_pipeline.jsonl list
```

### Execute end-to-end (recommended order)
```bash
./run_pipeline.sh --targets targets_kg_nav.yaml --stage classify
./run_pipeline.sh --targets targets_kg_nav.yaml --stage acquire_green --execute
./run_pipeline.sh --targets targets_kg_nav.yaml --stage acquire_yellow --execute
./run_pipeline.sh --targets targets_kg_nav.yaml --stage screen_yellow --execute
./run_pipeline.sh --targets targets_kg_nav.yaml --stage merge --execute
./run_pipeline.sh --targets targets_kg_nav.yaml --stage difficulty --execute
./run_pipeline.sh --targets targets_kg_nav.yaml --stage catalog
```

---

## Repository Layout

- `pipeline_driver.py` - classify targets, snapshot license evidence, emit queues with routing metadata
- `review_queue.py` - manual YELLOW review/signoff helper
- `acquire_worker.py` - download GREEN/YELLOW payloads into `raw/green|yellow/{pool}/{target}` (supports http/git/ftp/zenodo/dataverse/HF + KG-specific s3_sync/requester_pays/figshare/torrent)
- `yellow_screen_worker.py` - canonicalize YELLOW payloads into screened JSONL shards + ledgers
- `merge_worker.py` - merge GREEN + screened YELLOW into combined shards
- `difficulty_worker.py` - assign difficulty and write final shards bucketed by d01–d10
- `catalog_builder.py` - summarize raw/screened/combined/final outputs
- `run_pipeline.sh` - convenience wrapper for the v2 stage order
- `kg_worker.py`, `pii_scrub_worker.py`, `nav_episode_builder.py` - domain-specific scaffolds for KG normalization, PII scrubbing, and navigation episode synthesis
- `yellow_scrubber.py` - legacy helper for bespoke YELLOW transforms (optional); `yellow_scrubber_legacy.py` retains the v1 behavior for reference
- `pmc_worker.py` - optional PMC addon (run before merge if used)

### Configuration
- `targets_kg_nav.yaml` - inventory + roots/queues/routing (schema v0.8)
- `license_map.yaml` - SPDX normalization rules + gating policy
- `field_schemas.yaml` - versioned schemas for computed-only graph + navigation records
- `denylist.yaml` - explicit denylist patterns (v0.2 with severity and provenance)
- `difficulties_kg_nav.yaml` - routing → difficulty defaults for `difficulty_worker.py`

---

## Output Structure (default)

```
/data/kg_nav/
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
  _catalogs/global_catalog.json
  _logs/...
```

---

## Notes / Safety

- **RED items should never be included in training manifests**, even if you have local copies.
- Prefer **computed-only** and **record-level allowlisting** for graph registries and navigation evidence.
- Snapshot license/terms evidence before large downloads (OpenAlex/Wikidata/COCI/ROR refreshes can change terms).
- Treat "conditional" / ambiguous licenses as YELLOW until reviewed; keep copyleft pools isolated.
- Use `split_group_id` to prevent data leakage across navigation episodes.
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
pyarrow>=14.0.0     # Parquet output
datasketch>=1.6.0   # Near-duplicate detection
aria2>=1.36.0       # Torrent/magnet handling
awscli>=1.32.0      # s3 sync/requester-pays helpers
```

---

Pipeline code is provided as-is for research and development use. **Data sources retain their own licenses** — this tool helps you track and respect them.
