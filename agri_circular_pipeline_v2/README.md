# Agriculture + Circular Bioeconomy Pipeline (v2.0)

A staged, audit-friendly pipeline for building an agriculture + circular bioeconomy corpus. It adapts the v2 math pipeline layout so every step has explicit queues, manifests, and ledgers.

The pipeline now separates acquisition, strict screening, merging, and difficulty assignment:

1. **Classify** targets + snapshot license evidence → GREEN / YELLOW / RED queues
2. **Acquire** GREEN + YELLOW payloads into `raw/{green|yellow}/...` with per-target manifests
3. **Screen YELLOW** strictly (anything unclear is pitched) → `screened_yellow/...` + pass/pitch ledgers
4. **Merge** canonical GREEN + screened YELLOW → `combined/...` + combined index ledger
5. **Difficulty** final screening + difficulty assignment → `final/.../d01..d10/...` + final index ledger
6. **Catalog** summaries across stages → `_catalogs/catalog_v2.json`

> Not legal advice. This tool helps you *track* licenses and restrictions; you are responsible for compliance.

---

## Directory layout (v2)
```
/data/agri_circular/
  raw/
    green/{license_pool}/{target_id}/...
    yellow/{license_pool}/{target_id}/...

  screened_yellow/{license_pool}/shards/yellow_shard_00000.jsonl.gz
  combined/{license_pool}/shards/combined_00000.jsonl.gz
  final/{license_pool}/d01..d10/shards/final_00000.jsonl.gz

  _queues/{green_download,yellow_pipeline,red_rejected}.jsonl
  _manifests/{target_id}/...
  _ledger/{yellow_passed,yellow_pitched,combined_index,merge_summary,final_index,difficulty_summary}.jsonl
  _pitches/final_pitched.jsonl
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

# Screen/merge/difficulty
./run_pipeline.sh --targets targets_agri_circular.yaml --stage screen_yellow --execute
./run_pipeline.sh --targets targets_agri_circular.yaml --stage merge --execute
./run_pipeline.sh --targets targets_agri_circular.yaml --stage difficulty --execute

# Catalog
./run_pipeline.sh --targets targets_agri_circular.yaml --stage catalog
```

Additional helper stages:
- `--stage review` → list pending YELLOW items for manual signoff.
- `--stage all` → run classify → acquire → screen_yellow → merge → difficulty → catalog.

---

## Key files
- `pipeline_driver.py` — classifies targets into GREEN/YELLOW/RED queues with routing metadata.
- `acquire_worker.py` — downloads GREEN/YELLOW targets into the v2 raw layout and writes manifest markers.
- `yellow_screen_worker.py` — strict YELLOW screening with pass/pitch ledgers; handles JSONL plus common CSV/TSV/TXT/HTML inputs.
- `merge_worker.py` — merges canonical GREEN + screened YELLOW into `combined/` with deduplication and combined index ledger.
- `difficulty_worker.py` — assigns difficulty (d01–d10) using routing + heuristics; writes final shards and ledgers.
- `catalog_builder.py` — summarizes counts/bytes across stages into `_catalogs/catalog_v2.json`.
- `difficulties_agri_circular.yaml` — starter difficulty map keyed by routing (`subject: agri_circular`).

Legacy/compatibility helpers:
- `download_worker_legacy.py`, `yellow_scrubber_legacy.py` — kept for reference; not used in the v2 flow.
- `yellow_scrubber.py` — lightweight planning helper for YELLOW queues (dry-run only).

---

## Targets + routing
`targets_agri_circular.yaml` now points to the v2 roots and companions, including `difficulties_agri_circular.yaml`. Routing can be provided via `routing` or `agri_routing` blocks; default subject is `agri_circular`.
