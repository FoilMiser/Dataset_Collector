# Logic corpus pipeline v2 notes

This folder ports the original `logic_pipeline_v1` into the v2 layout used by `math_pipeline_v2`.
Key references:
- `logic_pipeline_v1_to_v2_adaptation.md` (source plan)
- `targets_logic.yaml` (v2 globals, queues, and routing-aware targets)
- `difficulties_logic.yaml` (routing → default difficulty mapping)

### Major changes vs. v1
- Switched to the v2 stage contract: `classify`, `acquire_green`, `acquire_yellow`, `screen_yellow`, `merge`, `difficulty`, `catalog`.
- Raw/processed layout now matches `/data/logic/{raw,screened_yellow,combined,final}` with per-pool shards and ledgers.
- `pipeline_driver.py` emits routing fields (nested + flattened) for downstream workers and uses `logic` as the default subject.
- `acquire_worker.py`, `yellow_screen_worker.py`, `merge_worker.py`, and `difficulty_worker.py` mirror math v2 behavior, writing manifests and ledgers in `_manifests` and `_ledger`.
- New `difficulties_logic.yaml` drives the difficulty worker and aligns with logic domains/categories.
- Queues and run orchestration mirror math v2 (`run_pipeline.sh`).

Use `./run_pipeline.sh --targets targets_logic.yaml --stage classify` to dry-run the new queue emission, then progress through the stages with `--execute` as needed.
