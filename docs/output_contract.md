# Output Contract (Natural Corpus)

Each pipeline writes a domain-specific dataset root under the Natural corpus destination. A
single domain folder must include the following structure:

```
<dataset_root>/
  raw/
    green/{permissive,copyleft,quarantine}/<target_id>/...
    yellow/{permissive,copyleft,quarantine}/<target_id>/...
  screened_yellow/
    {permissive,copyleft,quarantine}/shards/*.jsonl(.gz)
  combined/
    {permissive,copyleft,quarantine}/shards/*.jsonl(.gz)
  _queues/*.jsonl
  _ledger/*.jsonl
  _pitches/*.jsonl
  _manifests/...
  _catalogs/catalog.json
  _logs/...
```

## Stage outputs

- **classify**: emits queue files in `_queues/` describing which targets to acquire.
- **acquire_green / acquire_yellow**: downloads raw files into `raw/green/...` and
  `raw/yellow/...` respectively.
- **screen_yellow**: canonicalizes yellow data into `screened_yellow/<pool>/shards/`.
- **merge**: combines green and screened yellow data into `combined/<pool>/shards/`. The merge stage consumes JSONL (`.jsonl/.jsonl.gz`) and HF saved datasets (`datasets.load_from_disk`) from `raw/green/...`.
- **catalog**: writes summary metadata to `_catalogs/catalog.json`.
- **downstream normalization (optional, out-of-scope)**: no difficulty routing occurs in this
  repo; optional fields like `difficulty_level` may appear in queue rows for downstream use.
  Downstream workflows should consume `combined/<pool>/shards/` plus manifests/ledgers as needed.

## Merge deduplication strategy

The merge worker uses a SQLite-backed index (`_ledger/combined_dedupe.sqlite`) keyed by
`content_sha256` to enforce deterministic, first-seen wins deduplication without keeping all
hashes in memory. Tradeoffs: this keeps memory bounded and determinism stable across runs, but
adds local disk I/O and uses a single-writer index. Alternatives like hash-bucket partitioning
reduce DB overhead but create many intermediate files, while Bloom filter + verify can be faster
but introduces probabilistic false positives and requires a second pass to confirm duplicates.

## Ledger and manifests

- `_ledger/` tracks acquisition events and audit metadata.
- Canonical YELLOW ledgers:
  - `_ledger/yellow_passed.jsonl`
  - `_ledger/yellow_pitched.jsonl`
- Pitched samples (capped per run) live in `_pitches/yellow_pitch.jsonl`.
- `_pitches/` stores potential sources and metadata for future runs.
- `_manifests/` contains per-stage manifests and patched targets YAMLs.
- `_logs/` stores per-stage log output, including orchestrator logs.

## Pools

All pools are standardized:

- `permissive`
- `copyleft`
- `quarantine`

Downstream consumers may bucket or normalize these pools further, but that work is
outside the collector contract.
