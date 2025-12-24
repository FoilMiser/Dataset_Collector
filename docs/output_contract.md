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
  final/
    {permissive,copyleft,quarantine}/
      d01/shards/*.jsonl(.gz)
      ...
      d10/shards/*.jsonl(.gz)
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
- **merge**: combines green and screened yellow data into `combined/<pool>/shards/`.
- **difficulty**: routes records into `final/<pool>/d01..d10/shards/`.
- **catalog**: writes summary metadata to `_catalogs/catalog.json`.

## Ledger and manifests

- `_ledger/` tracks acquisition events and audit metadata.
- `_pitches/` stores potential sources and metadata for future runs.
- `_manifests/` contains per-stage manifests and patched targets YAMLs.
- `_logs/` stores per-stage log output, including orchestrator logs.

## Pools and difficulty shards

All pools are standardized:

- `permissive`
- `copyleft`
- `quarantine`

Difficulty folders are always two-digit (`d01`–`d10`). Ensure these folders exist
before writing outputs; the orchestrator initializes them automatically.
