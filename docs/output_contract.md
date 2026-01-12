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
- **yellow_screen**: canonicalizes yellow data into `screened_yellow/<pool>/shards/`.
- **merge**: combines green and screened yellow data into `combined/<pool>/shards/`. The merge stage consumes JSONL (`.jsonl/.jsonl.gz`) and HF saved datasets (`datasets.load_from_disk`) from `raw/green/...`.
- **catalog**: writes summary metadata to `_catalogs/catalog.json`.
- **downstream normalization (optional, out-of-scope)**: no difficulty routing occurs in this
  repo; optional fields like `difficulty_level` may appear in queue rows for downstream use.
  Downstream workflows should consume `combined/<pool>/shards/` plus manifests/ledgers as needed.

## Catalog summary fields

The catalog output includes aggregated rollups for acquisition and classification:

- `raw.buckets.<bucket>.pools.<pool>.{targets,bytes}`: per-license-pool counts and bytes for raw GREEN/YELLOW downloads.
- `license_pools.<bucket>.<pool>.{targets,bytes}`: per-license-pool totals across GREEN/YELLOW raw plus RED queue counts (bytes are `0` for RED).
- `strategy_counts`: counts of targets per `download.strategy` in the targets config.
- `top_targets_by_bytes`: top N raw targets by size with `{target_id,bucket,pool,bytes,files}`.
- `top_licenses`: top N most frequent licenses observed in queue rows, reported as `{license,count}`.

## Merge deduplication strategy

The merge worker uses a SQLite-backed index (`_ledger/combined_dedupe.sqlite`) keyed by
`content_sha256` to enforce deterministic, first-seen wins deduplication without keeping all
hashes in memory. Tradeoffs: this keeps memory bounded and determinism stable across runs, but
adds local disk I/O and uses a single-writer index.

For large merges, the dedupe index can be partitioned into multiple SQLite files by setting
`globals.merge.dedupe_partitions` (or `--dedupe-partitions`). This creates
`_ledger/combined_dedupe_partNNN.sqlite` files, routing hashes by prefix to reduce per-file
index size while retaining deterministic behavior.

## Ledger and manifests

- `_ledger/` tracks acquisition events and audit metadata.
- Canonical YELLOW ledgers:
  - `_ledger/yellow_passed.jsonl`
  - `_ledger/yellow_pitched.jsonl`
- Merge ledgers and summaries:
  - `_ledger/combined_index.jsonl`
  - `_ledger/merge_summary.json`
  - `_ledger/combined_dedupe.sqlite` (and optional `_ledger/combined_dedupe_partNNN.sqlite`)
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

## JSONL Record Contract

Each JSONL shard contains one record per line. Records are JSON objects that must include
the required fields below; optional fields are allowed but should not override required
semantics. Field names are stable across stages (`yellow_screen`, `merge`) so downstream
pipelines can rely on a consistent contract.

### Required fields and types

**Core identifiers (provenance)**
- `dataset_id` (string): stable identifier for the upstream dataset/source.
- `split` (string): source split label (for example `train`, `test`, `validation`).
- `config` (string): dataset configuration name (use `default` when none is defined).
- `row_id` (string): row-level identifier in the upstream dataset (stringified if numeric).

**License**
- `license_spdx` (string): SPDX license expression or `NOASSERTION` when unknown.
- `license_profile` (string): normalized collector license bucket (example: `permissive`).

**Evidence**
- `source_urls` (array of strings): URLs that support the content provenance.
- `reviewer_notes` (string): reviewer or pipeline notes justifying acceptance or flags.

**Hashing**
- `content_sha256` (string): SHA-256 hash of the raw content payload.
- `normalized_sha256` (string): SHA-256 hash of the normalized content payload.

**Routing**
- `pool` (string): pool routing label (`permissive`, `copyleft`, `quarantine`).
- `pipeline` (string): pipeline identifier that produced the record.
- `target_name` (string): canonical target name/slug for the record.
- `timestamp_created` (string): ISO-8601 timestamp when the record was created.
- `timestamp_updated` (string): ISO-8601 timestamp of the most recent update.

### Minimal example record

```
{
  "dataset_id": "open-webtext",
  "split": "train",
  "config": "default",
  "row_id": "123456",
  "license_spdx": "CC-BY-4.0",
  "license_profile": "permissive",
  "source_urls": [
    "https://example.com/article/123456"
  ],
  "reviewer_notes": "Auto-validated by crawler; no red flags detected.",
  "content_sha256": "d2f7b0cba1f651a1d0c0d4c4e5df85c90a0f6de9e4e0d2b1f8cf9f6b2e4e3a10",
  "normalized_sha256": "f1d2d2f924e986ac86fdf7b36c94bcdf32beec15a0f7c02e6dbf7b9d0a3a0f1e",
  "pool": "permissive",
  "pipeline": "nlp_pipeline_v2",
  "target_name": "open-webtext-article",
  "timestamp_created": "2024-02-12T18:21:45Z",
  "timestamp_updated": "2024-02-12T18:21:45Z"
}
```
