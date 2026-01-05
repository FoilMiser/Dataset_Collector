# Pipeline Runtime Contract (v2)

This document standardizes how **pipeline drivers** and **runtime worker entrypoints**
behave across every `*_pipeline_v2/` directory. It complements
`docs/output_contract.md` (what files are produced) by defining **how** the
CLI/runtime determines those locations and retries.

## Scope

Applies to:

- `*_pipeline_v2/pipeline_driver.py`
- `*_pipeline_v2/yellow_screen_worker.py` (via `collector_core`)
- `*_pipeline_v2/merge_worker.py` (via `collector_core`)

## Root resolution and precedence

All runtime stages resolve output roots in the same order:

1. **Explicit CLI flags** (per-root overrides).
2. **`--dataset-root`** (applies to standard subfolders).
3. **Environment** (`DATASET_ROOT` or `DATASET_COLLECTOR_ROOT`).
4. **Targets YAML** (`globals.*`).
5. **Built-in defaults**.

## Targets YAML globals

These `globals` keys tune runtime defaults without changing CLI usage:

- `globals.pitch_limits.sample_limit` — max pitch samples per `(target, reason)` (default: `25`).
- `globals.pitch_limits.text_limit` — max chars stored for pitched samples (default: `400`).
- `globals.retry.max` — default retry count for evidence/download fetchers (default: `3`).
- `globals.retry.backoff` — base for exponential backoff (default: `2.0`).
- `globals.sharding.max_records_per_shard` — shard size for screened/merged JSONL (default: `50000`).
- `globals.sharding.compression` — shard compression (`gzip` by default).

### `--dataset-root` / `DATASET_ROOT`

When provided, the dataset root is expanded and resolved, then used to derive
standard subfolders:

```
<dataset_root>/
  raw/
  screened_yellow/
  combined/
  _queues/
  _ledger/
  _pitches/
  _manifests/
  _catalogs/
  _logs/
```

Only the relevant subset is used per stage (for example, `pipeline_driver`
only uses `_queues`/`_manifests`).

## Standard CLI arguments

### `pipeline_driver.py`

Required:

- `--targets PATH`

Optional (standardized):

- `--dataset-root PATH`
- `--manifests-root PATH` (alias: `--out-manifests`)
- `--queues-root PATH` (alias: `--out-queues`)
- `--retry-max INT` (alias: `--max-retries`, default: `3`)
- `--retry-backoff FLOAT` (default: `2.0`)

### `acquire_worker.py`

Required:

- `--queue PATH`
- `--bucket {green,yellow}`

Optional (standardized):

- `--targets-yaml PATH`
- `--raw-root PATH`
- `--manifests-root PATH`
- `--logs-root PATH`
- `--retry-max INT` (default: `3`)
- `--retry-backoff FLOAT` (default: `2.0`)

### `yellow_screen_worker.py` / `merge_worker.py`

Required:

- `--targets PATH` (`merge_worker.py`)
- `--targets PATH` + `--queue PATH` (`yellow_screen_worker.py`)

Optional (standardized):

- `--dataset-root PATH`
- `--pitch-sample-limit INT` (yellow screen only)
- `--pitch-text-limit INT` (yellow screen only)

## Environment variables

These apply to the pipeline driver and yellow/merge stages:

- `DATASET_ROOT` or `DATASET_COLLECTOR_ROOT` — base dataset root.
- `PIPELINE_RETRY_MAX` — default retry max for evidence fetchers.
- `PIPELINE_RETRY_BACKOFF` — base for exponential backoff.

## Emitted artifacts (summary)

See `docs/output_contract.md` for the full layout; key stage outputs are:

| Stage | Key outputs |
| --- | --- |
| `pipeline_driver.py` | `_queues/{green_download,yellow_pipeline,red_rejected}.jsonl`, `_queues/run_summary.json`, per-target `_manifests/<target_id>/...` |
| `acquire_worker.py` | `raw/{green,yellow}/...`, per-target download manifests under `_manifests/<target_id>/`, `_logs/acquire_summary_<bucket>.json` |
| `yellow_screen_worker.py` | `screened_yellow/<pool>/shards/*`, `_ledger/yellow_passed.jsonl`, `_ledger/yellow_pitched.jsonl` |
| `merge_worker.py` | `combined/<pool>/shards/*`, `_ledger/combined_index.jsonl`, `_ledger/merge_summary.json` |

## Retry/backoff behavior

All stages that perform network I/O use **exponential backoff** with:

- `max_retries` controlled by `--retry-max`, `globals.retry.max`, or `PIPELINE_RETRY_MAX`.
- `backoff_base` controlled by `--retry-backoff`, `globals.retry.backoff`, or `PIPELINE_RETRY_BACKOFF`.

The pipeline driver’s evidence fetcher and the acquire worker’s download
retries now share the same defaults and naming.
