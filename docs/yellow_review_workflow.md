# Yellow Review Workflow

This document defines a single, consistent review workflow for all v2 pipelines. It standardizes review states, required evidence artifacts, queue locations, and how review decisions flow into `screen_yellow`, `merge`, and `catalog` stages.

## Review states

All manual YELLOW reviews MUST land in one of these states:

| Review state | Meaning | How to record in `review_signoff.json` |
| --- | --- | --- |
| `ALLOW` | Approved for inclusion with no special constraints. | `status: "approved"`, `constraints: ""` |
| `ALLOW_WITH_RESTRICTIONS` | Approved for inclusion but with attribution, usage limits, or other constraints. | `status: "approved"`, `constraints: "..."` (required) |
| `PITCH` | Not approved for inclusion. | `status: "rejected"`, include `reason` |

> Notes:
> - The `review-queue` helper writes `review_signoff.json` files and accepts `constraints`, `reason`, and optional `evidence_links_checked` and `notes` fields.
> - The `promote_to` field is optional and should be used only if your pipeline explicitly supports promotion logic.

## Required evidence artifacts

Each YELLOW target MUST have a manifest directory and review signoff artifacts. The minimum required artifacts are:

1. **Queue row in the YELLOW queue** (JSONL)
   - Emitted by `pipeline_driver.py` during `classify`.
   - Includes `id`, `name`, `license_profile`, `license_evidence_url`, and `manifest_dir`.
2. **`review_signoff.json` in the manifest directory**
   - Written by the `review-queue` helper when a reviewer approves or rejects a target.
   - Required fields for consistent workflow:
     - `target_id`
     - `status` (`approved` or `rejected`)
     - `reviewer`
     - `reason`
     - `reviewed_at_utc`
   - Required fields for `ALLOW_WITH_RESTRICTIONS`:
     - `constraints` (e.g., attribution requirements or usage limitations)
   - Strongly recommended fields:
     - `reviewer_contact`
     - `evidence_links_checked` (list)
     - `notes`
3. **`yellow_screen_done.json` in the manifest directory**
   - Written by the yellow-screen stage (`dc run --stage yellow_screen`) to record screening outcome.
   - Includes `status`, `reason`, and counts for `passed` and `pitched`.
4. **Ledger outputs**
   - `*_ledger/yellow_passed.jsonl`
   - `*_ledger/yellow_pitched.jsonl`
   - These provide audit trails for screening decisions.
   - Ledgers live under `<dataset_root>/_ledger/` when you pass `--dataset-root` or set
     `DATASET_ROOT` / `DATASET_COLLECTOR_ROOT`.

## Where review queues live

Each pipeline writes queue files to a per-pipeline queues root, configured via `globals.queues_root` in the targets YAML. The default layout is:

```
<queues_root>/_queues/
  green_download.jsonl
  yellow_pipeline.jsonl
```

If `globals.queues_root` is not set, each pipeline falls back to a default `/data/<pipeline>/_queues` path (the same defaults used by `dc run`). To use those defaults, pass `--allow-data-root` to the review helper.

### Unified review queue helper

Use the unified CLI to list and sign off on YELLOW targets:

```
dc review-queue --pipeline <pipeline> [args]
```

Examples:
- `dc review-queue --pipeline biology_pipeline_v2 list --limit 50`
- `dc review-queue --pipeline chem_pipeline_v2 approve --target <target_id> ...`

If you run from inside a pipeline directory, you can omit `--pipeline-id` and the CLI will infer it.

### Deprecated per-pipeline scripts

Per-pipeline helper scripts are deprecated and will be removed in a future release. The old script path pattern was:

```
<pipeline>_pipeline_v2/review_queue.py
```

These deprecated scripts still work but emit a warning. Prefer the unified CLI above.

## Standard workflow (single source of truth)

The workflow is consistent across all pipelines:

1. **Classify (`pipeline_driver.py`)**
   - Emits `green_download.jsonl` and `yellow_pipeline.jsonl` in the queues root.
2. **Acquire YELLOW (`dc run --stage acquire`)**
   - Downloads YELLOW data into the raw YELLOW bucket (legacy `acquire_worker.py` scripts are deprecated).
3. **Manual review (`review-queue`)**
   - `list` to see pending entries.
   - `approve` for `ALLOW` or `ALLOW_WITH_RESTRICTIONS`.
   - `reject` for `PITCH`.
   - Writes `review_signoff.json` into the target manifest directory.
4. **Screen YELLOW (`dc run --stage yellow_screen`)**
   - If `globals.require_yellow_signoff: true` and no signoff is present, the target is pitched (`yellow_signoff_missing`).
   - If signoff `status` is `rejected`, the target is pitched (`yellow_signoff_rejected`).
   - If signoff `status` is `approved`, the target is screened and eligible for inclusion.
   - Outputs `screened_yellow/*/shards/*.jsonl.gz` and `yellow_screen_done.json`.
5. **Merge (`dc run --stage merge`)**
   - Combines GREEN records with `screened_yellow` shards only.
   - Pitched YELLOW records never enter combined output.
6. **Catalog (`dc catalog-builder`)**
   - Summarizes raw, screened YELLOW, combined shards, and ledgers.

## Decision flow into `screen_yellow` / `merge` / `catalog`

- **`screen_yellow`** reads `review_signoff.json` from the manifest directory. Signoff status controls whether the target is screened or pitched when `require_yellow_signoff` is enabled.
- **`merge`** only reads `screened_yellow` shards. If a target was pitched or skipped in `screen_yellow`, it is never merged.
- **`catalog`** includes ledger entries (`yellow_passed.jsonl`, `yellow_pitched.jsonl`, `combined_index.jsonl`) and shard summaries to provide an auditable trail of review outcomes.

## Example commands

```bash
# List pending YELLOW targets
dc review-queue --pipeline <pipeline> --queue <queues_root>/yellow_pipeline.jsonl list --limit 50

# Approve (ALLOW)
dc review-queue --pipeline <pipeline> --queue <queues_root>/yellow_pipeline.jsonl \
  approve --target <target_id> --manifest-dir <manifests_root>/<target_id> \
  --reviewer "Name" --reason "Evidence supports inclusion"

# Approve with restrictions (ALLOW_WITH_RESTRICTIONS)
dc review-queue --pipeline <pipeline> --queue <queues_root>/yellow_pipeline.jsonl \
  approve --target <target_id> --manifest-dir <manifests_root>/<target_id> \
  --reviewer "Name" --reason "Allowed with attribution" \
  --constraints "Must include attribution text from source" \
  --evidence-links "https://example.com/license,https://example.com/terms"

# Reject (PITCH)
dc review-queue --pipeline <pipeline> --queue <queues_root>/yellow_pipeline.jsonl \
  reject --target <target_id> --manifest-dir <manifests_root>/<target_id> \
  --reviewer "Name" --reason "License is incompatible"
```
