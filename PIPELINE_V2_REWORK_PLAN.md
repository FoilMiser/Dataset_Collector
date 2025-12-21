# Math pipeline v2 rework plan

## Goal
Update **math_pipeline_v1.2** so it works in this simpler order:

1. **Acquire GREEN** targets (download) into a GREEN raw pool.
2. **Acquire YELLOW** targets (download) into a YELLOW raw pool.
3. **Screen YELLOW** at record level; as records pass, compile them into **new screened-YELLOW shards** with a **complete “passed ledger.”** Anything unclear is **pitched**.
4. After YELLOW screening finishes, **merge** GREEN raw outputs + screened-YELLOW shards into one “combined candidate corpus.”
5. Run a **final screen + difficulty assignment** over the combined corpus, then write **new shards** into `difficulty/` folders (`d01`…`d10`).

This plan preserves the existing safety posture:
- `pipeline_driver.py` still snapshots ToS/license evidence and emits GREEN/YELLOW/RED queues.
- copyleft vs permissive segregation stays available (recommended), even if you also maintain a single “combined” view.

---

## New high-level dataflow

```
targets_math.yaml
   |
   v
pipeline_driver.py  ->  _queues/{green,yellow,red}.jsonl  +  _manifests/{target_id}/...
   |
   +------------------------------+
   |                              |
   v                              v
acquire_worker.py (GREEN)     acquire_worker.py (YELLOW)
 -> raw/green/...              -> raw/yellow/...  (unprocessed)
                                 |
                                 v
                          yellow_screen_worker.py
                           -> screened_yellow/shards/*.jsonl.gz
                           -> _ledger/yellow_passed.jsonl
                           -> _pitches/yellow_pitched.jsonl
                                 |
                                 v
                           merge_worker.py
                           -> combined/shards/*.jsonl.gz
                           -> _ledger/combined_index.jsonl
                                 |
                                 v
                      difficulty_worker.py (final screen)
                       -> final/{permissive,copyleft}/d01..d10/shards/*.jsonl.gz
                       -> _ledger/final_index.jsonl
```

---

## Directory layout (recommended)
In `targets_math.yaml -> globals` add these roots (keep existing `_queues/_manifests/_logs`):

```
/data/math/
  raw/
    green/
      permissive/{target_id}/...
      copyleft/{target_id}/...
      quarantine/{target_id}/...
    yellow/
      permissive/{target_id}/...
      copyleft/{target_id}/...
      quarantine/{target_id}/...

  screened_yellow/
    permissive/shards/yellow_shard_00000.jsonl.gz
    copyleft/shards/yellow_shard_00000.jsonl.gz

  combined/
    permissive/shards/combined_00000.jsonl.gz
    copyleft/shards/combined_00000.jsonl.gz

  final/
    permissive/d01/shards/...
    permissive/d02/shards/...
    ...
    copyleft/d01/shards/...
    ...

  _ledger/
    yellow_passed.jsonl
    yellow_pitched.jsonl
    combined_index.jsonl
    final_index.jsonl

  _pitches/
    yellow_unclear_targets.jsonl
    yellow_unclear_records_samples.jsonl
    final_pitched_samples.jsonl
```

If you truly want “one place” without license segregation, make `combined/shards` and `final/dXX/shards` contain mixed license records **but keep per-record license fields** and generate an attribution bundle from the ledger.

---

## Canonical record contract (what every stage writes)
To make screening/merging/difficulty assignment simple, converge everything into one **canonical JSONL record** as early as possible (during YELLOW screening and GREEN normalization).

**Minimal required fields:**

```json
{
  "record_id": "<stable id>",
  "text": "<training text chunk>",
  "source": {
    "target_id": "...",
    "origin": "hf|git|http|...",
    "source_url": "...",
    "license_spdx": "...",
    "license_profile": "permissive|copyleft|quarantine",
    "license_evidence": "path-or-url",
    "retrieved_at_utc": "..."
  },
  "routing": {"subject": "math", "domain": "...", "category": "..."},
  "hash": {"content_sha256": "..."}
}
```

Later stages extend this with:
- `difficulty: {level: 1..10, method: "metadata|rules|heuristic", confidence: 0..1}`
- optional `split_group_id` and near-duplicate group ids

**Stable IDs & hashes**
- `record_id`: deterministic from `(target_id, source_locator, chunk_id)`.
- `content_sha256`: sha256 of a normalized form of `text` (NFKC + whitespace collapse while preserving LaTeX blocks).

---

## Sharding rules
Add a global sharding config (in `targets_math.yaml`):

- `max_records_per_shard` (e.g. 50k)
- `max_bytes_per_shard` (optional)
- `compression: gzip`
- naming: `*_00000.jsonl.gz`

Every stage that “compiles into new shards” uses the same sharder:
- append records until threshold, close shard, open next
- write a small `shard_index.json` alongside each shard (counts, hashes, min/max record_id)

---

## Stage-by-stage implementation details

### Stage 0 — Classify (keep)
**Keep `pipeline_driver.py` mostly as-is.** It already:
- snapshots terms/license evidence
- resolves SPDX-ish license
- emits GREEN/YELLOW/RED queues

**Small change recommended:** include a single normalized field to drive acquisition routing:
- `queue_bucket: "green"|"yellow"|"red"` or reuse `effective_bucket`.

No difficulty logic should happen here.

---

### Stage 1 — Acquire (GREEN + YELLOW)
**Replace/rename `download_worker.py` → `acquire_worker.py`** and remove difficulty-aware output routing.

**Inputs**
- `green_download.jsonl` and `yellow_pipeline.jsonl`

**Outputs**
- `raw/green/<license_pool>/<target_id>/...`
- `raw/yellow/<license_pool>/<target_id>/...`
- `download_manifest.json` per target (keep current behavior)

**Key simplifications**
- output dir depends only on `(bucket, license_pool, target_id)`
- no `difficulties_math.yaml` needed here

---

### Stage 2 — Screen YELLOW (record-level)
Add **`yellow_screen_worker.py`**.

**Purpose**
- Convert raw YELLOW acquisitions into canonical records.
- Enforce “anything unclear is pitched.”

**Typical YELLOW screen rules** (configurable per target)
- must have a usable `text` field (or a target-specific mapping)
- must have record-level license info if the target is mixed-license
- must *not* be NC/ND/NoAI/NoTDM (or other denylist signals)
- must pass basic sanity checks (min length, encoding, language if desired)

**Outputs**
- accepted records → `screened_yellow/<license_pool>/shards/*.jsonl.gz`
- append acceptance ledger rows → `_ledger/yellow_passed.jsonl`
- append pitch ledger rows → `_ledger/yellow_pitched.jsonl`
- per-target `screen_report.json` in `_manifests/{target_id}/` with counts + reasons

**Ledger row schema (example)**
```json
{
  "stage": "yellow_screen",
  "target_id": "...",
  "record_id": "...",
  "content_sha256": "...",
  "decision": "pass|pitch",
  "reason": "missing_record_license|no_text|denylist_hit|...",
  "output_shard": "... (if pass)",
  "seen_at_utc": "..."
}
```

**Completion criteria**
- for each YELLOW target: write `_manifests/{target_id}/yellow_screen_done.json` with `status: ok`.
- the merge stage checks for these before proceeding.

---

### Stage 3 — Normalize GREEN (minimal)
GREEN targets are “license-clear,” but still need to become canonical records.

You have two options:

**Option A (simplest): require GREEN targets to already be canonical JSONL**
- e.g., your own synthetic generator, or sources you’ve already chunked.

**Option B (practical): add `green_normalize_worker.py`**
- For common patterns (HF datasets, text files, PDFs you extracted), emit canonical records into `raw/green_canonical/...`.

Either way, the merge stage should consume **canonical** records from GREEN.

---

### Stage 4 — Merge GREEN + screened YELLOW
Add **`merge_worker.py`**.

**Inputs**
- GREEN canonical records
- screened_YELLOW shards

**Outputs**
- `combined/<license_pool>/shards/*.jsonl.gz`
- `_ledger/combined_index.jsonl` mapping `content_sha256 -> combined_shard` (+ source)

**Recommended behavior**
- dedupe on `content_sha256` (drop duplicates deterministically)
- ensure schema validity; pitch malformed records to `_pitches/final_pitched_samples.jsonl`

---

### Stage 5 — Final screen + difficulty sharding
Add **`difficulty_worker.py`**.

**Purpose**
- run a final “training readiness” screen
- assign difficulty **1–10**
- write final shards into difficulty folders

**Final screen (lightweight, deterministic)**
- text non-empty
- length constraints (min/max)
- Unicode normalization
- optional: “mostly English” heuristic (if you’re staying English-only)

**Difficulty assignment (deterministic + explainable)**
Use `difficulties_math.yaml` as the rulebook, but apply it **here**, not during download.

Priority order for determining `difficulty.level`:
1. **Explicit metadata** in record (grade, level, arXiv category, textbook section) mapped to 1–10.
2. **Routing hints** (`routing.domain/category`) mapped via `difficulties_math.yaml -> subjects/domains/categories`.
3. **Keyword rule sets** (already in `difficulties_math.yaml`) for fallback.
4. **Heuristic complexity** fallback:
   - token length buckets
   - equation density / LaTeX command count
   - presence of advanced markers (epsilon-delta, measure, Banach, scheme, etc.)

**Outputs**
- `final/<license_pool>/d{01..10}/shards/*.jsonl.gz`
- `_ledger/final_index.jsonl` containing `content_sha256, difficulty, output_path, source`.

**Pitching**
- If difficulty cannot be assigned with at least a minimal confidence, either:
  - assign default (e.g. level 5) **or**
  - pitch to `_pitches/final_pitched_samples.jsonl`

(Your prompt asks “anything unclear is pitched” mainly for the YELLOW phase; for final difficulty, defaulting is often more useful, but you can choose strict pitching.)

---

## YAML changes

### `targets_math.yaml` (globals)
Add:
```yaml
globals:
  raw_root: /data/math/raw
  screened_yellow_root: /data/math/screened_yellow
  combined_root: /data/math/combined
  final_root: /data/math/final
  ledger_root: /data/math/_ledger
  pitches_root: /data/math/_pitches
  sharding:
    max_records_per_shard: 50000
    compression: gzip
  screening:
    min_chars: 200
    max_chars: 12000
```

### `targets_math.yaml` (targets)
For YELLOW targets add optional per-target screen configuration:
```yaml
yellow_screen:
  text_field_candidates: ["text", "content", "body"]
  record_license_field_candidates: ["license", "license_spdx"]
  require_record_license: true
  allow_spdx: ["CC-BY-4.0", "CC0-1.0", "MIT", "Apache-2.0", "CC-BY-SA-4.0"]
  deny_phrases: ["noai", "no tdm", "no machine learning"]
```

This keeps the “anything unclear → pitch” policy enforceable without hardcoding per-dataset logic.

### `difficulties_math.yaml`
- Keep the rubric, domains, categories.
- Update `globals.folder_layout` to describe **final** output, not acquisition output. Example:

```yaml
globals:
  folder_layout: final/{license_pool}/d{level:02d}/{subject}/{domain}/{category}
```

(The difficulty worker uses this; acquisition ignores it.)

---

## Code changes (file-by-file)

### Keep (minor edits)
- `pipeline_driver.py` (classification + evidence)
- `license_map.yaml`, `denylist.yaml`, `field_schemas.yaml`

### Replace / simplify
- `download_worker.py` → `acquire_worker.py`
  - remove difficulty routing
  - output to `raw/{green|yellow}/{license_pool}/{target_id}`

### Add
- `yellow_screen_worker.py` (YELLOW record-level screening + sharding + ledger)
- `merge_worker.py` (combine green+yellow + dedupe + combined shards)
- `difficulty_worker.py` (final screen + assign difficulty + difficulty shards)

### Update
- `catalog_builder.py`
  - read the new folder layout
  - compute stats per stage + per difficulty
  - optionally build attribution bundles from `_ledger/final_index.jsonl`

### Wrapper script
Update `run_pipeline.sh` stages to:
- `classify`
- `acquire_green`
- `acquire_yellow`
- `screen_yellow`
- `merge`
- `difficulty`
- `catalog`

---

## Operational “done-ness” checks
To keep the pipeline deterministic and restartable:

- Each stage writes a **done marker** per target or per stage:
  - `_manifests/{target_id}/acquire_done.json`
  - `_manifests/{target_id}/yellow_screen_done.json`
- Workers skip targets with done markers unless `--overwrite`.
- `merge_worker.py` verifies all YELLOW targets in the queue have `yellow_screen_done.status == ok` (or else fails loudly).

---

## Suggested implementation order (fastest path)
1) **Refactor `download_worker.py` into `acquire_worker.py`** (mechanical change).
2) Implement **`yellow_screen_worker.py`** for HF datasets first (most of your volume), with strict pitch behavior.
3) Implement **`merge_worker.py`** (simple concatenation + optional hash-dedupe).
4) Implement **`difficulty_worker.py`** with rule-based assignment from `difficulties_math.yaml`.
5) Update `catalog_builder.py` + `run_pipeline.sh`.

---

## Example CLI sequence
```bash
# 0) classify
python pipeline_driver.py --targets targets_math.yaml

# 1) acquire
python acquire_worker.py --queue /data/math/_queues/green_download.jsonl --bucket green --execute
python acquire_worker.py --queue /data/math/_queues/yellow_pipeline.jsonl --bucket yellow --execute

# 2) screen yellow
python yellow_screen_worker.py --queue /data/math/_queues/yellow_pipeline.jsonl --execute

# 3) merge
python merge_worker.py --green-root /data/math/raw/green --yellow-screened-root /data/math/screened_yellow --execute

# 4) difficulty
python difficulty_worker.py --combined-root /data/math/combined --difficulty-yaml difficulties_math.yaml --execute

# 5) catalog
python catalog_builder.py --targets targets_math.yaml --output /data/math/_catalogs/global_catalog.json
```

