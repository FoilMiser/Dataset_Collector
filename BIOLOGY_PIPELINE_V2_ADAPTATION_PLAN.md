# Biology Pipeline v2 Adaptation Plan (match `math_pipeline_v2` behavior)

This document describes how to adapt `biology_pipeline_v1` into `biology_pipeline_v2` so it follows the **same stage flow, directory layout, and “strict pitch” screening behavior** as `math_pipeline_v2`.

## Target behavior (must match `math_pipeline_v2`)

`biology_pipeline_v2` should run in this order (same semantics as math):

1. **Classify** (`pipeline_driver.py`)
   - Snapshot license/terms evidence, normalize to SPDX, scan restriction phrases, apply denylist.
   - Emit queues:
     - `_queues/green_download.jsonl`
     - `_queues/yellow_pipeline.jsonl`
     - `_queues/red_rejected.jsonl`

2. **Acquire** (`acquire_worker.py`)
   - Download/ingest GREEN and YELLOW targets into a *raw* staging layout:
     - `raw/green/{license_pool}/{target_id}/...`
     - `raw/yellow/{license_pool}/{target_id}/...`
   - Write `download_manifest.json` in each target dir and `acquire_done.json` under `_manifests/{target_id}/`.

3. **Screen YELLOW** (`yellow_screen_worker.py`)
   - Convert raw YELLOW acquisitions into **canonical JSONL records**.
   - Enforce **“anything unclear is pitched”** behavior.
   - Output:
     - `screened_yellow/{license_pool}/shards/yellow_shard_*.jsonl.gz`
     - `_ledger/yellow_passed.jsonl`
     - `_ledger/yellow_pitched.jsonl`
     - `_manifests/{target_id}/yellow_screen_done.json`

4. **Merge** (`merge_worker.py`)
   - Merge **canonical GREEN records** + screened YELLOW shards into:
     - `combined/{license_pool}/shards/combined_*.jsonl.gz`
     - `_ledger/combined_index.jsonl` (content hash → shard mapping)

5. **Final screen + difficulty** (`difficulty_worker.py`)
   - Apply a second screening pass (same global min/max rules) and assign difficulty **d01–d10**.
   - Output:
     - `final/{license_pool}/d01..d10/shards/final_*.jsonl.gz`
     - `_ledger/final_index.jsonl`
     - `_pitches/final_pitched.jsonl` (optional)

6. **Catalog** (`catalog_builder.py`)
   - Build a catalog over the v2 layout (at minimum: per-pool/per-difficulty file counts + sizes + examples).
   - Output to `_catalogs/` (and keep attribution bundle hooks if you want parity with the older biology builder).

---

## 0) Quick gap analysis: `biology_pipeline_v1` vs v2 behavior

### What already matches
- `pipeline_driver.py` is already very close to `math_pipeline_v2` and emits the same queue names.
- `pmc_worker.py`, `review_queue.py`, and most of the license/denylist plumbing is compatible.

### What differs (must change)
- **Directory layout**:
  - v1 uses `globals.storage_root` + `globals.pools.{permissive,copyleft,quarantine}`.
  - v2 uses `globals.raw_root`, `screened_yellow_root`, `combined_root`, `final_root`, plus `_ledger/_pitches/_manifests`.
- **Stage structure**:
  - v1 has `download_worker.py` + `yellow_scrubber.py` (promotion quarantine→permissive).
  - v2 replaces those with `acquire_worker.py` + `yellow_screen_worker.py`, then adds `merge_worker.py` and `difficulty_worker.py`.
- **Routing + difficulty**:
  - v1 queue rows don’t carry `routing` metadata.
  - v2 expects `routing` and a `difficulties_*.yaml` companion file to drive d01–d10 assignment.
- **Ledgers**:
  - v1 doesn’t systematically write pass/pitch ledgers and shard indexes the way v2 does.

---

## 1) Create `biology_pipeline_v2/` by “math-v2 first” assembly

**Recommended approach** (lowest risk):
1. Copy the `math_pipeline_v2/` folder as the base of `biology_pipeline_v2/`.
2. Replace domain-specific assets from `biology_pipeline_v1/`:
   - `license_map.yaml` (biology-specific)
   - `denylist.yaml` (reuse or extend)
   - `field_schemas.yaml` (keep biology schemas; add v2 routing schema)
   - `pmc_worker.py` (keep as-is)
3. Rename the targets file to **`targets_biology.yaml`** (or keep `targets.yaml`, but be consistent across scripts).

This ensures you inherit the **v2 stage set** and the correct output layout.

---

## 2) Update `targets_biology.yaml` to use v2 roots + difficulty companion

### 2.1 Globals: replace v1 “pools” layout with v2 roots
In `globals`, remove/ignore these v1 keys:
- `storage_root`, `staging_root`, `pools:{permissive,copyleft,quarantine}`

Add/align these keys (match math naming):
- `raw_root: /data/bio/raw`
- `screened_yellow_root: /data/bio/screened_yellow`
- `combined_root: /data/bio/combined`
- `final_root: /data/bio/final`
- `ledger_root: /data/bio/_ledger`
- `pitches_root: /data/bio/_pitches`
- `manifests_root: /data/bio/_manifests`
- `queues_root: /data/bio/_queues`
- `catalogs_root: /data/bio/_catalogs`
- `logs_root: /data/bio/_logs`

Keep:
- `download_defaults`
- `sharding` (max records per shard, compression)
- `screening` (min_chars/max_chars, deny phrases, etc.)

### 2.2 Companion files: add a biology difficulty map
Update `companion_files`:
- Ensure `license_map: ./license_map.yaml` (fix the v1 mismatch that points to `bio_license_map.yaml`)
- Add:
  - `difficulties_map: ./difficulties_biology.yaml`

### 2.3 Per-target routing (required for difficulty)
For **every target** add a generic routing block (mirrors math):
```yaml
routing:
  subject: biology
  domain: genetics        # ex: cell_biology | ecology | immunology | ...
  category: genomics      # a smaller category within the domain
```

Optionally keep a bio-specific block if you want richer routing:
```yaml
bio_routing:
  subdomain: population_genetics
  organism: human         # only if it’s stable metadata
  modality: text          # text | sequences | images | tables
```

### 2.4 YELLOW screening overrides (optional but useful)
Where record-level licensing or extraction is target-specific, add:
```yaml
yellow_screen:
  text_field_candidates: ["text","abstract","body","full_text"]
  record_license_field_candidates: ["license","license_spdx","rights"]
  require_record_license: true
  allow_spdx: ["CC-BY-4.0","CC0-1.0"]
  min_chars: 200
  max_chars: 12000
  deny_phrases: ["noai","no tdm","no machine learning"]
```

---

## 3) Update `pipeline_driver.py` (biology v2)

Start from `math_pipeline_v2/pipeline_driver.py` and apply these deltas:

1. **Targets filename + IDs**
   - Accept `--targets targets_biology.yaml` (or keep `targets.yaml`, but match run script + docs).

2. **Queue row fields (parity with math v2)**
   Add the same queue fields math emits but biology v1 currently omits:
   - `queue_bucket` (duplicate of `effective_bucket`)
   - `output_pool` (license pool routing; typically same as `license_profile`)
   - `routing` (the generic routing dict)
   - `routing_subject`, `routing_domain`, `routing_category` (flattened copies, optional)
   - `bio_routing` (optional; parallel to math’s `math_routing`)

3. **Manifests + evidence**
   Keep behavior:
   - evidence snapshots + change detection
   - downgrade-to-YELLOW when evidence changes
   - restriction phrase scans + denylist gating

This makes downstream workers able to:
- locate raw acquisitions by bucket/pool
- assign difficulty from routing

---

## 4) Replace `download_worker.py` with `acquire_worker.py`

### 4.1 Implement/port `acquire_worker.py` from math v2
Behavior to match:
- reads queue JSONL rows
- writes to `raw/{bucket}/{license_pool}/{target_id}/...`
- writes `download_manifest.json` per target
- writes `_manifests/{target_id}/acquire_done.json`

### 4.2 Strategy support: keep biology-specific ingestion
`biology_pipeline_v1/download_worker.py` and `yellow_scrubber.py` contain biology-centric logic (PubChem/PMC). Decide where each belongs in v2:

- **Pure downloads** (Zenodo/HTTP/FTP/Git/Dataverse/HF) → stay in `acquire_worker.py`.
- **Transformations that create new safe text** (e.g., PubChem computed-only extraction) → move into `yellow_screen_worker.py` as a per-target “transform” plugin.
- **PMC OA fulltext downloads**:
  - Prefer integrating `pmc_worker.py` as an `acquire_worker` strategy (e.g., `download.strategy: pmc_oa`), so PMC is acquired into `raw/{bucket}/...` like everything else.

### 4.3 Canonical JSONL expectation (important)
`merge_worker.py` (v2) merges by reading `*.jsonl*` under `raw/green/...`.

So you need one of these (pick one; both are compatible with the stage order):
- **Option A (recommended):** Make `acquire_worker.py` emit JSONL for HF datasets (export rows to `*.jsonl.gz` shards).
- **Option B:** Extend `merge_worker.py` to also read HF `save_to_disk()` directories.

If you want strict parity with math v2 *as implemented*, choose Option A.

---

## 5) Implement `yellow_screen_worker.py` for biology

Port the math v2 worker and make these biology-specific adjustments:

### 5.1 Text field candidates
Biology sources often store text in:
- `abstract`, `title`, `body`, `full_text`, `article`, `sections`
Add those as defaults in `globals.screening.text_field_candidates`.

### 5.2 Record-level license fields
Add candidates like:
- `license`, `license_spdx`, `rights`, `copyright`, `license_url`

### 5.3 Strict pitch rules (must match v2 intent)
Pitch when:
- no extractable text
- text outside min/max bounds
- deny phrase hit
- record-level license missing when `require_record_license: true`
- record-level license not in allowlist when allowlist is configured

Write ledgers:
- `_ledger/yellow_passed.jsonl`
- `_ledger/yellow_pitched.jsonl`

### 5.4 Biology-specific transform plugins (from v1 yellow_scrubber)
Re-home these v1 behaviors inside v2 screening:
- **PubChem computed-only extraction**
  - Treat the raw PubChem dump as YELLOW (quarantine).
  - During screening, emit derived computed-only text records with `license_profile: permissive` and provenance in `source`.
- **PMC OA allowlist planner**
  - If you still need the allowlist file: generate it as a side artifact under `_manifests/{target_id}/` or `_ledger/`.
  - If not: skip, and only emit canonical text records.

---

## 6) Implement `merge_worker.py` (mostly a straight port)

Port math v2 and adjust defaults to `/data/bio/...`.

Ensure it:
- reads GREEN canonical records from `raw/green/*/*/**/*.jsonl*`
- reads screened YELLOW shards from `screened_yellow/*/shards/*.jsonl*`
- dedupes by `hash.content_sha256`
- routes by `source.license_profile` into:
  - `combined/permissive/...`
  - `combined/copyleft/...`
  - `combined/quarantine/...`

Write:
- `_ledger/combined_index.jsonl`
- `_ledger/merge_summary.json`

---

## 7) Implement `difficulty_worker.py` + create `difficulties_biology.yaml`

### 7.1 Difficulty config file
Create `difficulties_biology.yaml` with the same schema style as math:
- `subjects -> domains -> categories -> level.default (1..10)`

Example skeleton:
```yaml
schema_version: "1.0"
subjects:
  biology:
    domains:
      genetics:
        categories:
          genomics:
            level: { default: 6 }
          mendelian:
            level: { default: 3 }
      ecology:
        categories:
          population:
            level: { default: 4 }
          biogeochemistry:
            level: { default: 7 }
```

### 7.2 How difficulty is assigned (match math v2 logic)
- First try: map `record.routing.{subject,domain,category}` using `difficulties_biology.yaml`.
- Else fallback: heuristic (length-based) like math’s `heuristic_level()`.

### 7.3 Final screening (same gates as math)
Before writing into `final/.../dXX/`:
- enforce `globals.screening.min_chars/max_chars`
- optionally add additional “bio policy” gates later (e.g., medical PHI filters) but keep disabled by default to match math’s minimal final screen.

Write:
- `_ledger/final_index.jsonl`
- `_pitches/final_pitched.jsonl` for rejected examples (optional but recommended)

---

## 8) Refactor `catalog_builder.py` to the v2 layout

You have two choices:

### Choice A: Minimal parity (match math v2)
- Scan only `final_root/{license_pool}/d01..d10/shards/*.jsonl*`
- Output a JSON catalog with:
  - stage name
  - pools
  - per-difficulty: file counts, bytes, a few example file stats

### Choice B: Preserve biology v1 richer stats (recommended long-term)
Refactor the v1 catalog builder so it no longer relies on `globals.pools`.
Instead, build reports over:
- raw (optional)
- screened_yellow
- combined
- final

Keep optional features:
- token estimates
- near-duplicate report
- split_group_id leakage checks

---

## 9) Update `run_pipeline.sh` to v2 stages

Replace v1 stages with math v2 stages:
- `classify`
- `acquire_green`
- `acquire_yellow`
- `screen_yellow`
- `merge`
- `difficulty`
- `catalog`
- `all` runs the above in order

Make sure flags align:
- `--execute` default dry-run
- `--limit-targets`, `--limit-files`, `--workers`
- pass the correct queue file to each worker

---

## 10) Migration path for existing v1 artifacts (optional but practical)

If you already have a v1 dataset directory on disk, you can bootstrap v2 without re-downloading:

- Map v1 GREEN downloads into:
  - `raw/green/{license_pool}/{target_id}/...`
- Map v1 quarantine/YELLOW downloads into:
  - `raw/yellow/quarantine/{target_id}/...`

Fast method:
- create **symlinks** (WSL-friendly) from old pool dirs into new raw dirs.
- then run: `screen_yellow → merge → difficulty → catalog`.

---

## 11) Acceptance checks (definition of “matches math v2”)

After implementing the above, verify:

1. **Stage outputs exist** with the correct directory names:
   - raw/, screened_yellow/, combined/, final/, _ledger/, _pitches/, _manifests/
2. **YELLOW strict pitch**:
   - `_ledger/yellow_pitched.jsonl` grows when you inject “bad” samples (no text, too short, deny phrases, etc.)
3. **Reproducibility**:
   - `combined_index.jsonl` and `final_index.jsonl` map hashes → shard files
4. **Difficulty routing**:
   - records with `routing` land in expected `dXX` directories
5. **Dry-run safety**:
   - no files written unless `--execute`

---

## 12) File-by-file checklist (what to change/add)

Create/modify these files in `biology_pipeline_v2/`:

- **NEW / PORTED**
  - `acquire_worker.py` (from math v2; integrate PMC strategy)
  - `yellow_screen_worker.py` (from math v2; add biology text fields + PubChem/PMC plugins)
  - `merge_worker.py` (from math v2)
  - `difficulty_worker.py` (from math v2)
  - `difficulties_biology.yaml` (new)

- **UPDATED**
  - `pipeline_driver.py` (add routing + queue_bucket + output_pool parity)
  - `catalog_builder.py` (switch from v1 pools layout → v2 final layout)
  - `run_pipeline.sh` (v2 stage list)
  - `targets_biology.yaml` (v2 roots + routing + companion_files)

- **UNCHANGED (likely)**
  - `pmc_worker.py` (but wire it into acquisition)
  - `review_queue.py`
  - `denylist.yaml`
  - `license_map.yaml` (update only if you want more SPDX normalization rules)
  - `requirements.txt` (add deps only if new code paths require them)

