# Cyber Pipeline v2 Adaptation Guide
*(Adapt `cyber_pipeline_v1` to a `cyber_pipeline_v2` that matches the stage behavior + layout of `math_pipeline_v2`.)*

This guide assumes you have:
- `cyber_pipeline_v1/` (from `cyber_pipeline_v1.zip`)
- `math_pipeline_v2/` (from `math_pipeline_v2.zip`)

The end state is a **Cyber pipeline v2** that follows the same stage order and on-disk layout as the math v2 pipeline:

1. **classify** (queues + evidence snapshots)
2. **acquire_green**
3. **acquire_yellow**
4. **screen_yellow**
5. **merge**
6. **difficulty**
7. **catalog**

---

## 0) What “same behavior as math_pipeline_v2” means

### v2 directory layout (must match)
Cyber v2 should adopt the same roots and stage folders as math v2:

```
/data/cyber/
  raw/
    green/{license_pool}/{target_id}/...
    yellow/{license_pool}/{target_id}/...
  screened_yellow/
    {license_pool}/shards/*.jsonl.gz
  combined/
    {license_pool}/shards/*.jsonl.gz
  final/
    {license_pool}/d01..d10/{subject}/{domain}/{category}/*.jsonl.gz
  _ledger/
    yellow_passed.jsonl
    yellow_pitched.jsonl
    combined_index.jsonl
    difficulty_summary.json
  _pitches/
    yellow/*.jsonl
    final/*.jsonl
  _manifests/{target_id}/...
  _queues/...
  _logs/...
  _catalogs/...
```

### v2 “safe defaults” (must match)
- Dry-run/plan mode **by default**
- `--execute` is required for any worker to write payload files
- Workers write “done markers” under `_manifests/` so repeated runs can be idempotent

---

## 1) High-level file mapping (v1 → v2)

### Keep (minor edits only)
- `pipeline_driver.py`  
- `review_queue.py`
- `license_map.yaml`, `field_schemas.yaml`, `denylist.yaml`
- Domain helpers (optional): `pmc_worker.py`, `nvd_worker.py`, `stix_worker.py`, `advisory_worker.py`

### Replace with v2 versions (copy from math pipeline)
- `download_worker.py` → **replace with** `acquire_worker.py` (v2 raw layout)
- `yellow_scrubber.py` → **replace with** math v2 “review-plan helper”
- `catalog_builder.py` → **replace with** math v2 stage-aware catalog builder

### Add new v2 workers (copy from math pipeline, cyber-tune paths and difficulty map)
- `yellow_screen_worker.py`
- `merge_worker.py`
- `difficulty_worker.py`
- `difficulties_cyber.yaml` *(new)*

### Update orchestration script
- Replace cyber `run_pipeline.sh` stage list with math v2 stage list, plus cyber defaults.

---

## 2) Create `cyber_pipeline_v2/` skeleton

Recommended approach: **use `math_pipeline_v2/` as the base**, then add cyber-specific pieces.

1) Copy the math v2 folder:
```bash
cp -r math_pipeline_v2 cyber_pipeline_v2
```

2) Replace math-specific docs/names:
- Rename `targets_math.yaml` → `targets_cyber.yaml`
- Replace `difficulties_math.yaml` → `difficulties_cyber.yaml`
- Update `README.md` (optional but recommended)

3) Bring over cyber-specific assets from v1:
- `targets.yaml` (as a reference input; you will migrate it)
- `nvd_worker.py`, `stix_worker.py`, `advisory_worker.py` (optional but recommended)
- Any cyber-specific denylist patterns you already curated

---

## 3) Migrate `targets.yaml` → `targets_cyber.yaml`

### 3.1 Required globals changes (v1 pools → v2 roots)

In **v1** cyber targets, `globals` includes:
- `storage_root`, `staging_root`, and `pools: { permissive: ..., ... }`

In **v2**, this becomes the math-style roots:

```yaml
globals:
  raw_root: /data/cyber/raw
  screened_yellow_root: /data/cyber/screened_yellow
  combined_root: /data/cyber/combined
  final_root: /data/cyber/final
  ledger_root: /data/cyber/_ledger
  pitches_root: /data/cyber/_pitches

  manifests_root: /data/cyber/_manifests
  queues_root: /data/cyber/_queues
  catalogs_root: /data/cyber/_catalogs
  logs_root: /data/cyber/_logs

  require_yellow_signoff: false

  sharding:
    max_records_per_shard: 50000
    compression: gzip

  screening:
    # used by yellow_screen_worker + difficulty_worker
    text_field_candidates: ["text", "content", "body", "summary"]
    record_license_field_candidates: ["license", "license_spdx", "spdx"]
    require_record_license: false
    min_chars: 200
    max_chars: 12000
    deny_phrases:
      - "noai"
      - "no tdm"
      - "no machine learning"
```

**Remove or ignore** v1-only keys unless you are explicitly re-implementing them:
- `pools:` (replaced by raw_root-based layout)
- `near_duplicate_detection` (v2 merge currently dedupes on `content_sha256`, not fuzzy)
- `parquet_output` + `text_processing_defaults` (legacy yellow_scrubber behavior)

### 3.2 Keep your existing target list, but align fields to v2 workers
Your v1 targets already use the important fields:
- `id`, `name`, `enabled`
- `license_profile` (permissive|copyleft|quarantine)
- `download.strategy` + payload config

**Keep** those unchanged. v2 `acquire_worker.py` resolves the license pool from `license_profile`.

### 3.3 Add difficulty map pointer (new in cyber v2)
Add to `companion_files`:

```yaml
companion_files:
  difficulties_map: "./difficulties_cyber.yaml"
```

---

## 4) Acquisition: swap to `acquire_worker.py` (v2 raw layout)

### 4.1 Replace v1 `download_worker.py`
Cyber v1 uses:
- `download_worker.py (v0.9)` writing into `pools/...`

Math v2 uses:
- `acquire_worker.py (v2.0)` writing into:
  `raw/{green|yellow}/{license_pool}/{target_id}/...`

**Action:**
- Use math v2 `acquire_worker.py` in cyber v2.
- Update help text defaults (`targets_cyber.yaml`) and root defaults (`/data/cyber/...`).

### 4.2 Strategy support
Your cyber targets currently use strategies:
- `none`, `http`, `git`, `huggingface_datasets`

Math v2 acquire worker already supports:
- `http`, `ftp`, `git`, `zenodo`, `dataverse`, `huggingface_datasets`

So cyber v2 is immediately compatible **without** adding new handlers.

---

## 5) YELLOW screening: add `yellow_screen_worker.py`

Copy `yellow_screen_worker.py` from math v2 into cyber v2 and adjust:
- default roots (`/data/cyber/...`)
- any deny phrases / min-max char bounds you want for cyber

**Outputs (must match v2):**
- `screened_yellow/{license_pool}/shards/*.jsonl.gz`
- `_ledger/yellow_passed.jsonl`
- `_ledger/yellow_pitched.jsonl`
- `_pitches/yellow/*.jsonl`

### Important limitation (same as math v2)
`yellow_screen_worker.py` only screens **JSONL records**.

If you acquire *raw artifacts* (e.g. `nvdcve-2.0-2024.json.gz`, STIX bundles, YAML advisories), they will not be screened unless you **normalize them into JSONL** (see §8).

---

## 6) Merge: add `merge_worker.py`

Copy `merge_worker.py` from math v2 into cyber v2 and adjust roots to `/data/cyber/...`.

Merge behavior (same as math v2):
- Looks for `**/*.jsonl*` under `raw/green/{pool}/{target_id}/...`
- Also includes `screened_yellow/{pool}/shards/*.jsonl*`
- Writes `combined/{pool}/shards/*.jsonl.gz`
- Dedupes using `content_sha256` (or `hash.content_sha256`) and writes `_ledger/combined_index.jsonl`

---

## 7) Difficulty: add `difficulty_worker.py` + `difficulties_cyber.yaml`

### 7.1 Copy worker
Copy `difficulty_worker.py` from math v2 into cyber v2 and adjust:
- roots to `/data/cyber/...`
- default `--difficulties` points to `difficulties_cyber.yaml`

### 7.2 Create `difficulties_cyber.yaml`
Use the same schema structure as math v2, but with cyber domains/categories.

Minimal starter:

```yaml
schema_version: "2.0"
updated_utc: "2025-12-22"

globals:
  subject: "cyber"
  levels: 10

rubric:
  1: "Basic definitions and safe hygiene (passwords, updates, MFA)."
  2: "Intro defensive concepts (CIA triad, basic logs, simple phishing detection)."
  3: "Blue-team workflows (triage, containment, simple detection rules)."
  4: "Network + endpoint fundamentals (TCP/IP, syslogs, EDR concepts)."
  5: "Vulnerability management + patching at scale; config hardening."
  6: "Threat modeling + incident response coordination; playbooks."
  7: "Security engineering (authn/z, key management, secure SDLC)."
  8: "Advanced detection/forensics; complex environments (cloud/OT/ICS)."
  9: "Program-level risk + adversarial simulation; deep technical root-cause."
  10: "Research-grade security engineering + complex system design under constraints."

routing_map:
  # Example: route by domain/category if present on records
  - match: { domain: "incident_response" }
    level: 6
  - match: { domain: "threat_modeling" }
    level: 6
  - match: { domain: "vulnerability_data" }
    level: 5
  - match: { domain: "ics_security" }
    level: 8
```

Your records should carry a `routing` object similar to math v2:
```json
"routing": {"subject":"cyber","domain":"incident_response","category":"triage","level":6,"granularity":"record"}
```

If your records do not have routing, `difficulty_worker.py` will fall back to defaults (or pitch).

---

## 8) Cyber-specific: normalize non-JSONL sources so they can flow through v2

This is the key “cyber difference” versus math: many high-value cyber sources are *feeds/bundles* (gz/json/yaml), not ready-made JSONL.

### 8.1 You already have normalization helpers in v1
- `nvd_worker.py`: NVD CVE 2.0 JSON → normalized JSONL
- `stix_worker.py`: STIX bundle → text/graph JSONL
- `advisory_worker.py`: GHSA YAML → flattened JSONL *(stub in v1; may need finishing)*

### 8.2 Two integration options

#### Option A (recommended): normalize during acquisition
Extend `acquire_worker.py` with additional “post-process” hooks per target:
- Acquire raw artifact into `raw/green/.../{target_id}/payload/`
- If target has `normalize: nvd_cve_v2`, run `nvd_worker.py` to emit:
  `raw/green/.../{target_id}/records/*.jsonl.gz`

This keeps the v2 stage order identical (classify → acquire → screen → merge → difficulty).

Example target config addition:
```yaml
targets:
  - id: nvd_cve_2_0_year_2024
    ...
    download:
      strategy: http
      urls: [...]
    normalize:
      kind: nvd_cve_v2
      input_glob: "payload/*.json.gz"
      output_subdir: "records"
```

#### Option B: add a dedicated `normalize_worker.py` stage
Insert a new stage between acquire and screen_yellow:
- normalize any raw artifacts into JSONL
- write into `raw/*/.../records/*.jsonl.gz`

This is cleaner architecturally, but it is an extra stage beyond math v2.

### 8.3 Minimal acceptance criteria for normalized records
To participate in merge + difficulty, each record should have:
- `text` (or one of the configured `text_field_candidates`)
- `hash.content_sha256` *(sha256 of normalized text)* **or** `content_sha256`
- `source.*` metadata (target id, URLs, license profile)
- `routing` (if you want deterministic difficulty routing)

---

## 9) Replace `yellow_scrubber.py` (legacy) with v2 review-plan helper

Cyber v1 `yellow_scrubber.py` is a legacy chem-style transformer.

In v2, `yellow_scrubber.py` is a **manual review prep tool**:
- reads YELLOW queue
- groups by target/license/restriction hits
- outputs a concise plan for signoff

**Action:**
Copy math v2 `yellow_scrubber.py` into cyber v2 unchanged except:
- defaults to `targets_cyber.yaml`
- optionally add cyber-specific columns (e.g., “dual-use risk tags”)

---

## 10) Replace `catalog_builder.py` with the v2 catalog builder

Use the math v2 `catalog_builder.py` (stage-aware, lightweight). It:
- summarizes counts by stage (raw/screened_yellow/combined/final)
- groups by license pool
- reports shard sizes and basic metadata

If you still want deep stats from v1, keep the old file as:
- `catalog_builder_legacy.py`

---

## 11) Update `run_pipeline.sh` (orchestration)

Math v2 supports stages:
- `classify, acquire_green, acquire_yellow, screen_yellow, merge, difficulty, catalog`

Cyber v1 supports stages:
- `classify, review, download, catalog`

**Action:**
Copy math v2 `run_pipeline.sh` into cyber v2 and:
- rename banner text to Cyber
- default targets to `targets_cyber.yaml`
- keep `review` stage if you still want it (map it to `review_queue.py` and/or `yellow_scrubber.py`)
- ensure it passes `--execute` down to workers consistently

---

## 12) Validation & smoke tests

### 12.1 Dry-run (should produce plans only)
```bash
./run_pipeline.sh --targets targets_cyber.yaml --stage classify
./run_pipeline.sh --targets targets_cyber.yaml --stage acquire_green
./run_pipeline.sh --targets targets_cyber.yaml --stage acquire_yellow
```

### 12.2 Execute minimal end-to-end on 1–2 targets
Use `--limit-targets 1` or disable most targets, then:

```bash
./run_pipeline.sh --targets targets_cyber.yaml --stage classify --execute
./run_pipeline.sh --targets targets_cyber.yaml --stage acquire_green --execute
./run_pipeline.sh --targets targets_cyber.yaml --stage acquire_yellow --execute
./run_pipeline.sh --targets targets_cyber.yaml --stage screen_yellow --execute
./run_pipeline.sh --targets targets_cyber.yaml --stage merge --execute
./run_pipeline.sh --targets targets_cyber.yaml --stage difficulty --execute
./run_pipeline.sh --targets targets_cyber.yaml --stage catalog --execute
```

### 12.3 Common “nothing merged” failure
If `merge_worker.py` produces **0 combined records**, it usually means:
- your acquired targets are not producing `*.jsonl*` under `raw/green/...`
- or records lack `content_sha256`

Fix by normalizing sources into JSONL (see §8) and adding a hash field.

---

## 13) Suggested minimal MVP vs full cyber v2

### MVP (fastest path, matches math v2 exactly)
- Port math v2 workers and layout
- Only expect JSONL-ready sources (synthetic corpora, already-jsonl datasets)
- Non-JSONL feeds remain in raw but won’t reach final shards

### Full cyber v2 (recommended for real cyber coverage)
- Add normalization integration for NVD/STIX/GHSA into JSONL
- Ensure normalized records include `text` + `content_sha256` + `routing`
- Then they flow through screen_yellow → merge → difficulty → final

---

## Appendix: quick checklist (copy/paste)

- [ ] Create `cyber_pipeline_v2/` (copy from `math_pipeline_v2/`)
- [ ] Create `targets_cyber.yaml` (migrate globals to v2 roots)
- [ ] Create `difficulties_cyber.yaml` (levels 1–10 + routing_map)
- [ ] Copy/adjust: `pipeline_driver.py`, `review_queue.py`
- [ ] Ensure `run_pipeline.sh` stage list = math v2 stage list
- [ ] Normalize non-JSONL cyber feeds into JSONL (Option A or B)
- [ ] Run smoke test end-to-end on 1–2 targets
