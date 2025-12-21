# agri_circular_pipeline_v2_adaptation.md

## Purpose

Adapt **`agri_circular_pipeline_v1`** into **`agri_circular_pipeline_v2`** so it behaves like **`math_pipeline_v2`**:

1. **Classify** targets + snapshot license evidence → emit **GREEN/YELLOW/RED** queues  
2. **Acquire GREEN** into `raw/green/...`  
3. **Acquire YELLOW** into `raw/yellow/...`  
4. **Screen YELLOW** strictly (“anything unclear is pitched”) → `screened_yellow/...` + ledgers  
5. **Merge** canonical GREEN + screened YELLOW → `combined/...` + combined index ledger  
6. **Difficulty** final screening + difficulty assignment → `final/.../d01..d10/...` + final index ledger  
7. **Catalog** stage summaries across v2 layout  

This doc is meant to be dropped into the repo as a single implementation plan.

---

## 1) Target v2 behavior (match math_pipeline_v2)

### Stage order + responsibilities
- **`pipeline_driver.py`**  
  - Evaluate targets, snapshot license evidence, classify into **GREEN / YELLOW / RED**
  - Emit queue JSONLs and manifests
- **`acquire_worker.py`**  
  - Download GREEN and YELLOW targets into `raw/green/...` and `raw/yellow/...`
- **`yellow_screen_worker.py`**  
  - Strict screening of YELLOW; **anything unclear is pitched**
  - Emit canonical JSONL shards + pass/pitch ledgers
- **`merge_worker.py`**  
  - Merge GREEN canonical records + screened YELLOW shards into one combined corpus
  - Dedupe + write combined index ledger
- **`difficulty_worker.py`**  
  - Apply final screening + assign difficulty **d01..d10**
  - Emit final difficulty shards + final index ledger
- **`catalog_builder.py`**  
  - Summarize counts/bytes per stage/pool and ledger rollups

---

## 2) Target v2 directory layout

Replace v1 “pools/” as the primary output with the v2 staged layout:

```
/data/agri_circular/
  raw/
    green/{license_pool}/{target_id}/...
    yellow/{license_pool}/{target_id}/...

  screened_yellow/{license_pool}/shards/yellow_shard_00000.jsonl.gz
  combined/{license_pool}/shards/combined_00000.jsonl.gz
  final/{license_pool}/d01..d10/shards/final_00000.jsonl.gz

  _queues/
    green_download.jsonl
    yellow_pipeline.jsonl
    red_rejected.jsonl

  _manifests/{target_id}/...
  _ledger/
    yellow_passed.jsonl
    yellow_pitched.jsonl
    combined_index.jsonl
    merge_summary.json
    final_index.jsonl
    difficulty_summary.json
  _pitches/
    final_pitched.jsonl
  _catalogs/
    catalog_v2.json
  _logs/
    ...
```

**License pools** are unchanged conceptually: `permissive`, `copyleft`, `quarantine`.

---

## 3) File-level rework plan

### 3.1 Add (copy from math_pipeline_v2 and adapt roots/naming)
Bring these into `agri_circular_pipeline_v2/`:

- `acquire_worker.py`
- `yellow_screen_worker.py`
- `merge_worker.py`
- `difficulty_worker.py`
- `catalog_builder.py` (v2 version)
- `yellow_scrubber.py` (v2 helper; optional but recommended)
- **Create:** `difficulties_agri_circular.yaml`

### 3.2 Keep (but edit for parity)
- `pipeline_driver.py` **(edit for routing parity; see §5)**
- `review_queue.py` (keep; may remain unchanged)
- `license_map.yaml`, `denylist.yaml`, `field_schemas.yaml`, `requirements.txt`

### 3.3 Deprecate legacy mainline stages (keep only for reference)
- `download_worker.py` → `download_worker_legacy.py` (do not call in v2)
- `yellow_scrubber.py` (v1) → `yellow_scrubber_legacy.py` (v2 uses a different helper)

---

## 4) Targets YAML changes (v1 → v2)

Pick one canonical file for v2 (recommended: `targets_agri_circular.yaml`) and update it to match fields used by v2 workers.

### 4.1 Add companion difficulty map
```yaml
companion_files:
  license_map: "./license_map.yaml"
  field_schemas: "./field_schemas.yaml"
  denylist: "./denylist.yaml"
  difficulties_map: "./difficulties_agri_circular.yaml"
```

### 4.2 Replace v1 roots/pools with v2 roots
Remove reliance on:
- `globals.storage_root`, `globals.staging_root`
- `globals.pools.{permissive,copyleft,quarantine}` (you can keep for legacy, but v2 workers shouldn’t depend on them)

Add v2 roots:

```yaml
globals:
  raw_root: "/data/agri_circular/raw"
  screened_yellow_root: "/data/agri_circular/screened_yellow"
  combined_root: "/data/agri_circular/combined"
  final_root: "/data/agri_circular/final"

  ledger_root: "/data/agri_circular/_ledger"
  pitches_root: "/data/agri_circular/_pitches"
  manifests_root: "/data/agri_circular/_manifests"
  queues_root: "/data/agri_circular/_queues"
  catalogs_root: "/data/agri_circular/_catalogs"
  logs_root: "/data/agri_circular/_logs"

  require_yellow_signoff: false

  sharding:
    max_records_per_shard: 50000
    compression: gzip

  screening:
    min_chars: 200
    max_chars: 12000
    text_field_candidates: ["text", "content", "body"]
    record_license_field_candidates: ["license", "license_spdx"]
    require_record_license: false
    allow_spdx: ["CC-BY-4.0", "CC0-1.0", "MIT", "Apache-2.0", "CC-BY-SA-4.0"]
    deny_phrases:
      - noai
      - "no tdm"
      - "no machine learning"
```

### 4.3 Optional per-target YELLOW screening overrides
Any target that needs special handling can add:

```yaml
yellow_screen:
  require_record_license: true
  allow_spdx: ["CC-BY-4.0", "CC0-1.0"]
  text_field_candidates: ["abstract", "full_text", "text"]
  record_license_field_candidates: ["license_spdx", "license"]
  min_chars: 200
  max_chars: 12000
```

---

## 5) pipeline_driver.py parity edits (agri v1 → v2)

Downstream v2 stages expect queue rows to include consistent routing and metadata fields.

### 5.1 Add routing support (copy v2 resolve_routing_fields)
Implement a routing resolver (ported from math v2) that can read routing from:

- `target["routing"]` (generic)
- optionally `target["agri_routing"]` (legacy convenience)

Ensure every emitted queue row includes:

- `target_id`
- `license_profile`
- `effective_bucket` (GREEN/YELLOW/RED)
- `queue_bucket` (green/yellow/red; whichever terminology math v2 uses)
- `routing` (dict: `subject`, `domain`, `category`, optional `tags`)
- `download` block (strategy + source url + extraction hints)
- `manifest_dir`

### 5.2 Ensure queue filenames match v2
Emit exactly:

- `green_download.jsonl`
- `yellow_pipeline.jsonl`
- `red_rejected.jsonl`

Under `globals.queues_root`.

---

## 6) Replace download stage with acquire_worker.py (v2)

### 6.1 Use `acquire_worker.py` from math v2
Math v2’s acquire worker typically supports key strategies such as:
- `http`, `ftp`, `git`, `zenodo`, `dataverse`, `huggingface_datasets` (depending on your version)

If agri v1 targets rely on strategies not implemented in math v2, **port them** from `download_worker.py` into `acquire_worker.py` (keeping the v2 output layout).

### 6.2 Output location MUST be v2 raw layout
`acquire_worker.py` must write into:

```
raw/{green|yellow}/{license_pool}/{target_id}/...
```

### 6.3 Done markers
After successful acquisition, write an idempotent manifest marker such as:

- `_manifests/{target_id}/acquire_done.json`

(Match the pattern used by math v2.)

---

## 7) Implement strict YELLOW screening (yellow_screen_worker.py)

### 7.1 Copy `yellow_screen_worker.py` from math v2
Then adapt routing fallback for agri:

- Use `raw.get("routing") or raw.get("agri_routing") or {}`

### 7.2 Outputs must match v2
- `screened_yellow/{license_pool}/shards/yellow_shard_00000.jsonl.gz`
- `_ledger/yellow_passed.jsonl`
- `_ledger/yellow_pitched.jsonl`
- `_manifests/{target_id}/yellow_screen_done.json`

### 7.3 Critical agri note: raw inputs aren’t always JSONL
Math v2’s screen worker typically expects `*.jsonl*` in raw folders. Many agri sources are `csv/tsv/pdf/html/zip`.

To keep the **same stage order** while making it work, pick one approach:

**Option A (recommended): extend `yellow_screen_worker.py` to ingest common formats**
- `*.csv`/`*.tsv` → map each row to text using configured `text_column` or join selected columns
- `*.txt`/`*.md`/`*.html` → chunk into pseudo-records and screen them
- (Optional later) `*.pdf` → extract text (adds dependency + quality risks)

**Option B: add an “extract_to_jsonl” postprocess inside acquire**
- After acquisition, if the target specifies `postprocess.extract_to_jsonl: true`, run a converter and emit:
  `raw/{bucket}/{pool}/{target_id}/records.jsonl.gz`
- Then `yellow_screen_worker.py` remains unchanged (it just reads JSONL).

Either approach preserves the math v2 **Acquire → Screen Yellow** semantics.

---

## 8) Merge step (merge_worker.py)

Copy `merge_worker.py` from math v2 and adjust defaults to agri roots.

### Required outputs
- `combined/{license_pool}/shards/combined_00000.jsonl.gz`
- `_ledger/combined_index.jsonl`
- `_ledger/merge_summary.json`

### Important: GREEN must be canonical too
Merge expects GREEN canonical records. If GREEN acquisitions are not JSONL records, apply the same “extract_to_jsonl” logic to GREEN targets (Option A or B above) so merge can read canonical GREEN JSONL.

---

## 9) Difficulty assignment (difficulty_worker.py + difficulties YAML)

### 9.1 Copy `difficulty_worker.py` from math v2
It typically assigns difficulty using:
1) explicit routing → difficulty map, else
2) a length-based heuristic fallback

Outputs:
- `final/{license_pool}/d01..d10/shards/*.jsonl.gz`
- `_ledger/final_index.jsonl`
- `_pitches/final_pitched.jsonl`
- `_ledger/difficulty_summary.json`

### 9.2 Create `difficulties_agri_circular.yaml` (starter)
Start small and expand:

```yaml
subjects:
  agri_circular:
    domains:
      agronomy:
        categories:
          crop_management:
            level: { default: 3 }
          pest_disease:
            level: { default: 4 }
      soil:
        categories:
          soil_health:
            level: { default: 4 }
          nutrient_cycles:
            level: { default: 5 }
      waste_streams:
        categories:
          composting:
            level: { default: 3 }
          anaerobic_digestion:
            level: { default: 6 }
          recycling_sorting:
            level: { default: 5 }
      circular_bioeconomy:
        categories:
          biorefineries:
            level: { default: 7 }
          lca:
            level: { default: 6 }
      policy_compliance:
        categories:
          reporting_protocols:
            level: { default: 6 }
```

### 9.3 Ensure routing uses `subject: agri_circular`
In your target routing, set:
- `subject: agri_circular`
- plus `domain` and `category` where possible

---

## 10) Catalog builder parity (catalog_builder.py v2)

Replace agri v1 `catalog_builder.py` with the math v2 catalog builder and adjust defaults/paths.

### Expected output
- `/data/agri_circular/_catalogs/catalog_v2.json`

It should summarize:
- raw (counts/bytes per bucket/pool)
- screened_yellow shards
- combined shards
- final shards by difficulty
- ledger existence/size

---

## 11) Rewrite run_pipeline.sh to orchestrate v2 stages

Use agri v1 `run_pipeline.sh` as a base (it’s complete), but update stages to:

- `classify`
- `acquire_green`
- `acquire_yellow`
- `screen_yellow`
- `merge`
- `difficulty`
- `catalog`
- (optional) `review`

### Wiring (recommended)
- classify: `python pipeline_driver.py --targets <targets> ...`
- acquire_green: `python acquire_worker.py --queue <queues>/green_download.jsonl --bucket green --targets-yaml <targets> [--execute] --workers N`
- acquire_yellow: `python acquire_worker.py --queue <queues>/yellow_pipeline.jsonl --bucket yellow --targets-yaml <targets> [--execute] --workers N`
- screen_yellow: `python yellow_screen_worker.py --targets <targets> [--execute]`
- merge: `python merge_worker.py --targets <targets> [--execute]`
- difficulty: `python difficulty_worker.py --targets <targets> [--execute]`
- catalog: `python catalog_builder.py --targets <targets> --output <catalogs>/catalog_v2.json`

---

## 12) Migration for existing v1 outputs (optional)

If you already have data in:
`/data/agri_circular/pools/{permissive|copyleft|quarantine}/{target_id}`

Decide whether it represents:
- raw GREEN acquisitions
- raw YELLOW acquisitions
- already-transformed/cleaned outputs

Then either:
- move/copy into v2 raw layout:
  - `raw/green/<pool>/<target_id>/...`
  - `raw/yellow/<pool>/<target_id>/...`
- or keep as historical artifacts and re-run v2 acquisition.

---

## 13) Acceptance checklist (definition of “done”)

You’re v2-compatible when:

- [ ] `pipeline_driver.py` emits the 3 queues under `_queues/` and includes `routing` per queue row.
- [ ] `acquire_worker.py` writes to `raw/{green|yellow}/{pool}/{target_id}/...` and writes `_manifests/{target_id}/acquire_done.json`.
- [ ] `yellow_screen_worker.py` produces `screened_yellow/{pool}/shards/*.jsonl.gz` and writes `_ledger/yellow_passed.jsonl` + `_ledger/yellow_pitched.jsonl`.
- [ ] `merge_worker.py` produces `combined/{pool}/shards/*.jsonl.gz` and `_ledger/combined_index.jsonl`.
- [ ] `difficulty_worker.py` produces `final/{pool}/d01..d10/shards/*.jsonl.gz` and `_ledger/final_index.jsonl`.
- [ ] `catalog_builder.py` generates a v2 catalog referencing all stage roots.
- [ ] Ledgers + manifests let you enumerate exactly what passed/pitched at each stage.

---

## 14) Minimal first run (dry-run then execute)

```bash
# classify only (dry-run)
./run_pipeline.sh --targets targets_agri_circular.yaml --stage classify

# acquire (execute)
./run_pipeline.sh --targets targets_agri_circular.yaml --stage acquire_green --execute --workers 4
./run_pipeline.sh --targets targets_agri_circular.yaml --stage acquire_yellow --execute --workers 4

# screen/merge/difficulty (execute)
./run_pipeline.sh --targets targets_agri_circular.yaml --stage screen_yellow --execute
./run_pipeline.sh --targets targets_agri_circular.yaml --stage merge --execute
./run_pipeline.sh --targets targets_agri_circular.yaml --stage difficulty --execute

# catalog
./run_pipeline.sh --targets targets_agri_circular.yaml --stage catalog
```

---

## Design intent (why this adaptation is correct)

- v1 mixed acquisition and transformation into “pool outputs,” making it hard to audit and reproduce.
- v2 separates responsibilities cleanly:
  - **raw acquisition** (traceable)
  - **strict screening** with explicit pitch reasons
  - **merge + dedupe** into a stable combined set
  - **difficulty sorting** into curriculum-ready shards
- The only agri-specific extra work is ensuring non-JSONL raw formats can become canonical JSONL records before/while screening (Option A/B).
