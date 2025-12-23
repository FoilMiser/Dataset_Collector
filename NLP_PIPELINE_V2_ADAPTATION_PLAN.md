# NLP Pipeline v2 Adaptation Plan (from `nlp_pipeline_v1` → `nlp_pipeline_v2`)
**Goal:** Update `nlp_pipeline_v1` so it follows the **same stage flow + folder layout + “strict pitch” behavior** as `math_pipeline_v2`, including **difficulty sharding (d01–d10)** driven by a `difficulties_nlp.yaml` map.

This plan is written to be actionable for a Codex/engineer pass: it calls out **exact files to copy/rename**, **schema changes**, and **difficulty mapping rules**.

---

## 0) Behavioral parity target (what “same as math_pipeline_v2” means)

`nlp_pipeline_v2` should match the **math v2** stage order and semantics:

1. **Classify** targets + snapshot license/ToS evidence → emit `GREEN / YELLOW / RED` queues.
2. **Acquire** (download) GREEN and YELLOW payloads into a **raw** layout:
   - `raw/{green|yellow}/{license_pool}/{target_id}/...`
3. **Screen YELLOW** into canonical JSONL records with **strict pitch**:
   - accept only clearly compliant records; “anything unclear is pitched”
   - write `screened_yellow/{license_pool}/shards/*.jsonl.gz`
   - write `_ledger/yellow_passed.jsonl` and `_ledger/yellow_pitched.jsonl`
4. **Merge** GREEN + screened YELLOW into **combined** shards (dedupe by `content_sha256`):
   - `combined/{license_pool}/shards/*.jsonl.gz`
   - `_ledger/combined_index.jsonl`
5. **Final screen + difficulty assignment**:
   - `final/{license_pool}/d01..d10/{subject}/{domain}/{category}/shards/*.jsonl.gz`
   - `_ledger/final_index.jsonl`
   - optional `_pitches/final_pitched.jsonl`
6. **Catalog build** over v2 layout:
   - `_catalogs/catalog.json` (or similar) summarizing counts/sizes per stage & pool

---

## 1) Inventory: what you have vs what you need

### `nlp_pipeline_v1` (current)
- `targets.yaml` schema v0.7 (queues as a list, pools layout, staging)
- `download_worker.py` downloads to `pools/{permissive|copyleft|quarantine}/...`
- `yellow_scrubber.py` is **chem-oriented** (PubChem/PMC planning) and not v2 screening
- `catalog_builder.py` is v0.9 (pool-oriented catalog)
- wrapper script stages: `classify`, `download`, `scrub_yellow`, `review`, `catalog`, etc.

### `math_pipeline_v2` (desired behavior)
- `targets_math.yaml` schema v0.8 (queues under `queues.emit`, new globals)
- `acquire_worker.py` downloads into **raw/** with green/yellow split
- `yellow_screen_worker.py` turns raw YELLOW into canonical JSONL w/ strict pitch + ledgers
- `merge_worker.py` merges raw GREEN + screened YELLOW into combined shards
- `difficulty_worker.py` assigns difficulty using `difficulties_math.yaml` and writes `final/`
- `catalog_builder.py` is layout-aware (raw/screened/combined/final)
- `run_pipeline.sh` stages: `classify`, `acquire_green`, `acquire_yellow`, `screen_yellow`, `merge`, `difficulty`, `catalog`

---

## 2) Create `nlp_pipeline_v2/` by cloning math v2 structure (recommended)
Fastest, lowest-risk path is:

1. Copy the **math v2** scaffolding, then swap in NLP-specific config and parsers:
   - copy: `acquire_worker.py`, `yellow_screen_worker.py`, `merge_worker.py`, `difficulty_worker.py`, v2 `catalog_builder.py`, v2 `run_pipeline.sh`, `review_queue.py`
2. Bring over NLP-specific artifacts from v1:
   - `license_map.yaml`, `denylist.yaml`, `field_schemas.yaml`
   - any gov helper docs (`README_gov_addons.md`) if you still use them
3. Replace only the NLP-specific extraction logic inside `yellow_screen_worker.py` (and optionally `difficulty_worker.py` heuristics).

> Outcome: minimal diff vs math v2, maximal parity.

---

## 3) Update the targets config: `targets.yaml` → `targets_nlp.yaml` (schema v0.8)

### 3.1 Rename and bump schema
- Rename: `targets.yaml` → `targets_nlp.yaml`
- Set:
  - `schema_version: "0.8"`
  - add `companion_files.difficulties_map: "./difficulties_nlp.yaml"`

### 3.2 Replace v1 pool/staging globals with v2 roots
Mirror the math v2 `globals` keys (keep NLP-only knobs as needed):

```yaml
globals:
  raw_root: /data/nlp/raw
  screened_yellow_root: /data/nlp/screened_yellow
  combined_root: /data/nlp/combined
  final_root: /data/nlp/final

  queues_root: /data/nlp/_queues
  manifests_root: /data/nlp/_manifests
  ledger_root: /data/nlp/_ledger
  pitches_root: /data/nlp/_pitches
  logs_root: /data/nlp/_logs
  catalogs_root: /data/nlp/_catalogs

  sharding:
    compression: gzip
    max_records_per_shard: 50000

  screening:
    # for YELLOW screening and final screen bounds
    min_chars: 300
    max_chars: 12000
    text_field_candidates: ["text", "content", "body"]
    record_license_field_candidates: ["license", "license_spdx", "spdx"]
    require_record_license: false
    allow_spdx: ["CC0-1.0", "MIT", "Apache-2.0", "CC-BY-4.0"]   # tune per your policy
    deny_phrases: ["noai", "no tdm", "no machine learning"]
```

### 3.3 Queues: convert v1 list → v2 `queues.emit`
Replace:

```yaml
queues:
  - id: green_download ...
```

with:

```yaml
queues:
  emit:
    - { id: green_download, path: /data/nlp/_queues/green_download.jsonl, criteria: { effective_bucket: GREEN, enabled: true } }
    - { id: yellow_pipeline, path: /data/nlp/_queues/yellow_pipeline.jsonl, criteria: { effective_bucket: YELLOW, enabled: true } }
    - { id: red_rejected,  path: /data/nlp/_queues/red_rejected.jsonl,  criteria: { effective_bucket: RED, enabled: true } }
```

### 3.4 Add `routing` to targets (required for difficulty mapping)
Add a **generic** `routing:` block to each target (same fields used by math v2):

```yaml
routing:
  subject: nlp
  domain: gov
  category: regulations
  granularity: target     # "target" means “apply to every extracted record”
  confidence: 0.8
  reason: "source_default"
```

You can also keep `nlp_routing:` as an alias if you want, but **ensure `routing` exists** because the v2 workers consume it.

---

## 4) Update `pipeline_driver.py` for v2 targets + routing emission

### 4.1 Adopt math v2 driver behavior
Make `nlp_pipeline_v2/pipeline_driver.py` match math v2:
- reads schema v0.8 (`queues.emit`)
- writes per-target evaluation JSON as before
- **adds routing fields** to queue rows (like math v2 does with `routing_subject`, `routing_domain`, ...)

### 4.2 Routing propagation rule (important)
When classifying a target, compute:

1. `routing = target.get("routing") or {}`
2. Ensure defaults:
   - `subject = routing.subject or "nlp"`
   - `domain = routing.domain or "misc"`
   - `category = routing.category or "misc"`
   - `granularity = routing.granularity or "target"`
3. Emit those both:
   - in the per-target evaluation manifest
   - in each queue row (`routing_subject`, `routing_domain`, ...)

This makes v2 workers deterministic without re-parsing the targets file.

---

## 5) Replace v1 download stage with v2 acquisition (`acquire_worker.py`)

### 5.1 File change
- Delete or retire `download_worker.py`
- Add `acquire_worker.py` (copy from math v2)

### 5.2 Strategy parity: port NLP v1 resolvers into v2
Your v1 `download_worker.py` likely already supports:
- HuggingFace dataset resolver
- GitHub release resolver
- Figshare resolver
- HTTP downloads w/ retry + resume

Ensure `acquire_worker.py` implements the same strategy set. The simplest approach:
- copy strategy handlers from v1 into v2 worker
- keep the v2 output layout:

```
raw/
  green/{license_pool}/{target_id}/...
  yellow/{license_pool}/{target_id}/...
```

### 5.3 Determine `license_pool` routing (same as math v2)
Keep the mapping:
- `license_profile: permissive` → pool `permissive`
- `license_profile: copyleft` → pool `copyleft`
- `license_profile: record_level` → default pool `quarantine` **or** pool determined per-record at screening time

---

## 6) Implement NLP YELLOW screening (`yellow_screen_worker.py`)

### 6.1 Copy math v2 worker, then replace extraction
Start from math v2 `yellow_screen_worker.py` and adapt:
- **Input:** raw YELLOW files under `raw/yellow/{license_pool}/{target_id}/...`
- **Output:** canonical JSONL records under `screened_yellow/{license_pool}/shards/*.jsonl.gz`

### 6.2 Canonical record schema (match math v2 expectations)
At minimum:

```json
{
  "record_id": "stable-id",
  "text": "chunk text...",
  "source": {
    "target_id": "...",
    "file": "...",
    "url": "...",
    "retrieved_at_utc": "...",
    "license_evidence": {...}
  },
  "license": { "spdx": "CC-BY-4.0", "profile": "permissive" },
  "routing": { "subject": "nlp", "domain": "gov", "category": "regulations", "granularity": "target" },
  "hash": { "content_sha256": "..." }
}
```

### 6.3 Strict pitch rules (YELLOW)
Pitch (write to `_ledger/yellow_pitched.jsonl`) if any of these are true:
- text missing/empty after cleaning/chunking
- cannot confidently determine license SPDX (when `require_record_license: true` for that target)
- license SPDX not in allowlist for the relevant pool (or violates your policy)
- deny-phrases hit in metadata/terms or in-record fields you choose to scan
- language mismatch (if `force_language: en`)
- chunk outside bounds (`min_chars`, `max_chars`)
- parsing ambiguity (unknown format, failed decoder, garbled OCR without confidence)

Accept → `_ledger/yellow_passed.jsonl` and write the canonical record.

### 6.4 NLP extraction: supported raw formats
Implement handlers for likely NLP sources:
- `.jsonl` / `.jsonl.gz` with `text`-like fields (use `text_field_candidates`)
- `.json` arrays of objects (same)
- `.txt` plain text (chunk into records)
- optional `.pdf` (only if you already have a robust extractor; otherwise pitch PDFs in v0 and add a dedicated `pdf_worker.py` later)

Chunking should respect your existing `text_processing_defaults` (from v1), moved under:
- `globals.text_processing_defaults` (optional)
- and/or per-target overrides (`target.yellow_screen.text_processing`)

---

## 7) Merge stage (`merge_worker.py`)
Copy from math v2 with no changes except default paths:
- Reads:
  - raw GREEN records (if any are already canonical JSONL) and/or their extracted GREEN text records
  - screened YELLOW shards
- Dedupe by `hash.content_sha256`
- Writes:
  - `combined/{license_pool}/shards/*.jsonl.gz`
  - `_ledger/combined_index.jsonl` mapping hash → shard

> If your GREEN payloads are *files*, not *records*, add a tiny “green_extract_worker.py” before merge to convert GREEN raw documents into canonical records (same record schema as screening). Math v2 assumes GREEN can participate in merge.

---

## 8) Difficulty assignment (`difficulty_worker.py`) + difficulty map (`difficulties_nlp.yaml`)

### 8.1 Keep math v2 difficulty worker shape
Use the same algorithmic skeleton:

1. Iterate combined shards
2. Final length bounds screen (`globals.screening.min_chars/max_chars`)
3. Assign difficulty:
   - If `record.difficulty.level` exists → keep it
   - Else if routing matches difficulties map → use `level_from_routing()`
   - Else fallback heuristic → assign by readability/length

4. Route into output path:
   - `final/{license_pool}/d{level:02d}/{subject}/{domain}/{category}/shards/...`

### 8.2 `difficulties_nlp.yaml` schema (mirror `difficulties_math.yaml`)
Create a new companion file:

```yaml
schema_version: "2.0"
updated_utc: "YYYY-MM-DD HH:MM:SSZ"

globals:
  destination_root_windows: E:/AI-Research/datasets/Natural/nlp
  destination_root_wsl: /mnt/e/AI-Research/datasets/Natural/nlp
  folder_layout: final/{license_pool}/d{level:02d}/{subject}/{domain}/{category}
  sanitize_path_segments: true
  default_subject: nlp
  default_domain: misc
  default_category: misc
  default_level: 5

rubric:
  scale: { min: 1, max: 10, name: "Difficulty 1–10" }
  meaning:
    d01: "Very simple language; short; minimal jargon; basic instructions/children's"
    d03: "Everyday prose; short articles; simple reports"
    d05: "Standard nonfiction/news; moderate length; some structure"
    d07: "Technical or policy writing; sustained argument; domain jargon"
    d09: "Legal/regulatory dense text; heavy cross-references; formal standards"
    d10: "Highly technical standards / research monographs; extreme density"

subjects:
  nlp:
    name: "General NLP (English)"
    domains:
      gov:
        name: "Government / public sector"
        categories:
          press_releases:  { level: { default: 4, min: 3, max: 5 } }
          guidance_policy: { level: { default: 6, min: 5, max: 7 } }
          reports:         { level: { default: 6, min: 5, max: 8 } }
          regulations:     { level: { default: 9, min: 8, max: 10 } }
          legislation:     { level: { default: 8, min: 7, max: 10 } }
      legal:
        name: "Case law / legal text"
        categories:
          case_law:        { level: { default: 9, min: 8, max: 10 } }
          contracts:       { level: { default: 8, min: 7, max: 10 } }
      scientific:
        name: "Scientific literature"
        categories:
          abstracts:       { level: { default: 7, min: 6, max: 8 } }
          full_papers:     { level: { default: 8, min: 7, max: 10 } }
      technical:
        name: "Technical documentation"
        categories:
          patents:         { level: { default: 8, min: 7, max: 10 } }
          specs:           { level: { default: 7, min: 6, max: 9 } }
          manuals:         { level: { default: 6, min: 5, max: 8 } }
      education:
        name: "Education / OER"
        categories:
          textbooks:       { level: { default: 6, min: 4, max: 8 } }
          lessons:         { level: { default: 5, min: 3, max: 7 } }
      literature:
        name: "Fiction / longform"
        categories:
          fiction:         { level: { default: 5, min: 3, max: 7 } }
          nonfiction:      { level: { default: 6, min: 4, max: 8 } }

rule_sets:
  # Optional: readability/structure overrides. Implement in difficulty_worker.py.
  - id: "readability_easy"
    when:
      flesch_kincaid_grade_max: 5
    set:
      level: 2
      confidence: 0.7
  - id: "readability_hard"
    when:
      flesch_kincaid_grade_min: 14
    set:
      level: 8
      confidence: 0.7
```

### 8.3 Difficulty heuristics (fallback when routing misses)
Implement a lightweight heuristic in `difficulty_worker.py`:
- **Primary**: readability proxy (FK grade or similar)
- **Secondary**: length buckets (as math v2 does)
- **Signals for “hard”**:
  - long sentences + high punctuation density (`;:()[]{}`) and citations
  - frequent section numbering (`1.1`, `§`, `CFR`, `U.S.C.`)
  - high jargon ratio (tokens with caps/underscores, long words)

Return:
```json
{ "level": 7, "method": "readability", "confidence": 0.6 }
```

---

## 9) Update `run_pipeline.sh` stage wiring
Rename stages to match math v2:

- `classify` → run `pipeline_driver.py`
- `acquire_green` → run `acquire_worker.py --queue green_download.jsonl`
- `acquire_yellow` → run `acquire_worker.py --queue yellow_pipeline.jsonl`
- `screen_yellow` → run `yellow_screen_worker.py`
- `merge` → run `merge_worker.py`
- `difficulty` → run `difficulty_worker.py`
- `catalog` → run `catalog_builder.py`

Keep `--execute` as the “actually write” flag everywhere.

---

## 10) Concrete routing + difficulty mapping for your current NLP targets
Suggested default routing blocks you can paste into each target in `targets_nlp.yaml`:

| target_id | routing.domain | routing.category | suggested default difficulty |
|---|---|---:|---:|
| `cp_usgpo_filtered` | `gov` | `reports` | 6 |
| `cp_regulations_filtered` | `gov` | `regulations` | 9 |
| `cp_caselaw_access_project_filtered` | `legal` | `case_law` | 9 |
| `cp_uspto_filtered` | `technical` | `patents` | 8 |
| `cp_libretexts_filtered` | `education` | `textbooks` | 6 |
| `cp_pressbooks_filtered` | `education` | `textbooks` | 6 |
| `cp_oercommons_filtered` | `education` | `lessons` | 5 |
| `cp_pubmed_filtered` | `scientific` | `full_papers` | 8 |
| `cp_python_peps_filtered` | `technical` | `specs` | 7 |
| `cp_library_of_congress_filtered` | `gov` | `reports` | 6 |
| (disabled) `cp_project_gutenberg_filtered` | `literature` | `fiction` | 5 |
| (disabled) `cp_pre_1929_books_filtered` | `literature` | `nonfiction` | 6 |
| (disabled) `uk_govuk_policy_ogl` | `gov` | `guidance_policy` | 6 |
| (disabled) `can_open_gov_portal_ckan` | `gov` | `reports` | 6–7 |
| (disabled) `au_datagov_ckan_ccby` | `gov` | `reports` | 6–7 |
| (disabled) `nz_datagov_ccby` | `gov` | `reports` | 6–7 |
| (disabled) `eu_commission_ccby_site` | `gov` | `guidance_policy` | 7 |
| (exclude) `cp_wikimedia_filtered_EXCLUDE` | — | — | excluded |
| (exclude) `cp_stackexchange_filtered_EXCLUDE` | — | — | excluded |

Implementation detail:
- Put these defaults into each target’s `routing` block.
- `difficulty_worker.py` will then use `difficulties_nlp.yaml` to map (subject/domain/category) → `level.default`.

---

## 11) Validation checklist (parity tests)
Run these in order (all should work in dry-run first):

1. **Classify**:
   - queues emitted under `/data/nlp/_queues/`
   - `_manifests/{target_id}/evaluation.json` contains routing fields
2. **Acquire** (dry-run → execute):
   - `raw/green/...` and `raw/yellow/...` directories created
   - `_manifests/{target_id}/acquire_done.json` written after execute
3. **Screen YELLOW**:
   - `_ledger/yellow_passed.jsonl` and `_ledger/yellow_pitched.jsonl` both non-empty on mixed inputs
   - screened shards exist under `screened_yellow/{license_pool}/shards/`
4. **Merge**:
   - combined shards under `combined/{license_pool}/shards/`
   - `_ledger/combined_index.jsonl` grows monotonically with new content hashes
5. **Difficulty**:
   - outputs under `final/{license_pool}/dXX/nlp/...`
   - `difficulty.method` indicates routing vs heuristic
6. **Catalog**:
   - `_catalogs/catalog.json` summarizes counts by stage/pool/difficulty

---

## 12) Deliverables summary (what to produce in `nlp_pipeline_v2/`)
Minimum file set to match math v2 behavior:
- `pipeline_driver.py` (v2 queues + routing emission)
- `acquire_worker.py`
- `yellow_screen_worker.py` (NLP extraction + strict pitch)
- `merge_worker.py`
- `difficulty_worker.py` (difficulty map + readability heuristics)
- `catalog_builder.py` (v2 layout catalog)
- `review_queue.py` (optional; for manual signoff workflow)
- `run_pipeline.sh` (v2 stages)
- `targets_nlp.yaml`
- `difficulties_nlp.yaml`
- `license_map.yaml`, `denylist.yaml`, `field_schemas.yaml`, `requirements.txt`, `todo.txt`, docs

---

If you want, I can also draft a **first-pass `difficulties_nlp.yaml`** and a patched **`targets_nlp.yaml`** with routing blocks filled for the targets you already have.
