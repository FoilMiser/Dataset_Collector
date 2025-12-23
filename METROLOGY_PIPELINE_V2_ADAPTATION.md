# METROLOGY_PIPELINE_V2_ADAPTATION.md

**Goal:** Adapt `metrology_pipeline_v1` into `metrology_pipeline_v2` so it behaves like `math_pipeline_v2` (same stage order, folder layout, ledgers/manifests, and difficulty sorting).

This document is written against these bundles:

- `metrology_pipeline_v1.zip` contents:
  - `pipeline_driver.py`, `download_worker.py`, `yellow_scrubber.py (legacy)`, `catalog_builder.py (v0.9)`, `run_pipeline.sh (v1.0)`, `targets.yaml`, etc.
- `math_pipeline_v2.zip` contents:
  - `acquire_worker.py`, `yellow_screen_worker.py`, `merge_worker.py`, `difficulty_worker.py`, `catalog_builder.py (v2.0)`, `run_pipeline.sh (v2.0)`, `targets_math.yaml`, `difficulties_math.yaml`, etc.

---

## 0) Behavioral parity target (the “math v2” stage contract)

Your **metrology v2** must run in this order and produce these artifacts:

1. **classify** (`pipeline_driver.py`)
   - Inputs: `targets_metrology.yaml`
   - Outputs: `_queues/green.jsonl`, `_queues/yellow.jsonl`, evidence snapshots in `_manifests/…`, plus per-target decision manifests

2. **acquire_green** + **acquire_yellow** (`acquire_worker.py`)
   - Inputs: `_queues/{green,yellow}.jsonl`
   - Outputs: `raw/{green|yellow}/{license_pool}/{target_id}/…`
   - Writes: per-target acquisition manifest in `_manifests/{target_id}/acquire_done.json`

3. **screen_yellow** (`yellow_screen_worker.py`)
   - Inputs: `raw/yellow/**/{target_id}/**/*.jsonl(.gz)`
   - Outputs:
     - `screened_yellow/{license_pool}/shards/yellow_shard_00000.jsonl.gz`
     - `_ledger/yellow_passed.jsonl`, `_ledger/yellow_pitched.jsonl`
     - `_manifests/{target_id}/yellow_screen_done.json`

4. **merge** (`merge_worker.py`)
   - Inputs: `raw/green/**` + `screened_yellow/**`
   - Outputs:
     - `combined/{license_pool}/shards/combined_00000.jsonl.gz`
     - `_ledger/combined_index.jsonl` (dedupe index by `content_sha256`)

5. **difficulty** (`difficulty_worker.py`)
   - Inputs: `combined/**`
   - Outputs:
     - `final/{license_pool}/d01..d10/{subject}/{domain}/{category}/shards/*.jsonl.gz`
     - `_ledger/final_index.jsonl`
     - optional `_pitches/final_pitched.jsonl`

6. **catalog** (`catalog_builder.py v2.0`)
   - Inputs: all stage roots
   - Outputs: `_catalogs/catalog.json` (and optional human-readable summaries)

**Non-negotiables for parity:**
- Same root keys in `targets_*` (`raw_root`, `screened_yellow_root`, `combined_root`, `final_root`, `ledger_root`, `pitches_root`, etc.)
- Same stage names in `run_pipeline.sh`
- Records must have `text` and `content_sha256` (either `hash.content_sha256` or top-level `content_sha256`)
- Difficulty assignment must be deterministic via `routing` → `difficulties_metrology.yaml`, with fallback heuristic.

---

## 1) Create `metrology_pipeline_v2/` by copying math v2 as the base

Start by copying the **v2 worker set** from `math_pipeline_v2` and then “metrology-ize” naming and defaults.

### 1.1 Files to copy from `math_pipeline_v2` into `metrology_pipeline_v2`
Copy these verbatim first:

- `acquire_worker.py`
- `yellow_screen_worker.py`
- `merge_worker.py`
- `difficulty_worker.py`
- `catalog_builder.py` (v2.0)
- `run_pipeline.sh` (v2.0)
- `review_queue.py` (same)
- `requirements.txt` (then extend; see below)

Also copy these as a starting point, then adapt:
- `field_schemas.yaml` (contains routing schema used by v2)
- `license_map.yaml` (optionally merge with your metrology customizations)
- `denylist.yaml` (optionally merge with your metrology customizations)

### 1.2 Files to keep from `metrology_pipeline_v1`
Bring these over (and possibly lightly edit):

- `pipeline_driver.py` (or use the math v2 one; see §3)
- `pmc_worker.py` (kept for compatibility; not part of the v2 stage order)
- Any metrology-specific `targets` content (but in v2 format; see §2)

### 1.3 Files to delete / deprecate in metrology v2
- `download_worker.py` → replaced by `acquire_worker.py`
- `yellow_scrubber.py (legacy)` → replaced by the v2 helper or omitted
- `run_pipeline.sh (v1.0)` → replaced by v2 script with v2 stage names

---

## 2) Convert `targets.yaml` (v1 layout) → `targets_metrology.yaml` (v2 layout)

Your metrology v1 uses legacy roots:

- `storage_root`, `staging_root`, and `pools: {permissive, copyleft, quarantine}`

Your metrology v2 must use math v2 roots:

```yaml
schema_version: "0.8"
updated_utc: "YYYY-MM-DD"
companion_files:
  license_map: ./license_map.yaml
  field_schemas: ./field_schemas.yaml
  denylist: ./denylist.yaml
  difficulties_map: ./difficulties_metrology.yaml   # NEW

globals:
  raw_root: /data/metrology/raw
  screened_yellow_root: /data/metrology/screened_yellow
  combined_root: /data/metrology/combined
  final_root: /data/metrology/final
  ledger_root: /data/metrology/_ledger
  pitches_root: /data/metrology/_pitches
  manifests_root: /data/metrology/_manifests
  queues_root: /data/metrology/_queues
  catalogs_root: /data/metrology/_catalogs
  logs_root: /data/metrology/_logs

  # v2 workers read these to find text and license fields inside records.
  record_defaults:
    text_field_candidates: [text, content, body]
    record_license_field_candidates: [license, license_spdx]
    require_record_license: false
    allow_spdx:
      - CC0-1.0
      - CC-BY-4.0
      - MIT
      - Apache-2.0
      - CC-BY-SA-4.0
      - US-PUBLIC-DOMAIN
```

### 2.1 Update target entries to include v2 routing (for difficulty)
Metrology v1 targets include `data_type` tags (a list). Keep those, but also add a **routing** object (deterministic difficulty mapping) per target.

Example (BIPM SI brochure PDF):

```yaml
- id: bipm_si_brochure_latest
  enabled: true
  license_profile: permissive
  download:
    strategy: http
    urls:
      - https://www.bipm.org/documents/.../SI-Brochure-9-EN.pdf
  routing:
    subject: metrology
    domain: si_units
    category: si_brochure
  output:
    pool: permissive
    formats: [pdf]
```

**Where routing is used:**
- `difficulty_worker.py` reads `record.routing` and maps it via `difficulties_metrology.yaml`.

### 2.2 Remove / ignore legacy “pools:” directories
In v2, license pools are logical labels (`permissive|copyleft|quarantine`) that map to output **subfolders** under each root:

- `raw/green/permissive/...`
- `final/copyleft/d07/...`, etc.

You no longer need `globals.pools.*` absolute paths.

---

## 3) Ensure `pipeline_driver.py` is v2-compatible

Your metrology v1 `pipeline_driver.py` is already close to the math v2 driver, but the docstring and some help text references `download_worker.py`.

**Recommended approach (lowest risk):**
- Copy `pipeline_driver.py` from `math_pipeline_v2` into metrology v2 (it already references the v2 stage contract), then adjust:
  - README text / pipeline name
  - any metrology-specific “resolver” blocks in `targets_metrology.yaml` (keep them)

**Minimum changes if keeping metrology v1 driver:**
- Update help strings:
  - “download_worker” → “acquire_worker”
- Confirm it writes queues to `globals.queues_root`
- Confirm queue rows include `license_profile` or `license_pool` (needed by acquire worker’s `resolve_license_pool()`).

---

## 4) Critical metrology-specific gap: PDFs/HTML must become JSONL text chunks

`yellow_screen_worker.py` and `merge_worker.py` expect **JSONL** records (optionally gzipped).  
But many metrology targets are `formats: [pdf]` or `[html]`.

To keep the exact math v2 stage order **without adding extra stages**, implement extraction inside **acquire** (recommended).

### 4.1 Modify `acquire_worker.py` (metrology v2) to extract after downloading
Add a post-processing step for downloaded artifacts:

- If file is PDF → extract text → chunk → emit `*.jsonl.gz`
- If file is HTML → extract readable text → chunk → emit `*.jsonl.gz`
- Always write raw artifact too (PDF/HTML), but downstream screening reads JSONL.

**Output convention (recommended):**
For each downloaded artifact, write:

- Raw file:
  - `raw/{bucket}/{license_pool}/{target_id}/artifacts/<filename>.pdf`
- Extracted chunks:
  - `raw/{bucket}/{license_pool}/{target_id}/chunks/chunk_00000.jsonl.gz`

### 4.2 Minimal JSONL record schema for metrology chunks (compatible with v2 workers)
Each line should include at least:

```json
{
  "text": "...chunk text...",
  "source": {
    "target_id": "bipm_si_brochure_latest",
    "source_url": "https://…pdf",
    "retrieved_at_utc": "2025-12-23T…Z",
    "content_type": "application/pdf",
    "publisher": "BIPM"
  },
  "routing": {"subject":"metrology","domain":"si_units","category":"si_brochure"},
  "hash": {"content_sha256":"<sha256 of text>"}
}
```

**Why this is enough:**
- `yellow_screen_worker.py` reads `text`
- `merge_worker.py` dedupes on `hash.content_sha256` (or `content_sha256`)
- `difficulty_worker.py` uses `routing` + heuristic fallback

### 4.3 Extraction libraries (requirements)
Extend `requirements.txt` in metrology v2 (optional but strongly recommended):

- PDF:
  - `pypdf>=4.0.0` (fast, simple)
  - optionally `pdfminer.six>=20221105` as fallback
- HTML:
  - `beautifulsoup4>=4.12.0`
  - `lxml>=5.0.0`
  - optionally `trafilatura>=1.6.0` for cleaner extraction

Keep them optional if you want a minimal install, but metrology usefulness depends on extraction.

---

## 5) Replace legacy YELLOW logic with v2 `yellow_screen_worker.py`

### 5.1 What screen_yellow does in v2
- Reads the YELLOW acquisition output (`raw/yellow/**/chunks/*.jsonl(.gz)`)
- Applies hard filters:
  - missing/empty text → pitch
  - too short/too long → pitch
  - (optionally) deny phrases / restriction phrases → pitch
- Writes canonical shards to `screened_yellow/{pool}/shards/…`
- Writes ledgers:
  - `_ledger/yellow_passed.jsonl`
  - `_ledger/yellow_pitched.jsonl`

### 5.2 Metrology-specific consideration: “tables are useful”
Your v1 `text_processing_defaults.drop_tables: false` indicates tables are valuable.  
So: do **not** over-aggressively strip tables during extraction. Instead:
- keep tables as text blocks (rows separated by `\n` or ` | `)
- keep captions and section headers where possible

---

## 6) Merge stage: reuse `merge_worker.py` unchanged

No metrology-specific changes are required if:
- both green and screened yellow ultimately provide JSONL records with `content_sha256`.

**Key rule:** ensure `content_sha256` is stable.
- compute it from normalized text (e.g., strip trailing spaces, normalize newlines)
- do not include timestamps inside chunk text (put timestamps in `source`)

---

## 7) Difficulty stage: create `difficulties_metrology.yaml`

### 7.1 Difficulty meaning for metrology (1–10)
A consistent rubric aligned to typical measurement practice:

- **1–2:** Everyday units and basic measurement literacy
- **3–4:** Intro measurement practice (sig figs, tolerances, basic calibration concepts)
- **5–6:** Professional metrology practice (traceability, uncertainty budgets, calibration curves, reporting)
- **7–8:** Advanced statistical metrology & lab systems (covariance, Monte Carlo, interlab comparisons, ISO 17025)
- **9:** Primary standards & quantum/advanced realizations (Kibble balance, frequency combs, quantum electrical)
- **10:** Research frontier (novel methods, new realizations, fundamental constants inference)

### 7.2 Starter `difficulties_metrology.yaml` (drop-in)
Create `metrology_pipeline_v2/difficulties_metrology.yaml`:

```yaml
schema_version: "2.0"
updated_utc: "2025-12-23T00:00:00Z"
globals:
  default_subject: metrology
  default_domain: misc
  default_category: misc
  default_level: 5

rubric:
  scale: {min: 1, max: 10, name: "Difficulty 1–10"}
  levels:
    1: {label: "Everyday basics"}
    2: {label: "Basic conversions"}
    3: {label: "Intro measurement"}
    4: {label: "Applied measurement"}
    5: {label: "Metrology practice"}
    6: {label: "Professional metrology"}
    7: {label: "Advanced statistical metrology"}
    8: {label: "Standards & comparisons"}
    9: {label: "Primary standards"}
    10:{label: "Research frontier"}

subjects:
  metrology:
    name: "Metrology & Measurement Science"
    domains:
      si_units:
        name: "SI units & conventions"
        categories:
          prefixes_symbols: {level: {default: 1, min: 1, max: 2}}
          derived_units: {level: {default: 2, min: 2, max: 4}}
          si_brochure: {level: {default: 5, min: 3, max: 6}}
      uncertainty:
        name: "Uncertainty, error, and propagation"
        categories:
          significant_figures: {level: {default: 3, min: 2, max: 4}}
          propagation_basic: {level: {default: 4, min: 3, max: 6}}
          gum_type_a_b: {level: {default: 5, min: 4, max: 7}}
          uncertainty_budget: {level: {default: 6, min: 5, max: 8}}
          monte_carlo: {level: {default: 7, min: 6, max: 9}}
          covariance_correlated_errors: {level: {default: 7, min: 6, max: 9}}
      calibration_traceability:
        name: "Calibration & traceability"
        categories:
          calibration_basics: {level: {default: 4, min: 3, max: 5}}
          traceability_chain: {level: {default: 5, min: 4, max: 7}}
          calibration_curves_regression: {level: {default: 6, min: 5, max: 8}}
          interlaboratory_comparisons: {level: {default: 8, min: 7, max: 10}}
      standards_regulatory:
        name: "Standards, compliance, reporting"
        categories:
          terminology_definitions: {level: {default: 5, min: 3, max: 7}}
          reporting_templates: {level: {default: 4, min: 3, max: 6}}
          iso_17025: {level: {default: 8, min: 7, max: 10}}
      instrumentation:
        name: "Instrumentation & sensors"
        categories:
          sensors_basic: {level: {default: 4, min: 3, max: 6}}
          signal_conditioning: {level: {default: 6, min: 5, max: 8}}
          interferometry_dimensional: {level: {default: 8, min: 7, max: 10}}
          time_frequency_allan: {level: {default: 7, min: 6, max: 9}}
          frequency_combs: {level: {default: 9, min: 8, max: 10}}
      primary_standards:
        name: "Primary standards & realizations"
        categories:
          quantum_electrical: {level: {default: 9, min: 8, max: 10}}
          kibble_balance_mass: {level: {default: 9, min: 8, max: 10}}
          primary_thermometry: {level: {default: 9, min: 8, max: 10}}
          fundamental_constants_inference: {level: {default: 10, min: 9, max: 10}}
```

### 7.3 Map your existing metrology v1 targets to routing categories (recommended defaults)
For the targets present in `metrology_pipeline_v1/targets.yaml`:

- `bipm_si_brochure_latest` → `metrology / si_units / si_brochure` (defaults to ~5)
- `nist_sp_330_si_brochure_us` → `metrology / si_units / si_brochure` (~5)
- `nist_rdaf_sp_1500_18r2_html` → `metrology / standards_regulatory / terminology_definitions` (5–6)
- `nist_technical_series_publication_metadata` → `metrology / standards_regulatory / reporting_templates` (4–5)
- `nist_nvlpubs_fulltext_harvest` → `metrology / calibration_traceability / traceability_chain` OR `uncertainty / uncertainty_budget` (6–7)
- `nasa_ntrs_openapi_public_harvest` → `metrology / instrumentation / sensors_basic` (4–7; often mixed)
- `usgs_pubs_warehouse_harvest` → `metrology / instrumentation / sensors_basic` (4–7; often mixed)
- `noaa_ir_json_api_harvest` → `metrology / measurement_systems / reporting_templates` (if you add that domain) or standards_regulatory/reporting_templates
- `noaa_seed_tech_memos` → same as above
- `faa_advisory_circulars_harvest` → `metrology / standards_regulatory / reporting_templates` (5–7)
- `common_pile_arxiv_abstracts` → mixed; either omit routing (heuristic) or route to `primary_standards/fundamental_constants_inference` only when filtered
- `common_pile_doab` → mixed; heuristic (or route to misc/books)
- `iso_standards_paywalled`, `ieee_standards_paywalled` → keep disabled / red bucket (do not acquire)

---

## 8) Update `catalog_builder.py` (v2.0) and outputs

Replace metrology v1 `catalog_builder.py (v0.9)` with the math v2 `catalog_builder.py (v2.0)`.

This ensures catalogs include:
- counts by stage (`raw`, `screened_yellow`, `combined`, `final`)
- counts by license pool
- counts by difficulty folder `d01..d10`

---

## 9) Update `run_pipeline.sh` to v2 stage names (and metrology defaults)

Use math v2 `run_pipeline.sh` and make these edits:

- Rename printed pipeline name to “Metrology Corpus Pipeline”
- Default targets file: `targets_metrology.yaml`
- Stages list must be exactly:
  - `all, classify, acquire_green, acquire_yellow, screen_yellow, merge, difficulty, catalog`

Optionally add **aliases** for convenience (not required for parity):
- `download` → `acquire_green`
- `yellow` → `screen_yellow`

---

## 10) Directory layout (what metrology v2 should produce)

Assuming `globals.*_root` under `/data/metrology`:

```
/data/metrology/
  raw/
    green/
      permissive/<target_id>/{artifacts/,chunks/}
      copyleft/...
      quarantine/...
    yellow/
      permissive/<target_id>/{artifacts/,chunks/}
      ...

  screened_yellow/
    permissive/shards/yellow_shard_00000.jsonl.gz
    ...

  combined/
    permissive/shards/combined_00000.jsonl.gz
    ...

  final/
    permissive/
      d01/metrology/si_units/si_brochure/shards/...
      d06/metrology/calibration_traceability/calibration_curves_regression/shards/...
      ...
    copyleft/...
    quarantine/...

  _queues/
    green.jsonl
    yellow.jsonl

  _manifests/
    <target_id>/classify_done.json
    <target_id>/acquire_done.json
    <target_id>/yellow_screen_done.json
    ...

  _ledger/
    yellow_passed.jsonl
    yellow_pitched.jsonl
    combined_index.jsonl
    final_index.jsonl

  _pitches/
    final_pitched.jsonl

  _catalogs/
    catalog.json

  _logs/
    ...
```

---

## 11) Implementation checklist (fast path)

Do these in order:

1. **Create folder**
   - `metrology_pipeline_v2/` (copy math v2 contents)

2. **Add new files**
   - `targets_metrology.yaml` (converted v2 roots + routing)
   - `difficulties_metrology.yaml` (from §7.2)

3. **Swap config pointers**
   - `targets_metrology.yaml: companion_files.difficulties_map: ./difficulties_metrology.yaml`

4. **Modify acquire_worker**
   - add PDF/HTML extraction → emits JSONL chunks
   - stamp `routing` from target onto each chunk record
   - compute `content_sha256`

5. **Keep v2 screen/merge/difficulty unchanged**
   - do not change stage behavior unless extraction demands it

6. **Update run_pipeline defaults**
   - default targets path and pipeline name

7. **Run end-to-end dry-run**
   - `./run_pipeline.sh --targets targets_metrology.yaml --stage all` (no `--execute`)
   - verify queues/manifests are emitted

8. **Execute for a small set**
   - enable only 1–2 PDF targets (BIPM + NIST SP 330)
   - `./run_pipeline.sh --targets targets_metrology.yaml --stage all --execute --workers 2 --limit-targets 2`

---

## 12) Validation (what to check for parity)

After a small run, verify:

- `_queues/green.jsonl` and `_queues/yellow.jsonl` exist
- `raw/green/permissive/bipm_si_brochure_latest/chunks/*.jsonl.gz` exists
- `screened_yellow/**/shards/` exists (if any yellow targets enabled)
- `combined/**/shards/combined_*.jsonl.gz` exists
- `final/**/d01..d10/…/shards/*.jsonl.gz` exists
- `_ledger/combined_index.jsonl` contains `content_sha256` entries
- Catalog includes difficulty counts

---

## 13) Optional but recommended: add a metrology chunk schema to field_schemas.yaml

Math v2’s `field_schemas.yaml` includes `math_text_chunk_v1.0.0`.  
For clarity (not required by workers), you may add:

- `metrology_text_chunk_v1.0.0` (same fields, but notes about tables/standards language)
- keep `queue_record_routing_v2.0.0` (important)

If you prefer minimal changes, simply keep math v2 `field_schemas.yaml` as-is.

---

## 14) “Known sharp edges” (metrology-specific)

- **HTML noise:** prefer a readability extractor (`trafilatura`) or at least strip nav/menus.
- **PDF math/symbols:** preserve Unicode symbols; don’t normalize away `±`, `µ`, `Ω`, etc.
- **Tables:** do not drop tables; many standards encode definitions that way.
- **Paywalled standards (ISO/IEEE):** keep disabled/red; do not acquire.

---

## 15) Deliverables summary (what metrology_pipeline_v2 must contain)

Minimum file set:

- `pipeline_driver.py`
- `acquire_worker.py` (modified to extract PDF/HTML → JSONL)
- `yellow_screen_worker.py`
- `merge_worker.py`
- `difficulty_worker.py`
- `catalog_builder.py`
- `run_pipeline.sh`
- `targets_metrology.yaml`
- `difficulties_metrology.yaml`
- `license_map.yaml`
- `denylist.yaml`
- `field_schemas.yaml`
- `requirements.txt`
- `README.md` (updated to v2 stages)

That’s everything needed to make metrology v2 behave like math v2, with deterministic difficulty mapping.

