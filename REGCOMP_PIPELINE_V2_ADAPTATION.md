# RegComp Pipeline v2 Adaptation Plan (from regcomp_pipeline_v1 → regcomp_pipeline_v2)

_updated_utc: 2025-12-24 04:06:05Z_

## Goal

Rework **`regcomp_pipeline_v1`** (Regulation & Compliance corpus prototype) into **`regcomp_pipeline_v2`** so it follows the **same stage-order, directory layout, strict YELLOW pitch behavior, ledgering, and difficulty assignment flow** used by your **`math_pipeline_v2`**.

This plan is intentionally implementation-oriented (file-by-file), and includes a **difficulty mapping rubric (1–10)** tailored to regulation/compliance corpora that plugs into the existing `difficulty_worker.py` routing logic used in `math_pipeline_v2`.

---

## Target behavior to match (math_pipeline_v2)

The v2 “behavior contract” to replicate is:

1. **Classify + queue emission** (`pipeline_driver.py`)
   - Read `targets_*.yaml`, `license_map.yaml`, `denylist.yaml`
   - Produce **queue JSONL** for GREEN and YELLOW (and keep RED excluded)
   - Snapshot license evidence and record an “effective bucket” decision
2. **Acquire** (`acquire_worker.py`)
   - Download (or snapshot/clone) into **raw pools**:
     - `raw/green/{license_pool}/{target_id}/...`
     - `raw/yellow/{license_pool}/{target_id}/...`
   - Write `_manifests/{target_id}/acquire_done.json`
3. **Screen YELLOW** (`yellow_screen_worker.py`)
   - Convert YELLOW raw artifacts into canonical JSONL shards
   - **“Anything unclear is pitched”** enforced by config + denyphrases + allowlists
   - Write ledgers: `_ledger/yellow_passed.jsonl`, `_ledger/yellow_pitched.jsonl`
4. **Merge** (`merge_worker.py`)
   - Merge canonical GREEN + screened YELLOW into `combined/{license_pool}/shards/*.jsonl.gz`
   - Lightweight dedup by `content_sha256`
5. **Final screen + difficulty assignment** (`difficulty_worker.py`)
   - Write final shards into:
     - `final/{license_pool}/d01..d10/shards/*.jsonl.gz`
   - Write `_ledger/final_index.jsonl` and optional `_pitches/final_pitched.jsonl`
6. **Catalogs** (`catalog_builder.py`)
   - Summarize stage outputs + counts + bytes for reproducibility

**Non-negotiables (for parity):**
- Folder layout and stage naming must match (raw → screened_yellow → combined → final).
- Ledger files must exist and be append-only JSONL.
- YELLOW screening must pitch on ambiguity (missing/unclear license, deny phrases, missing text, out-of-bounds length).
- Difficulty is assigned **only** in `difficulty_worker.py`, primarily from `routing` → difficulty map defaults.

---

## What exists today in regcomp_pipeline_v1

### Current modules
- `pipeline_driver.py` — already close to v2, but reads old schema (`targets.yaml` v0.1)
- `download_worker.py` — v0.9 downloader into `{license_pool}/{target_id}` (no green/yellow raw split)
- `review_queue.py`, `yellow_scrubber.py` — manual review helper
- `catalog_builder.py` — v0.9, expects older layout and does heavy sampling/token stats
- `field_schemas.yaml` — regcomp-specific provenance/versioning/hierarchy fields (good to keep!)
- `targets.yaml` — regcomp inventory v0.1 (needs schema upgrade)
- `license_map.yaml`, `denylist.yaml` — compatible concepts; may need small phrase tuning

### Gaps vs v2 behavior
- No `raw/green|yellow/...` layout
- No `acquire_worker.py`, `yellow_screen_worker.py`, `merge_worker.py`, `difficulty_worker.py`
- No v2 difficulty map file + routing integration
- `catalog_builder.py` incompatible with v2 folder layout

---

## Step-by-step adaptation (recommended implementation order)

### 0) Create the v2 package skeleton

Create a new folder `regcomp_pipeline_v2/` by copying **the v2 worker set** from `math_pipeline_v2/`, then apply regcomp-specific config + metadata:

- Copy from math v2:
  - `acquire_worker.py`
  - `yellow_screen_worker.py`
  - `merge_worker.py`
  - `difficulty_worker.py`
  - `catalog_builder.py` (v2)
  - `run_pipeline.sh` (then de-ellipsis / complete it; see §7)
- Keep from regcomp v1 (then adapt):
  - `pipeline_driver.py` (already has the license/bucket logic you want)
  - `review_queue.py`
  - `yellow_scrubber.py` (optional but useful)
  - `pmc_worker.py` (optional; only if you actually ingest PMC-adjacent compliance papers)

### 1) Upgrade `targets.yaml` → `targets_regcomp.yaml` (schema parity)

Create `targets_regcomp.yaml` with the **same schema surface** as `targets_math.yaml`:

Required top-level keys:
- `schema_version: "0.8"`
- `companion_files: { license_map, field_schemas, denylist, difficulties_map }`
- `globals: { raw_root, screened_yellow_root, combined_root, final_root, ledger_root, pitches_root, manifests_root, queues_root }`
- `screening: { min_chars, max_chars, text_field_candidates, record_license_field_candidates, require_record_license, allow_spdx, deny_phrases }`
- `resolvers:` (optional; include the same resolver blocks you actually use)
- `targets:` list (each target is the unit of classification and routing)

#### Mapping from v0.1 target fields → v0.8 fields

| v0.1 field (regcomp v1) | v0.8 field (regcomp v2) | Notes |
|---|---|---|
| `id` | `id` | unchanged |
| `name` | `name` | unchanged |
| `jurisdiction` | `classification.jurisdiction` (custom) or `tags` | Store in `routing.domain/category` if it matters for difficulty |
| `doc_types` | `data_type` + `routing.category` | Example: `doc_types: ["final_rule"]` → `routing.category: final_rule` |
| `access/worker/url` | `download.strategy` + strategy args | Use `http`, `git`, `zenodo`, `huggingface_datasets`, or add an `api` strategy (see §6) |
| `license_expected` | `license_profile` + `license_evidence.spdx_hint` | `license_profile` in {permissive,copyleft,quarantine} |
| `notes` | `notes` | keep |

#### Routing fields you must add per target (for difficulty)

Every target should have a `routing` block like:
```yaml
routing:
  subject: regcomp
  domain: privacy
  category: gdpr
  level: 7          # optional hint (worker uses difficulty map defaults anyway)
  granularity: target
```

And optionally a `classification` block for discovery/search:
```yaml
classification:
  subject: regcomp
  jurisdiction: EU
  doc_type: regulation_text
  language: en
```

### 2) Update `pipeline_driver.py` to read the v2 regcomp targets file

Changes to make (mirror math v2 behavior):
- Input file name: `--targets targets_regcomp.yaml`
- Emit queues to `globals.queues_root`:
  - `green_queue.jsonl`
  - `yellow_queue.jsonl`
  - `red_excluded.jsonl` (optional but nice)
- Ensure queue rows include the **generic routing fields** your v2 workers expect:
  - `routing_subject`, `routing_domain`, `routing_category`, `routing_level`, `routing_granularity`
  - (Keep any regcomp extras as additional fields; the v2 workers ignore unknown keys.)
- Keep evidence snapshotting and “manual review required” flags:
  - Use restriction phrases and denylist hits to push targets into YELLOW.
  - Preserve `effective_bucket` and `license_profile`.

**Why this matters:** `difficulty_worker.py` (v2) looks for `record.routing.*`, and when absent it falls back to length heuristics. Targets must provide sane defaults via routing.

### 3) Replace `download_worker.py` with `acquire_worker.py`

In regcomp v2:
- `download_worker.py` becomes obsolete (keep it only if you need legacy behavior).
- Use `acquire_worker.py` with:
  - `--bucket green` and `--bucket yellow` runs
  - raw output layout:
    - `raw/green/{license_pool}/{target_id}/...`
    - `raw/yellow/{license_pool}/{target_id}/...`
  - done markers: `_manifests/{target_id}/acquire_done.json`

**Regcomp-specific adjustment (optional but recommended):**
- Add a `--max-bytes-per-target` limit for bulky regulatory PDFs or comment dumps.
- Add per-target overrides like:
  - `download.max_bytes`
  - `download.allow_file_ext` (e.g., [".pdf",".html",".txt",".xml",".jsonl",".gz"])

### 4) Implement YELLOW canonicalization in `yellow_screen_worker.py`

To match math v2 behavior:
- Keep the strict pitch checks:
  - no text → pitch
  - length outside bounds → pitch
  - require record-level license if configured → pitch
  - not allowlisted SPDX → pitch
  - deny phrase hit → pitch

**Regcomp-specific additions without changing core behavior:**
- Expand `screening.text_field_candidates` to include common regcomp fields:
  - ["text","content","body","summary","abstract","section_text","reg_text"]
- Add PII-aware pitch or redaction (choose one; keep it deterministic):
  - Pitch if detected PII density is high (names + addresses + emails) **unless** the target is explicitly designated “public comments” and routed to a quarantine pool.
- Preserve hierarchy metadata if present:
  - If raw records include `cfr_title`, `part`, `section`, `agency`, etc., carry them through unchanged in the canonical record (do not drop fields—just add `record_id`, `hash`, `source`, and `routing`).

**Output contract (same as math v2):**
- `screened_yellow/{license_pool}/shards/yellow_shard_00000.jsonl.gz`
- `_ledger/yellow_passed.jsonl`
- `_ledger/yellow_pitched.jsonl`
- `_manifests/{target_id}/yellow_screen_done.json`

### 5) Merge: `merge_worker.py`

Use the math v2 merge worker with minimal changes:
- Inputs:
  - GREEN raw records that are already JSONL (or produced by your custom parsers)
  - screened YELLOW shards
- Dedup key: `(rec.hash.content_sha256)`
- Outputs:
  - `combined/{license_pool}/shards/combined_00000.jsonl.gz`
  - `_ledger/combined_index.jsonl`

**Important regcomp note:** If your GREEN acquisitions are mostly PDFs/repos, you’ll need a **GREEN transform step** to emit JSONL records (see §6). Don’t “teach” `merge_worker.py` to parse PDFs; keep it a dumb merger like math v2.

### 6) Add missing “domain transforms” as optional plug-in workers (regcomp-specific)

Math v2 implicitly expects some targets to require extra transforms (HF filtering, formal chunking, etc.). Regcomp will need the same pattern.

Add optional workers that run **between acquire → screen/merge**, each producing JSONL into the same raw bucket folder so downstream workers work unchanged.

Recommended minimal set:

1. **`reg_text_extract_worker.py` (new)**
   - Input: `raw/{green|yellow}/{pool}/{target_id}/**/*.pdf|html|xml`
   - Output: `raw/{green|yellow}/{pool}/{target_id}/extracted/*.jsonl.gz`
   - Responsibilities:
     - deterministic text extraction
     - preserve section hierarchy (`title/part/section/subsection`)
     - attach `routing` from target defaults

2. **`federalregister_api_worker.py` (new, if you use FR)**
   - Input: query config from `targets_regcomp.yaml`
   - Output: JSONL with per-document fields + full text where allowed
   - Must attach record-level license markers (`license_spdx: CC0-1.0` or PD where appropriate) and **pitch embedded third-party**.

3. **`pii_scrub_worker.py` (optional)**
   - If you prefer redaction over pitching:
     - redact emails/phones/street addresses into placeholders
     - add `pii_redacted: true` and keep originals out of output entirely

**Compatibility rule:** these workers must emit records shaped like:
```json
{
  "id": "...",
  "text": "...",
  "license_spdx": "...",
  "license_profile": "permissive|copyleft|quarantine",
  "routing": { "subject":"regcomp","domain":"...","category":"...","granularity":"section" },
  "source": { "target_id":"...", "source_url":"...", "retrieved_at_utc":"..." }
}
```

### 7) Bring `run_pipeline.sh` up to real v2 parity

Your v2 wrapper should run stages in this order (same as math v2 contract):

1. `classify` → `pipeline_driver.py`
2. `acquire_green` → `acquire_worker.py --bucket green`
3. `acquire_yellow` → `acquire_worker.py --bucket yellow`
4. `screen_yellow` → `yellow_screen_worker.py`
5. `merge` → `merge_worker.py`
6. `difficulty` → `difficulty_worker.py`
7. `catalog` → `catalog_builder.py`

Also support:
- `--execute` (dry-run default)
- `--limit-targets`, `--limit-files`, `--workers`
- Stage selection: `--stage all|classify|...`

*(Your current `math_pipeline_v2/run_pipeline.sh` is stubbed with ellipses; in regcomp v2, make it fully functional.)*

### 8) Update catalogs and field schemas

- Replace regcomp v1 `catalog_builder.py` with the **math v2** one (lightweight, layout-aware).
- Keep regcomp’s `field_schemas.yaml` but:
  - bump `schema_version` to something like "0.2" or "0.3"
  - add the v2-required record fields: `record_id`, `hash.content_sha256`, `routing.*`, `source.*`
  - explicitly mark which regcomp fields are optional but preserved (`jurisdiction`, `authority`, `effective_date`, etc.)

---

## Difficulty mapping for RegComp (1–10)

### A. The rubric: what 1 vs 10 means in this domain

This rubric should be written into `difficulties_regcomp.yaml` and used by `difficulty_worker.py` via routing defaults.

| Level | Meaning in Regulation/Compliance corpora | Typical examples |
|---:|---|---|
| 1 | Plain-language definitions; glossary entries; “what is X” | compliance glossary, short FAQs |
| 2 | Simple checklists and policy summaries; minimal citations | onboarding compliance checklist, “privacy basics” |
| 3 | Practitioner guidance for one topic; step-by-step procedures | how-to guides for filing/reporting, basic controls |
| 4 | Single regulation/law guidance with citations; limited cross-references | agency guidance docs, interpretive notes |
| 5 | Primary source text at section-level; moderate legal density | statute/reg section text; annotated forms |
| 6 | Standards/frameworks + implementation mapping | NIST/ISO control mappings, audit programs |
| 7 | Enforcement + administrative decisions; nuanced interpretations | consent orders, ALJ decisions, enforcement actions |
| 8 | Complex regimes or multi-reg interaction; exemptions, edge cases | securities/derivatives rules, cross-border privacy |
| 9 | Advanced legal analysis; precedent-heavy; rulemaking history | appellate opinions, regulatory impact analysis |
| 10 | Expert treatises / law review / multi-jurisdiction architecture | academic analyses, comprehensive compliance frameworks |

### B. How the pipeline assigns difficulty (to keep v2 parity)

`difficulty_worker.py` assigns:
1. Existing record difficulty (if present)
2. Else routing-based difficulty (default levels from `difficulties_regcomp.yaml`)
3. Else length heuristic fallback

So: **ensure targets and transform workers attach routing**.

### C. Proposed `difficulties_regcomp.yaml` structure

Match the math file structure, but set defaults for subject `regcomp`:

```yaml
schema_version: "2.0"
updated_utc: "2025-12-24 04:06:05Z"
globals:
  default_subject: regcomp
  default_domain: misc
  default_category: misc
  default_level: 5
subjects:
  regcomp:
    name: Regulation & Compliance
    domains:
      compliance_basics:
        name: Compliance basics
        categories:
          glossary_faq:
            level: { default: 1, min: 1, max: 2 }
          checklists_templates:
            level: { default: 2, min: 1, max: 3 }
          training_materials:
            level: { default: 3, min: 2, max: 4 }
      primary_law_and_regs:
        name: Statutes and regulations (primary sources)
        categories:
          statutes_section:
            level: { default: 5, min: 4, max: 6 }
          regulations_section:
            level: { default: 5, min: 4, max: 7 }
          rulemaking_notices:
            level: { default: 6, min: 5, max: 7 }
      privacy_data_protection:
        name: Privacy & data protection
        categories:
          general_guidance:
            level: { default: 4, min: 3, max: 5 }
          gdpr:
            level: { default: 8, min: 7, max: 9 }
          us_state_privacy:
            level: { default: 7, min: 6, max: 8 }
      cybersecurity_controls:
        name: Security controls & standards
        categories:
          nist_iso_controls:
            level: { default: 6, min: 5, max: 7 }
          audits_assessments:
            level: { default: 7, min: 6, max: 8 }
      financial_services:
        name: Financial services regulation
        categories:
          aml_kyc:
            level: { default: 7, min: 6, max: 8 }
          securities_market_rules:
            level: { default: 8, min: 7, max: 9 }
          derivatives_clearing:
            level: { default: 9, min: 8, max: 10 }
      enforcement_case_law:
        name: Enforcement and case law
        categories:
          admin_orders:
            level: { default: 7, min: 6, max: 8 }
          appellate_opinions:
            level: { default: 9, min: 8, max: 10 }
```

Expand domains/categories to reflect your actual target inventory (environmental, healthcare, labor, trade/sanctions, procurement, corporate governance, etc.).

### D. Practical routing conventions (so the mapping works)

Use a small, consistent taxonomy:

- `routing.domain`: broad program area (privacy, financial_services, environmental, labor, etc.)
- `routing.category`: document type or regime (gdpr, hipaa, aml_kyc, consent_order, etc.)
- `routing.granularity`: one of:
  - `target` (entire dataset)
  - `document` (one notice/order/opinion)
  - `section` (CFR/USC section, policy subsection)
  - `clause` (contract clause, requirement statement)

---

## Validation checklist (parity tests)

### Folder/layout parity
- [ ] Outputs exist for each stage: `raw/`, `screened_yellow/`, `combined/`, `final/`
- [ ] Pools are segregated by `{permissive|copyleft|quarantine}`
- [ ] Ledgers exist under `_ledger/` and are JSONL

### Behavioral parity
- [ ] Dry-run default everywhere (`--execute` required to write)
- [ ] YELLOW screening pitches on: missing text, missing/unknown license, deny phrases, out-of-bounds length
- [ ] Difficulty assignment happens only in `difficulty_worker.py`
- [ ] Difficulty derived primarily from routing defaults

### Difficulty mapping sanity
- [ ] For each target, verify `routing.subject/domain/category` are set and appear in final records
- [ ] Sample final shards show sensible spread across `d01..d10`
- [ ] Anything uncategorized lands in `default_level` (e.g., d05) rather than failing

---

## Deliverables to produce in regcomp_pipeline_v2

Minimum files (mirroring math v2):
- `targets_regcomp.yaml` (schema v0.8)
- `difficulties_regcomp.yaml` (schema v2.0)
- `pipeline_driver.py` (v2-compatible I/O)
- `acquire_worker.py`
- `yellow_screen_worker.py`
- `merge_worker.py`
- `difficulty_worker.py`
- `catalog_builder.py`
- `run_pipeline.sh` (fully implemented)
- Existing: `license_map.yaml`, `denylist.yaml`, `review_queue.py`

Optional (high value for regcomp):
- `reg_text_extract_worker.py`
- `federalregister_api_worker.py`
- `pii_scrub_worker.py`

---

## Notes on “same behavior” vs regcomp-specific requirements

This plan preserves the **mechanics** of math_pipeline_v2 (stage order, directory layout, ledgers, strict pitching, difficulty assignment strategy), while allowing regcomp to keep its domain-specific strengths:

- hierarchy/reg versioning metadata is preserved (not discarded)
- PII risk is handled deterministically (pitch or redact)
- embedded third-party licensing pitfalls are treated as “unclear → pitch” in YELLOW

If you implement the optional transform workers as **pre-screen emitters** (producing JSONL into raw buckets), you keep downstream v2 workers unchanged and maintain behavioral parity.
