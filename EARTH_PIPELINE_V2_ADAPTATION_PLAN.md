# Earth Systems pipeline: v1 → v2 adaptation plan
_Make `earth_pipeline_v2` behave like `math_pipeline_v2` (same stages, layout, queues, ledgers, pitch rules)_

## What “same behavior as math_pipeline_v2” means
Earth v2 should run the same **pipeline stages** and produce the same **artifacts** (queues/manifests/ledgers/shards), just with Earth-specific targets and routing.

**Stage flow (match Math v2):**
1. `pipeline_driver.py` — classify targets (GREEN/YELLOW/RED), snapshot license evidence, emit queues.
2. `acquire_worker.py` — download/acquire GREEN + YELLOW targets into `raw/` pools.
3. `yellow_screen_worker.py` — convert raw YELLOW acquisitions into canonical JSONL shards; **pitch anything unclear**.
4. `merge_worker.py` — merge canonical GREEN + screened YELLOW into `combined/` shards (dedupe by content hash).
5. `difficulty_worker.py` — final screening + assign difficulty 1–10; write `final/` shards.
6. `catalog_builder.py` — v2-style lightweight catalog over all stages.

Helper scripts remain:
- `review_queue.py` (manual signoff/approve/reject for YELLOW)
- `yellow_scrubber.py` (triage report for YELLOW)

---

## 1) Adopt the v2 folder layout (verbatim)
Use the same directory semantics as Math v2, rooted at `/data/earth`:

```
/data/earth/
  raw/
    green/{license_pool}/{target_id}/...
    yellow/{license_pool}/{target_id}/...
  screened_yellow/{license_pool}/shards/*.jsonl.gz
  combined/{license_pool}/shards/*.jsonl.gz
  final/{license_pool}/d01..d10/shards/*.jsonl.gz
  _queues/*.jsonl
  _manifests/{target_id}/...
  _ledger/*.jsonl
  _pitches/*.jsonl
  _catalogs/*.json
  _logs/*.log
```

**Invariant (same as Math v2):** Anything that reaches merge/difficulty must exist as JSONL records containing at least `text` and a stable `content_sha256` (in `hash.content_sha256`).

---

## 2) Convert `targets.yaml` → `targets_earth.yaml` (schema v0.8)
Math v2’s `targets_math.yaml` is schema v0.8 and drives every stage. Earth v2 must adopt the same schema.

### 2.1 Required top-level structure
Create `targets_earth.yaml` with:
- `schema_version: '0.8'`
- `updated_utc: <timestamp>`
- `companion_files`:
  - `license_map: ./license_map.yaml`
  - `field_schemas: ./field_schemas.yaml`
  - `denylist: ./denylist.yaml`
  - `difficulties_map: ./difficulties_earth.yaml`
- `globals` (paths + defaults):
  - `raw_root`, `screened_yellow_root`, `combined_root`, `final_root`
  - `ledger_root`, `pitches_root`, `manifests_root`, `queues_root`, `catalogs_root`, `logs_root`
  - `sharding.max_records_per_shard`, `sharding.compression`
  - `screening.min_chars`, `screening.max_chars`
  - `screening.text_field_candidates`
  - `screening.record_license_field_candidates`
  - `screening.deny_phrases`
  - `require_yellow_signoff` (optional)
- `queues.emit` definitions identical to Math v2:
  - `green_download`
  - `yellow_pipeline`
  - `red_rejected`
- `resolvers` section (documentation + config stubs; optional but recommended)
- `targets:` list

### 2.2 Per-target fields (match Math v2)
Every Earth target should support these keys (even if some are empty):
- `id`, `name`, `enabled`, `priority`
- `license_profile` (permissive/copyleft/quarantine)
- `license_evidence` (`spdx_hint`, `url`)
- `download` block with `strategy` + params
- `output.pool` and/or implicit pool rules
- `routing` block (see below)
- optional: `yellow_screen` overrides (text fields, license fields, allowlist, bounds)

### 2.3 Routing (Earth equivalent of Math routing)
Math v2 expects generic `routing` fields and also carries `math_routing` legacy fields.

Earth v2 should standardize on **generic** routing:
- `routing.subject: earth`
- `routing.domain`: climate | hydrology | remote_sensing | geology_geophysics | ecology_biodiversity | env_policy_reports | …
- `routing.category`: dataset_docs | stac_metadata | netcdf_metadata | reports | glossary | indicator_tables | …
- `routing.level`: (optional) 1–10
- `routing.granularity`: target | file | record

You may also allow an `earth_routing` alias for backward compatibility, but `pipeline_driver.py` must output `routing_*` fields in queue rows.

---

## 3) Update `pipeline_driver.py` (classification stage)
Start from Math v2’s driver and apply Earth changes.

### 3.1 Must-match outputs
For each target, write:
- `_manifests/{target_id}/evaluation.json`
- queues: `_queues/green_download.jsonl`, `_queues/yellow_pipeline.jsonl`, `_queues/red_rejected.jsonl`

Queue rows must include the same “v2 fields” Math uses:
- `effective_bucket` and `queue_bucket`
- `license_profile`, `resolved_spdx`, confidence + restriction hits
- `output_pool`
- `download` config
- `manifest_dir`
- routing expansion fields:
  - `routing_subject`, `routing_domain`, `routing_category`, `routing_level`, `routing_granularity`

### 3.2 Earth-specific edits
- default `--targets` help text: `targets_earth.yaml`
- user-agent string: `earth-corpus-pipeline/{VERSION}`
- routing resolver: prefer `target['routing']`, else `target['earth_routing']`, else defaults.

---

## 4) Replace `download_worker.py` with `acquire_worker.py`
Earth v1 uses `download_worker.py` with old pool dirs; Earth v2 must use the v2 raw layout and stage semantics.

### 4.1 Implementation approach
Copy `math_pipeline_v2/acquire_worker.py` → `earth_pipeline_v2/acquire_worker.py` and adjust:
- defaults for roots (`/data/earth/...`)
- output paths:
  - GREEN queue → `raw/green/{license_pool}/{target_id}/...`
  - YELLOW queue → `raw/yellow/{license_pool}/{target_id}/...`
- manifest marker: `_manifests/{target_id}/acquire_done.json`

### 4.2 Strategies
Keep all generic strategies supported by the v1 worker ecosystem:
- `http`, `ftp`, `git`, `zenodo`, `dataverse`, `huggingface_datasets`, `github_release`, `figshare`

Add Earth strategies (define in schema now; implement incrementally):
- `stac_catalog` (metadata-only → JSONL “dataset card” records)
- `thredds_opendap` (metadata-only → JSONL variable dictionary records)
- `api_tabular` (CSV/JSON indicators → JSONL with schema + optional sample)
- optional: `http_html`, `http_pdf` (download + lightweight extraction hooks)

**Compatibility note:** merge/difficulty expects JSONL records. If a target downloads PDFs, you must either:
- make acquisition emit extracted JSONL alongside the binary, or
- classify those targets as YELLOW so they get converted by `yellow_screen_worker.py`.

---

## 5) Add `yellow_screen_worker.py` (strict screening + pitching)
Copy Math v2’s `yellow_screen_worker.py` and customize only what Earth needs.

### 5.1 Screening defaults (globals)
Set Earth defaults in `targets_earth.yaml`:
- `min_chars: 200`, `max_chars: 12000`
- `text_field_candidates: [text, content, body, description, abstract]`
- `record_license_field_candidates: [license, license_spdx, rights, usage_rights]`
- `deny_phrases`: `noai`, `no tdm`, `no machine learning`, `all rights reserved`

### 5.2 Canonical record shape (keep identical to Math v2)
At minimum, every screened record must contain:
- `record_id`
- `text`
- `source.{target_id, source_url, license_spdx, license_profile, license_evidence, retrieved_at_utc}`
- `routing` (copied through)
- `hash.content_sha256`

Earth may add extra optional fields (but don’t break the above):
- `earth_meta.geo_extent`, `earth_meta.temporal_extent`
- `earth_meta.variables`, `earth_meta.units`
- `earth_meta.sensitivity_flags`

### 5.3 Ledgers/manifests (must match Math v2)
Write:
- `screened_yellow/{pool}/shards/yellow_shard_*.jsonl.gz`
- `_ledger/yellow_passed.jsonl`
- `_ledger/yellow_pitched.jsonl` (include `target_id` + reason + minimal sample pointers)
- `_manifests/{target_id}/yellow_screen_done.json`

Pitch reasons should include the same categories as Math v2 (`no_text`, `length_bounds`, `missing_license`, `deny_phrase_hit`, `parse_error`, …).

---

## 6) Add `merge_worker.py` (GREEN + screened YELLOW)
Copy Math v2’s `merge_worker.py` and change only path defaults.

Requirements to keep behavior identical:
- iterate GREEN records from `raw/green/**/**.jsonl*`
- iterate screened YELLOW from `screened_yellow/**/shards/*.jsonl*`
- dedupe by `hash.content_sha256`

Outputs:
- `combined/{pool}/shards/combined_*.jsonl.gz`
- `_ledger/combined_index.jsonl`
- `_ledger/merge_summary.json`

---

## 7) Add `difficulty_worker.py` + `difficulties_earth.yaml`
Copy Math v2’s `difficulty_worker.py` and point it at `difficulties_earth.yaml`.

### 7.1 `difficulties_earth.yaml`
Model it after `difficulties_math.yaml`:
- rubric levels 1–10 (labels + descriptions + signals)
- `subjects.earth.domains.<domain>.categories.<category>.level.default`

Starter rubric suggestion:
- 1–2: basic weather/Earth science concepts + simple plots
- 3–4: observational datasets + time series + GIS basics
- 5–6: domain methods + uncertainty + remote sensing basics
- 7–8: modeling + assimilation + advanced retrieval methods
- 9–10: graduate/monograph/research papers

**Folder layout:** keep the Math v2 default `final/{license_pool}/d{level}/shards` to preserve behavior.

Outputs:
- `final/{pool}/d01..d10/shards/final_*.jsonl.gz`
- `_ledger/final_index.jsonl`
- `_pitches/final_pitched.jsonl`

---

## 8) Replace `catalog_builder.py` with v2-style catalog
Earth v1’s catalog builder expects the old pools. Use the Math v2 lightweight catalog structure:
- count lines/bytes under each stage
- optionally summarize per-difficulty counts under `final/`

Output:
- `_catalogs/global_catalog.json`

---

## 9) Update helpers + orchestration

### 9.1 `review_queue.py`
Keep logic; change default queue path to `/data/earth/_queues/yellow_pipeline.jsonl`.

### 9.2 `yellow_scrubber.py`
Align to Math v2’s role: triage + planning for human review. Don’t rely on it for transforms.

### 9.3 `run_pipeline.sh`
Create a complete orchestrator (Math’s shipped script in your zip is truncated). Earth v2 should include a full version that supports:
- `classify`
- `acquire_green`
- `acquire_yellow`
- `screen_yellow`
- `merge`
- `difficulty`
- `catalog`

---

## 10) Field schema updates (`field_schemas.yaml`)
Keep your Earth-specific schemas (dataset cards, STAC summaries, NetCDF var dictionaries), and add one schema that exactly matches the canonical record emitted by screening:
- `earth_canonical_record_v1.0.0`

This makes downstream expectations explicit and keeps the pipeline reproducible.

---

## 11) Acceptance checklist (definition of done)
- `pipeline_driver.py` emits the three queues and per-target `evaluation.json`, with routing + output_pool fields.
- `acquire_worker.py` writes to `raw/green` and `raw/yellow` under `{license_pool}/{target_id}` and drops `acquire_done.json` manifests.
- `yellow_screen_worker.py` produces `screened_yellow/*/shards/*.jsonl.gz` and fills `_ledger/yellow_passed.jsonl` + `_ledger/yellow_pitched.jsonl`.
- `merge_worker.py` outputs `combined/*/shards/*.jsonl.gz`, `_ledger/combined_index.jsonl`, `_ledger/merge_summary.json`.
- `difficulty_worker.py` outputs `final/*/d01..d10/shards/*.jsonl.gz`, `_ledger/final_index.jsonl`, `_pitches/final_pitched.jsonl`.
- `catalog_builder.py` produces `_catalogs/global_catalog.json` summarizing all stages.

---

## 12) Concrete “copy then edit” map
**Copy from Math v2 → Earth v2 (then edit defaults/paths/routing):**
- `pipeline_driver.py`
- `acquire_worker.py`
- `yellow_screen_worker.py`
- `merge_worker.py`
- `difficulty_worker.py`
- `catalog_builder.py`
- `review_queue.py`
- `yellow_scrubber.py`
- `README.md` (stage table + usage)

**Create new:**
- `targets_earth.yaml` (schema v0.8)
- `difficulties_earth.yaml`

**Deprecate in v2 orchestration:**
- `download_worker.py` (keep in v1 for reference)
