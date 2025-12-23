# Materials Science Pipeline v1 → v2 Adaptation Plan (parity with `math_pipeline_v2`)

Updated: **2025-12-23 18:04:39Z** (UTC)

## Goal

Adapt **`materials_science_pipeline_v1`** into **`materials_science_pipeline_v2`** so it behaves like **`math_pipeline_v2`**:

- Same **stage model** (`classify → acquire_green → acquire_yellow → screen_yellow → merge → difficulty → catalog`)
- Same **directory layout** (`raw/`, `screened_yellow/`, `combined/`, `final/`, `_ledger/`, `_pitches/`, `_queues/`, `_manifests/`)
- Same **ledger + pitch semantics** (“anything unclear is pitched” for YELLOW screening)
- Same **difficulty routing behavior** (target-/record-level `routing` drives default `d01…d10` sharding)

Materials-specific differences (field schemas, deny/allow lists, some acquisition strategies) should be layered *without* changing the core stage semantics.

---

## 1) What you have today (v1) vs what v2 must look like

### v1 (materials)
`run_pipeline.sh` stages:
- `classify` (pipeline_driver produces queues)
- `download` (downloads GREEN now)
- `yellow` (runs yellow_scrubber transforms)
- `pmc` (PMC-specific downloader)
- `catalog`

Storage layout is **pool-based** (`pools/permissive`, `pools/quarantine`, `pools/red`) and doesn’t include the v2 merge+difficulty sorting flow.

### v2 target behavior (math_pipeline_v2)
Stages:
1. `classify` → `_queues/green_download.jsonl`, `_queues/yellow_pipeline.jsonl`
2. `acquire_green` → `raw/green/<pool>/<target_id>/...`
3. `acquire_yellow` → `raw/yellow/<pool>/<target_id>/...`
4. `screen_yellow` → `screened_yellow/<pool>/shards/*.jsonl.gz` + `_ledger/yellow_*` + `_pitches/`
5. `merge` → `combined/<pool>/shards/combined_*.jsonl.gz`
6. `difficulty` → `final/<pool>/d01..d10/shards/*.jsonl.gz` + `_ledger/final_index.jsonl`
7. `catalog` → rollups and global catalog outputs

---

## 2) Repository-level changes (file map)

### Add (from math v2 template)
- `acquire_worker.py` (replaces `download_worker.py`)
- `yellow_screen_worker.py` (new)
- `merge_worker.py` (new)
- `difficulty_worker.py` (new)
- `difficulties_materials.yaml` (new; mirrors `difficulties_math.yaml` structure)
- `targets_materials.yaml` (new; mirrors `targets_math.yaml` structure)

### Keep (ported with minimal edits)
- `pipeline_driver.py` (classify stage; update to write v2 routing/output_pool fields)
- `review_queue.py`
- `catalog_builder.py`
- `yellow_scrubber.py` (optional; integrate as “yellow postprocess” inside `screen_yellow`)
- `pmc_worker.py` (optional; integrate as an acquire strategy rather than a separate stage)
- `denylist.yaml`, `license_map.yaml`
- `domain_allowlist.yaml`, `phrase_denylist.yaml` (but integrate into v2 screening config)

### Remove or deprecate
- `download_worker.py` (superseded by `acquire_worker.py`)
- v1 pool layout docs/options (superseded by v2 roots)

---

## 3) Stage-by-stage adaptation plan

### Stage 0: Standardize directory roots (targets `globals`)
Update `targets.yaml` → `targets_materials.yaml` and replace v1 roots with v2 roots:

```yaml
globals:
  raw_root: "/data/materials/raw"
  screened_yellow_root: "/data/materials/screened_yellow"
  combined_root: "/data/materials/combined"
  final_root: "/data/materials/final"
  ledger_root: "/data/materials/_ledger"
  pitches_root: "/data/materials/_pitches"
  manifests_root: "/data/materials/_manifests"
  queues_root: "/data/materials/_queues"
  catalogs_root: "/data/materials/_catalogs"
  logs_root: "/data/materials/_logs"

  sharding:
    max_records_per_shard: 50000
    compression: "gzip"

  screening:
    # tuned for materials corpora; keep close to math v2 defaults initially
    min_chars: 200
    max_chars: 20000
    text_field_candidates: ["text","content","body","abstract"]
    record_license_field_candidates: ["license","license_spdx"]
    require_record_license: true          # recommended for mixed Zenodo/PMC pulls
    allow_spdx: ["CC-BY-4.0","CC0-1.0","MIT","Apache-2.0","CC-BY-SA-4.0"]
    deny_phrases: ["noai","no tdm","no machine learning"]
```

**Why:** these keys are what the v2 workers (`acquire_worker`, `yellow_screen_worker`, `merge_worker`, `difficulty_worker`) expect.

---

### Stage 1: `classify` (pipeline_driver.py)

**Keep the v1 license evidence + denylist + signoff logic**, but align outputs to match math v2:

1. Ensure each queue row contains:
   - `queue_bucket` (same as `effective_bucket`)
   - `output_pool` (permissive/copyleft/quarantine) and/or `license_profile`
   - `routing` object with `{subject, domain, category, level, granularity}`
2. Preserve `split_group_id`, `license_change_detected`, `review_required`, etc.

#### Concrete edits
- Add a routing resolver function analogous to math v2’s `resolve_routing_fields()`:
  - Prefer `target.routing.*`
  - Optionally fall back to `target.materials_routing.*` (for backwards compatibility)
  - Default `subject: "materials_science"`
- Add output-pool selection logic identical to math v2:
  - If `target.output.pool` present → use it
  - Else if `license_profile` implies copyleft/db-copyleft → `copyleft`
  - Else if `effective_bucket == GREEN` → `permissive`
  - Else → `quarantine`
- Write queues:
  - `_queues/green_download.jsonl`
  - `_queues/yellow_pipeline.jsonl`
  - `_queues/red_rejected.jsonl`
- Write per-target evaluation manifest under `_manifests/<target_id>/evaluation.json`.

---

### Stage 2: `acquire_green` + `acquire_yellow` (acquire_worker.py)

Replace v1 `download_worker.py` with v2 `acquire_worker.py` behavior:

- Input: the queue jsonl produced by classify
- Output:
  - `raw/{green|yellow}/{license_pool}/{target_id}/...`
  - `_ledger/acquire_summary.json` (or per-run summaries)
  - per-target done marker in `_manifests/<target_id>/acquire_done.json`

#### Important compatibility detail (to make merge work)
`merge_worker.py` looks for **`.../<target_id>/shards/*.jsonl*`**.  
Therefore, make sure acquisitions land record-carrying payloads in a `shards/` subfolder.

Two options:
- **(Preferred)** in `acquire_worker.py`: if the downloaded file is `.jsonl`/`.jsonl.gz`, default it into `shards/<filename>`.
- **(Config-only)** in each target: set `download.filename: "shards/<yourfile>.jsonl.gz"` (works because acquire_worker ensures parent dirs).

#### Materials-specific acquisition strategies
Keep the same stage name (`acquire_*`) but extend strategy handlers as needed:

- `pmc_oa` / `arxiv`: prefer producing jsonl shards directly (text + license per record)
- `api` sources (Materials Project, NOMAD, OQMD):
  - treat as **YELLOW** by default (ToS uncertainty)
  - acquisition should snapshot ToS/license evidence into `_manifests/<target>/evidence/`
  - output jsonl shards with explicit per-record license if available; otherwise force pitch later

---

### Stage 3: `screen_yellow` (yellow_screen_worker.py)

This stage enforces the v2 “anything unclear is pitched” rule.

Inputs:
- `raw/yellow/<pool>/<target_id>/shards/*.jsonl*` (or any jsonl in target dir you choose to glob)

Outputs:
- `screened_yellow/<pool>/shards/*.jsonl.gz` (only accepted records)
- `_ledger/yellow_passed.jsonl` (accepted ledger rows)
- `_ledger/yellow_pitched.jsonl` (pitched ledger rows)
- `_pitches/<target_id>/*.jsonl` (pitched samples + reason)

#### Integrate v1 yellow_scrubber transforms
To keep materials-specific value while staying v2-shaped:

- Run **v1 `yellow_scrubber` transforms inside** `screen_yellow` as an optional “pre-screen normalize” step:
  - schema normalization using `field_schemas_materials.yaml`
  - computed-only extraction / record filtering for restricted corpora
  - dedupe pass (if you keep it, run it *after* normalization)

Then apply the v2 screening gates:
- missing or non-allowlisted record license → pitch
- deny phrase hit → pitch
- too short/too long → pitch
- optional: “materials relevance” filter (keyword/topic routing) → pitch if out-of-scope

---

### Stage 4: `merge` (merge_worker.py)

Unchanged from math v2 semantics:

- Read canonical GREEN records from `raw/green/*/*/shards/*.jsonl*`
- Read screened YELLOW shards from `screened_yellow/*/shards/*.jsonl*`
- Deduplicate on `content_sha256` (or the same hash field you standardize on)
- Write `combined/<pool>/shards/combined_00000.jsonl.gz ...`

**Key requirement:** ensure each record carries:
- `hash.content_sha256` (or equivalent)
- `license_spdx` (record-level)
- `routing` (target/record level routing object)

---

### Stage 5: `difficulty` (difficulty_worker.py)

This is the big v2 feature missing in materials v1.

Unchanged from math v2 semantics:
- Read `combined/<pool>/shards/*.jsonl*`
- Assign a `difficulty.level` (1–10) primarily from `routing.subject/domain/category`
- Shard into `final/<pool>/d01..d10/shards/*.jsonl.gz`
- Write `_ledger/final_index.jsonl` mapping `content_sha256 → {pool, difficulty, shard}`

**Fallback behavior (match math v2):**
- If no routing match, fall back to a coarse heuristic (e.g., length-based level).

---

### Stage 6: `catalog` (catalog_builder.py)

Update catalog builder to scan the v2 roots and ledgers:

- counts/bytes per:
  - pool (permissive/copyleft/quarantine)
  - stage (raw_green/raw_yellow/screened/combined/final)
  - difficulty bucket (d01..d10)
- emit global catalog json + optional CSV

---

## 4) Difficulty mapping for Materials Science (1–10)

### 4.1 Rubric (how levels should “feel”)

These levels should mirror math v2’s intent (1 = foundations, 10 = frontier), but tuned to materials science.

| Level | Label | Typical content |
|---:|---|---|
| 1 | Foundations | Basic terminology (atom, molecule, crystal vs amorphous), qualitative explanations. |
| 2 | Basic quantitative | Units, density/porosity, simple mixing rules, reading simple plots. |
| 3 | Intro materials | Crystal structures, Miller indices basics, simple phase diagram reading, elastic vs plastic. |
| 4 | Core undergrad I | Stress–strain calculations, Hooke’s law, basic thermodynamics, processing basics. |
| 5 | Core undergrad II | Diffusion (Fick), defects/dislocations, strengthening mechanisms, binary phase diagrams. |
| 6 | Intro graduate | Kinetics/transport PDEs, fracture/fatigue intro, electronic structure foundations. |
| 7 | Graduate core | Defect thermodynamics, band theory details, CALPHAD basics, advanced characterization. |
| 8 | Advanced grad / early research | DFT/MD workflows, multiscale modeling, synchrotron/TEM analysis, catalysis surfaces. |
| 9 | Research specialist | High-throughput screening, ML potentials, complex failure analysis, novel process optimization. |
| 10 | Frontier | New methods/theory, new materials discovery paradigms, SOTA experimental/computational techniques. |

### 4.2 `difficulties_materials.yaml` structure (drop-in compatible)

Use the same structure as `difficulties_math.yaml` (only `subjects/domains/categories.level.default` is required by the current v2 difficulty worker):

```yaml
schema_version: "2.0"
updated_utc: "2025-12-23 18:04:39Z"
globals:
  default_subject: "materials_science"

rubric:
  scale: "1-10"
  levels:
    1: { label: "Foundations" }
    2: { label: "Basic quantitative" }
    3: { label: "Intro materials" }
    4: { label: "Core undergrad I" }
    5: { label: "Core undergrad II" }
    6: { label: "Intro graduate" }
    7: { label: "Graduate core" }
    8: { label: "Advanced grad / early research" }
    9: { label: "Research specialist" }
    10:{ label: "Frontier" }

subjects:
  materials_science:
    name: "Materials science"
    domains:
      materials_reference:
        categories:
          encyclopedia: { level: { default: 3 } }
          glossary:     { level: { default: 2 } }
          handbooks:    { level: { default: 6 } }

      materials_literature:
        categories:
          oa_fulltext:  { level: { default: 6 } }
          preprints:    { level: { default: 7 } }
          patents:      { level: { default: 8 } }
          textbooks:    { level: { default: 6 } }

      computational_materials:
        categories:
          structures_properties_api:  { level: { default: 7 } }
          dft_databases:              { level: { default: 7 } }
          thermo_phase_database:      { level: { default: 8 } }
          high_throughput_repository: { level: { default: 8 } }
          repository_archive:         { level: { default: 8 } }
          ml_potentials:              { level: { default: 9 } }

      thermo_phase:
        categories:
          phase_diagrams_binary: { level: { default: 5 } }
          calphad:               { level: { default: 7 } }

      kinetics_diffusion:
        categories:
          diffusion_fick:        { level: { default: 5 } }
          phase_transformations: { level: { default: 6 } }

      mechanical_behavior:
        categories:
          stress_strain:     { level: { default: 4 } }
          fracture_fatigue:  { level: { default: 6 } }
          creep:             { level: { default: 6 } }

      characterization:
        categories:
          xrd:         { level: { default: 5 } }
          sem_tem:     { level: { default: 7 } }
          synchrotron: { level: { default: 8 } }

      surfaces_corrosion:
        categories:
          corrosion:        { level: { default: 6 } }
          electrochemistry: { level: { default: 6 } }

      processing_manufacturing:
        categories:
          additive_manufacturing: { level: { default: 7 } }
          thin_films:             { level: { default: 7 } }
          heat_treatment:         { level: { default: 5 } }

      materials_ml:
        categories:
          benchmarks:           { level: { default: 6 } }
          screening_benchmarks: { level: { default: 7 } }
          gnn_models:           { level: { default: 8 } }

  auxiliary:
    name: "Auxiliary corpora (context, policy, supply chain, geo)"
    domains:
      environment:
        categories:
          emissions_factors: { level: { default: 3 } }
          lca_public:        { level: { default: 5 } }
      supply_chain:
        categories:
          commodity_stats: { level: { default: 4 } }
      geodata:
        categories:
          osm: { level: { default: 4 } }
```

### 4.3 Target-level routing assignments (map your existing v1 targets)

Add `routing` blocks to each target in `targets_materials.yaml`. Suggested defaults:

| Target | domain | category | default level | granularity |
|---|---|---|---:|---|
| `jarvis_dft_figshare` | `computational_materials` | `dft_databases` | 7 | `target` |
| `open_catalyst_oc20_oc22` | `catalysis_surfaces` | `open_catalyst_benchmarks` | 8 | `target` |
| `matbench_benchmarks` | `materials_ml` | `benchmarks` | 6 | `target` |
| `matbench_discovery` | `materials_ml` | `screening_benchmarks` | 7 | `target` |
| `usgs_critical_minerals_aux` | `supply_chain` | `commodity_stats` | 4 | `target` |
| `epa_egrid_aux` | `environment` | `emissions_factors` | 3 | `target` |
| `materials_project_api` | `computational_materials` | `structures_properties_api` | 7 | `target` |
| `nomad_api` | `computational_materials` | `repository_archive` | 8 | `target` |
| `oqmd_api_or_dump` | `computational_materials` | `thermo_phase_database` | 8 | `target` |
| `pmc_oa_materials_text` | `materials_literature` | `oa_fulltext` | 6 | `record` |
| `arxiv_materials_text` | `materials_literature` | `preprints` | 7 | `record` |
| `aflowlib` | `computational_materials` | `high_throughput_repository` | 8 | `target` |
| `zenodo_materials_collections` | `materials_literature` | `mixed_zenodo` | 6 | `record` |
| `wikipedia_materials_text` | `materials_reference` | `encyclopedia` | 3 | `record` |
| `osm_geodata` | `geodata` | `osm` | 4 | `target` |
| `proprietary_lca_databases` | `environment` | `lca_proprietary` |  | `target` |


For corpora where difficulty varies heavily within the dataset (PMC, arXiv, Zenodo collections, Wikipedia subsets), set `granularity: record` and rely on:
- (baseline) length heuristic fallback (math v2 parity)
- (recommended next) a light-weight materials difficulty classifier (optional enhancement; can be added later without breaking stage semantics)

---

## 5) License pool and screening policy mapping (materials-specific)

### License profiles → pools (match math v2 behavior)
Normalize v1 materials license profiles into the same pool names used by v2:

- `public_domain` → `permissive`
- `permissive_code` / `permissive` → `permissive`
- `attribution` → `permissive` (but keep attribution bundle output)
- `copyleft` / `copyleft_db` → `copyleft`
- `mixed_record_level` / `unknown_needs_review` → `quarantine` (and typically YELLOW)
- `proprietary` → RED (do not acquire for training; pitch)

### Phrase denylist integration
Fold `phrase_denylist.yaml` into `globals.screening.deny_phrases` so `yellow_screen_worker` enforces it uniformly.

---

## 6) Parity checklist (acceptance tests)

Run these tests to confirm “same behavior” as math v2:

1. **Dry-run parity**
   - `./run_pipeline.sh --stage classify` produces queues and per-target evaluation manifests without downloading.
2. **Acquisition layout**
   - `acquire_green` writes into `raw/green/<pool>/<target_id>/...`
   - `acquire_yellow` writes into `raw/yellow/<pool>/<target_id>/...`
3. **Yellow screening enforcement**
   - pitched rows are written to `_ledger/yellow_pitched.jsonl` + `_pitches/`
   - passed rows land in `screened_yellow/<pool>/shards/`
4. **Merge correctness**
   - `combined/<pool>/shards/` exists and contains merged shards
   - dedupe reduces duplicates across green+yellow
5. **Difficulty sharding**
   - `final/<pool>/d01..d10/shards/` exists
   - `_ledger/final_index.jsonl` contains hashes pointing to output shards
6. **Catalog**
   - catalog builder summarizes counts by pool and by difficulty directory.

---

## 7) Minimal implementation order (fastest path)

1. Copy math v2 worker set into `materials_science_pipeline_v2/`
2. Convert `targets.yaml` → `targets_materials.yaml`:
   - replace roots
   - add `resolvers`
   - add `routing` for each target
   - add `companion_files.difficulties_map: ./difficulties_materials.yaml`
3. Update materials `pipeline_driver.py`:
   - emit `output_pool` + `routing` fields in evaluation and queue rows
4. Implement `difficulties_materials.yaml` (start with the domain/category mapping above)
5. Integrate `yellow_scrubber.py` optionally inside `yellow_screen_worker` (or run as a sub-step before screening)
6. Run an end-to-end on 1–2 small targets (Matbench-like) and verify the parity checklist

---

### Appendix: recommended defaults for three “hard” target types
- **Materials Project / NOMAD / OQMD:** default to YELLOW + quarantine until you’ve captured explicit “AI training / redistribution” allowances in evidence.
- **Wikipedia / OSM:** likely copyleft-style pools; keep separate and don’t merge into permissive unless your training policy allows.
- **Proprietary LCA:** keep as RED; do not acquire for training.
