# Engineering Pipeline v1 → v2 Adaptation Guide (math_pipeline_v2 parity)

This document explains **exactly how to adapt `engineering_pipeline_v1` into `engineering_pipeline_v2`** so it behaves like **`math_pipeline_v2`** (same stage contract, same directory layout, same queue semantics, and the same “difficulty assignment happens late” behavior).

The biggest functional change is that engineering v2 becomes a **two-pool acquisition + canonicalization pipeline**:

1. **Classify + evidence snapshot**  
2. **Acquire raw payloads** into `raw/green` and `raw/yellow`  
3. **Screen/canonicalize YELLOW** into JSONL shards  
4. **Merge GREEN + screened YELLOW** into combined shards  
5. **Assign difficulty + final screen** into difficulty-bucketed shards  
6. **Catalog** the results

---

## 0) Source of truth

Use these two directories as the reference implementations:

- `engineering_pipeline_v1/` (what you have now)
- `math_pipeline_v2/` (the behavior to match)

This guide assumes the following v2 scripts (as in math v2) exist in the new engineering repo:

- `pipeline_driver.py`
- `acquire_worker.py`
- `yellow_screen_worker.py`
- `merge_worker.py`
- `difficulty_worker.py`
- `catalog_builder.py`
- `review_queue.py` (optional)
- `run_pipeline.sh`

---

## 1) v1 → v2 stage mapping

| Concern | Engineering v1 | Engineering v2 (match math v2) |
|---|---|---|
| Target classification + evidence snapshots | `pipeline_driver.py` | `pipeline_driver.py` (ported from math v2 + eng tweaks) |
| Download GREEN | `download_worker.py` → `pools/*` | `acquire_worker.py` → `raw/green/*` |
| Process YELLOW | `yellow_scrubber.py` (chem legacy transforms) | `yellow_screen_worker.py` (generic canonicalization) |
| Combine GREEN + YELLOW | *implicit* | `merge_worker.py` |
| Difficulty sorting | *not present / ad hoc* | `difficulty_worker.py` (late-stage assignment) |
| Catalog | v1 `catalog_builder.py` (pools layout) | v2 `catalog_builder.py` (raw/screened/combined/final layout) |
| Optional PMC addon | `pmc_worker.py` stage | Keep as optional addon (after `acquire_*`, before merge, if you want) |

**Key contract change:** in v2, **difficulty assignment happens only in `difficulty_worker.py`**, after merging, and is driven by a **routing → difficulty map** (plus fallback heuristics), not during download.

---

## 2) Required directory layout (v2)

Engineering v2 must produce the same layout pattern as math v2:

```
/data/engineering/
  raw/
    green/{license_pool}/{target_id}/...
    yellow/{license_pool}/{target_id}/...
  screened_yellow/{license_pool}/shards/*.jsonl.gz
  combined/{license_pool}/shards/*.jsonl.gz
  final/{license_pool}/d01..d10/shards/*.jsonl.gz
  _ledger/*.jsonl
  _pitches/*.jsonl
  _queues/*.jsonl
  _manifests/{target_id}/...
  _catalogs/global_catalog.json
  _logs/...
```

**License pools** must match math v2 behavior:
- `permissive`
- `copyleft`
- `quarantine` (everything ambiguous/record-level falls here unless explicitly resolved)

---

## 3) File-by-file adaptation plan

### 3.1 Create a new repo folder: `engineering_pipeline_v2/`

Start by copying these files from `math_pipeline_v2/` into the new engineering v2 folder:

- `acquire_worker.py`
- `yellow_screen_worker.py`
- `merge_worker.py`
- `difficulty_worker.py`
- `catalog_builder.py`
- `review_queue.py`
- `run_pipeline.sh`

Then copy the engineering v1 domain-specific YAMLs and adjust them:

- `license_map.yaml`
- `denylist.yaml`
- `field_schemas.yaml` (optional)
- **convert** `targets.yaml` → `targets_engineering.yaml` (schema v0.8)

You will also add:

- `difficulties_engineering.yaml` (new; see Section 6)

---

### 3.2 Convert `targets.yaml` (v1 schema 1.0) → `targets_engineering.yaml` (v2 schema 0.8)

**Engineering v1** uses `globals.storage_root` and `globals.pools.*`.  
**Math v2** uses explicit roots:

```yaml
schema_version: "0.8"

companion_files:
  license_map: ./license_map.yaml
  denylist: ./denylist.yaml
  field_schemas: ./field_schemas.yaml   # optional
  difficulties_map: ./difficulties_engineering.yaml

globals:
  raw_root: /data/engineering/raw
  screened_yellow_root: /data/engineering/screened_yellow
  combined_root: /data/engineering/combined
  final_root: /data/engineering/final
  ledger_root: /data/engineering/_ledger
  pitches_root: /data/engineering/_pitches
  manifests_root: /data/engineering/_manifests
  queues_root: /data/engineering/_queues
  catalogs_root: /data/engineering/_catalogs
  logs_root: /data/engineering/_logs

  require_yellow_signoff: false   # set true if you want the v1 strictness

  sharding:
    max_records_per_shard: 50000
    compression: gzip

  screening:
    min_chars: 200
    max_chars: 12000
    text_field_candidates: [text, content, body, abstract, description, markdown]
    record_license_field_candidates: [license, license_spdx, rights]
    require_record_license: false
    allow_spdx: [CC0-1.0, MIT, Apache-2.0, CC-BY-4.0, CC-BY-SA-4.0]
    deny_phrases: ["noai", "no tdm", "no machine learning"]

queues:
  emit:
    - id: green_download
      path: /data/engineering/_queues/green_download.jsonl
      criteria: { effective_bucket: GREEN, enabled: true }
    - id: yellow_pipeline
      path: /data/engineering/_queues/yellow_pipeline.jsonl
      criteria: { effective_bucket: YELLOW, enabled: true }
    - id: red_rejected
      path: /data/engineering/_queues/red_rejected.jsonl
      criteria: { effective_bucket: RED, enabled: true }
```

#### Per-target routing (required for difficulty mapping)
For each target, add a `routing:` block (and optionally an `engineering_routing:` alias if you want backwards compatibility):

```yaml
- id: commonpile_usgpo_filtered
  name: CommonPile: USGPO (filtered)
  enabled: true
  license_profile: quarantine   # or permissive/copyleft; see note below
  download: { strategy: huggingface_datasets, dataset_id: common-pile/usgpo_filtered, splits: [train] }

  routing:
    subject: engineering
    domain: safety_standards
    category: compliance_docs
    level: 5          # optional; difficulty_worker prefers diff-map defaults
    granularity: target
```

**Note about engineering v1 `record_level`:**  
Math v2 only routes pools via `permissive|copyleft|quarantine`. If your engineering target has `license_profile: record_level`, you must do one of:

- **Parity option (recommended):** treat `record_level` as `quarantine` at the target level; keep it YELLOW unless and until record-level licensing is enforced downstream.
- **Extension option:** add a new pool (`record_level`) and update `route_pool()` in `merge_worker.py` and `difficulty_worker.py` to recognize it (this diverges from math v2 behavior).

For strict parity, **map record-level sources to `quarantine`** and rely on screening + future record-level filters.

---

### 3.3 Update `pipeline_driver.py` to match math v2 routing + queues

Engineering v1 `pipeline_driver.py` already:
- snapshots evidence
- resolves SPDX
- emits queues

But it does **not** emit v2 routing fields, and it uses v1 root keys.

**Action: port these pieces from `math_pipeline_v2/pipeline_driver.py`:**

1. `resolve_routing_fields(target)`  
   - Support `target.routing` first  
   - Optionally also read `target.engineering_routing` for compatibility  
   - Default `subject="engineering"`

2. Flatten routing into queue rows:
   - `routing_subject`, `routing_domain`, `routing_category`, `routing_level`, `routing_granularity`
   - Also include full nested `routing` object in the queue row for downstream scripts

3. Ensure queue file paths come from `globals.queues_root` (v2), not v1 `queues:`.

**Output compatibility requirement:** downstream v2 workers expect queue rows like:

```json
{
  "id": "target_id",
  "license_profile": "quarantine",
  "download": {...},
  "routing": {"subject":"engineering","domain":"mechanical","category":"thermo","level":5,"granularity":"target"},
  "routing_subject":"engineering",
  "routing_domain":"mechanical",
  "routing_category":"thermo",
  ...
}
```

---

### 3.4 Replace `download_worker.py` with `acquire_worker.py` (v2 behavior)

Engineering v1 `download_worker.py` writes to `pools/*`.  
Engineering v2 must write to:

- `raw/green/{license_pool}/{target_id}/...`
- `raw/yellow/{license_pool}/{target_id}/...`

**Action:**
- Start from math v2 `acquire_worker.py`
- Re-add missing resolver strategies from engineering v1 if needed (e.g., `figshare`, `github_release`)
- Keep the **same CLI contract** as math v2:
  - `--queue`, `--targets-yaml`, `--execute`, `--limit-targets`, etc.

**Important parity note (HF datasets):**  
Math v2’s acquire step may materialize HF datasets in a format that is not directly mergeable unless you later extract to JSONL. To keep parity, do not “fix” this yet; just ensure raw acquisition matches.

---

### 3.5 Deprecate `yellow_scrubber.py` and use `yellow_screen_worker.py`

Engineering v1’s `yellow_scrubber.py` is chem-legacy and includes PubChem/PMC-specific logic.

Engineering v2 should use math v2’s generic **canonical record screening**:

- input: raw YELLOW payloads
- output: `screened_yellow/{pool}/shards/*.jsonl.gz`
- plus pass/pitch ledgers in `_ledger/` and `_pitches/`

**Action:**
- Copy math v2 `yellow_screen_worker.py`
- Adjust defaults in `targets_engineering.yaml -> globals.screening` (text field candidates, etc.)
- Keep the “pitch anything unclear” posture (i.e., strict filtering)

---

### 3.6 Add `merge_worker.py` (new stage)

Engineering v2 must explicitly merge:

- canonical GREEN records (if any)
- screened YELLOW shards

Output goes to:

- `combined/{pool}/shards/combined_00000.jsonl.gz`
- `_ledger/combined_index.jsonl`

**Action:** copy math v2 `merge_worker.py` and update root defaults to `/data/engineering/...`.

---

### 3.7 Add `difficulty_worker.py` (new stage)

Engineering v2 assigns difficulty *after merge* and writes final shards:

- `final/{pool}/d01..d10/shards/final_00000.jsonl.gz`
- `_ledger/final_index.jsonl`
- `_pitches/final_pitched.jsonl` (optional samples)

**Action:** copy math v2 `difficulty_worker.py` and update default roots.

Then create `difficulties_engineering.yaml` (Section 6) and point to it via:

```yaml
companion_files:
  difficulties_map: ./difficulties_engineering.yaml
```

---

### 3.8 Replace v1 `catalog_builder.py` with v2 `catalog_builder.py`

Engineering v1’s catalog builder expects the v1 pools layout.

**Action:** copy math v2 `catalog_builder.py`, adjust root defaults, and ensure it summarizes:

- raw acquisitions
- screened yellow shards
- combined shards
- final difficulty shards
- ledgers

---

### 3.9 Rewrite `run_pipeline.sh` to match math v2 stages

Engineering v1 wrapper stages: `classify`, `download`, `yellow`, `pmc`, `catalog`.

Engineering v2 must match math v2 stages:

- `classify`
- `acquire_green`
- `acquire_yellow`
- `screen_yellow`
- `merge`
- `difficulty`
- `catalog`
- `all`

Keep `review` optionally as a convenience stage.

---

## 4) Handling PMC / standards / PDFs (optional addons)

To keep strict parity with math v2, PMC and PDF chunking should be **optional** and run **before merge** if used.

Recommended pattern:
- acquire raw → chunk/extract → write canonical JSONL in raw green/yellow (or screened_yellow) → merge consumes JSONL

If you keep `pmc_worker.py`:
- treat it as an **addon** that produces canonical JSONL records with license metadata intact
- store them under `raw/green|yellow/...` or directly into `screened_yellow/` (if it is logically a “screening” step)

---

## 5) Migration from existing v1 outputs (optional)

If you have v1 data under `/data/engineering/pools/*`, you can “lift-and-shift” it:

- `pools/permissive/<target>` → `raw/green/permissive/<target>`
- `pools/copyleft/<target>` → `raw/green/copyleft/<target>`
- `pools/quarantine/<target>` → `raw/yellow/quarantine/<target>`

Then run:
- `screen_yellow` (if you moved quarantine into raw/yellow)
- `merge`
- `difficulty`
- `catalog`

---

# 6) Difficulty mapping (extra detail)

Difficulty mapping is the biggest “new” behavior introduced by v2. The goal is to take a heterogeneous engineering corpus and **assign each record a difficulty level 1–10** so it can be:

- sharded by difficulty for curriculum training
- audited (distribution checks)
- selectively sampled (e.g., avoid d09–d10 for smaller students)

## 6.1 Where difficulty comes from in v2

`difficulty_worker.py` (math v2) assigns difficulty in this order:

1. **Existing record difficulty**  
   If `record["difficulty"]["level"]` already exists → keep it.

2. **Routing-based mapping**  
   If `record["routing"]` exists and a matching mapping exists in `difficulties_*.yaml`, assign `default` level.

3. **Heuristic fallback**  
   If no mapping exists, use a length heuristic (shorter → easier).

In pseudocode:

```python
if rec.difficulty.level:
  use it
elif difficulty_map has subjects[subj].domains[domain].categories[category]:
  use categories[category].level.default
else:
  use heuristic_level(len(rec.text))
```

**Implication:** your *targets and/or record builder* must populate `routing` if you want stable, intentional difficulty levels.

---

## 6.2 What “routing” means in practice

Routing is a lightweight classification attached to each record:

```json
"routing": {
  "subject": "engineering",
  "domain": "mechanical",
  "category": "thermodynamics",
  "level": 5,
  "granularity": "target"
}
```

You can set routing at different granularities:

- `granularity: target`  
  All records from this target share the same routing. (Most common.)
- `granularity: record`  
  A preprocessor attaches routing per record (e.g., classify each arXiv paper).
- `granularity: chunk`  
  A chunker assigns routing per chunk (rare, but useful for textbooks split by chapter topic).

**For parity with math v2:** target-level routing is sufficient and easiest.

---

## 6.3 Difficulty map file structure (engineering)

Create `difficulties_engineering.yaml` with the same schema style as `difficulties_math.yaml`.

Minimum viable file:

```yaml
schema_version: "2.0"
updated_utc: "YYYY-MM-DD hh:mm:ssZ"

globals:
  default_subject: engineering
  default_domain: misc
  default_category: misc
  default_level: 5

rubric:
  scale: { min: 1, max: 10, name: "Difficulty 1–10" }
  levels:
    1: { label: "Foundations", description: "...", signals: ["..."] }
    2: { ... }
    ...
    10: { label: "Research frontier", ... }

subjects:
  engineering:
    name: Engineering
    domains:
      mechanical:
        name: Mechanical Engineering
        categories:
          statics:
            level: { default: 4, min: 3, max: 5 }
          thermodynamics:
            level: { default: 5, min: 4, max: 7 }
```

### Important: min/max are documentation today (parity)
The current `difficulty_worker.py` in math v2 **only uses `default`** (it does not clamp to `min/max` from the YAML).  
Still include `min/max` in the YAML because:
- it documents intended bounds
- it enables future upgrades without re-authoring your taxonomy

If you want to enforce min/max *now*, you would extend `difficulty_worker.py` to read and clamp, but that would diverge from strict parity.

---

## 6.4 Recommended engineering rubric (levels 1–10)

Use this as the “meaning” of each difficulty bucket. Keep it broad and consistent:

1. **Foundations**  
   Units, dimensional analysis basics, simple measurements, safety fundamentals, basic tool use.
2. **Core quantitative literacy**  
   Multi-step arithmetic, basic geometry, basic graphs, everyday engineering calculations.
3. **Pre-engineering / algebraic modeling**  
   Linear equations, ratios/rates, introductory physics word problems, simple circuits.
4. **Intro undergraduate**  
   Statics basics, DC circuits, basic programming for engineers, introductory materials concepts.
5. **Undergrad core**  
   Thermodynamics I, fluids I, strength of materials, basic control systems, basic signal analysis.
6. **Upper undergrad depth**  
   Heat transfer, vibrations, embedded systems, power electronics intro, numerical methods intro.
7. **Advanced undergrad / early grad**  
   FEA intro, CFD intro, robust design, advanced controls, comms systems.
8. **Graduate methods**  
   Advanced CFD/FEA, modern control (state space), estimation, optimization, advanced EM.
9. **Graduate core + research methods**  
   Specialized monographs, heavy derivations, rigorous proofs, experimental methods papers.
10. **Research frontier**  
   Cutting-edge papers, highly specialized jargon, new methods, dense math + novel contributions.

---

## 6.5 Recommended domains and categories (starter map)

Below is a starter taxonomy that works well for most engineering corpora. You should add/rename categories to match your targets.

### Mechanical
- `statics` (4)
- `dynamics` (5)
- `thermodynamics` (5)
- `fluids` (5)
- `heat_transfer` (6)
- `vibrations` (6)
- `machine_design` (6)
- `fea` (7)
- `cfd` (7–8)

### Electrical / Computer
- `circuits_dc_ac` (4)
- `digital_logic` (5)
- `signals_systems` (6)
- `control_systems` (6)
- `power_electronics` (6–7)
- `power_systems` (6–7)
- `communications` (7)
- `embedded_systems` (6)

### Civil
- `structural_analysis` (5–6)
- `geotechnical` (6)
- `water_resources` (6)
- `transportation` (5)

### Materials
- `materials_intro` (4–5)
- `metallurgy` (6)
- `fracture_fatigue` (7)
- `polymers_composites` (6–7)

### Chemical / Process
- `process_fundamentals` (5)
- `separations` (6)
- `reactor_design` (7)
- `process_control` (6–7)

### Aerospace
- `aerodynamics` (6)
- `propulsion` (7)
- `orbital_mechanics` (8)

### Safety / Standards / Compliance
- `safety_engineering` (5)
- `codes_standards` (6)
- `incident_investigation` (6)

### Software for Engineering
- `numerical_methods` (6)
- `simulation_tools` (6–7)
- `robotics` (7)

---

## 6.6 How to map targets to difficulty intentionally

**Rule of thumb:** map based on the **expected “median reader”** for that source.

Examples:

- **OSHA / safety bulletins** → `domain: safety_standards`, `category: safety_engineering`, `default: 4–5`
- **Undergrad thermo textbook** → `mechanical / thermodynamics / 5–6`
- **arXiv full papers** → `aerospace / propulsion / 8–10` depending on subfield
- **Wikipedia engineering articles** → `misc / reference / 4–6`

### Handling mixed sources (CommonPile filtered corpora)
For “mixed” sources (e.g., `commonpile_arxiv_papers_filtered`), do:

- **target-level routing** to “research/general” (e.g., default 8)
- later, upgrade to **record-level routing** if you add a classifier (e.g., use arXiv categories to map to domains)

---

## 6.7 How to audit difficulty mapping quality

After running `difficulty_worker.py`, inspect:

- `/_ledger/difficulty_summary.json` (counts + shards)
- `/_ledger/final_index.jsonl` (per-record routing + assigned difficulty + output shard)

Recommended checks:

1. **Distribution sanity**: do you have data across d01–d10?  
2. **Routing coverage**: what % used `method="routing"` vs `method="length"`?  
3. **Outliers**: very short records assigned high difficulty (likely routing mismatch)  
4. **Pool separation**: copyleft sources never leak into permissive outputs

If routing coverage is low, it means you need to:
- add `routing:` to more targets
- or add record-level routing in extractors

---

# 7) Smoke-test checklist (parity)

Run a 3-target test (one GREEN, one YELLOW, one RED):

1. `classify` creates `_queues/green_download.jsonl`, `_queues/yellow_pipeline.jsonl`, `_queues/red_rejected.jsonl`
2. `acquire_green --execute` downloads into `raw/green/...`
3. `acquire_yellow --execute` downloads into `raw/yellow/...`
4. `screen_yellow --execute` writes `screened_yellow/.../shards/*.jsonl.gz`
5. `merge --execute` writes `combined/.../shards/*.jsonl.gz`
6. `difficulty --execute` writes `final/.../d01..d10/shards/*.jsonl.gz`
7. `catalog` writes `_catalogs/global_catalog.json`

---

# 8) Recommended “parity first, improvements later” policy

To match math v2 behavior, implement only:

- v2 layout
- v2 stage order
- routing emission in classify
- difficulty mapping via YAML + fallback heuristic

Then add engineering-specific extractors as separate workers without changing the outer contract.

---

## Appendix A — Minimal changes summary (checklist)

- [ ] Create `engineering_pipeline_v2/` folder
- [ ] Copy v2 workers from `math_pipeline_v2/`
- [ ] Convert targets to schema v0.8 and add `globals.*_root`
- [ ] Add `routing:` to targets (subject/domain/category)
- [ ] Port routing support into `pipeline_driver.py` queue row output
- [ ] Use `acquire_worker.py` (not `download_worker.py`)
- [ ] Use `yellow_screen_worker.py` (not `yellow_scrubber.py`)
- [ ] Add `merge_worker.py`
- [ ] Add `difficulty_worker.py` + `difficulties_engineering.yaml`
- [ ] Replace catalog builder with v2 version
- [ ] Rewrite `run_pipeline.sh` to v2 stages

