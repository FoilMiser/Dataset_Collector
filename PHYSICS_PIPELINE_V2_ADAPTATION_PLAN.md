# Physics pipeline v1 → v2 adaptation plan (parity with `math_pipeline_v2`)

Updated UTC: 2025-12-23 00:00:00Z

This plan adapts `/mnt/data/physics_pipeline_v1.zip` into **`physics_pipeline_v2/`** so it follows the same stage ordering, directory layout, ledgers, and “strict pitch” behavior as `/mnt/data/math_pipeline_v2.zip`.

The intent is that **running `physics_pipeline_v2/run_pipeline.sh` produces the same *kinds* of artifacts as math v2**:

- `_queues/green_download.jsonl`, `_queues/yellow_pipeline.jsonl`, `_queues/red_rejected.jsonl`
- `raw/{green|yellow}/{license_pool}/{target_id}/...` (acquired payloads)
- `screened_yellow/{license_pool}/shards/*.jsonl.gz` + pass/pitch ledgers
- `combined/{license_pool}/shards/*.jsonl.gz` + combined ledger
- `final/{license_pool}/d01..d10/shards/*.jsonl.gz` + final ledger
- `catalogs/*` (catalog + attribution bundle)

> Not legal advice. Keep your own compliance checks.

---

## 0) Snapshot of what you have today

### `physics_pipeline_v1/` contains
- `pipeline_driver.py` (classification + queue emission)
- `download_worker.py` (acquire/download)
- `yellow_scrubber.py` (legacy “stage 2” transformations; not v2 screen stage)
- `catalog_builder.py` (v0.9 layout assumptions)
- `run_pipeline.sh` stages: `classify`, `download`, `yellow`, `pmc`, `catalog`

### `math_pipeline_v2/` adds (and these are the missing pieces)
- `acquire_worker.py` (replaces `download_worker.py`)
- `yellow_screen_worker.py` (strict pass/pitch → screened_yellow shards)
- `merge_worker.py` (dedup + combined shards)
- `difficulty_worker.py` (final screen + difficulty assignment → d01..d10)
- `catalog_builder.py` updated to v2 layout
- `run_pipeline.sh` stages: `classify`, `acquire_green`, `acquire_yellow`, `screen_yellow`, `merge`, `difficulty`, `catalog`

---

## 1) File-level migration map

| Physics v1 file | Physics v2 equivalent | Action |
|---|---|---|
| `download_worker.py` | `acquire_worker.py` | **Replace** (keep old as `legacy_download_worker.py` if desired) |
| `yellow_scrubber.py` | `yellow_screen_worker.py` | **Replace for v2 flow** (keep scrubber only for special transform utilities) |
| *(none)* | `merge_worker.py` | **Add** |
| *(none)* | `difficulty_worker.py` | **Add** |
| `catalog_builder.py` (v0.9) | `catalog_builder.py` (v2.0) | **Replace** |
| `run_pipeline.sh` (v1 stages) | `run_pipeline.sh` (v2 stages) | **Replace** |
| `targets.yaml` | `targets_physics.yaml` | **Rename + update globals/companion files/routing** |
| *(none)* | `difficulties_physics.yaml` | **Add** (difficulty map) |

---

## 2) Directory layout parity (v2)

Update `targets_physics.yaml.globals` to match math v2:

```yaml
globals:
  raw_root: /data/physics/raw
  screened_yellow_root: /data/physics/screened_yellow
  combined_root: /data/physics/combined
  final_root: /data/physics/final
  ledger_root: /data/physics/_ledger
  pitches_root: /data/physics/_pitches
  manifests_root: /data/physics/_manifests
  queues_root: /data/physics/_queues
```

**Why:** every v2 worker reads these roots from the targets file. This is the key behavioral parity point.

---

## 3) Script adaptation steps (recommended order)

### Step A — Create `physics_pipeline_v2/` by copying math v2 as the base
1. Copy the entire `math_pipeline_v2/` folder as a new folder `physics_pipeline_v2/`.
2. Rename math-specific docs/filenames:
   - `targets_math.yaml` → `targets_physics.yaml`
   - `difficulties_math.yaml` → `difficulties_physics.yaml`
   - README title + wrapper comments → Physics

This “copy math v2, then specialize” approach is safest because it preserves the exact stage semantics you want to match.

### Step B — Port/merge physics target inventory into v2 targets schema
Start from the current `physics_pipeline_v1/targets.yaml` and move targets into the v2-style file.

**Required v2 additions:**
- `companion_files.difficulties_map: ./difficulties_physics.yaml`
- `globals.*_root` keys (see Section 2)
- Keep `outputs` queue definitions but ensure they point into `globals.queues_root`.

**Required per-target additions for difficulty:**
Add a `routing:` block per target (or per record if your raw records carry routing).

Example target-level routing:
```yaml
- id: arxiv_metadata_physics
  name: arXiv metadata (physics categories)
  routing:
    subject: physics
    domain: quantum
    category: arxiv_abstracts
    level: 7
    granularity: record
    confidence: 0.6
    reason: "Use arXiv category mapping during screening"
```

### Step C — Update `pipeline_driver.py` to emit the v2 queue row fields
Physics v1 driver is close, but math v2 adds/standardizes these queue fields:
- `queue_bucket` (mirror of `effective_bucket` for downstream routing)
- `output_pool` (from `target.output.pool` if present; fallback to `license_profile`)
- Flattened routing fields `routing_subject/domain/category/level/granularity/confidence/reason`
- Keep backwards compatibility: accept `physics_routing:` and normalize into `routing:`.

**Implementation approach**
- Copy `pipeline_driver.py` from math v2 into physics v2 unchanged *except*:
  - add `physics_routing` alias support in `normalize_routing()`
  - default subject to `physics` when routing missing

### Step D — Replace acquisition + screening stages with v2 workers
- Use `acquire_worker.py`, `yellow_screen_worker.py`, `merge_worker.py`, `difficulty_worker.py` from math v2.
- Update only *domain naming strings* and any default roots in help text.

**Important behavioral constraint:**  
`yellow_screen_worker.py` assumes raw inputs are already JSONL-ish with `text` (or can be converted cheaply). For physics targets that are PDFs or binary data, you have two options:

1) **Keep them YELLOW but add lightweight extractors** inside `yellow_screen_worker.py` keyed off `queue_row.build.extractor` (recommended), producing canonical `text` rows.  
2) Keep them **out of the text corpus** (RED or “non_text” targets) and only include their metadata/abstracts.

If you want parity with math v2 *and* physics PDF inclusion, add extractor plugins but keep the stage order and ledgers identical.

### Step E — Replace catalog builder with v2 layout-aware version
Use math v2 `catalog_builder.py` and update any “Math” wording. No schema changes needed; it discovers stage directories via `targets.*_root`.

### Step F — Replace wrapper script
Replace physics v1 `run_pipeline.sh` with math v2 wrapper structure and adapt names.

Stages:
- `classify`
- `acquire_green`
- `acquire_yellow`
- `screen_yellow`
- `merge`
- `difficulty`
- `catalog`

---

## 4) Difficulty mapping design for Physics (1–10)

### 4.1 Rubric (human meaning for each level)
Encode this under `rubric.levels` in `difficulties_physics.yaml`:

1. **Pre-physics / units**: unit conversion, dimensional analysis, single-step arithmetic in context.
2. **Early algebra physics**: plug-and-chug with one equation; constant velocity/force; Ohm’s law basics.
3. **High-school physics**: Newton’s laws w/ components, energy/momentum basics, simple circuits, wave basics.
4. **AP/IB (algebra-based)**: rotation/torque, gravitation, electrostatics, optics, thermodynamics basics.
5. **Intro calc-based**: derivatives/integrals for motion, work/energy via integrals, basic differential equations.
6. **Intermediate undergrad**: E&M with Gauss/Ampere/Faraday, vector calculus intro, thermo with partial derivatives.
7. **Upper-division undergrad**: Lagrangian/Hamiltonian mechanics, QM operators, PDE/ODE methods, EM differential form.
8. **Early graduate**: perturbation theory, scattering, ensembles, GR fundamentals, advanced EM/radiation.
9. **Graduate core**: QFT, GR at depth, many-body, advanced condensed matter/particle, multi-stage derivations.
10. **Research/specialist**: niche subfields, cutting-edge methods, highly technical derivations.

### 4.2 Domain/category map (routing keys)
Use the same structure as math v2: `subjects.physics.domains.<domain>.categories.<category>.level`.

Recommended starter domains:
- `units_metrology`
- `mechanics`
- `electromagnetism`
- `waves_optics`
- `thermo_statmech`
- `quantum`
- `relativity_cosmology`
- `astro`
- `condensed_matter`
- `computational_experimental`

Example category mapping (illustrative):
```yaml
subjects:
  physics:
    name: Physics
    domains:
      mechanics:
        name: Mechanics
        categories:
          kinematics_1d:
            level: { default: 2, min: 1, max: 3 }
          newton_laws:
            level: { default: 3, min: 2, max: 4 }
          rotation_torque:
            level: { default: 4, min: 3, max: 5 }
          oscillations_shm:
            level: { default: 4, min: 3, max: 5 }
          lagrangian_hamiltonian:
            level: { default: 7, min: 6, max: 8 }
      quantum:
        name: Quantum
        categories:
          quantum_intro:
            level: { default: 6, min: 5, max: 7 }
          schrodinger_operators:
            level: { default: 7, min: 6, max: 8 }
          perturbation_scattering:
            level: { default: 8, min: 7, max: 9 }
          qft:
            level: { default: 9, min: 8, max: 10 }
```

### 4.3 How difficulty gets assigned in v2 (and how to make it work)
`difficulty_worker.py` assigns difficulty in this order:

1) Existing `record.difficulty.level` (if present)  
2) `record.routing` → lookup in `difficulties_physics.yaml`  
3) Fallback heuristic by text length (coarse)

So you get best results if canonical records include a meaningful `routing` dict.

Practical ways to provide routing:
- **Target-level routing** (`targets_physics.yaml`: `routing:`). Good for single-topic corpora.
- **Record-level routing**: inject during screening based on record metadata (recommended for mixed sources).
- **Post-hoc routing pass (optional)**: a future `routing_worker.py` applying keyword rules.

---

## 5) Suggested record-level routing rules for common physics sources

### arXiv category mapping (example)
Map `source.arxiv_primary_category` prefixes:

- `physics.ed-ph` → `computational_experimental` or `mechanics` (level ~5)
- `hep-th`, `hep-ph`, `gr-qc` → `quantum`/`relativity_cosmology` (level 8–10)
- `cond-mat.*` → `condensed_matter` (level 7–9)
- `astro-ph.*` → `astro` (level 7–9)

Implement as a small function in `yellow_screen_worker.py`:
- read `raw.get("source", {}).get("arxiv_primary_category")`
- set/override `routing` accordingly
- keep conservative confidence (0.5–0.7) and set `routing_reason`

---

## 6) Validation checklist (parity tests)

Run these with `--execute` **off** first (dry-run):

1. `./run_pipeline.sh --targets targets_physics.yaml --stage classify`
   - queues emitted; each row contains `queue_bucket`, `license_profile`, routing fields.

2. `... --stage acquire_green` and `... --stage acquire_yellow`
   - files land in `raw/green/...` and `raw/yellow/...`
   - each acquired target has `acquire_done.json` marker in `_manifests/{target_id}`

3. `... --stage screen_yellow`
   - `screened_yellow/{pool}/shards/yellow_shard_00000.jsonl.gz`
   - `_ledger/yellow_passed.jsonl` and `_ledger/yellow_pitched.jsonl`

4. `... --stage merge`
   - `combined/{pool}/shards/combined_00000.jsonl.gz`
   - `_ledger/combined_index.jsonl`

5. `... --stage difficulty`
   - `final/{pool}/d01..d10/shards/*.jsonl.gz`
   - `_ledger/final_index.jsonl`

6. `... --stage catalog`
   - catalog summary references the v2 roots correctly.

---

## 7) Optional physics-specific enhancements (still preserves v2 behavior)

These do **not** change stage order; they only improve routing + extraction quality.

- Add extractor plugins in `yellow_screen_worker.py`:
  - `pdf_to_text` (for open PDFs where you have rights)
  - `jats_to_text` (for JATS XML)
  - `html_to_text` (for permissive docs/webpages)
- Add routing enrichment in screening:
  - arXiv category mapping
  - INSPIRE-HEP field mapping
  - NASA/ADS keywords → astro domain mapping
- Extend `difficulties_physics.yaml.rule_sets` with keyword rules for future use.

---

## 8) Deliverables to commit

Minimum set for `physics_pipeline_v2/`:

- `README.md` (Physics v2 overview)
- `run_pipeline.sh` (v2 wrapper)
- `targets_physics.yaml`
- `difficulties_physics.yaml`
- v2 scripts: `pipeline_driver.py`, `acquire_worker.py`, `yellow_screen_worker.py`, `merge_worker.py`, `difficulty_worker.py`, `catalog_builder.py`
- companion files: `license_map.yaml`, `denylist.yaml`, `field_schemas.yaml`, `requirements.txt`

---

## 9) “Done” definition (behavioral parity)

You’re at parity with math v2 when:
- stage names and wrapper behavior match
- workers read roots from the targets file
- strict pitch behavior produces ledgers
- merged + difficulty-sharded outputs exist under `final/`
- queue rows include v2 fields (`queue_bucket`, routing fields)
