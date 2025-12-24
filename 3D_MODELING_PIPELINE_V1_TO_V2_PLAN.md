# 3D Modeling Pipeline v1 → v2 (Math-pipeline-v2 parity plan)

Goal: create `3d_modeling_pipeline_v2` that follows the **same stage flow, directory layout, ledger/pitch behavior, and difficulty-sharded outputs** as `math_pipeline_v2`, while keeping 3D-specific acquisition + mesh normalization.

This plan assumes the starting code is in `3d_modeling_pipeline_v1.zip` and the reference behavior is `math_pipeline_v2.zip`.

---

## 0) “Same behavior as math_pipeline_v2” checklist

Your v2 3D pipeline should match math v2 on the following invariants:

1. **Stage order**
   1) `pipeline_driver.py` (classify + snapshot evidence + queues)  
   2) `acquire_worker.py` (download to `raw/{green|yellow}/...`)  
   3) `yellow_screen_worker.py` (strict screen of raw yellow → `screened_yellow/...` + pitches/ledger)  
   4) `merge_worker.py` (GREEN + screened YELLOW → `combined/...` with dedupe)  
   5) `difficulty_worker.py` (final screen + difficulty assignment → `final/.../d01..d10/...`)  
   6) `catalog_builder.py` (catalog + attribution bundles)

2. **Directory layout**
```
/data/3d/
  raw/
    green/{license_pool}/{target_id}/...
    yellow/{license_pool}/{target_id}/...
  screened_yellow/{license_pool}/shards/*.jsonl.gz
  combined/{license_pool}/shards/*.jsonl.gz
  final/{license_pool}/d01..d10/shards/*.jsonl.gz   (or nested by subject/domain/category)
  _ledger/*.jsonl
  _pitches/*.jsonl
  _manifests/{target_id}/...
  _queues/*.jsonl
  _catalogs/...
  _logs/...
```

3. **Strict pitch semantics**
- Any record that fails screening is **not silently dropped**: it is written to `_pitches/*.jsonl` (reasoned) and counted.
- All accepted records get a ledger entry (`_ledger/*_index.jsonl` / summaries).

4. **Difficulty sharding**
- Final output is partitioned into `d01..d10` under each `license_pool`.
- Difficulty is assigned primarily via `record.routing` and a `difficulties_3d.yaml` mapping; heuristics are allowed as a fallback.

---

## 1) Repo/file migration plan

### A) Start the v2 folder by copying the math v2 skeleton
Create `3d_modeling_pipeline_v2/` by cloning the **math v2** structure, then swap domain specifics:

Keep (and rename only where cosmetic):
- `pipeline_driver.py` (upgrade v1 → v2 parity fields)
- `acquire_worker.py` (replace v1 `download_worker.py`)
- `yellow_screen_worker.py` (new, replaces v1 “yellow_scrubber” stage)
- `merge_worker.py`
- `difficulty_worker.py`
- `catalog_builder.py`
- `review_queue.py`
- `license_map.yaml`, `denylist.yaml`

Bring forward from 3D v1:
- `mesh_worker.py` (used inside screening to compute geometry metadata + thumbnails/pointclouds)
- any 3D-specific resolvers already implemented in v1 `download_worker.py` (port handlers into v2 `acquire_worker.py`).

Deprecate in v2 (keep only if you still need them as internal helpers):
- `download_worker.py` (superseded by `acquire_worker.py`)
- `yellow_scrubber.py` (superseded by `yellow_screen_worker.py` for “strict pitch” + canonical shards)

### B) Update `run_pipeline.sh` to math-v2 stages
Replace v1 stages `{classify, review, download, yellow, catalog}` with math-v2 compatible stages:

- `classify`
- `acquire_green`
- `acquire_yellow`
- `screen_yellow`
- `merge`
- `difficulty`
- `catalog`
- `all`

Make `--execute` semantics identical (dry-run by default).

---

## 2) Targets file migration (`targets.yaml` → `targets_3d.yaml`)

### A) Replace v1 `globals.storage_root` + `globals.pools.*` with math-v2 roots
In v2, use **math’s root keys**:

```yaml
globals:
  raw_root: /data/3d/raw
  screened_yellow_root: /data/3d/screened_yellow
  combined_root: /data/3d/combined
  final_root: /data/3d/final
  ledger_root: /data/3d/_ledger
  pitches_root: /data/3d/_pitches
  manifests_root: /data/3d/_manifests
  queues_root: /data/3d/_queues
  catalogs_root: /data/3d/_catalogs
  logs_root: /data/3d/_logs
  require_yellow_signoff: true  # keep 3D’s safety stance
  sharding:
    max_records_per_shard: 50000
    compression: gzip
  screening:
    # For 3D, do NOT rely only on text length
    min_chars: 0
    max_chars: 12000
    text_field_candidates: [text, description, caption, title]
    record_license_field_candidates: [license, license_spdx, spdx, license_id]
    require_record_license: true  # important for community repositories
    allow_spdx: [CC0-1.0, CC-BY-4.0, MIT, Apache-2.0, BSD-3-Clause, CC-BY-SA-4.0]
    deny_phrases: [noai, "no tdm", "no machine learning", "noncommercial", "no derivatives", "nd", "nc"]
```

### B) Add **generic routing** fields to every target
Math v2 expects targets to optionally specify:
```yaml
routing:
  subject: 3d
  domain: cad_parametric
  category: part_design
  level: 4          # optional, can omit if using difficulties_3d.yaml defaults
  granularity: target
  confidence: 0.8
  reason: target_metadata
```

Also allow a 3D-specific alias (like math’s `math_routing`) if you want:
```yaml
three_d_routing: { domain: ..., category: ..., level: ..., granularity: target }
```
…but **always** emit `routing_*` fields in the queue rows (see §3A).

### C) Output pool / license pool rules (match math v2)
Keep the same three pools:
- `permissive`
- `copyleft`
- `quarantine`

In `pipeline_driver.py`, set `output_pool` (and therefore default `license_pool`) using:
1) explicit `target.output.pool` if provided  
2) else map from `license_profile` (permissive/copyleft/quarantine)  
3) else default to `quarantine`

---

## 3) Worker-by-worker adaptation details

### A) `pipeline_driver.py` (3D v1 → v2 parity)
Bring the 3D driver up to math v2’s emitted-row shape:

**Add fields to each queue row** (mirroring math v2):
- `queue_bucket` (same as `effective_bucket`)
- `output_pool` (resolved pool)
- a domain-specific routing blob (optional): `three_d_routing: {...}`
- generic routing flattening fields:
  - `routing_subject`, `routing_domain`, `routing_category`, `routing_level`, `routing_granularity`, `routing_confidence`, `routing_reason`

**Why:** later stages can propagate routing into records, enabling difficulty assignment by mapping instead of guessing from text length.

### B) `acquire_worker.py` (new in 3D v2)
Adopt math v2’s behavior:
- Reads a queue JSONL (`green_download.jsonl` or `yellow_pipeline.jsonl`)
- Downloads into `raw/{bucket}/{license_pool}/{target_id}/...`
- Writes `acquire_done.json` markers per target under `_manifests/{target_id}/...`

**3D-specific additions**
1. Port v1 3D resolvers (Figshare/GitHub releases/S3 public/etc.) as strategy handlers.
2. After downloading an archive of meshes, optionally emit an **index JSONL** under the target dir:
   - `raw/.../{target_id}/records.index.jsonl`
   - one row per asset with `asset_path`, `license_spdx` (if known), `source`, and inherited `routing`.

This keeps the *math-v2 assumption* that later stages primarily consume JSONL rows, while still allowing local asset files to sit alongside the index.

### C) `yellow_screen_worker.py` (3D-specific canonicalization + strict pitch)
Match math v2 semantics but extend the canonical record format for 3D.

**Inputs**
- `raw/yellow/{pool}/{target_id}/**/*.jsonl*` (index rows or already-structured rows)

**Outputs**
- `screened_yellow/{pool}/shards/yellow_shard_*.jsonl.gz`
- `_ledger/yellow_passed.jsonl` and `_ledger/yellow_pitched.jsonl`
- `_pitches/yellow_pitched.jsonl` (optional detailed pitch samples)
- `_manifests/{target_id}/yellow_screen_done.json`

**3D screening rules (recommended defaults)**
- Require per-record license if `globals.screening.require_record_license: true`
- Denylist scan:
  - license restriction phrases (NoAI/NoTDM/NC/ND)
  - “character rip / trademark” phrases if you want conservative filtering
  - “weapon” content indicators **only for filtering/labeling** (don’t do anything unsafe—just pitch)
- Mesh processing:
  - If a record references `asset_path` and it’s a mesh/cad format, run `mesh_worker` to compute:
    - face/vertex counts, bbox, watertightness, file hash, optional thumbnails/point clouds
  - Attach `mesh` metadata into the canonical record.

**Canonical record minimum fields (keep merge/difficulty compatible)**
- `record_id`
- `text` (can be a generated description if mesh-only)
- `hash.content_sha256` (for 3D: prefer sha256 of *normalized mesh bytes*; fallback to sha256(text))
- `source.license_profile` + `source.license_spdx` (+ original URL / dataset)
- `routing` (inherited from row or target)
- optional: `assets[]` with local relative paths and their hashes

### D) `merge_worker.py` (mostly reusable)
Reuse math v2 `merge_worker.py` unchanged if you ensure:
- GREEN and screened YELLOW both produce JSONL rows with `hash.content_sha256` (or `content_sha256`)

For 3D, a good dedupe key is:
- sha256 of **normalized mesh bytes** (preferred)
- else sha256 of original asset bytes
- else sha256 of text

### E) `difficulty_worker.py` (3D mapping + heuristics)
Keep math v2 behavior, but add one 3D-specific capability:

1. Primary: use `record.routing` + `difficulties_3d.yaml`
2. Secondary: if routing missing or too coarse, estimate within bounds using mesh/tutorial complexity (see §4)

Outputs remain math-v2 compatible:
- `final/{pool}/d01..d10/.../*.jsonl.gz`
- `_ledger/final_index.jsonl`
- `_pitches/final_pitched.jsonl`

### F) `catalog_builder.py`
Adopt math v2’s v2 catalog builder so it understands:
- the `final/` layout
- ledger indices
- attribution bundle generation

Extend the schema list to include:
- `mesh_record_v1.0.0` (already in v1 `field_schemas.yaml`)
- any tutorial/text record schema you standardize on

---

## 4) Difficulty mapping for 3D (1–10) — detailed plan

### A) Create `difficulties_3d.yaml` (same schema as `difficulties_math.yaml`)
Use:
- `subjects: { 3d: ... }`
- `folder_layout: final/{license_pool}/d{level:02d}/{subject}/{domain}/{category}`

#### Suggested domains/categories (defaults)
Below are **default levels**. Use `min/max` to permit later heuristic refinement.

**Domain: `modeling_mesh`**
- `primitives_transforms`: default 1 (min 1, max 2)
- `extrude_inset_loopcut`: default 2 (min 1, max 3)
- `modifiers_basics` (mirror/array/subdiv): default 3 (min 2, max 4)
- `booleans_hard_surface`: default 4 (min 3, max 6)
- `uv_unwrap_basics`: default 4 (min 3, max 5)
- `retopo_basics`: default 5 (min 4, max 6)
- `sculpting_intermediate`: default 6 (min 5, max 7)
- `geometry_nodes_procedural`: default 7 (min 6, max 9)
- `character_modeling_rigging`: default 7 (min 6, max 9)
- `topology_shading_advanced`: default 8 (min 7, max 9)

**Domain: `cad_parametric`**
- `sketch_constraints`: default 3 (min 2, max 4)
- `part_design`: default 4 (min 3, max 5)
- `assemblies_joints`: default 6 (min 5, max 7)
- `tolerances_dfm`: default 6 (min 5, max 8)
- `surfacing_lofts`: default 7 (min 6, max 9)
- `cam_toolpaths`: default 8 (min 7, max 9)
- `generative_design`: default 9 (min 8, max 10)

**Domain: `three_d_printing`**
- `printer_setup_slicer_basics`: default 2 (min 1, max 3)
- `supports_orientation`: default 4 (min 3, max 6)
- `calibration_troubleshooting`: default 5 (min 4, max 7)
- `resin_printing`: default 6 (min 5, max 7)
- `multi_material`: default 7 (min 6, max 9)
- `industrial_workflows`: default 9 (min 8, max 10)

**Domain: `rendering_materials`**
- `lighting_basics`: default 3 (min 2, max 4)
- `pbr_materials`: default 5 (min 4, max 6)
- `shader_nodes_advanced`: default 7 (min 6, max 9)
- `optimization_baking`: default 7 (min 6, max 8)

**Domain: `scanning_reconstruction`**
- `photogrammetry_basics`: default 5 (min 4, max 6)
- `mesh_cleanup_repair`: default 6 (min 5, max 8)
- `nerf_gaussian_splatting`: default 8 (min 7, max 10)

**Domain: `scripting_pipeline`**
- `blender_python_basics`: default 5 (min 4, max 6)
- `addon_development`: default 7 (min 6, max 8)
- `pipeline_automation_ci`: default 8 (min 7, max 9)

### B) Heuristic refinement (optional but recommended)
To avoid “mesh-only records collapse to low difficulty”, add a 3D heuristic:

**For mesh/CAD assets**
Compute a complexity score from metadata (from `mesh_worker`):
- `face_count` bands:
  - < 2k → +1
  - 2k–20k → +2
  - 20k–200k → +4
  - 200k–2M → +6
  - > 2M → +8
- `parts_count` (or number of objects): +0..+2
- `has_textures/materials/uv`: +1
- `format`:
  - STEP/IGES/parametric → +2 (CAD leaning)
  - GLB/GLTF with PBR → +1
- `repair_flags` (non-manifold, inverted normals): +1 (if your dataset includes repair tasks)

Map score to level:
- 0–2 → level 2
- 3–4 → level 3
- 5–6 → level 4
- 7–8 → level 5
- 9–10 → level 6
- 11–12 → level 7
- 13–14 → level 8
- 15–16 → level 9
- 17+ → level 10

Then clamp within the routing category’s `(min,max)`.

**For tutorial/text records**
Use keyword + structure signals:
- basic UI/tool usage words → ≤3
- topology/UV/modifiers → 4–6
- rigging/procedural/scripting/simulation → 7–10
Optionally incorporate:
- number of steps / headings
- code blocks (Blender Python) → bump to ≥5

Implementation hook:
- extend `assign_difficulty()` to:
  1) honor explicit `record.difficulty.level`
  2) else choose default via routing
  3) else refine via heuristics (if mesh metadata exists)
  4) else fallback to length-based

---

## 5) Validation: acceptance tests (quick, deterministic)

1. **Dry-run parity**
   - Run `run_pipeline.sh --stage classify` and verify queue files + manifests match expected paths.
2. **Acquire layout**
   - `--stage acquire_green --execute` writes only under `raw/green/...`
   - `--stage acquire_yellow --execute` writes only under `raw/yellow/...`
3. **Strict pitch**
   - `--stage screen_yellow --execute` produces:
     - `screened_yellow/{pool}/shards/*.jsonl.gz`
     - `_ledger/yellow_passed.jsonl`, `_ledger/yellow_pitched.jsonl`
4. **Merge/dedupe**
   - `--stage merge --execute` produces `combined/{pool}/shards/*.jsonl.gz`
   - `_ledger/combined_index.jsonl` exists and has unique `content_sha256`
5. **Difficulty output**
   - `--stage difficulty --execute` produces `final/{pool}/d01..d10/...`
   - `_ledger/final_index.jsonl` references those shards
6. **Catalog**
   - `--stage catalog --execute` emits a global catalog referencing `final/` and includes attribution bundles.

---

## 6) Minimal deliverables to ship `3d_modeling_pipeline_v2`

1. `targets_3d.yaml` migrated to v2 roots + routing fields
2. `difficulties_3d.yaml` with the mapping above
3. Updated scripts:
   - `pipeline_driver.py` (v2 parity queue fields)
   - `acquire_worker.py` (v2 layout + 3D strategy handlers)
   - `yellow_screen_worker.py` (3D canonicalization + strict pitch)
   - `merge_worker.py` (reuse)
   - `difficulty_worker.py` (routing + optional heuristics)
   - `catalog_builder.py` (v2)
   - `run_pipeline.sh` (v2 stages)
4. Keep `mesh_worker.py` and ensure it can be called from screen/difficulty stages.

