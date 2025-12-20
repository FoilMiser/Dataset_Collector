# Adapting the chemistry dataset pipeline to an ethical 3D modeling / 3D printing pipeline

This guide explains how to take **chem_pipeline_v1** and retarget it from chemistry text/structures into a pipeline that collects **legally-cleared 3D assets + 3D-printing knowledge** (meshes/CAD + manuals/tutorials) while keeping a strong, auditable licensing posture.

Deliverables in this package:
- `targets_3d_modeling.yaml` — a starter target manifest (CC0/PD first, record-level where needed)
- This doc — what to change in code, schemas, gates, and review workflow

---

## 1) What changes when you move from chemistry → 3D?

### Data types and “units of training”
Chem pipeline mostly ingests **text documents** and optionally normalizes chemical structures. 3D needs to ingest **binary geometry assets** (STL/OBJ/GLB/GLTF/STEP, textures) plus **text instructions**.

A practical 3D training record often looks like:
- **Mesh/CAD asset** (primary)
- **Metadata** (license, attribution, tags, scale/unit, category)
- **Derived representations** (thumbnails, point clouds, SDFs, voxel grids)
- **Optional printing artifacts** (gcode + slicer settings) for “how to print this” supervision

### Failure modes are different
3D corpora have unique problems you must actively screen:
- **Hidden / missing license** or mixed licenses inside a single collection
- **Trademark/character rips** (logos, brand shapes, famous characters)
- **Weapons** and regulated objects in printable form
- **Mesh quality issues** (non-manifold, inverted normals, self-intersections, absurd scale)
- **NoAI / NoTDM / anti-ML restrictions** in platform ToS even if individual files look open

---

## 2) Targets manifest: how `targets.yaml` should change

You already have the same schema hooks in the chem manifest:
- `license_profile` + `license_evidence`
- `download.strategy`
- `gates_override`
- optional `record_level_policy`

### Recommended buckets
**GREEN / permissive pool (default training):**
- CC0 / Public Domain (ideal)
- CC-BY (OK, but attribution bundle must be generated and carried into deployment)

**YELLOW / quarantine pool (requires record-level filter + signoff):**
- Community uploads (even if mostly CC)
- Any dataset where license is per-record and includes NC/ND/unknown

**COPyleft pool (segregated):**
- CC-BY-SA docs/manuals
- GPL/AGPL codebases used for “how-to” information

The starter file `targets_3d_modeling.yaml` already implements this idea:
- Smithsonian Open Access (CC0)
- Blender Education (CC0)
- Thingi10K (record-level) → derived safe slice (CC0/CC-BY only)
- Optional Objaverse (record-level)
- Blender Manual + CuraEngine (copyleft segregated)

---

## 3) Code changes in the pipeline

### 3.1 Add new download strategies (minimal changes)
Your current `download_worker.py` supports: `http`, `ftp`, `git`, `zenodo`, `dataverse`, `huggingface_datasets`, `figshare`, `github_release`.

For 3D, you will quickly need two more:

#### A) `s3_public` (public buckets, no-sign-request)
Use cases:
- Smithsonian Open Access bulk assets (and other public heritage collections)

Implementation sketch:
- Use `aws s3api list-objects-v2 --no-sign-request` (or boto3 with unsigned config)
- Snapshot the listing + etags to `/_catalogs/<target_id>/listing.json`
- Stream-download only matching globs to staging

#### B) `web_crawl` (curated seed pages)
Use cases:
- Datasets whose download endpoints shift over time

Rules:
- Must snapshot ToS first (`snapshot_terms` gate)
- Must obey robots + rate limit
- Only crawl allowlisted domains and file types

In code terms: add a new handler alongside `download_http/download_git/...`.

### 3.2 Add a `mesh_worker.py` (analog to your chemistry extractors)
Create a worker that runs after download/extract and produces a normalized, schema-validated record.

Suggested Python libs:
- `trimesh` (robust mesh IO + basic repair)
- `meshio` (format conversion)
- `open3d` (point cloud + geometry utilities)
- (optional) `pyrender` / `moderngl` for thumbnails

Worker responsibilities:
1. **Detect** file type (stl/obj/gltf/glb/ply/step) and parse
2. **Validate** geometry (bounds sane, vertices finite)
3. **Sanitize** (strip metadata; re-export triangles; unify axis conventions)
4. **Normalize scale** (store unit guess; optional scaling rules)
5. **Extract metadata** (vertex/face counts, bbox, surface area, volume when closed)
6. **Emit derived** artifacts (thumbnails, point clouds, simplified mesh)
7. **Write JSONL record** that references outputs + includes license/attribution

### 3.3 Make record-level filtering a first-class path
For sources like Thingi10K / Objaverse:
- Ingest *metadata first*
- Map per-record license strings → SPDX via `license_map.yaml`
- Keep only allowlisted licenses
- Copy/convert only those assets into the permissive pool

This can be implemented as:
- A new stage in `yellow_scrubber.py` (rename to something like `record_filter.py`)
- Or a separate “build” step that generates derived targets (your manifest already supports this pattern)

### 3.4 Geometry-aware dedupe (optional but high value)
Your existing dedupe (MinHash/LSH) is text-oriented.

For meshes:
- Start with **exact file hash** (already enabled in the 3D targets manifest)
- Then add a geometry fingerprint stage later:
  - Normalize mesh
  - Compute a compact descriptor (light-field / spherical harmonics / multi-view image hashes)
  - Cluster by approximate similarity

---

## 4) Schema updates (`field_schemas.yaml`)

The chemistry schema is molecule-centric. Add new schemas like:

### `mesh_record_v1.0.0`
Minimum useful fields:
- `source_id`, `record_id`, `source_url`
- `license_spdx`, `license_url`, `attribution_text`, `creator`
- `formats` (original + normalized)
- `geometry`: `vertex_count`, `face_count`, `bbox`, `surface_area`, `volume_closed`
- `units`: `unit_guess`, `scale_applied`
- `hashes`: `sha256_original`, `sha256_normalized`
- `tags`: category labels, printing intent

### `printing_profile_v1.0.0` (optional)
If you generate gcode:
- slicer name/version
- printer profile id
- layer height, nozzle, temps, infill
- output paths + hashes

Keep schemas tight and auditable: everything should be reproducible from raw inputs.

---

## 5) Denylist updates (`denylist.yaml`)

Add two kinds of rules:

### A) Legal restriction phrases
Extend your restriction scan phrases with:
- "NoAI", "No AI", "No-ML", "No machine learning"
- "no text and data mining", "no TDM"
- "training of neural networks prohibited"

### B) Content categories you want to exclude
For a general-purpose model (and to reduce legal/policy risk), add filters for:
- Weapons (gun, firearm, suppressor, receiver, switchblade, etc.)
- Famous brands/characters and obvious IP rips
- Items that look like regulated keys or bypass tools (depends on your scope)

For 3D assets, you’ll often need **metadata-based filtering** (tags/categories) plus **lightweight classification** (e.g., thumbnail classifier) to catch what text doesn’t.

---

## 6) Recommended folder layout and outputs

Reuse your existing roots, just swap `/data/chem` → `/data/3d`.

Recommended outputs:
- `/pools/permissive/...` — normalized meshes + JSONL records
- `/pools/copyleft/...` — docs/code (isolated)
- `/pools/quarantine/...` — raw record-level corpora pending filtering/signoff
- `/_manifests/<run_id>/training_manifest.jsonl` — exact included files + hashes
- `/_catalogs/<target_id>/...` — ToS snapshots, license pages, dataset cards
- `/_logs/<run_id>.log` — full audit trail

---

## 7) What to avoid (high-risk / “RED” sources)

These are common 3D sources that are *tempting* but usually not clean for model training:

- **Direct scraping of community platforms** (even if assets are CC) when the platform ToS restricts automated use or ML training.
- **ShapeNet / ModelNet / similar** if they have NonCommercial or other restrictive terms (many do).
- **Onshape-derived datasets** (ABC/SketchGraphs/others) unless you have explicit ML rights and a clear redistribution/training grant.
- **Sketchfab / marketplaces** unless you have an explicit dataset license granting ML training rights, and you can enforce NoAI flags.
- Anything with **NC / ND / unknown license**, or where license is missing for a large fraction of items.

---

## 8) Suggested incremental build plan

1. **Get GREEN running end-to-end**
   - Blender Education (git) + Blender Manual (git, copyleft pool)
   - Implement `mesh_worker.py` and `extract_text_chunks`

2. **Add Thingi10K as record-level**
   - Ingest metadata + assets
   - Filter to CC0/CC-BY
   - Produce `thingi10k_safe_cc0_ccby`

3. **Add optional large corpora**
   - Objaverse only after you’re confident the record-level filter is solid

4. **Add printing supervision**
   - Generate gcode for a subset of safe meshes with a fixed slicer + profiles
   - Store slicer settings as part of the record schema

---

## 9) Quick checklist for “legally cleared enough to train”

A dataset is a good candidate when you can answer **YES** to all:
- Do you have a **license grant** that covers ML training (CC0/PD/CC-BY, or explicit TDM permission)?
- Can you **prove** the license (snapshot evidence + URL + retrieval time)?
- If record-level: can you **enforce** an allowlist and exclude restricted items?
- Can you **carry attribution** where required (CC-BY) and keep an auditable manifest?
- Have you filtered obvious **IP rips and weapons**?

If any answer is “maybe”, put it in **quarantine** and require signoff.
