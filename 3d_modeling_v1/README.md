# 3D Modeling / 3D Printing Corpus Pipeline (v1.0 prototype)

A safety-first pipeline for assembling an **ethically sourced 3D modeling + 3D printing corpus** from open datasets and documentation, with emphasis on **license compliance, provenance tracking, and mesh hygiene**. This package is adapted from `chem_pipeline_v1` and re-scoped for geometry assets (meshes/CAD), printing artifacts, and instructional text.

> Not legal advice. This tool helps you *track* licenses and restrictions; you are responsible for compliance.

---

## What This Pipeline Does

```
+-----------------------+     +---------------------+     +-------------------------+
|  targets.yaml         |---->| pipeline_driver.py  |---->|  Queue JSONLs           |
|  (3D inventory)       |     | (classify + gate)   |     |  GREEN / YELLOW / RED   |
+-----------------------+     +---------+-----------+     +------------+------------+
                                    |                              |
                                    |                              v
                                    |                   +------------------------+
                                    |                   | red_rejected.jsonl     |
                                    |                   | (do not process)       |
                                    |                   +------------------------+
                                    |
                                    v
                          +------------------+
                          | review_queue.py  |   (manual signoff for YELLOW)
                          +--------+---------+
                                   |
                                   v
         +----------------------------------------------------------------------+
         |                         DATA ACQUISITION + TRANSFORMS                |
         +----------------------------------------------------------------------+
                  |                                |
                  v                                v
         +-------------------+              +----------------------+
         | download_worker.py|              | mesh / text workers  |
         | (GREEN downloads) |              | (YELLOW transforms)  |
         +---------+---------+              +----------+-----------+
                   |                                   |
                   v                                   v
         +--------------------+              +------------------------+
         | pools/permissive   |              | pools/permissive       |
         | pools/copyleft     |              | (post-filtered/derived)|
         | pools/quarantine   |              +------------------------+
         +--------------------+
                   |
                   v
         +--------------------+
         | catalog_builder.py |  -> global_catalog.json + training manifests
         +--------------------+
```

### Buckets
- **GREEN**: clear, compatible licensing + no disallowed restrictions -> can be downloaded as-is
- **YELLOW**: ambiguous licensing, record-level licenses, or sources that need mesh validation -> requires **manual signoff** and/or a safe transform (record-level filter, mesh sanitizer, etc.)
- **RED**: explicitly incompatible licenses/restrictions/denylist match -> rejected

---

## 3D-Specific Adaptations

1. **Data types**: primary artifacts are mesh/CAD assets (STL/OBJ/GLB/GLTF/STEP/PLY), textures, and printing metadata; text workers still handle manuals/tutorials.
2. **Download strategies**: add `s3_public` for unsigned public buckets and `web_crawl` for curated seed URLs (obey robots + snapshot ToS).
3. **Mesh worker**: new stage (`mesh_worker.py`, see `gates_catalog` and `field_schemas.yaml`) to parse/validate/sanitize meshes, emit metadata (vertex/face counts, bbox, units), and generate derived artifacts (thumbnails/point clouds/simplified meshes + gcode stubs).
4. **Record-level filtering**: enforce per-record license allowlists for community uploads (e.g., Thingi10K/Objaverse) before copying into permissive pools.
5. **Risk filters**: denylist and restriction scans expanded for NoAI/NoTDM terms plus weapon/trademark filters for printable objects.
6. **Schema updates**: added `mesh_record_v1.0.0` (geometry metadata) and optional `printing_profile_v1.0.0`.

---

## Quick Start

### Install
```bash
pip install -r requirements.txt
```

### Dry-run (recommended first)
Creates manifests + queues, but does not download or transform:
```bash
./run_pipeline.sh --targets targets.yaml
```

### Review pending YELLOW items
```bash
./run_pipeline.sh --targets targets.yaml --stage review
# or:
python3 review_queue.py --queue /data/3d/_queues/yellow_pipeline.jsonl list
```

### Execute downloads or transforms
```bash
# Download GREEN targets (CC0/CC-BY sources, etc.)
./run_pipeline.sh --targets targets.yaml --stage download --execute

# Process YELLOW targets after manual approval (record-level filters, mesh validation)
./run_pipeline.sh --targets targets.yaml --stage yellow --execute --limit-files 5

# Run mesh normalization directly (e.g., after download stage)
python mesh_worker.py --targets targets.yaml --target-id thingi10k_raw --input-dir /data/3d/pools/quarantine/thingi10k_raw --output-dir /data/3d/pools/permissive/thingi10k_safe_cc0_ccby --emit-parquet
```

### Build catalog
```bash
./run_pipeline.sh --targets targets.yaml --stage catalog
```

---

## File Overview

- `targets.yaml` — 3D modeling target manifest (companion: `license_map.yaml`, `denylist.yaml`, `field_schemas.yaml`)
- `pipeline_driver.py` — classifies targets into GREEN/YELLOW/RED and snapshots license evidence
- `download_worker.py` — executes download strategies (HTTP/Git/HF datasets/Figshare/GitHub releases; includes `s3_public` + `web_crawl`)
- `review_queue.py` — manual signoff helper for YELLOW targets (extended signoff schema v0.2)
- `catalog_builder.py` — aggregates manifests/catalogs and emits training manifests
- `yellow_scrubber.py` — record-level filtering entrypoint; delegates mesh work to `mesh_worker.py`
- `mesh_worker.py` — mesh validation/sanitization + metadata/thumbnails/point clouds + optional parquet + gcode stubs
- `requirements.txt` — dependencies (mesh stack optional but recommended)
- `denylist.yaml` — high-risk sources/phrases (NoAI/NoTDM + weapon/trademark filters)
- `field_schemas.yaml` — structured record definitions (mesh + printing profiles)

---

## Safety + Compliance Notes

- **Always snapshot Terms/ToS + license pages** before downloading (`snapshot_terms` gate).
- Treat community uploads as **record-level** and enforce license allowlists before copying into permissive pools.
- Keep **attribution bundles** for CC-BY sources and segregate **copyleft** (CC-BY-SA/GPL/AGPL) into dedicated pools.
- Add geometry-aware deduping + weapon/trademark classifiers before large-scale training exports.
- When enabling `web_crawl`, restrict to allowlisted domains/file globs and respect robots + rate limits.

---

## Versioning

- Pipeline scripts: v1.0 (aligned with chem prototype)
- Targets schema: v0.8 (compatible with `chem_pipeline_v1` loader)
- Field schemas: v0.7 with `mesh_record_v1.0.0` additions
- Denylist: v0.2 with provenance fields
