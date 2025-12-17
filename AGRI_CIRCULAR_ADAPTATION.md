# Agriculture + Waste Streams + Circular Bioeconomy — pipeline adaptation plan (v0.1)

This document describes how to adapt the **chem_pipeline_v1** package into a domain pack focused on:

- **Agriculture & land systems** (production, soils, land cover, inputs/outputs)
- **Waste streams & infrastructure** (solid waste, hazardous waste, wastewater, permits/compliance)
- **Circular bioeconomy** (biomass → materials/energy, LCA, policy/standards where legally clear)

It pairs with: `targets_agri_circular.yaml`.

---

## 1) Minimal mechanical changes (so you can run immediately)

1. **Clone the chem pipeline folder** as a new package (e.g., `agri_circular_pipeline_v1/`).
2. Replace storage roots:
   - `/data/chem/...` → `/data/agri_circular/...`
3. Use the new targets file:
   - `targets_agri_circular.yaml`

### Running the pipeline (same workflow as chem)

1) Build queues (classification / evidence):
```bash
python pipeline_driver.py --targets targets_agri_circular.yaml --min-license-confidence 0.8
```

2) Download GREEN queue (dry run → execute):
```bash
python download_worker.py --queue /data/agri_circular/_queues/green.queue.jsonl --targets-yaml targets_agri_circular.yaml
python download_worker.py --queue /data/agri_circular/_queues/green.queue.jsonl --targets-yaml targets_agri_circular.yaml --execute --workers 4
```

3) Process YELLOW queue (optional, after you implement scrubbers/filters for this domain):
```bash
python yellow_scrubber.py --queue /data/agri_circular/_queues/yellow.queue.jsonl --targets-yaml targets_agri_circular.yaml --execute
```

---

## 2) Domain-specific adaptations you should add (recommended)

### A. New “builders” (turn raw dumps into training-ready JSONL)
Chem has a strong start (download + license triage), but ag/waste/circular needs **more structured normalization**.

Add lightweight domain builders analogous to `pmc_worker.py`:

- `echo_worker.py`
  - Reads ECHO ZIPs, extracts CSVs, emits:
    - `jsonl` rows with a consistent schema (facility_id, program, status, dates, geography, pollutants, quantities)
    - **aggregation views** (county/year totals) to reduce re-identification risk
  - Adds joins/crosswalks (FRS registry ↔ program tables).

- `agrovoc_worker.py`
  - Parses AGROVOC RDF/NT/NQ dumps and emits:
    - canonical term table (id, prefLabel, altLabel, lang, broader/narrower)
    - synonym expansions to help unify crop names, residues, feedstocks, etc.

- `geo_worker.py` (optional but powerful)
  - Converts GeoTIFF / shapefile sources into:
    - tiled patches + metadata OR vector summaries
  - Includes **spatial downsampling** and **resolution caps** by default (privacy + safety).

### B. Field schemas for this domain (update `field_schemas.yaml`)
Add schema families like:

- `echo_facility_record.v1`
- `echo_permit_record.v1`
- `waste_stream_event.v1`
- `agrovoc_term.v1`
- `lca_process_stub.v1` (even if you only store metadata at first)
- `geospatial_tile_manifest.v1`

### C. Safety / ethics gates (extend denylist + rule checks)
Add automated checks for:
- **Personal addresses** in contexts where they could map to individuals (rare, but guard anyway).
- **Sensitive operational details** (e.g., site-level security notes).
- **Biosecurity red flags** (e.g., detailed pathogen cultivation methods) if you ingest broader “bioeconomy” literature.

---

## 3) Dataset candidate inventory (GREEN / YELLOW / RED)

These are *candidates*; the YAML file includes a runnable starter subset.

### GREEN (generally safe / permissive / public-domain)
**Waste streams & compliance / infrastructure**
- EPA ECHO bulk downloads (FRS, RCRA pipeline, NPDES, DMR, SDWA, CSO/SSO tables, enforcement cases) — directory index includes:
  - `frs_downloads.zip`, `pipeline_rcra_downloads.zip`, `rcra_downloads.zip`
  - `npdes_downloads.zip`, `npdes_biosolids_downloads.zip`, `npdes_dmrs_fy20xx.zip`
  - `SDWA_latest_downloads.zip`, sewer overflow tables, etc.
- EPA program crosswalks (DMR/TRI point source category crosswalks)

**US government agriculture & land**
- USDA NASS (Census of Agriculture tables; public data)
- USDA AMS Market News (commodity prices; confirm redistribution terms per product)
- USDA FoodData Central bulk downloads (ingredients/nutrients; generally public data)
- USDA NRCS soils (SSURGO/STATSGO2; public data)
- USGS water/land datasets (NWIS, land cover products; public domain generally)
- NOAA climate normals + daily station observations (when using open access products)

**Policy and standards (open legal text)**
- U.S. federal laws/regulations (eCFR, Federal Register) about agriculture, waste, environmental compliance
- EU legal texts (EUR-Lex) on circular economy (check terms; usually permissive for legal documents)

### YELLOW (needs screening / per-record license / copyleft / ToS review)
- **AGROVOC** (open, but attribution requirements; keep attribution metadata attached)
- **CommonPile subsets** (heterogeneous; require per-component license filtering + streaming)
- **GBIF occurrence records** (mix of CC0/CC-BY/CC-BY-NC → you must exclude BY-NC for commercial use)
- **Open Food Facts** (ODbL share-alike; isolate in copyleft pool)
- **FAOSTAT** (often usable but confirm terms/attribution and keep citations)
- **Eurostat** (usually reuse-permitted with attribution; confirm the current license)
- **World Bank / IMF / OECD datasets** (often licensed, but verify each dataset’s terms)
- **Academic datasets on Dataverse/Zenodo** (license varies by record; must verify)
- **Some Earth observation portals** (may require accounts / specific use terms even when data is free)

### RED (do not ingest for training)
- Proprietary LCA databases: **ecoinvent**, **GaBi**, **Agri-footprint**, paid ELCD packs, etc.
- Vendor/commercial farm-management data (equipment telemetry, private agronomy datasets)
- Paywalled journals/books, standards PDFs (ISO, ASTM) unless explicitly open-licensed
- “Scraped” copies of copyrighted datasets from unofficial mirrors

---

## 4) What’s included in `targets_agri_circular.yaml` (starter set)

Enabled by default:
- EPA ECHO: FRS, RCRA pipeline, NPDES, DMR, SDWA, sewer overflow tables, enforcement cases
- ECHO “exporter” bundle and several crosswalks
- AGROVOC dumps (kept permissive, but you should preserve attribution in derived artifacts)

Disabled by default (documented candidates):
- Open Food Facts (ODbL copyleft)
- CommonPile (requires streaming + filtering before enabling)
- GBIF / FAOSTAT (require custom workers + license handling)
- Proprietary LCA (RED)

---

## 5) Suggested near-term TODOs (v0.2 → v0.3)

1. Implement `echo_worker.py` to emit **training-ready JSONL** + join keys.
2. Add a **unit harmonization layer** (kg/ton, wet vs dry mass, COD/BOD, nutrient units).
3. Add `agrovoc_worker.py` and wire it into normalization so crop/feedstock synonyms unify text + tables.
4. Add **geo support** (optional): tile + downsample land cover/soils, store manifests, keep raw geotiffs separate.
5. Add “license-carrying metadata” so every derived record preserves:
   - source_id, license, attribution text, retrieval timestamp, checksums.

---

## Appendix: Why ECHO is a strong anchor source

ECHO provides large, well-documented, machine-readable compliance datasets that touch:
- hazardous waste (RCRA),
- water permits (NPDES/DMR),
- drinking water (SDWA),
- enforcement cases,
- and crosswalks to categorize industrial activity.

That makes it unusually valuable for “waste streams + circular systems” modeling — while remaining largely public-domain U.S. government data.

