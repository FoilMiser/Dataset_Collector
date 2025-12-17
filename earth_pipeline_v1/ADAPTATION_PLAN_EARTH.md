# ADAPTATION_PLAN_EARTH.md
Environmental / Earth Systems specialization — adapting the chemistry ethical-data pipeline

Generated: 2025-12-17

## Goals
Build a legally-cleared, ethically-scrubbed training corpus specializing in Environmental / Earth Systems science:
- high-signal *text* (methods, dataset docs, reports, glossaries, standards)
- high-value *structured* context (schemas, variable dictionaries, indicator tables)
- optional metadata-only hooks to geospatial/EO assets (STAC/NetCDF) without pulling huge imagery volumes

This adaptation keeps the same “green/yellow/red” governance you used for chemistry, but extends the ingestion stack to handle Earth-data formats.

---

## Why Earth Systems needs a slightly different ingestion strategy
Environmental science content is frequently:
- **PDF-heavy** (assessment reports, technical manuals)
- **metadata-centric** (NetCDF/GRIB/HDF products where the *docs* and variable dictionaries are more valuable for an LLM than raw arrays)
- **catalog-driven** (STAC catalogs, THREDDS/OPeNDAP endpoints)

Therefore, the pipeline should prioritize:
1) narrative text + dataset documentation
2) machine-readable metadata → “dataset cards”
3) tables + schemas
4) only later: sampled rasters/tiles (optional)

---

## Licensing + ethics baseline (high-level)
Key operational rules reflected in `licenses_earth.yaml`:

### Green (fast path)
- US federal public-domain sources (with exceptions for third-party media)
- CC0 / CC BY sources
- UK Open Government Licence (OGL v3) and Canada OGL where used on environmental portals

Gov sites explicitly note that some pages may contain third-party copyrighted material; this is why the pipeline should still capture and audit rights strings even for green targets.

### Yellow (screened path)
- Copernicus / ECMWF / CDS content where per-dataset licensing and attribution varies
- GBIF (record-level CC0/CC BY allowed; exclude CC BY-NC)
- PANGAEA and other repositories with per-record license variation
- ODbL sources (OpenStreetMap): allow docs; treat data extracts as a special case

### Red (blocked)
- IPCC assessment report text/figures (restricted reuse; “no-alteration” figure policy, etc.)
- paywalled journals / textbooks / proprietary satellite imagery providers
- any CC BY-NC or CC BY-ND corpora

Note: Some IPCC *data* resources may be licensed separately (e.g., CC BY), but do not ingest the assessment report text/graphics themselves unless licensing clearly allows.

---

## Workers to add (or extend) vs chemistry pipeline

### 1) `stac_worker` (new)
Purpose: ingest *metadata only* from STAC catalogs for EO data.
Output: a normalized “dataset card” per collection + item:
- title, description, providers, license, assets list, bands/eo:bands, time/space extent

### 2) `thredds_opendap_worker` (new)
Purpose: ingest *global attributes + variable dictionaries* from NetCDF-heavy catalogs.
Output:
- dataset card with: variables (name, long_name, units, standard_name), dimensions, coverage

### 3) `api_tabular_worker` (extend)
Purpose: pull and normalize indicator tables (CSV/JSON endpoints).
Output:
- dataset card + table schema + sample rows (optional small sample)

### 4) `web_pdf_html_worker` (reuse + strengthen)
Earth Systems uses PDFs constantly.
Add:
- stronger PDF structure handling (headings, figure captions, tables)
- “third-party media” string detection for exclusions

---

## Metadata schema changes (recommended)
Add these fields to your canonical record schema:

- `geo_extent`: bbox or region name
- `temporal_extent`: start/end, resolution, update cadence
- `variables`: [{name, description, units, standard_name, valid_range}]
- `platform_sensor`: e.g., MODIS/VIIRS/Sentinel-2
- `processing_level`: L1/L2/L3, reanalysis, modeled
- `license_id`, `license_url`, `attribution_text`
- `preferred_citation`
- `sensitivity_flags`: e.g., “endangered_species_location”, “indigenous_data_sovereignty”

---

## Chunking + output layout
Mirror the chem pipeline output layout, but split into 3 logical corpora:

1) `text/`:
- reports, manuals, ATBDs, dataset documentation, glossaries

2) `structured/`:
- dataset cards, variable dictionaries, indicator schema summaries

3) `raw/`:
- original PDFs and any downloaded ancillary files
- for NetCDF/EO, store only metadata JSON unless you explicitly opt in to arrays/tiles

---

## Practical run order (recommended)
1) **Green text** first (USGS/NOAA/NASA/EPA + OGL/CC BY portals)
2) Generate **dataset cards** (NetCDF/STAC/indicator tables)
3) Bring in **yellow** sources after the license gate + scrubbers are proven
4) Only then consider “data sampling” for EO (tiles) if you need multimodal grounding

---

## What you get in this drop
- `targets_earth.yaml` — initial target list (green/yellow/red) with worker hints
- `licenses_earth.yaml` — license policy and heuristics + ethics scrub rules
- this `ADAPTATION_PLAN_EARTH.md`

---

## Next recommended follow-up (optional)
If you want, the next incremental upgrade is:
- `earth_keyword_taxonomy.yaml` (controlled vocabulary for filtering + tagging)
- `earth_units_normalizer.py` (normalize units like K/°C, ppm/ppb, m/s, kg m-2 s-1)
- `netcdf_card_generator.py` (extract variable dictionaries via xarray/netCDF4)
- `stac_card_generator.py` (extract eo:bands and summaries robustly)
