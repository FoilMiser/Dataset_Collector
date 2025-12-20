# Metrology + Technical Reports Dataset Pipeline Adaptation (v0.1)

**Goal:** adapt the existing chemistry-focused collection pipeline into a **metrology / technical-report** collector that produces legally-cleared, provenance-rich training text with the “how NIST/NASA write” style: structured sections, requirements language, uncertainty/traceability vocabulary, tables/figures, and references.

This plan assumes the current repo layout from `chem_pipeline_v1.zip` (notably: `pipeline_driver.py`, `download_worker.py`, `yellow_scrubber.py`, `license_map.yaml`, `denylist.yaml`), and adds new harvesters + extractors needed for large technical-report corpora.

---

## 1) Data model changes (minimal, additive)

### 1.1 New record metadata fields (recommended)
Extend `field_schemas.yaml` with a `tech_report_chunk_v1` schema:

- `source.target_id`, `source.publisher`, `source.series` (e.g., NISTIR, NASA TP, NOAA TM, USGS Techniques & Methods)
- `source.doc_id` (report number / DOI / accession / NTRS id)
- `source.url_pdf`, `source.url_landing`, `source.retrieved_at_utc`
- `rights.license_spdx`, `rights.evidence_urls[]`, `rights.record_level_notes`
- `screening.restriction_flags[]` (e.g., `export_control`, `no_redistribution`, `all_rights_reserved`, `third_party_material`)
- `parse.pdf_tool`, `parse.page_count`, `parse.table_count`
- `text.section_path` (e.g., `["3 Methods", "3.2 Calibration"]`)

### 1.2 Output format
Keep the pipeline’s “pool” concept, but standardize **training-ready output** as:

- `jsonl.gz` chunks (one chunk per line) for streaming training
- optional `parquet` for analytics + slicing

---

## 2) Pipeline flow (metrology specialization)

### 2.1 Recommended buckets and gates
Use the existing GREEN/YELLOW/RED model:

- **GREEN** → auto-emit to `pools/permissive`
- **YELLOW** → quarantine + require signoff (or batch review)
- **RED** → reject

For metrology, add (or expand) these gates:

1. **`distribution_statement_scan`**: detect DoD/NASA “Distribution A/B/C…” and similar.  
2. **`export_control_scan`**: detect ITAR/EAR language, “export controlled,” “technical data,” etc.  
3. **`third_party_notice_scan`**: detect “copyrighted material used with permission,” “permission required,” etc.  
4. **`extract_text_chunks`**: robust PDF/HTML extraction *with table + caption retention*.  
5. **`near_duplicate_detection`**: avoid multiple copies of the same report (mirrors, revisions, reposts).

> In the provided prototype, `pipeline_driver.py` already routes targets based on `license_profile` and runs a restriction phrase scan. The extra scans above should be added in the same spirit: cheap, deterministic, and evidence-preserving.

---

## 3) New harvesters/resolvers you’ll need

`download_worker.py` currently supports: **http/ftp/git/zenodo/dataverse/huggingface_datasets**.

Technical-report corpora require **index harvesting + paging** (APIs, HTML listings, metadata feeds). Implement these strategies:

### 3.1 `ntrs_openapi` (NASA NTRS OpenAPI)
- Query NTRS OpenAPI for `distribution=PUBLIC` and `disseminated=DOCUMENT_AND_METADATA`
- Store:
  - the JSON query you sent
  - the JSON response pages (as evidence snapshots)
  - each record’s landing page + PDF download URL
- **Record-level rights:** treat as YELLOW by default because NASA pages can link to non-NASA journal articles or third-party material.

Reference: NASA provides OpenAPI documentation and usage guidance. citeturn0search4turn0search8

### 3.2 `usgs_pubs_warehouse` (USGS Publications Warehouse REST services)
- Use the documented web services to retrieve records in JSON and follow links to full text PDFs.
- Store query parameters and service responses as evidence snapshots.

Reference: USGS web service documentation. citeturn1search0turn1search10

### 3.3 `noaa_ir_json` (NOAA Institutional Repository JSON API)
- Use NOAA IR JSON API (and/or series pages like NOAA Tech Memo listings)
- Prefer the IR API because it gives structured metadata with stable identifiers.

Reference: NOAA IR JSON API repo and NOAA TM listings. citeturn2search0turn2search16

### 3.4 `faa_ac_crawl` (FAA Advisory Circulars crawler)
- Crawl the FAA AC listing endpoint (or DRS) and extract PDF links + metadata.
- Store the listing HTML and PDFs; treat as YELLOW until you confirm rights language is consistently public domain.

Reference: FAA AC listing pages. citeturn1search2turn1search5turn1search9

---

## 4) Extraction: “technical report aware” chunker

### 4.1 PDF extraction (high priority)
Implement `tech_report_worker.py` (or `pdf_extract_worker.py`) that:

- Extracts text per page (and optionally per block) using a deterministic library
- Preserves:
  - headings (numbered sections)
  - figure captions
  - table captions
  - inline math symbols where possible
- Emits structured chunks that align to section boundaries, not arbitrary token windows.

### 4.2 HTML extraction
NIST NVL PubS has both PDF and HTML views for some publications. Support:

- `readability-lxml` style extraction for clean content
- keep headings + lists (requirements often appear as numbered bullets)

### 4.3 Table handling
Metrology standards and calibration guides frequently use tables for:
- symbol definitions
- uncertainty budgets
- calibration intervals
- instrument specs

So:
- **do not drop tables by default**
- store “table as text” fallback if full table extraction fails (cell grid is optional)

---

## 5) Legal/ethics screening rules (metrology-specific)

### 5.1 Default conservative stance for technical-report aggregators
Even when the publisher is a government agency, repositories can contain:
- third-party journal articles
- contractor-authored works with restrictions
- export-controlled/limited-distribution documents

So for NASA/USGS/NOAA aggregators:
- **ingest metadata at scale**
- treat full-text as **YELLOW until record-level checks pass**

### 5.2 Restriction phrase dictionary
Expand `denylist.yaml` (or a new `restriction_phrases.yaml`) with:
- “ITAR”, “EAR”, “export controlled”, “technical data”
- “distribution statement”, “Distribution A”, “Distribution C”
- “may not be reproduced”, “no redistribution”
- “copyrighted material”, “permission required”
- “all rights reserved”

---

## 6) Dataset candidate list (GREEN / YELLOW / RED)

### GREEN (strongly recommended; clear rights or explicit open license)
- **BIPM SI Brochure** (explicit CC BY 4.0). citeturn2search1turn2search5
- **NIST publications that are clearly US public domain under NIST’s open license page** (e.g., many NIST SP/TN/NISTIR). citeturn3view0
- **NIST Technical Series Publication Metadata** from data.nist.gov (metadata feed). citeturn3view0
- **NTSB aviation accident data + manuals** (useful for “coding manuals / definitions” rigor; not pure metrology). citeturn1search1turn1search19

### YELLOW (high value but needs record-level review / mixed rights likely)
- **NASA NTRS OpenAPI fulltext** (mixed rights & distribution statements; start with PUBLIC only). citeturn0search4turn0search8turn0search12
- **USGS Publications Warehouse fulltext** (government, but still do record-level checks). citeturn1search0turn1search22
- **NOAA IR / NOAA Technical Memoranda** (government repository; check per-record notices). citeturn2search0turn2search12turn2search16
- **FAA Advisory Circulars** (likely public domain, but still confirm per-document notices). citeturn1search5turn1search9
- **OIML documents & recommendations** (global standards body; rights statements vary; treat as review-required). citeturn2search10turn2search14
- **BIPM/CGPM/CCM meeting reports and technical docs** beyond the SI brochure (licenses vary across document families).

### RED (avoid; paywalled/restrictive or unclear to use at scale)
- ISO/IEC standards text, IEC standards text, IEEE standards text (typically paywalled / restrictive).
- Most commercial calibration manuals / vendor PDFs without explicit licenses.
- Any archive that bundles “free previews” of copyrighted standards.

---

## 7) How the new `targets_metrology.yaml` maps to this plan

I drafted a starter `targets_metrology.yaml` with:

- a **GREEN seed set** (BIPM SI brochure, NIST SP 330, NIST RDaF)
- **NIST Tech Series metadata feed** (GREEN) as the bridge to fulltext crawling
- **disabled placeholders** for high-value harvesters you’ll implement next:
  - `ntrs_openapi`, `usgs_pubs_warehouse`, `noaa_ir_json`, `faa_ac_crawl`
- **Common Pile** entries kept disabled by default (use only when you have aggressive filtering).

---

## 8) Implementation checklist (fast path)

1. **Add API harvester framework**
   - One new worker (e.g., `api_harvest_worker.py`) that:
     - pages through results
     - snapshots request/response evidence
     - emits per-record download jobs into `green_download` or `yellow_pipeline`
2. **Implement `ntrs_openapi`**
   - Start with `distribution=PUBLIC` + `disseminated=DOCUMENT_AND_METADATA`
3. **Implement `extract_text_chunks` for PDFs**
   - deterministic extraction, section-aware chunking
4. **Turn on dedup**
   - exact hash + MinHash for near-duplicate PDFs
5. **Batch review workflow**
   - sample 200 docs per source, confirm rights patterns, then promote to GREEN rules

---

## 9) Files included
- `targets_metrology.yaml` — metrology/technical-report target definitions compatible with the current pipeline schema
- This document — adaptation plan and candidate list
