# Safety engineering & incident analysis: adapting the chemistry dataset pipeline

This document describes how to adapt the existing **chem_pipeline_v1** dataset collection pipeline to specialize in **safety engineering + incident analysis** corpora (investigation reports, incident databases, safety alerts, regulations, and lessons learned).

## Goals

1. **Collect legally-cleared text** for training and evaluation:
   - Investigation reports (root cause, contributing factors, recommendations)
   - Structured incident/event datasets (rows for occurrences)
   - Safety bulletins/alerts/recall notices
   - Regulatory and compliance language relevant to safety systems

2. **Preserve provenance and license evidence** per file/record.

3. **Protect privacy and security**:
   - Remove personal identifiers (names, phone/email, exact addresses)
   - Coarsen sensitive location fields (e.g., city/state only; rounded coordinates)
   - Suppress unusually identifying narratives in small communities / rare incidents (optional)

## What changes from the chemistry pipeline

### 1) New target list focused on safety/incident sources

Use: `safety_incident_targets.yaml`

Key source families covered:
- **US federal**: PHMSA, MSHA, FRA/DOT Open Data, NOAA OR&R, (optionally NRC, NHTSA FARS)
- **UK GOV.UK** (OGL v3.0): AAIB / RAIB / MAIB reports (treat as *record-level* due to third-party exceptions)
- **Common Pile**: high-value filtered subsets (`regulations_filtered`, `usgpo_filtered`) to augment safety & compliance language
- **Yellow sources**: ASRS (requires query-slicing export), EU eMARS (auth), France ARIA (export limits/auth)

### 2) New schemas (field_schemas)

Use: `field_schemas_safety_incident.yaml`

Two core record types:
- `incident_report_chunk_v1.0.0`: chunked narrative text from reports
- `incident_event_row_v1.0.0`: structured event/occurrence row with normalized fields

### 3) Add safety-specific “gates” (quality + compliance)

The current pipeline already supports a simple “gates list” in targets. To make safety data truly usable, plan to implement:

- **pii_scan_and_redact / pii_scan_and_redact_strict**
  - Remove: personal names in narratives, phone/email, exact street addresses
  - Replace with typed placeholders: `[PERSON]`, `[EMAIL]`, `[PHONE]`, `[ADDRESS]`
  - Track redaction types in `meta.entities_redacted`

- **sensitive_location_suppress** (optional)
  - For rare events, tiny towns, or incidents that can re-identify individuals, suppress or coarsen location beyond a threshold.

- **strip_third_party_media**
  - Remove embedded images/logos/maps from PDFs where copyright is unclear.
  - Keep text; preserve a pointer to original for audit.

- **pdf_extract / html_crawl**
  - PDF text extraction and chunking.
  - Respect robots/terms for HTML crawling; use rate limiting.

- **license_metadata_validate**
  - For record-level sources, require each record carry license metadata (or be excluded).
  - Maintain a denylist of known-problem domains or licenses.

### 4) Incident-specific chunking & normalization

Safety reports benefit from structure-aware parsing:

- Split by:
  - Executive summary
  - Sequence of events / timeline
  - Findings
  - Analysis / causes
  - Contributing factors (human / organizational / technical)
  - Recommendations / corrective actions
  - Appendices (often tables; treat separately)

- Normalize to common safety frameworks:
  - “Event → barriers failed → hazards → consequences → recommendations”
  - Optional tags: HFACS, STAMP/STPA concepts, bow-tie elements, Swiss-cheese barrier language

## Suggested folder layout (pools)

Use output pools like:
- `raw/safety/<agency>/<dataset>` for downloaded raw artifacts
- `clean/safety/<agency>/<dataset>` for extracted + redacted text/rows
- `manifests/safety/...` for per-target run manifests

## Minimal viable workflow (how to run it)

1. Copy the two files into your pipeline repo:
   - `safety_incident_targets.yaml`
   - `field_schemas_safety_incident.yaml`

2. Run the downloader in **dry-run** first to validate wiring:
   - Verify each enabled target has an output pool and strategy.

3. Execute downloads with limits:
   - Use small `max_bytes_per_target` to sanity-check.

4. Add (or stub) workers:
   - `pdf_chunker_worker.py` (PDF → chunks)
   - `tabular_ingest_worker.py` (XLSX/CSV → rows)
   - `pii_redactor_worker.py` (strict de-id)
   - `incident_normalizer_worker.py` (domain mapping + severity normalization)

5. Merge into your “green/yellow scrubber” phase:
   - Yellow sources require stricter controls and possibly human spot checks.

## Quality checks for safety corpora

- Dedup:
  - Hash exact files + near-duplicate chunk-level hashing (MinHash) to remove re-posted reports.
- Field validation:
  - Dates parse, non-negative counts, plausible ranges.
- PII audit:
  - Sample 200 random chunks from each source; ensure redaction catches names/emails/phones.
- License audit:
  - Maintain `license_evidence` URLs and store snapshots of license pages (HTML) alongside runs.

## Dataset candidates by tier (quick view)

- **Green**: PHMSA pipeline incidents, MSHA accidents & fatality reports, FRA/DOT rail incidents, NOAA IncidentNews CSV, NASA LLIS, CommonPile regulations/usgpo (with record-level checks).
- **Yellow**: UK AAIB/RAIB/MAIB (OGL with exceptions), CSB investigations (embedded media), ASRS (export limits + narrative), EU eMARS (auth), France ARIA (export limits/auth), case-law (PII).
- **Red**: ISO/IEC/ASME standards PDFs, commercial accident databases (FACTS/TAD), paywalled publisher corpora, commercial legal databases, confidential corporate safety logs.

## TODOs to make v1 → “safety-specialized” robust

- Implement **Socrata export** helper (many DOT/OSHA-like portals): `rows.csv?accessType=DOWNLOAD`
- Add **facility-security filter** (remove pipeline exact milepost, detailed facility schematics)
- Add **taxonomy mapper** for event types and barrier/cause terms
- Add **evaluation set builder**: create held-out tasks like “root cause summary”, “barrier analysis”, “recommendation synthesis” from authoritative reports
