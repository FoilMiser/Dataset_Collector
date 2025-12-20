# Metrology & Technical Reports Dataset Pipeline (v1.0)

A safety-first **prototype** pipeline for building a metrology/technical-report training corpus with strong emphasis on **license compliance, provenance tracking, and safe-by-default execution**. This package adapts the chemistry pipeline to focus on NIST/NASA/USGS/NOAA/BIPM style documents and keeps tables, section headers, and captions that encode measurement rigor.

> Not legal advice. This tool helps you *track* licenses and restrictions; you are responsible for compliance.

---

## What This Pipeline Does

```
+------------------+     +---------------------+     +-------------------------+
|  targets.yaml    |---->| pipeline_driver.py  |---->|  Queue JSONLs           |
|  (inventory)     |     | (classify + gate)   |     |  GREEN / YELLOW / RED   |
+------------------+     +---------+-----------+     +------------+------------+
                                  |                              |
                                  |                              v
                                  |                   +------------------------+
                                  |                   | red_rejected.jsonl     |
                                  |                   | (do not process)       |
                                  |                   +------------------------+
                                  |
                                  v
                        +------------------+
                        | review_queue.py  |
                        | (manual signoff) |
                        +--------+---------+
                                 |
                                 v
       +----------------------------------------------------------------------+
       |         DATA ACQUISITION + TECHNICAL-REPORT EXTRACTION               |
       +----------------------------------------------------------------------+
                |                                |
                v                                v
       +-------------------+              +---------------------+
       | download_worker.py|              | yellow_scrubber.py  |
       | (GREEN downloads) |              | (YELLOW transforms) |
       +---------+---------+              +----------+----------+
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

### Metrology-focused defaults
- **Tables and captions kept** during chunking (measurement definitions live in tables).
- **Distribution/export control scans planned** for NASA/DoD-style statements.
- **API harvester placeholders** for NTRS, USGS Pubs Warehouse, NOAA IR, and FAA Advisory Circulars.
- **Tech-report chunk schema** captures section paths, publisher/series metadata, and parsing provenance.

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
python3 review_queue.py --queue /data/metrology/_queues/yellow_pipeline.jsonl list
```

### Approve/reject a target (writes review_signoff.json)
```bash
python3 review_queue.py approve \
  --target nasa_ntrs_openapi_public_harvest \
  --reviewer "Your Name" \
  --reviewer-contact "you@example.com" \
  --reason "Distribution A only" \
  --evidence-links "https://sti.nasa.gov/harvesting-data-from-ntrs/"

# Reject
python3 review_queue.py reject \
  --target iso_standards_paywalled \
  --reviewer "Your Name" \
  --reason "Paywalled/terms prohibit redistribution"
```

### Export reviewed items
```bash
python3 review_queue.py export --output /data/reviews.csv --format csv
python3 review_queue.py export --output /data/reviews.json --format json
```

---

## Domain Highlights

### What changed vs. chemistry baseline
- Target inventory, defaults, and documentation point to metrology/technical-report sources.
- Tech-report chunk schema added (`tech_report_chunk_v1.0.0`) with section paths and provenance.
- Denylist strengthened for distribution/export-control phrases; license map recognizes open government licences.
- Wrapper script messaging updated to emphasize metrology focus; PubChem/PMC helpers marked legacy.

- **Seed targets:** BIPM SI Brochure (CC BY 4.0), NIST SP 330 (US public domain), NIST Research Data Framework HTML.
- **Metadata-first approach:** ingest NIST Technical Series metadata as GREEN to bootstrap identifiers before full-text crawling.
- **High-value harvesters (disabled until implemented):** NASA NTRS OpenAPI, USGS Publications Warehouse, NOAA IR JSON API, FAA Advisory Circular crawler.
- **Safety stance:** default YELLOW for mixed-rights repositories; keep RED for ISO/IEC/IEEE standards.

See `METROLOGY_PIPELINE_ADAPTATION.md` in the repo root for the detailed adaptation plan and candidate list.
