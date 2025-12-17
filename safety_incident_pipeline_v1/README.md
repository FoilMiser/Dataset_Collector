# Safety Incident & Compliance Corpus Pipeline (v1.0)

A safety-first **prototype** pipeline for building a safety engineering, incident analysis, and compliance-focused training corpus. It emphasizes **license compliance, provenance tracking, PII scrubbing, and safe-by-default execution** while adapting the chemistry pipeline patterns to incident reports, structured event tables, and regulatory text.

This repo helps you:
- maintain a single inventory (`targets.yaml`) of candidate safety datasets,
- snapshot license/terms evidence into per-target manifests,
- classify each source into **GREEN / YELLOW / RED** queues,
- run **download** (GREEN) and **scrub/extract** (YELLOW) stages with PII redaction gates,
- and build a global catalog / training manifests for downstream model training.

> Not legal advice. This tool helps you *track* licenses and restrictions; you are responsible for compliance and respecting jurisdictional privacy rules.

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
                         | review_queue.py  |   (manual signoff for YELLOW)
                         | (manual signoff) |
                         +--------+---------+
                                  |
                                  v
        +----------------------------------------------------------------------+
        |                 DATA ACQUISITION + TRANSFORMS (SAFETY)               |
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

### Buckets
- **GREEN**: clear, compatible licensing + no disallowed restrictions -> can be downloaded as-is
- **YELLOW**: ambiguous licensing or "restricted" sources -> requires **manual signoff** and/or safe transforms (PII redaction, record-level filters, etc.)
- **RED**: explicitly incompatible licenses/restrictions/denylist match -> rejected

---

## Safety-specific additions

- Targets focus on **incident investigations, structured occurrence tables, safety alerts, and regulatory/compliance corpora**.
- `field_schemas_safety_incident.yaml` defines incident report chunks (`incident_report_chunk_v1.0.0`) and structured event rows (`incident_event_row_v1.0.0`).
- Default gates stay conservative and you can enable additional ones per target (`pii_scan_and_redact`, `pii_scan_and_redact_strict`, `pdf_extract`, `html_crawl`, `license_metadata_validate`, `strip_third_party_media`).
- Storage roots/pools default to `/data/safety/**` for isolation from other domains.

---

## What's New vs Chemistry Pipeline

- Inventory (`targets.yaml`) rewritten for safety engineering + compliance datasets (PHMSA, MSHA, FRA, NOAA, NASA LLIS, Common Pile filtered regulation corpora, etc.).
- Stricter defaults: `require_yellow_signoff: true` and PII redaction gates attached to narrative-heavy sources.
- Emphasis on removing personal identifiers and coarsening locations for sensitive incident records.

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
python3 review_queue.py --queue /data/safety/_queues/yellow_pipeline.jsonl list
```

### Approve/reject a target
```bash
python3 review_queue.py approve \
  --target csb_completed_investigations \
  --reviewer "Your Name" \
  --reviewer-contact "you@example.com" \
  --reason "License ok; PII redaction configured" \
  --evidence-links "https://www.csb.gov/investigations/completed-investigations/" \
  --constraints "Strip third-party media by default"

python3 review_queue.py reject \
  --target nist_disaster_failure_studies_repo \
  --reviewer "Your Name" \
  --reason "Mixed third-party materials; terms unclear"
```

### Export reviewed items
```bash
python3 review_queue.py export --output /data/reviews.csv --format csv
python3 review_queue.py export --output /data/reviews.json --format json
```
