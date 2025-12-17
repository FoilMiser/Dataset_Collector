# Regulation & Compliance Corpus Pipeline Prototype (v1.0)

A safety-first **prototype** pipeline for building a regulation & compliance training corpus from open datasets and authoritative legal sources. This package is adapted from the chemistry pipeline and aligns with the **REGCOMP_ADAPTATION_PLAN_v0.1.md** guidance in the repo root. It is tuned for:

- license-gated government and quasi-government sources,
- version-aware metadata (effective dates, amendments, supersessions),
- section-preserving chunking for statutory/regulatory hierarchies,
- third-party/embedded content detection (incorporation by reference, exhibits), and
- PII-aware handling of public comments and enforcement datasets.

## Key references
- Adaptation guidance: [`../REGCOMP_ADAPTATION_PLAN_v0.1.md`](../REGCOMP_ADAPTATION_PLAN_v0.1.md)
- Targets inventory: [`./targets.yaml`](./targets.yaml) (copied from `targets_regcomp_v0.1.yaml`)

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
                        | review_queue.py  |   (extended in v0.9)
                        | (manual signoff) |
                        +--------+---------+
                                 |
                                 v
       +----------------------------------------------------------------------+
       |             DATA ACQUISITION + REG/COMPLIANCE TRANSFORMS             |
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
- **YELLOW**: ambiguous licensing or "restricted" sources -> requires **manual signoff** and/or a safe transform (computed-only extraction, record-level filtering, etc.)
- **RED**: explicitly incompatible licenses/restrictions/denylist match -> rejected

---

## Domain-specific adaptations

- **Metadata contract**: see `field_schemas.yaml` for versioned schemas capturing jurisdiction, authority, document type, effective dates, section hierarchy, citations, license detection, PII flags, and third-party content flags.
- **Targets**: `targets.yaml` mirrors the `targets_regcomp_v0.1.yaml` inventory of regulatory and compliance sources (govinfo, Federal Register API, NIST, CourtListener, EU/UK/CA/AU legislation, etc.).
- **Yellow-stage focus**: placeholder hooks for third-party content detection, PII scrubbing, and derivative-risk routing per the adaptation plan. Implement new workers (e.g., `govinfo_bulk_worker`, `federalregister_api_worker`, `regulationsgov_worker`) before executing downloads.
- **Default storage roots**: `/data/regcomp/...` to keep outputs isolated from chemistry runs.

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
python3 review_queue.py --queue /data/regcomp/_queues/yellow_pipeline.jsonl list
```

### Approve/reject a target (writes review_signoff.json)
```bash
python3 review_queue.py approve \
  --target govinfo_cfr_bulk \
  --reviewer "Your Name" \
  --reviewer-contact "you@example.com" \
  --reason "Public domain; evidence ok" \
  --constraints "Attribution required per source terms"

# Reject
python3 review_queue.py reject \
  --target edgar_filings \
  --reviewer "Your Name" \
  --reason "Corporate copyright; restricted"
```

### Export reviewed items (v0.8 NEW)
```bash
python3 review_queue.py export --output /data/regcomp/reviews.csv --format csv
python3 review_queue.py export --output /data/regcomp/reviews.json --format json
```
