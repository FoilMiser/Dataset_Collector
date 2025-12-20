# KG + Literature Navigation Pipeline (v1.0)

A safety-first **pipeline** for assembling ethical, grounded datasets for scientific
knowledge graph and literature navigation. It adapts the chemistry prototype to
focus on identifier-first corpora (OpenAlex, OpenCitations, ROR, Wikidata) and
navigation episodes that teach retrieval + citation behavior.

> Not legal advice. The tooling helps you collect evidence and enforce conservative
> gates; you are responsible for compliance.

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
                        | review_queue.py  |   (manual signoff when needed)
                        +--------+---------+
                                 |
                                 v
       +----------------------------------------------------------------------+
       |        DATA ACQUISITION + NORMALIZATION FOR GRAPHS + NAV EPISODES    |
       +----------------------------------------------------------------------+
                |                                |
                v                                v
       +-------------------+              +------------------------+
       | download_worker.py|              | kg/nav build workers   |
       | (GREEN downloads) |              | (kg_worker, pii_scrub, |
       +---------+---------+              |  nav_episode_builder)  |
                 |                         +------------------------+
                 v
       +--------------------+
       | pools/permissive   |
       | pools/copyleft     |
       | pools/quarantine   |
       +--------------------+
                 |
                 v
       +--------------------+
       | catalog_builder.py |  -> global_catalog.json + training manifests
       +--------------------+
```

### Buckets
- **GREEN**: clear licensing + minimal PII risk -> download directly
- **YELLOW**: requires manual signoff and/or transforms (computed-only extraction, PII scrub)
- **RED**: rejected by denylist or incompatible terms

---

## KG/Lit-Navigation Focus
- New storage root: `/data/kg_nav` with `_staging`, `_manifests`, `_queues`, `_catalogs`, `_logs`.
- Download strategy extensions for KG sources: `s3_sync`, `aws_requester_pays`, `torrent`, `figshare`.
- New build workers (scaffolds provided):
  - `kg_worker.py`: normalize graph dumps into nodes/edges/provenance.
  - `pii_scrub_worker.py`: strip emails/biographies from person registries (e.g., ORCID).
  - `nav_episode_builder.py`: synthesize grounded navigation episodes using OpenAlex/COCI/ROR/Wikidata.
- Field schemas tuned for identifier-only outputs (OpenAlex/Crossref/DataCite minimal graphs + nav episodes).
- Licensing posture: CC0 defaults to GREEN; copyleft and record-level sources flow to YELLOW for scrutiny.

---

## Quick Start

### Install
```bash
pip install -r requirements.txt
```

### Dry-run classification (no downloads)
```bash
./run_pipeline.sh --targets targets.yaml
```

### Download GREEN targets (OpenAlex, Wikidata, ROR, COCI)
```bash
./run_pipeline.sh --targets targets.yaml --stage download --execute
```

### Review pending YELLOW items
```bash
./run_pipeline.sh --targets targets.yaml --stage review
# or
python3 review_queue.py --queue /data/kg_nav/_queues/yellow_pipeline.jsonl list --limit 50
```

### Build catalog
```bash
./run_pipeline.sh --targets targets.yaml --stage catalog
# or
python3 catalog_builder.py --targets targets.yaml --output /data/kg_nav/_catalogs/global_catalog.json
```

---

## Files
- `targets.yaml` - KG/literature inventory + download/transform settings (schema v0.8)
- `license_map.yaml` - SPDX normalization + gating policy
- `denylist.yaml` - domain and publisher restrictions with severities
- `field_schemas.yaml` - identifier-first schemas for computed-only extraction
- `pipeline_driver.py` - classification + manifest generation
- `download_worker.py` - download strategies (http, ftp, git, zenodo, dataverse, HF datasets, figshare, s3 sync, requester-pays, torrent)
- `yellow_scrubber.py` - placeholder for YELLOW transforms (record-level filtering, schema validation)
- `kg_worker.py` - scaffold for graph normalization
- `pii_scrub_worker.py` - scaffold for PII scrubbing registries
- `nav_episode_builder.py` - scaffold for grounded navigation episode synthesis
- `catalog_builder.py` - builds global catalogs and manifests
- `run_pipeline.sh` - orchestration helper

---

## Safety Notes
- Always run a dry-run first to snapshot license evidence (`--no-fetch` is used by default).
- For requester-pays or torrent sources, ensure policy approval before running with `--execute`.
- Navigation outputs should always cite identifiers (DOI/OpenAlex/ROR/ORCID/QID) and include provenance.
- Keep copyleft data in the dedicated pool; do not mix into permissive training sets.

