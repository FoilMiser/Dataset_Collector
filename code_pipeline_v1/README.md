# Code Corpus Pipeline Prototype (v1.0)

A safety-first **prototype** pipeline for building an **ethical, code-focused training corpus** from open datasets and repositories. It adapts the chemistry pipeline while following the controls in `CODE_PIPELINE_ADAPTATION.md`:

- Keep a single inventory (`targets_code.yaml`) of candidate sources.
- Snapshot license/terms evidence into per-target manifests.
- Classify each source into **GREEN / YELLOW / RED** queues with conservative defaults.
- Add code-specific gates: secrets scanning, vendored/build stripping, AST-aware chunking, and attribution bundle emission.
- Run **download** (GREEN) and **scrub/extract** (YELLOW) stages with provenance captured.
- Build catalogs and attribution bundles for any material that advances.

> Not legal advice. This tool helps you *track* licenses and restrictions; you are responsible for compliance.

---

## What This Pipeline Does

```
+-----------------------+     +---------------------+     +-------------------------+
|  targets_code.yaml    |---->| pipeline_driver.py  |---->|  Queue JSONLs           |
|  (inventory)          |     | (classify + gate)   |     |  GREEN / YELLOW / RED   |
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
                            | review_queue.py  |   (manual signoff helper)
                            +--------+---------+
                                     |
                                     v
       +----------------------------------------------------------------------+
       |             DATA ACQUISITION + CODE-SPECIFIC TRANSFORMS             |
       +----------------------------------------------------------------------+
                |                                |
                v                                v
       +-------------------+              +---------------------------+
       | download_worker.py|              | yellow_scrubber.py        |
       | (GREEN downloads) |              | (YELLOW transforms + code |
       +---------+---------+              | hygiene/licensing)        |
                 |                        +-----------+---------------+
                 v                                    |
       +--------------------+                        v
       | pools/permissive   |              +-------------------------+
       | pools/copyleft     |              | pools/permissive        |
       | pools/quarantine   |              | (post-filtered/derived) |
       +--------------------+              +-------------------------+
                 |
                 v
       +--------------------+
       | catalog_builder.py |  -> global_catalog.json + training manifests
       +--------------------+
```

### Code-specific focus areas
- **Benchmarks and evaluations**: HumanEval, EvalPlus MBPP+.
- **Specs and documentation**: Python PEPs and other clearly licensed specs.
- **Large open corpora**: Common Pile code slices, BigCode Stack variants (gated behind YELLOW with provenance).
- **Synthetic/internal data**: your own generation pipelines with auditable provenance.

Planned workers called out in the adaptation plan: `code_worker.py`, `secret_scanner.py`, `code_chunker.py`, and a `yellow_scrubber_code.py` helper to move YELLOW sources toward GREEN.

### Key files
- `targets_code.yaml`: code-focused targets (storage roots set to `/data/code`).
- `license_map.yaml`: SPDX normalization + restriction scanning rules tuned for code licenses.
- `field_schemas.yaml`: code schemas (raw files, code chunks, doc/test payloads).
- `denylist.yaml`: optional denylist with rationale and severity.
- `code_worker.py`: shim that validates targets and emits a stub artifact until the full extractor is available.
- `CODE_PIPELINE_ADAPTATION.md`: guidance followed to adapt this package.

---

## Quick Start

### Install
```bash
pip install -r requirements.txt
```

### Dry-run (recommended first)
Creates manifests + queues, but does not download or transform:
```bash
./run_pipeline.sh --targets targets_code.yaml
```

### Review pending YELLOW items
```bash
./run_pipeline.sh --targets targets_code.yaml --stage review
# or:
python3 review_queue.py --queue /data/code/_queues/yellow_pipeline.jsonl list
```

### Emit a YELLOW review plan (summary + JSON)
```bash
./run_pipeline.sh --targets targets_code.yaml --stage yellow
# or call the helper directly:
python3 yellow_scrubber.py --targets targets_code.yaml --output /tmp/yellow_plan.json
```

### Approve/reject a target (writes review_signoff.json)
```bash
python3 review_queue.py approve \
  --target openai_humaneval \
  --reviewer "Your Name" \
  --reviewer-contact "you@example.com" \
  --reason "License and repo evidence reviewed" \
  --constraints "Attribution required per upstream license"

# Reject
python3 review_queue.py reject \
  --target redlisted_repo \
  --reviewer "Your Name" \
  --reason "Terms restrict ML training"
```

### Build the global catalog
```bash
python3 catalog_builder.py --targets targets_code.yaml --output /data/code/_catalogs/global_catalog.json
```
