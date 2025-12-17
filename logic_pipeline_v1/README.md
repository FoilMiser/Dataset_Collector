# Logic Corpus Pipeline Prototype (v1.0)

A safety-first **prototype** pipeline for building an **ethical logic-focused training corpus** from open datasets and repositories. It adapts the chemistry pipeline while honoring the guidance in `LOGIC_PIPELINE_ADAPTATION.md`:

- Keep a single inventory (`targets_logic.yaml`) of candidate sources.
- Snapshot license/terms evidence into per-target manifests.
- Classify each source into **GREEN / YELLOW / RED** queues with conservative defaults.
- Run **download** (GREEN) and **scrub/extract** (YELLOW) stages with provenance captured.
- Build catalogs and attribution bundles for any material that advances.

> Not legal advice. This tool helps you *track* licenses and restrictions; you are responsible for compliance.

---

## What This Pipeline Does

```
+-----------------------+     +---------------------+     +-------------------------+
|  targets_logic.yaml   |---->| pipeline_driver.py  |---->|  Queue JSONLs           |
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
       |                    DATA ACQUISITION + TRANSFORMS                     |
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

### Logic-specific focus areas
- **Formal proof corpora**: Lean/Coq/Isabelle/HOL/Metamath repositories.
- **Benchmarks/solver formats**: SMT-LIB v2, DIMACS CNF, ATP problem sets.
- **NL logic datasets**: natural language reasoning, entailment chains, explanations.
- **Open educational texts**: open textbooks/lectures with clear licenses.

Planned workers called out in the adaptation plan: `proof_repo_worker.py`, `benchmark_worker.py`, `nl_logic_worker.py`, and a `commonpile_import_worker.py` stub for logic-focused slices.

### Key files
- `targets_logic.yaml`: logic-focused targets (storage roots already set to `/data/logic`).
- `license_map.yaml`: SPDX normalization + restriction scanning rules.
- `field_schemas.yaml`: logic schemas (theorems, proof scripts, SMT/SAT problems, NL reasoning).
- `denylist.yaml`: optional denylist with rationale and severity.
- `LOGIC_PIPELINE_ADAPTATION.md`: guidance followed to adapt this package.

---

## Quick Start

### Install
```bash
pip install -r requirements.txt
```

### Dry-run (recommended first)
Creates manifests + queues, but does not download or transform:
```bash
./run_pipeline.sh --targets targets_logic.yaml
```

### Review pending YELLOW items
```bash
./run_pipeline.sh --targets targets_logic.yaml --stage review
# or:
python3 review_queue.py --queue /data/logic/_queues/yellow_pipeline.jsonl list
```

### Emit a YELLOW review plan (summary + JSON)
```bash
./run_pipeline.sh --targets targets_logic.yaml --stage yellow
# or call the helper directly:
python3 yellow_scrubber.py --targets targets_logic.yaml --output /tmp/yellow_plan.json
```

### Approve/reject a target (writes review_signoff.json)
```bash
python3 review_queue.py approve \
  --target lean_mathlib4 \
  --reviewer "Your Name" \
  --reviewer-contact "you@example.com" \
  --reason "License and repo evidence reviewed" \
  --constraints "Attribution required per upstream license"

# Reject
python3 review_queue.py reject \
  --target proprietary_logic_repo \
  --reviewer "Your Name" \
  --reason "Terms restrict ML training"
```

### Build the global catalog
```bash
python3 catalog_builder.py --targets targets_logic.yaml --output /data/logic/_catalogs/global_catalog.json
```

---

## Paths and Pools
All defaults point to `/data/logic`, with separate pools for permissive, copyleft, and quarantine data. See `targets_logic.yaml` for overrides.

## Safety gates
The pipeline keeps YELLOW sources blocked until explicitly approved and stores license evidence snapshots for reproducibility. Restriction phrase scanning and SPDX normalization rely on `license_map.yaml`.

