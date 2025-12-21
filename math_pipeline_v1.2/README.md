# Math Corpus Pipeline Prototype (v1.2)

A safety-first **prototype** pipeline for building an **ethical mathematics-focused training corpus** from open datasets, open educational resources, and formal libraries. It adapts the chemistry pipeline while honoring the guidance in `MATH_PIPELINE_ADAPTATION.md`:

- Keep a single inventory (`targets_math.yaml`) of candidate sources spanning **basic → advanced** math.
- Snapshot license/terms evidence into per-target manifests with math-specific restriction scanning.
- Classify each source into **GREEN / YELLOW / RED** queues with conservative defaults.
- Run **download** (GREEN) and **review/transform** (YELLOW) stages with provenance captured.
- Build catalogs and attribution bundles for any material that advances, keeping copyleft segregated.

> Not legal advice. This tool helps you *track* licenses and restrictions; you are responsible for compliance.

---

## What This Pipeline Does

```
+------------------------+     +---------------------+     +-------------------------+
|  targets_math.yaml     |---->| pipeline_driver.py  |---->|  Queue JSONLs           |
|  (inventory)           |     | (classify + gate)   |     |  GREEN / YELLOW / RED   |
+------------------------+     +---------+-----------+     +------------+------------+
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
       | (GREEN downloads) |              | (YELLOW summaries)  |
       +---------+---------+              +----------+----------+
                 |                                   |
                 v                                   v
       +--------------------+              +------------------------+
       | pools/permissive   |              | pools/permissive       |
       | pools/copyleft     |              | pools/copyleft         |
       | pools/quarantine   |              | pools/quarantine       |
       +--------------------+              +------------------------+
                 |                                   |
                 v                                   v
       +--------------------+              +------------------------+
       | catalog_builder.py |  -> global_catalog.json + manifests   |
       +--------------------+              +------------------------+
```

### Math-specific priorities
- **Narrative math text**: textbooks, lecture notes, encyclopedia-style articles with LaTeX preserved.
- **Formal mathematics**: Lean/Coq/Agda/Isabelle/Metamath libraries chunked by theorem/lemma.
- **Problem corpora**: exercises and word problems with verified licenses and answer formats.
- **Ethics first**: CC BY-SA kept in copyleft pools; NC/ND and “NoAI/TDM” patterns flagged early.

---

## Quick Start

### Install
```bash
pip install -r requirements.txt
```

### Dry-run (recommended first)
Creates manifests + queues, but does not download or transform:
```bash
./run_pipeline.sh --targets targets_math.yaml
```

### Review pending YELLOW items
```bash
./run_pipeline.sh --targets targets_math.yaml --stage review
# or
python3 review_queue.py --queue /data/math/_queues/yellow_pipeline.jsonl list
```

### Approve/reject a target (writes review_signoff.json)
```bash
python3 review_queue.py approve \
  --target common_pile_arxiv_papers_math \
  --reviewer "Your Name" \
  --reviewer-contact "you@example.com" \
  --reason "Math category filtered; CC BY rows only" \
  --constraints "Attribution bundle required"

python3 review_queue.py reject \
  --target webwork_open_problem_library \
  --reviewer "Your Name" \
  --reason "NC/SA license incompatible with training"
```

### Execute downloads + transforms
```bash
./run_pipeline.sh --targets targets_math.yaml --execute
```

### Build catalog
```bash
python3 catalog_builder.py --targets targets_math.yaml --output /data/math/_catalogs/global_catalog.json
```

---

## Repository Layout

- `targets_math.yaml` - math dataset inventory + download/transform settings (schema v0.8, now emits generic routing fields)
- `license_map.yaml` - SPDX normalization rules + gating policy tuned for math sources
- `field_schemas.yaml` - versioned schemas for math text, formal units, and problem records
- `denylist.yaml` - explicit denylist patterns (NoAI/NoTDM/NC/ND and paywalled publishers)
- `pipeline_driver.py` - classifies targets (GREEN/YELLOW/RED), snapshots evidence, emits queues
- `review_queue.py` - manual YELLOW review/signoff helper
- `download_worker.py` - downloads GREEN items into the appropriate pool using the folder layout from `difficulties_math.yaml`
- `yellow_scrubber.py` - stage-2 summaries for YELLOW items to drive human triage
- `catalog_builder.py` - builds a global catalog and training manifests
- `MATH_PIPELINE_ADAPTATION.md` - design notes for adapting chem → math (workers, chunking)

### Planned math workers (from adaptation doc)
- `hf_math_filter_worker.py`: filter HF datasets by math domains + record-level license routing.
- `formal_math_worker.py`: chunk Lean/Coq/Isabelle/Agda/Metamath by theorem/lemma.
- `pdf_math_worker.py`: PDF extraction that preserves LaTeX blocks and equation density metadata.

---

## Notes / Safety

- **RED items should never be included in training manifests**, even if you have local copies.
- Treat NC/ND/NoAI/NoTDM signals as RED unless a lawyer approves otherwise.
- Keep CC BY-SA and GPL/LGPL outputs in the **copyleft pool** with attribution bundles.
- Prefer record-level allowlisting for mixed-license OER sources; quarantine until filtered.
- Preserve LaTeX/math symbols—avoid ASCII-fying equations during extraction.
- Use `split_group_id` to prevent data leakage across train/valid splits for exercises and theorems.

---

## License

Pipeline code is provided as-is for research and development use.

**Data sources retain their own licenses** - this tool helps you track and respect them.
