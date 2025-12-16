# Adapting the chemistry pipeline to an Engineering Dataset Pipeline (ethical + legally-cleared)

This doc assumes you’re starting from `chem_pipeline_v1` and adding an **engineering-specialized** dataset collection configuration.

The two deliverables in this update are:

- `targets_engineering_v0.1.yaml` — candidate datasets & legal evidence URLs (GREEN/YELLOW/RED patterns)
- The plan below — how to adapt the existing pipeline *without losing the ethics/licensing guarantees*

---

## 1) Engineering-specific goals and threat model

### What “engineering specialization” means (for training data)
Engineering LLM training benefits from **three types of signals**:

1. **Technical text corpora** (patents, technical reports, manuals, specs, regulations)
2. **Structured/metadata corpora** (patent tables, bibliographic/affiliation graphs, classification systems)
3. **Engineering code/tooling corpora** (simulation/EDA/CAD software docs + code; carefully separated by license)

### What “ethical + legally-cleared” means in this pipeline
You want a pipeline that:

- **Never silently mixes licenses**
- Defaults to **record-level filtering** when a dataset’s license varies across records (CommonPile, arXiv, GitHub, NASA/USGS)
- Keeps **copyleft/share-alike** content in a separate pool, or excludes it entirely, depending on your training policy
- Handles **PII-bearing public records** (notably patents) with conservative redaction before training

---

## 2) What you can reuse unchanged

You can keep these modules mostly as-is:

- `pipeline_driver.py` (license evidence snapshotting + queue routing)
- `download_worker.py` (HTTP, Git, HF datasets, etc.)
- `yellow_scrubber.py` (but you will extend it for engineering-specific PII/topic filters)
- `catalog_builder.py` (for creating a human-auditable catalog of what you collected)
- `license_map.yaml`, `denylist.yaml` (you’ll extend both)

---

## 3) Key engineering-specific adaptations you should implement

### A) Add a patent-aware PII scrubber (high priority)
Patents are public, but they routinely contain:
- inventor names
- inventor addresses
- attorney/agent details
- correspondence addresses / emails / phone numbers

**Recommended policy:**
- Remove or hash *inventor personal identifiers* (name/address/email/phone) unless you have a specific reason to keep them.
- Keep technical content (claims, abstract, description, diagrams text) intact.
- Keep *organization/assignee* names (useful for engineering context), but consider normalizing.

**Implementation sketch:**
- Add a new gate stage in `yellow_scrubber.py` and (optionally) a dedicated `pii_scrub_patents.py`.
- Use regex + deterministic redaction tokens:
  - `[INVENTOR_NAME]`, `[INVENTOR_ADDRESS]`, `[EMAIL]`, `[PHONE]`
- Do not try to “perfectly” scrub PII; prefer high recall.

### B) Add an engineering topic filter (optional but very helpful)
You can specialize without throwing away general language by *biasing* toward engineering:

- **Patents:** filter/weight by CPC/IPC codes (e.g., electrical H, mechanical F, chemistry C (if you want), physics G, etc.)
- **arXiv:** filter by categories relevant to engineering (eess.*, cs.RO, cs.SY, cs.SE, cs.LG for controls/robotics, etc.)
- **Gov docs:** keyword/topic filter (standards, design, safety, testing, reliability, manufacturing, materials, etc.)

In practice:
- For record-level datasets, add a `record_level_topic_gate` that:
  - reads metadata (e.g., tags/categories)
  - passes through only engineering-relevant records (or assigns a “sampling weight” for curriculum training)

### C) Add a USPTO ODP bulk downloader (planned)
The YAML includes targets for the USPTO Open Data Portal, but the current `download_worker.py` lacks an **API enumerator**.

Add a new worker module, e.g. `uspto_odp_worker.py`, that:
1. Calls the ODP “search/product” endpoint to list bulk artifacts for a dataset (e.g., PTGRXML).
2. Downloads artifacts with resume support and chunking.
3. Emits per-artifact manifests including:
   - dataset id, artifact id, timestamp, size, checksum (if provided), source URL

You’ll also want:
- retry/backoff
- optional API key support (env var `USPTO_ODP_API_KEY`)

### D) Add a GovInfo / “directory style” bulk downloader (planned)
Many government bulk repositories are “directory listings” with many files.
Your current `download_worker.py` doesn’t crawl directories.

Add either:
- a generic `http_directory` strategy (parse HTML listings + regex include/exclude)
or
- source-specific resolvers (govinfo, uspto, etc.)

### E) Extend `license_map.yaml` for engineering-adjacent licenses
Engineering sources often use licenses not already in your map:
- ODbL (OpenStreetMap)
- OGL (UK Open Government Licence)
- IETF Trust / W3C document licenses (custom terms)
- “NIST-PD” style notices (SPDX has a NIST-PD family)

Decision rule:
- if license is permissive (CC0/CC-BY/Apache/MIT/BSD) → GREEN
- if share-alike / copyleft (CC-BY-SA/GPL/LGPL/AGPL/ODbL) → YELLOW or separate “copyleft” pool
- if “no derivatives”, “non-commercial”, ToS restrictions, or unclear → RED / deny or record-level filter only

### F) Fix a small schema mismatch (recommended)
Right now, `download_worker.py` expects `row["output_pool"]`, but `pipeline_driver.py` doesn’t write that field into queue rows.

Two quick fixes:
1. In `pipeline_driver.py`, add `output_pool = target["output"]["pool"]` to the row.
2. Or in `download_worker.py`, prefer `row["target"]["output"]["pool"]` (if you pass full target).

This matters if you want automatic routing into `public_domain / permissive / copyleft / record_level`.

---

## 4) How to use the new engineering targets YAML

1. Copy `targets_engineering_v0.1.yaml` into your pipeline package (same folder as `pipeline_driver.py`).
2. Run the driver:

```bash
python pipeline_driver.py --targets targets_engineering_v0.1.yaml
```

3. Review:
- `queues/green_queue.jsonl` → safe to download automatically
- `queues/yellow_review_queue.jsonl` → requires signoff; then run scrubber + ingest
- `queues/red_rejected_queue.jsonl` → should not be downloaded; keep evidence snapshots only

4. Download green targets:

```bash
python download_worker.py --queue queues/green_queue.jsonl
```

5. After manual signoff, process yellow targets:

```bash
python yellow_scrubber.py --queue queues/yellow_review_queue.jsonl
```

6. Build catalog:

```bash
python catalog_builder.py --manifests manifests/
```

---

## 5) What’s inside `targets_engineering_v0.1.yaml`

### “Good defaults” that are immediately useful
- **CommonPile engineering-adjacent slices**: patents, regs, USGPO, arXiv, GitHub, StackExchange  
  → these give you *coverage + structure* with record-level licensing metadata.

### “Planned” sources that need a small downloader extension
- USPTO ODP official bulk
- OpenAlex full snapshot (S3 sync)
- OpenStax textbook scraping (curated list, HTML export preferred)

### “Red targets” included for traceability
IEEE / ASME / ASTM (standards and paywalled libraries) are included as **deny** targets so the pipeline
creates a rejection manifest + evidence snapshot.

---

## 6) Next recommended iteration (v0.2)

1. Implement `uspto_odp_worker.py` (highest impact)
2. Add `pii_scrub_patents` gate + tests
3. Extend `license_map.yaml` for:
   - ODbL, OGL-UK, W3C, IETF
4. Add an “engineering topic score” to each record for curriculum sampling

If you want, I can also draft:
- the `uspto_odp_worker.py` skeleton
- the `pii_scrub_patents.py` module + unit tests
- the small patch to `pipeline_driver.py` to propagate `output_pool`
