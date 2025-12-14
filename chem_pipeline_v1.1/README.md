# Chemistry Corpus Pipeline Prototype (v1.1)

A safety-first **prototype** pipeline for building a chemistry-focused training corpus from open datasets and open-access literature, with strong emphasis on **license compliance, provenance tracking, and safe-by-default execution**.

This repo helps you:
- keep a single inventory (`targets.yaml`) of candidate sources,
- snapshot license/terms evidence into per-target manifests,
- classify each source into **GREEN / YELLOW / RED** queues,
- run **download** (GREEN) and **scrub/extract** (YELLOW) stages,
- and build a global catalog / training manifests.

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
        |                         DATA ACQUISITION + TRANSFORMS                |
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

## What's New in v1.1

### 1) Deployment + Reproducibility
- **Dockerfile** and **docker-compose.yaml** for reproducible containerized runs
- **Pinned requirements** (`requirements-lock.txt`) for deterministic builds
- Easy orchestration: `docker-compose run --rm pipeline ./run_pipeline.sh --targets targets.yaml`

### 2) CI/CD + Testing
- **GitHub Actions workflow** with lint checks (ruff) and automated tests
- **Unit tests** for license normalization, denylist patterns, and field schemas
- Test coverage reporting with artifact retention

### 3) Code Quality
- All components upgraded to v1.1 with consistent versioning
- Improved documentation and inline comments
- Schema version bumped to v1.1 across all configuration files

---

## What's New in v1.0

### 1) License confidence + change detection
- SPDX normalization now returns a confidence score and will **force YELLOW** when below a configurable threshold
- Evidence snapshots track SHA256 digests and automatically flag **license content changes** for manual review
- New `--min-license-confidence` CLI option to tune gating strictness per run

### 2) Better handling of license-gated/dynamic pages
- Evidence fetcher accepts custom headers (`--evidence-header KEY=VALUE`) to support cookies or tokens
- Metadata records which headers were used in the manifest for traceability

### 3) Documentation + checklists
- Added legal review checklist for RED/YELLOW edge cases
- Version metadata bumped to v1.0 across manifests and wrapper script

## What's New in v0.9

### 1) Parallel + Incremental Processing
- Async/concurrent evidence fetching with HTTP connection pooling
- Parallel catalog building for large pools with progress reporting
- Delta-only mode for `pipeline_driver.py` to skip unchanged targets
- Incremental catalog updates with timestamp-based change detection and checkpointed resume

### 2) Hardened Audit Trail
- Cryptographic signatures for `evaluation.json` files
- Append-only audit log capturing decisions and pipeline run IDs
- Full provenance chain recorded per decision for traceability

### 3) Resilience + Recovery
- Checkpoint/resume for interrupted downloads and catalog jobs
- Automatic retry with circuit breaker safety
- Health checks for external services and partial-download recovery paths

### 4) Deduplication + Filtering Enhancements
- MinHash/LSH stage implemented via `datasketch` for cross-dataset deduplication
- Fuzzy chemical name/synonym matching plus spectrum-based deduplication hooks
- Spectrum normalization with MSP/MGF parsing for MoNA/GNPS

---

## What's New in v0.8

### 1) Enhanced Review Workflow (Compliance Hardening)
- Extended signoff schema (v0.2) with:
  - `evidence_links_checked`: URLs of checked evidence
  - `reviewer_contact`: Reviewer email/contact
  - `constraints`: Attribution or usage constraints
  - `notes`: Additional notes
- `globals.require_yellow_signoff`: Enforce signoff for all YELLOW items
- **NEW** `review_queue.py export`: Export reviewed targets to CSV/JSON reports

### 2) Enhanced Denylist (v0.2)
- Structured patterns with domain extraction and publisher/provider tags
- Per-pattern severity levels:
  - `hard_red`: Forces RED immediately (default)
  - `force_yellow`: Forces YELLOW for deeper manual review
- **Mandatory provenance fields**: `link` and `rationale` for auditability

### 3) --no-fetch Safety (License Evidence)
- When `--no-fetch` is set, require existing license_evidence snapshot
- If no snapshot exists, force YELLOW to ensure manual review

### 4) Dataset-aware Splitting (Leak Prevention)
- `split_group_id` field keeps related artifacts together across train/valid splits
- Prevents data leakage from related records
- Split reports with counts and token estimates in catalog

### 5) Near-duplicate Detection
- Optional MinHash/LSH stage (configurable in `globals.near_duplicate_detection`)
- Emit duplicate groups into catalog for deduplication

### 6) Expanded Record-level Filtering
- **MoNA** (MassBank of North America): Mass spectrometry data
- **GNPS** (Global Natural Products Social Networking): Natural products spectra
- ChemSpider (RSC): Requires API key

### 7) Parquet Output Option
- `--emit-parquet` for schema-validated outputs
- Configurable compression: snappy, gzip, zstd
- Configure in `globals.parquet_output`

### 8) Enhanced Resolvers
- **Figshare**: API v2 support with rate limiting
- **GitHub**: Release resolver with rate limit handling and exponential backoff

### 9) InChIKey/SMILES Normalization
- Optional RDKit-based canonicalization
- Capture normalization coverage in catalog stats
- Configure in `globals.normalization`

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
python3 review_queue.py --queue /data/chem/_queues/yellow_pipeline.jsonl list
```

### Approve/reject a target (writes review_signoff.json)
```bash
# Approve with extended metadata (v0.8)
python3 review_queue.py approve \
  --target pmc_oa_fulltext \
  --reviewer "Your Name" \
  --reviewer-contact "you@example.com" \
  --reason "PMC OA allowlist; evidence ok" \
  --evidence-links "https://pmc.ncbi.nlm.nih.gov/tools/openftlist/" \
  --constraints "Attribution required per article license"

# Reject
python3 review_queue.py reject \
  --target wikipedia_en \
  --reviewer "Your Name" \
  --reason "Terms restrict ML training"
```

### Export reviewed items (v0.8 NEW)
```bash
python3 review_queue.py export --output /data/reviews.csv --format csv
python3 review_queue.py export --output /data/reviews.json --format json
```

### Optional promotion (conservative; explicit):
```bash
python3 review_queue.py approve \
  --target some_target \
  --reviewer "Your Name" \
  --reason "Compatible after review" \
  --promote-to GREEN
```

### Execute downloads + transforms
```bash
./run_pipeline.sh --targets targets.yaml --execute
```

### Build catalog
```bash
python3 catalog_builder.py --targets targets.yaml --output /data/chem/_catalogs/global_catalog.json
```

---

## Repository Layout

- `pipeline_driver.py` - classifies targets (GREEN/YELLOW/RED), snapshots evidence, emits queues
- `review_queue.py` - manual YELLOW review/signoff helper (extended in v0.9)
- `download_worker.py` - downloads GREEN items into the appropriate pool
- `yellow_scrubber.py` - stage-2 transforms for YELLOW items (PubChem computed-only extraction, PMC OA allowlist planner)
- `pmc_worker.py` - downloads + chunks allowlisted PMC OA full text
- `catalog_builder.py` - builds a global catalog and training manifests

### Configuration
- `targets.yaml` - dataset inventory + download/transform settings (schema v1.1)
- `license_map.yaml` - SPDX normalization rules + gating policy
- `field_schemas.yaml` - versioned schemas for extracted/normalized records
- `denylist.yaml` - explicit denylist patterns (v0.2 with severity and provenance)

---

## Output Structure (default)

```
/data/chem/
  pools/
    permissive/
    copyleft/
    quarantine/
  _staging/
  _queues/
    green_download.jsonl
    yellow_pipeline.jsonl
    red_rejected.jsonl
    run_summary.json
  _manifests/
    {target_id}/
      evaluation.json
      license_evidence.{html,pdf,txt}
      review_signoff.json          # (optional)
  _catalogs/
    global_catalog.json
    training_manifest.json
```

---

## v1.1 Configuration Options

### globals.require_yellow_signoff
```yaml
globals:
  require_yellow_signoff: true  # Enforce signoff for all YELLOW items
```

### globals.near_duplicate_detection
```yaml
globals:
  near_duplicate_detection:
    enabled: true
    method: "minhash_lsh"  # minhash_lsh | exact_hash
    num_perm: 128
    threshold: 0.8
    emit_duplicate_groups: true
```

### globals.parquet_output
```yaml
globals:
  parquet_output:
    enabled: true
    compression: "snappy"  # snappy | gzip | zstd
    row_group_size: 100000
```

### globals.normalization
```yaml
globals:
  normalization:
    enabled: true
    rdkit_canonicalize: true
    validate_inchikey: true
    report_coverage: true
```

### pipeline_driver CLI (evidence handling)
```
# Require at least 0.8 confidence for GREEN classification
python pipeline_driver.py --targets targets.yaml --min-license-confidence 0.8

# Pass cookies or tokens to evidence fetcher for license-gated pages
python pipeline_driver.py --targets targets.yaml \
  --evidence-header "Cookie=session=abc" \
  --evidence-header "X-Requested-With=XMLHttpRequest"
```

---

## Notes / Safety

- **RED items should never be included in training manifests**, even if you have local copies.
- Prefer **computed-only** and **record-level allowlisting** when possible.
- Always snapshot license/terms evidence before large downloads.
- Treat "conditional" / ambiguous licenses as YELLOW until reviewed.
- Use `split_group_id` to prevent data leakage across train/valid splits.
- For RED/YELLOW decisions, run through the legal checklist:
  - Confirm evidence snapshot hash matches the manifest and note any change detections
  - Verify restriction phrases manually even if automated scan is empty
  - Check provenance links in denylist hits and document rationale for overrides
  - Capture reviewer contact + constraints in `review_signoff.json`

---

## Dependencies

```
pyyaml>=6.0         # Core
requests>=2.31.0    # Core

# Optional
tiktoken>=0.5.0     # Token counting
pyarrow>=14.0.0     # Parquet output (v0.9)
datasketch>=1.6.0   # Near-duplicate detection (v0.9)
rdkit>=2023.9.0     # SMILES/InChIKey normalization (v0.9)
```

---

## License

Pipeline code is provided as-is for research and development use.

**Data sources retain their own licenses** - this tool helps you track and respect them.

---

## Docker Usage (v1.1)

### Build the image
```bash
cd chem_pipeline_v1.1
docker build -t chem-pipeline:v1.1 .
```

### Run with Docker
```bash
# Dry-run
docker run -v /data/chem:/data/chem chem-pipeline:v1.1 \
    ./run_pipeline.sh --targets targets.yaml

# Execute
docker run -v /data/chem:/data/chem chem-pipeline:v1.1 \
    ./run_pipeline.sh --targets targets.yaml --execute
```

### Run with Docker Compose
```bash
# Classify stage
docker-compose run --rm classify

# Review YELLOW items
docker-compose run --rm review

# Full pipeline
docker-compose run --rm pipeline ./run_pipeline.sh --targets targets.yaml --execute
```

---

## Running Tests (v1.1)

```bash
# Install test dependencies
pip install pytest pytest-cov

# Run tests
cd chem_pipeline_v1.1
pytest tests/ -v

# Run with coverage
pytest tests/ -v --cov=. --cov-report=term-missing
```

---

## Changelog

### v1.1 (2025-12-14)
- Dockerfile and docker-compose for reproducible containerized runs
- CI/CD workflow with lint checks and automated tests
- Unit tests for license normalization, denylist, and field schemas
- Pinned requirements (requirements-lock.txt) for deterministic builds
- All components upgraded to v1.1 with consistent versioning

### v1.0 (2026-06-01)
- SPDX resolution confidence scoring with bucket gating
- License evidence change detection to force manual review
- Customizable evidence headers for license-gated sources
- Added legal review checklist in documentation

### v0.9 (2026-02-01)
- Parallel + incremental processing with checkpoint/restart
- Hardened audit trail with signatures and run IDs
- Resilience improvements (health checks, retries, recovery)
- Deduplication and spectrum processing improvements

### v0.8 (2025-12-14)
- Extended signoff schema with evidence tracking
- Enhanced denylist with severity and provenance
- --no-fetch safety improvements
- Dataset-aware splitting (split_group_id)
- Near-duplicate detection (MinHash/LSH)
- MoNA/GNPS record-level filtering
- Parquet output option
- Figshare/GitHub resolver improvements
- InChIKey/SMILES normalization
- Export reviewed targets to CSV/JSON

### v0.7 (2025-12-13)
- Explicit denylist support
- Manual review signoff workflow
- Review stage in wrapper script
