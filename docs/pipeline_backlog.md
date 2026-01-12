# Pipeline Backlog Issues

This file tracks open backlog items migrated from per-pipeline `todo.txt` files.
Please file new work as GitHub Issues (or entries here if offline) instead of adding `todo.txt` files.

## Core Infrastructure

### Type Annotations (P2.1.2)
- [ ] Expand type annotations on hot paths (classification, evidence, acquire strategies, merge, yellow screening)
- [ ] Add mypy to CI with non-strict mode for collector_core
- [ ] Tighten mypy strictness over time as coverage improves

### Archive Safety Integration
- [ ] Integrate `archive_safety.safe_extract()` into acquire strategies that handle archives
- [ ] Add archive safety configuration options to targets YAML schema

## 3d_modeling_pipeline_v2

### HIGH PRIORITY (Production readiness)
- [ ] Harden mesh_worker with watertight checks + repair flags
- [ ] Emit normalized mesh hashes for all assets in yellow_screen_worker
- [ ] Add optional thumbnail + point cloud generation toggles
- [ ] Enforce record-level license parsing for community datasets (Thingi10K/Objaverse)
- [ ] Add per-record SPDX normalization in screening stage
- [ ] Unit tests for mesh hashing + routing metadata coverage
- [ ] Smoke test classify → acquire → screen → merge → catalog on small fixture (sorting handled in sorter pipeline)

### MEDIUM PRIORITY (Quality + coverage)
- [ ] Add Figshare/GitHub releases download handlers if needed
- [ ] Enrich s3_public listings with checksums and file counts
- [ ] Geometry-aware dedupe (lightfield descriptors / embeddings)
- [ ] Texture/material metadata extraction

### LOW PRIORITY (Ergonomics + future-proofing)
- [ ] Dockerfile for 3D pipeline + example volume mounts
- [ ] Example orchestration (cron/Argo) for recurring runs
- [ ] Metrics on queue sizes, download throughput, and review turnaround
- [ ] Alerts for missing license evidence or stalled downloads

## agri_circular_pipeline_v2

### Backlog
- [ ] Implement echo_worker.py for ECHO ZIPs (facility + permit schemas, aggregation views) and emit JSONL for v2 merge
- [ ] Implement agrovoc_worker.py (term tables + synonym expansion) with JSONL canonical outputs
- [ ] Optional: geo_worker.py for safe tiling/downsampling of geospatial products
- [ ] Harden denylist/rules for personal addresses and operational security details
- [ ] Preserve attribution metadata in manifests for AGROVOC and other YELLOW sources
- [ ] Add unit harmonization layer for waste stream metrics (kg/ton, wet vs dry mass, COD/BOD)
- [ ] Wire CommonPile/GBIF/FAOSTAT candidates with per-record license filtering before enabling

## biology_pipeline_v2

### HIGH PRIORITY (Parity + routing)
- [ ] Wire PMC OA acquisition as a first-class acquire_worker strategy.
- [ ] Move PubChem computed-only transform into yellow_screen_worker plugins.
- [ ] Add record-level license validation hooks for GBIF/iNaturalist when enabled.
- [ ] Expand routing coverage for more domains/categories (sorter pipeline ownership).
- [ ] Add regression fixtures ensuring routing fields are present in queues.
- [ ] Unit tests for restriction phrase scans and SPDX normalization.
- [ ] Smoke test classify → acquire → yellow_screen → merge → catalog on small fixtures (sorting handled in sorter pipeline).

### MEDIUM PRIORITY (Quality + safety)
- [ ] Add PHI/PII scanning stubs for clinical/human-subjects buckets.
- [ ] Implement denylist enrichments for biosecurity-sensitive content.
- [ ] Extend catalog_builder to surface raw/screened_yellow stats and token estimates.
- [ ] Emit metrics on queue sizes, download throughput, and review turnaround.

### LOW PRIORITY (Ergonomics)
- [ ] CLI to assign reviewers + notes per target (plugs into review_queue).
- [ ] Dockerfile + example volume mounts for end-to-end runs.
- [ ] Publish pinned requirements/lockfile.

## chem_pipeline_v2

### v2 skeleton (based on math v2) landed:
- [ ] Add MoNA/GNPS record-level filters with license-aware routing
- [ ] Tighten PMC OA handling with per-article license evidence
- [ ] Expand PubChem plugin field schemas and validation coverage
- [ ] Enforce record-level license checks when require_record_license is true
- [ ] Add deny phrase coverage for common chemistry publisher terms
- [ ] Wire chem_routing into merge and sorter fallbacks everywhere
- [ ] Tiny end-to-end dry-run fixture (classify->acquire->screen) for CI
- [ ] Sample raw inputs for PubChem/PMC plugin regression tests
- [ ] Lightweight metrics: shard counts/bytes per stage
- [ ] How-to guides for adding new chemistry targets and screening modes

## code_pipeline_v2

### Backlog
- [ ] Expand code_worker language coverage (tree-sitter/AST-aware chunking).
- [ ] Add optional minhash duplicate reporting in merge_worker.
- [ ] Harden secrets scanning with detect-secrets patterns.
- [ ] Integrate Parquet output once schema stabilizes.
- [ ] Add regression fixtures for yellow_screen_worker strict pitch behavior.

## cyber_pipeline_v2

### Next up (full cyber coverage):
- [ ] Integrate normalization hooks (Option A) in acquire_worker for NVD/STIX/GHSA feeds.
- [ ] Define routing defaults per target (incident_response, vulnerability_data, threat_modeling, etc.).
- [ ] Expand routing coverage for more domains/categories (sorter pipeline ownership).
- [ ] Add smoke-test fixtures covering classify → merge → catalog with sample JSONL shards (sorting handled in sorter pipeline).
- [ ] Refresh denylist + gates once normalization emits cyber-specific fields.

## earth_pipeline_v2

### Near-term follow-ups:
- [ ] Add acquisition handlers for stac_catalog, thredds_opendap, and api_tabular strategies.
- [ ] Flesh out pipelines/targets/targets_earth.yaml with additional NASA/NOAA/USGS collections and richer routing.
- [ ] Create tiny fixture queues to smoke-test classify → acquire → screen → merge → catalog locally (sorting handled in sorter pipeline).
- [ ] Wire schema validation for earth_canonical_record_v1.0.0 in yellow_screen_worker.
- [ ] Add sensitivity scrubbing hooks (geo coarsening, PII trimming) before merge.
- [ ] Build Dockerfile + example volume mounts for /data/earth roots.

### Nice-to-haves:
- [ ] Optional near-duplicate detection pass tuned for geo/temporal text.
- [ ] Metrics dashboard from _ledger files (counts, bytes, pitch reasons).
- [ ] Example notebooks demonstrating catalog exploration and shard sampling.

## engineering_pipeline_v2

### HIGH PRIORITY (Production readiness)
- [ ] Flask/FastAPI dashboard for YELLOW review with visual diffs
- [ ] Batch approval/rejection + reviewer assignment/notifications
- [ ] Run-level audit views (surface run IDs, signatures, provenance)
- [ ] Access controls for reviewers and approvers
- [ ] Unit tests for license normalization and resolvers
- [ ] Integration tests for download workers and scrubbers with sample data
- [ ] End-to-end smoke tests (green/yellow/red queues + catalog build)
- [ ] CI pipeline (lint/type checks/tests) with artifact retention
- [ ] Dockerfile + docker-compose for reproducible runs
- [ ] Kubernetes/Helm job templates with secrets/volume wiring
- [ ] Versioned configuration bundles (schemas + denylist + defaults)
- [ ] Publish pinned requirements/lockfiles

### MEDIUM PRIORITY (Quality + coverage)
- [ ] Add ZINC, BindingDB, DrugBank (where licensed), and USPTO pipelines
- [ ] Expand spectrum ingestion for MoNA/GNPS and other MSP/MGF sources
- [ ] Cross-dataset deduplication reports that include spectra + structure IDs
- [ ] Harden SMILES canonicalization and InChIKey validation
- [ ] Molecular property calculation + substructure filtering hooks
- [ ] Performance tuning for large SDF ingestion (Cython/numba optional)
- [ ] Metrics on download times, error rates, queue throughput
- [ ] Prometheus/StatsD exporters and Grafana dashboard examples
- [ ] Slack/email notifications for failures or stalled queues
- [ ] Architecture doc + diagrams
- [ ] Tutorial for adding new targets and resolvers
- [ ] Troubleshooting guide and FAQ for common failures
- [ ] API/programmatic access examples

### LOW PRIORITY (Ergonomics + future-proofing)
- [ ] Streaming processing for large files + memory profiling
- [ ] Optional progress dashboards (CLI + web) for long jobs
- [ ] Configurable caching/prefetch for common resolvers
- [ ] Helm chart hardening (quotas, pod disruption budgets)
- [ ] Airflow/Argo examples for scheduled runs
- [ ] Backup/restore procedures for catalogs and manifests

## kg_nav_pipeline_v2

### HIGH PRIORITY
- [ ] Unit tests for each yellow_screen adapter (wikidata, OpenAlex, Crossref, COCI, ORCID, MeSH)
- [ ] Sample fixtures that exercise PII scrubbing + deny phrase hits
- [ ] Implement navigation episode generator to transform combined graph shards into episodes before sorter bucketting
- [ ] Add metrics for episode coverage per task_type/domain/category
- [ ] Smoke test `s3_sync`/`aws_requester_pays` paths on small fixtures
- [ ] Add resumable snapshot logic for large dumps (OpenAlex, Crossref) with checksum validation
- [ ] Extend catalog_builder to summarize routing + structural metadata coverage
- [ ] Add per-routing shard counts for audit

### MEDIUM PRIORITY
- [ ] Populate denylist patterns for restricted scholarly providers
- [ ] Add gating for ORCID/PII-sensitive fields beyond simple key scrubbing
- [ ] Lint + minimal unit tests for heuristics and path layout
- [ ] Example `dc` workflow on tiny fixtures (dry-run + execute)

## logic_pipeline_v2

### HIGH PRIORITY (Production readiness)
- [ ] hf_logic_filter_worker.py for Common Pile logic subsets with record-level routing
- [ ] formal_logic_worker.py for Lean/Coq/Isabelle/Metamath chunking
- [ ] pdf_logic_worker.py to preserve symbols + proof structure
- [ ] Hook workers into yellow stage after review approval
- [~] YELLOW review plan export (yellow_scrubber.py)
- [ ] CLI to assign reviewers + notes per target
- [ ] Optional dashboard for approvals with evidence previews
- [ ] Unit tests for restriction scans and SPDX normalization
- [ ] Sample queue fixtures for logic targets
- [ ] Smoke test classify -> acquire -> screen -> merge -> catalog on tiny logic fixtures (sorting handled in sorter pipeline)

### MEDIUM PRIORITY (Quality + coverage)
- [ ] Implement record-level license checks for OER/StackExchange/SEP filters
- [ ] Expand restriction phrase library for NoAI/NoTDM language
- [ ] Enforce copyleft segregation in download outputs + catalogs
- [ ] Unicode + symbol normalization per worker
- [ ] Split-aware partitioning with split_group_id for exercises
- [ ] Near-duplicate detection tuned for proofs/SAT-SMT benchmarks

### LOW PRIORITY (Ergonomics + future-proofing)
- [ ] Dockerfile for logic pipeline + example volume mounts
- [ ] Example orchestration (cron/Argo) for recurring runs
- [ ] Publish pinned requirements/lockfile
- [ ] Metrics on queue sizes, download throughput, and review turnaround
- [ ] Alerts for missing license evidence or stalled downloads

## materials_science_pipeline_v2

### HIGH PRIORITY (Production readiness)
- [ ] hf_materials_filter_worker.py for Common Pile materials subsets with record-level routing
- [ ] structure_worker.py for crystallography/structure dumps with schema validation
- [ ] pdf_materials_worker.py to preserve formulas + figures + page ranges
- [ ] Hook workers into yellow stage after review approval
- [~] YELLOW review plan export (yellow_scrubber.py)
- [ ] CLI to assign reviewers + notes per target
- [ ] Optional dashboard for approvals with evidence previews
- [ ] Unit tests for materials restriction scans and SPDX normalization
- [ ] Sample queue fixtures for materials targets
- [ ] Smoke test classify -> acquire -> merge -> catalog on tiny materials fixtures (sorting handled in sorter pipeline)

### MEDIUM PRIORITY (Quality + coverage)
- [ ] Implement record-level license checks for OER/StackExchange filters
- [ ] Expand restriction phrase library for NoAI/NoTDM language
- [ ] Enforce copyleft segregation in download outputs + catalogs
- [ ] Unicode + LaTeX normalization per worker
- [ ] Split-aware partitioning with split_group_id for structure/property splits
- [ ] Near-duplicate detection tuned for materials text and structure fingerprints

### LOW PRIORITY (Ergonomics + future-proofing)
- [ ] Dockerfile for materials pipeline + example volume mounts
- [ ] Example orchestration (cron/Argo) for recurring runs
- [ ] Publish pinned requirements/lockfile
- [ ] Metrics on queue sizes, download throughput, and review turnaround
- [ ] Alerts for missing license evidence or stalled downloads

## math_pipeline_v2

### HIGH PRIORITY (Production readiness)
- [ ] hf_math_filter_worker.py for Common Pile math subsets with record-level routing
- [ ] formal_math_worker.py for Lean/Coq/Agda/Isabelle/Metamath chunking
- [ ] pdf_math_worker.py to preserve equations + page ranges
- [ ] Hook workers into yellow stage after review approval
- [~] YELLOW review plan export (yellow_scrubber.py)
- [ ] CLI to assign reviewers + notes per target
- [ ] Optional dashboard for approvals with evidence previews
- [ ] Unit tests for math restriction scans and SPDX normalization
- [ ] Sample queue fixtures for math targets
- [ ] Smoke test classify -> download -> catalog on tiny math fixtures

### MEDIUM PRIORITY (Quality + coverage)
- [ ] Implement record-level license checks for OER/StackExchange filters
- [ ] Expand restriction phrase library for NoAI/NoTDM language
- [ ] Enforce copyleft segregation in download outputs + catalogs
- [ ] Unicode + LaTeX normalization per worker
- [ ] Split-aware partitioning with split_group_id for exercises
- [ ] Near-duplicate detection tuned for math text/equations

### LOW PRIORITY (Ergonomics + future-proofing)
- [ ] Dockerfile for math pipeline + example volume mounts
- [ ] Example orchestration (cron/Argo) for recurring runs
- [ ] Publish pinned requirements/lockfile
- [ ] Metrics on queue sizes, download throughput, and review turnaround
- [ ] Alerts for missing license evidence or stalled downloads

## metrology_pipeline_v2

### HIGH PRIORITY (Production readiness)
- [ ] nvlpubs/ntrs/usgs/noaa/faa API harvesters + evidence snapshots
- [ ] Record-level license checks for mixed-rights collections
- [ ] Add paging + resume support for large API pulls
- [~] YELLOW review plan export (review_queue.py)
- [ ] CLI to assign reviewers + notes per target
- [ ] Optional dashboard for approvals with evidence previews
- [ ] Unit tests for restriction scans and SPDX normalization
- [ ] Sample queue fixtures for metrology targets
- [ ] Smoke test classify -> acquire -> merge -> catalog on small PDFs (sorting handled in sorter pipeline)

### MEDIUM PRIORITY (Quality + coverage)
- [ ] Improve table serialization (row/column preservation)
- [ ] Improve HTML readability extraction for standards pages
- [ ] Add OCR fallback for scanned PDFs
- [ ] Unicode normalization for metrology symbols (µ, Ω, ±, etc.)
- [ ] Split-aware partitioning with split_group_id for datasets
- [ ] Near-duplicate detection tuned for technical reports

### LOW PRIORITY (Ergonomics + future-proofing)
- [ ] Dockerfile for metrology pipeline + example volume mounts
- [ ] Example orchestration (cron/Argo) for recurring runs
- [ ] Publish pinned requirements/lockfile
- [ ] Metrics on queue sizes, download throughput, and review turnaround
- [ ] Alerts for missing license evidence or stalled downloads

## nlp_pipeline_v2

### HIGH PRIORITY (Production readiness)
- [ ] Flask/FastAPI dashboard for YELLOW review with visual diffs
- [ ] Batch approval/rejection + reviewer assignment/notifications
- [ ] Run-level audit views (surface run IDs, signatures, provenance)
- [ ] Access controls for reviewers and approvers
- [ ] Unit tests for license normalization and resolvers
- [ ] Integration tests for download workers and scrubbers with sample data
- [ ] End-to-end smoke tests (green/yellow/red queues + catalog build)
- [ ] CI pipeline (lint/type checks/tests) with artifact retention
- [ ] Dockerfile + docker-compose for reproducible runs
- [ ] Kubernetes/Helm job templates with secrets/volume wiring
- [ ] Versioned configuration bundles (schemas + denylist + defaults)
- [ ] Publish pinned requirements/lockfiles

### MEDIUM PRIORITY (Quality + coverage)
- [ ] Add ZINC, BindingDB, DrugBank (where licensed), and USPTO pipelines
- [ ] Expand spectrum ingestion for MoNA/GNPS and other MSP/MGF sources
- [ ] Cross-dataset deduplication reports that include spectra + structure IDs
- [ ] Harden SMILES canonicalization and InChIKey validation
- [ ] Molecular property calculation + substructure filtering hooks
- [ ] Performance tuning for large SDF ingestion (Cython/numba optional)
- [ ] Metrics on download times, error rates, queue throughput
- [ ] Prometheus/StatsD exporters and Grafana dashboard examples
- [ ] Slack/email notifications for failures or stalled queues
- [ ] Architecture doc + diagrams
- [ ] Tutorial for adding new targets and resolvers
- [ ] Troubleshooting guide and FAQ for common failures
- [ ] API/programmatic access examples

### LOW PRIORITY (Ergonomics + future-proofing)
- [ ] Streaming processing for large files + memory profiling
- [ ] Optional progress dashboards (CLI + web) for long jobs
- [ ] Configurable caching/prefetch for common resolvers
- [ ] Helm chart hardening (quotas, pod disruption budgets)
- [ ] Airflow/Argo examples for scheduled runs
- [ ] Backup/restore procedures for catalogs and manifests

## physics_pipeline_v2

### HIGH PRIORITY (Production readiness)
- [ ] arxiv_physics_filter_worker.py for physics subsets with record-level routing
- [ ] physics_pdf_worker.py to preserve equations + page ranges
- [ ] Hook workers into yellow stage after review approval
- [~] YELLOW review plan export (yellow_scrubber.py)
- [ ] CLI to assign reviewers + notes per target
- [ ] Optional dashboard for approvals with evidence previews
- [ ] Unit tests for physics restriction scans and SPDX normalization
- [ ] Sample queue fixtures for physics targets
- [ ] Smoke test classify -> download -> catalog on tiny physics fixtures

### MEDIUM PRIORITY (Quality + coverage)
- [ ] Implement record-level license checks for OER/StackExchange filters
- [ ] Expand restriction phrase library for NoAI/NoTDM language
- [ ] Enforce copyleft segregation in download outputs + catalogs
- [ ] Unicode + LaTeX normalization per worker
- [ ] Split-aware partitioning with split_group_id for exercises
- [ ] Near-duplicate detection tuned for physics text/equations

### LOW PRIORITY (Ergonomics + future-proofing)
- [ ] Dockerfile for physics pipeline + example volume mounts
- [ ] Example orchestration (cron/Argo) for recurring runs
- [ ] Publish pinned requirements/lockfile
- [ ] Metrics on queue sizes, download throughput, and review turnaround
- [ ] Alerts for missing license evidence or stalled downloads
