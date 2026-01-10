# Dataset Collector v2 - Repo Update Checklist (Comprehensive)

This checklist merges:
- My repo review notes (architecture + implementation drift)
- The additional Claude 4.5 Opus assessment

Purpose: list every update that should be made to improve correctness, auditability, elegance, DX, and scalability.

Conventions:
- Priority: P0 (must), P1 (next), P2 (quality), P3 (scale), P4 (polish)
- Each item has a short "Done when" acceptance test.

---

## P0 - Correctness, safety, and policy integrity

### P0.1 Make configuration truthfully executable

- [ ] Replace the overloaded "gates" concept with two explicit fields
  - Change in targets YAML:
    - license_gates (license-routing only)
    - content_checks (PII scan, secret scan, dual-use scan, etc.)
  - Update JSON Schemas so unknown values fail validation.
  - Done when: there is no configuration field that implies a check that the code does not run.

- [ ] Add a check registry that executes content checks and records results
  - Create collector_core/checks/ with a registry + base interface.
  - Each check outputs a structured record under _ledger/ (per target).
  - Done when: a run can be audited by reading ledger artifacts only.

- [ ] Implement minimum viable checks (even if heuristic first)
  - PII scan: emails, phone numbers, SSNs, API keys patterns, obvious addresses.
  - Secret scan: common token formats (AWS, GitHub, etc.).
  - Dual-use scan: high-level classifier/keyword rules for disallowed content.
  - Done when: checks can block or quarantine datasets (configurable).

### P0.2 Unify dataset-root behavior across all stages

- [ ] Make dataset_root resolution consistent in classify, acquire, merge, yellow_screen
  - Add --dataset-root to acquire worker and use DATASET_ROOT consistently.
  - Remove stage-specific surprises (raw-root/logs-root only behavior).
  - Done when: one command can run end-to-end using only --dataset-root and --pipeline.

- [ ] Remove or guard default writes to /data/<domain>
  - Require an explicit opt-in flag for /data fallbacks.
  - Done when: CI/dev never writes to /data unless explicitly allowed.

### P0.3 Make classification decisions fully auditable

- [ ] Emit a policy_snapshot.json on every run
  - Include: repo git SHA, collector_core version, license_map hash, denylist hash, schema versions, enabled gates/checks.
  - Done when: every run has a policy snapshot in _ledger/.

- [ ] Record "why" a target became GREEN/YELLOW/RED in machine-readable form
  - Add fields to emitted queue rows: bucket_reason + signals (confidence, restriction hits, evidence status, etc.).
  - Done when: reviewers do not need to grep logs to understand routing.

- [ ] Strengthen evidence snapshot guarantees
  - Normalize evidence fetch status handling so snapshot failures cannot silently pass.
  - Add a strict mode: if snapshot_terms required and fetch fails, force YELLOW and record reason.
  - Done when: the evidence snapshot is always present OR a recorded reason explains why it is missing.

### P0.4 Acquisition and extraction safety hardening

- [ ] Archive extraction safety
  - Block path traversal (.., absolute paths, symlinks), enforce max file count, max extracted size.
  - Add "decompression bomb" protections.
  - Done when: tests demonstrate malicious archives are rejected.

- [ ] Expand SSRF protections
  - Enforce host/IP checks after redirects.
  - Consider DNS rebinding-resistant resolution strategy.
  - Add explicit allowlists for internal mirrors (optional).
  - Done when: redirect-to-private is blocked by default with tests.

- [ ] Enforce per-target max bytes and global per-run byte budgets (if not already)
  - Done when: a misconfigured target cannot accidentally download terabytes.

---

## P1 - Architecture and elegance (finish consolidation)

### P1.1 Remove per-pipeline wrapper duplication

- [ ] Delete per-pipeline wrappers that only set DOMAIN or forward to core
  - Targets: catalog_builder.py, review_queue.py, acquire_worker.py, merge_worker.py, run_pipeline.sh, thin pipeline_driver wrappers.
  - Replace with: dc CLI + PipelineSpec registry + collector_core/generic_workers.py.
  - Done when: adding a new pipeline requires only YAML + a registry entry (no new Python wrapper files).

- [ ] Optional repo layout cleanup
  - Move all targets to pipelines/targets/targets_<domain>.yaml
  - Keep PipelineSpec registry or move specs to pipelines/specs/<domain>.yaml.
  - Done when: pipeline code is centrally located, and pipelines are configuration-only unless custom workers exist.

### P1.2 Remove sys.path hacks and rely on proper packaging

- [ ] Require editable installs for development
  - Docs: pip install -e .
  - Remove sys.path.insert(0, ...) blocks repo-wide (including tests).
  - Done when: imports work without any sys.path mutation.

- [ ] Optional: adopt a src/ layout
  - Done when: accidental local import shadowing is impossible.

### P1.3 Split monolithic core modules into cohesive packages

- [ ] Split pipeline_driver_base.py
  - Suggested modules:
    - evidence/fetcher.py
    - evidence/change_detection.py
    - classification/engine.py
    - queue/emitter.py
    - gates_or_checks/apply.py
  - Done when: each module is small and unit-tested.

- [ ] Split acquire_strategies.py into strategies/
  - Create strategies/registry.py and per-strategy modules (http, git, ftp, zenodo, hf, etc.).
  - Done when: strategies are independently testable and discoverable.

- [ ] Split merge.py into merge/
  - Separate: dedupe, shard, hf integration, contract enforcement.
  - Done when: merge behavior can be benchmarked and tested per component.

### P1.4 Make dc CLI the single source of truth

- [ ] Consolidate CLIs
  - Fold pipeline_cli helpers into dc subcommands.
  - Deprecate/remove extra entrypoints once dc covers them.
  - Done when: docs, notebook, and CI all use dc.

- [ ] Remove deprecated scripts (or isolate under legacy/)
  - Specifically remove run_pipeline.sh clones across pipelines.
  - Done when: there are no maintained-but-deprecated runners.

### P1.5 Reduce duplication in pipeline discovery

- [ ] Ensure there is one authoritative pipeline registry
  - If pipeline_registry.py and pipeline_cli.py overlap, unify helper logic.
  - Done when: pipeline listing, discovery, and default-target resolution is implemented once.

---

## P2 - Quality: typing, testing, and logging

### P2.1 Add typing (incrementally)

- [ ] Add py.typed and run mypy in CI
  - Start non-strict for collector_core, tighten over time.
  - Done when: CI type-checks the core library.

- [ ] Add missing type annotations on hot paths
  - Focus: classification, evidence, acquire strategies, merge, yellow screening.
  - Done when: public APIs have stable signatures.

### P2.2 Expand tests to cover strategy and CLI behavior

- [ ] Strategy unit tests (completeness)
  - For each strategy: success path + key failure modes (timeouts, checksum mismatch, redirect, resume, rate limit).
  - Done when: coverage includes every strategy handler used in production.

- [ ] CLI integration tests
  - Add a small fixture pipeline with 1-2 targets and run dc stages in CI.
  - Done when: CI verifies the end-to-end workflow with artifacts created.

- [ ] Regression tests for dataset_root precedence
  - Done when: dataset_root behavior cannot regress without failing CI.

- [ ] Security regression tests
  - Archive extraction tests + SSRF redirect tests.
  - Done when: protections are enforced by tests.

### P2.3 Structured logging

- [ ] Adopt structured JSON logs
  - Include run_id, domain, target_id, strategy, bytes, duration_ms, and error types.
  - Done when: logs are machine-parsable and easy to aggregate.

- [ ] Ensure secrets/PII never leak into logs
  - Extend existing redaction tests as needed.
  - Done when: any accidental leakage fails tests.

---

## P3 - Performance, observability, and scaling

- [ ] Parallelize evidence fetching during classification
  - Use a bounded thread pool for network-bound evidence fetch.
  - Preserve determinism in outputs.
  - Done when: classify throughput improves without breaking audit logs.

- [ ] Add in-run caching for repeated evidence URLs
  - Done when: repeated URLs are fetched once per run.

- [ ] Add lightweight run metrics
  - Output metrics.json under _ledger/ (counts, bytes, timings).
  - Optional: Prometheus / OpenTelemetry integration later.
  - Done when: every run emits metrics.

---

## P4 - Config ergonomics, docs, and repo hygiene

- [ ] Simplify targets YAMLs
  - Move shared resolver configs to a shared file.
  - Use YAML anchors for repeated patterns.
  - Done when: per-domain targets files shrink and are easier to review.

- [ ] Tighten schemas for provenance
  - Require source URLs, evidence URLs, and dataset version/tag where applicable.
  - Done when: provenance omissions are caught at validation time.

- [ ] Update README and docs to match reality
  - Describe: install, set dataset_root, run dc pipeline, review YELLOW, interpret ledger artifacts.
  - Done when: a new contributor can run one pipeline end-to-end without guesswork.

- [ ] Update the Jupyter notebook to call dc only
  - Done when: notebook is aligned with the canonical CLI.

---

## Recommended migration sequence (to avoid breaking users)

1. Add new fields (license_gates + content_checks) while still reading old gates_override.
2. Provide a migration script to rewrite targets YAMLs.
3. Update schemas to allow both, then later remove the old field.
4. Remove deprecated wrappers and scripts after dc CLI coverage is complete.

