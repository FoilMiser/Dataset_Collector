# Dataset Collector Repo — Fix Checklist

> Numbered, prioritized checklist of issues that still need fixing.

## P0 — Fix before you trust the corpus output

1. **Eliminate `validate_repo` warnings for HuggingFace targets (canonical text field selection).**  
   - Add `canonicalize.text_field_candidates` to every HuggingFace target (tailor per dataset where possible).  
   - Goal: avoid noisy “join all strings” fallbacks and stabilize extraction quality.

2. **Make the output JSONL record schema explicit (stop relying on “whatever the code emits”).**  
   - Update `docs/output_contract.md` to define the **JSONL record contract**:
     - required fields + types
     - provenance fields (dataset id, split, config, row id, etc.)
     - license fields (resolved SPDX / license profile)
     - evidence fields (URLs, snapshots, reviewer notes if applicable)
     - hashing fields (content hash, normalized hash if used)
     - routing fields (pool, pipeline, target name, timestamps)

3. **Add/strengthen schema validation at merge time (fail fast).**  
   - Validate merged JSONL records against the schema.  
   - Ensure missing required fields or malformed types cause a clear error and stop the run.

4. **Clarify “lock file” truthfulness (reproducibility risk).**  
   - Either:
     - generate a **fully resolved lock** (preferred), **or**
     - rename current “lock” files to reflect they’re top-level pins/constraints (not a full lock).

## P1 — Reduce future breakage and maintenance burden

5. **Refactor shared pipeline code into a common core module.**  
   - Move duplicated logic (drivers/workers/helpers) into something like `collector_core/`.  
   - Keep each `*_pipeline_v2/` mostly as config + thin adapters.

6. **Standardize behavior across all `*_pipeline_v2` pipelines.**  
   - Align: CLI args/env vars, folder outputs, manifest + ledger emission, logging format, error handling, retries/backoff (if any).

7. **Normalize version labels and stale copy/paste headers.**  
   - Remove/replace inaccurate “v1.0” references in docstrings, comments, requirements headers, and docs.

## P2 — CI and developer experience improvements

8. **Add a strict mode for `tools.validate_repo` (or CI option).**  
   - After warnings are cleared, allow CI to fail on newly introduced warnings.

9. **Increase test coverage for high-consequence paths.**  
   - Tests for:
     - canonicalization selection (`text_field_candidates`)
     - license mapping + denylist decisions
     - merge output stability and schema compliance

10. **Improve “one-command run” documentation (Windows happy path + clean-room rerun).**  
   - Document:
     - expected output folders
     - where logs/manifests/ledgers land
     - how to rerun from a clean slate (what to delete vs keep)

## Nice-to-have polish

11. **Add `__init__.py` where it improves tooling/import clarity (optional).**

12. **Add a `CHANGELOG.md` (or `docs/release_notes.md`) for traceable repo evolution.**

13. **Add a “known limitations” section** (yellow screening boundaries, excluded sources, non-goals).

---
Generated: 2026-01-04
