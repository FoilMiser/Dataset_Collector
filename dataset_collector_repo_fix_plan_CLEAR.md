# Dataset Collector v2 — Repo Fix/Improve Plan (Clear Version)
*Updated: 2026-01-01 (from prior plan generated 2025-12-31)*

This document is a **do-this-next checklist** for getting the repo to reliably deliver its stated promise:

> **Collect → License-screen (green/yellow/red) → Produce training-ready combined JSONL shards → Catalog + audit trail**

It’s written to minimize ambiguity: **each item has “What to change”, “Where”, and “Definition of Done”.**

---

## Table of contents
1. [One-page plan](#one-page-plan)
2. [Definition of Done](#definition-of-done)
3. [Decisions you must make](#decisions-you-must-make)
4. [P0 blockers](#p0--blockers-do-these-first)
5. [P1 correctness and safety](#p1--correctness-safety-and-auditability)
6. [P2 maintainability and drift reduction](#p2--maintainability-and-drift-reduction)
7. [P3 scalability](#p3--scalability-and-performance)
8. [PR plan](#pr-plan-recommended-pr-slices)
9. [Runbook: verify end-to-end](#runbook-verify-end-to-end-after-each-pr)

---

## Snapshot (for context)
- Pipelines: **18** (`*_pipeline_v2`)
- Targets (all YAMLs): **None** total — **None enabled**, **None disabled**
- Repo health checks (local) previously reported: ✅ validator / ✅ pytest / ✅ preflight (with external CLI warnings)

### Enabled download strategies in targets
| Strategy | Enabled targets |
|---|---:|
| `http` | 109 |
| `huggingface_datasets` | 38 |
| `git` | 37 |
| `ftp` | 5 |
| `s3_public` | 1 |
| `zenodo` | 1 |
| `s3_sync` | 1 |
| `aws_requester_pays` | 1 |
| `figshare` | 1 |

---

## One-page plan
If you do nothing else, do these **in order**:

1. **Add canonicalization (raw → JSONL shards)** so non-JSONL sources actually flow into `screened_yellow/` and `combined/`.
2. **Make `validate_repo.py` strategy-complete** (unknown strategy = error; missing required keys = error).
3. **Make `pipeline_map` safe by default** (no running with placeholder paths).
4. **Unify stage naming** (`screen_yellow` vs `yellow_screen`) and update docs to match the orchestrator.
5. **Add one tiny end-to-end fixture test** proving HF + HTTP sources produce combined shards.

Everything else is valuable, but those five turn the repo from “download tools” into a **reliable corpus builder**.

---

## Definition of Done
Call the repo “done enough” when a newcomer can do **one** command and get:

- `raw/` contains acquired artifacts
- `screened_yellow/**/shards/*.jsonl.gz` exists (or `screened_green/**/shards` if you split pools)
- `combined/**/shards/*.jsonl.gz` exists
- `catalog/` exists with per-target and per-record provenance
- ledgers/queues clearly show GREEN/YELLOW/RED routing
- `python tools/validate_repo.py --repo-root .` catches misconfigured enabled targets

---

## Decisions you must make
These aren’t optional — the repo can’t be “clear” until you pick and document the answers.

### D1 — Where does canonical JSONL get produced?
You must choose one:

**Option A (recommended): explicit stage**
- Add `canonicalize_*` stage(s) after `acquire_*`.
- Pros: clean separation, easier to test, clearer contract.
- Cons: another stage to maintain.

**Option B: canonicalize inside acquisition**
- Each strategy writes `raw/.../shards/*.jsonl.gz` during download.
- Pros: fewer stages.
- Cons: acquisition code becomes a “god worker”; harder to reason about.

**Decision:** pick A or B, then update `docs/output_contract.md` and `tools/build_natural_corpus.py` accordingly.

### D2 — What is the *canonical record schema*?
Write it once (and enforce it in code + tests). Minimum recommended fields:

- `source_id` (stable internal id)
- `source_url`
- `retrieved_utc`
- `license_pool` (`green|yellow|red`)
- `license_spdx` (or `license_text` if unknown)
- `content_sha256`
- `text` (or `content`)

**Decision:** define the fields + types in one place and ensure every shard conforms.

---

## P0 — Blockers (do these first)

### P0.1 — Missing canonicalization path for most acquired data
**Why it matters:** `merge_worker.py` / `yellow_screen_worker.py` primarily process `*.jsonl(.gz)`, but many strategies produce raw artifacts (HF `save_to_disk`, tarballs, PDFs, zips). Those targets “acquire successfully” but **never enter screening/merge**.

**Symptoms you can observe:**
- `raw/` fills up, but `screened_yellow/` and `combined/` stay empty or incomplete.

**What to change (minimum viable fix):**
- Add a canonicalization step that converts raw artifacts → canonical `jsonl.gz` shards.

**Where:**
- New worker: `canonicalize_worker.py` (or `canonicalize_green_worker.py` / `canonicalize_yellow_worker.py`)
- Orchestrator: `tools/build_natural_corpus.py` stage list
- Contract: `docs/output_contract.md`

**Steps (Option A recommended):**
1. Create a shared shard writer utility (e.g., `collector_v2/io/shards.py`) with:
   - `write_jsonl_gz(records, out_path, shard_size=...)`
   - deterministic naming and atomic writes (temp file → rename)
2. Implement canonicalizers per strategy:
   - `huggingface_datasets`: iterate dataset rows and emit `text` + metadata to shards (streaming if possible)
   - archive formats: enumerate + extract allowable files, parse to text, chunk, shard
   - PDF/HTML: parse to text, chunk
3. Update orchestrator stage graph:
   - `classify → acquire_* → canonicalize_* → screen_yellow → merge → catalog`

**Definition of Done:**
- A `huggingface_datasets` target produces at least one `*.jsonl.gz` shard that appears in `combined/**/shards/`.
- Running a tiny fixture (see runbook) results in non-empty combined shards.

---

### P0.2 — `validate_repo.py` doesn’t validate *all* supported strategies
**Why it matters:** enabled targets can pass validation but fail at runtime if strategy config is incomplete.

**What to change:**
- Make validation strategy-aware and complete.

**Where:**
- `tools/validate_repo.py`
- New: `tools/strategy_registry.py` (or `policies/strategy_registry.yaml`)
- `tools/preflight.py` should share the same registry (no duplicated truth).

**Steps (recommended):**
1. Create a registry mapping:
   - strategy → required keys
   - strategy → optional keys
   - strategy → required external tools (if any)
2. Validator rules:
   - enabled target with unknown strategy → **error**
   - enabled target missing required keys → **error**
   - disabled targets can be lenient (warning-only)

**Definition of Done:**
- Any enabled target with missing config fails validation *before* running acquisition.
- `preflight.py` reads the same registry for tool dependencies.

---

### P0.3 — Stage naming drift across docs/tooling (`screen_yellow` vs `yellow_screen`)
**Why it matters:** small naming drift causes user confusion and wrong CLI usage.

**What to change:**
- Pick **one** stage name and apply it everywhere.
- Recommendation: keep orchestrator name `screen_yellow` and rename docs to match.

**Where:**
- `docs/PIPELINE_V2_REWORK_PLAN.md`
- per-pipeline README(s)
- any CLI examples in docs

**Definition of Done:**
- Grep for `yellow_screen` (or the old name) in `docs/` returns zero *unless it’s a filename* you intentionally keep.

---

### P0.4 — `tools/pipeline_map.yaml` is a sample but treated like default
**Why it matters:** easy to run with placeholder paths and “successfully” write data somewhere unintended.

**What to change:**
- Make “sample vs local” explicit and fail fast if placeholder values exist.

**Where:**
- `tools/build_natural_corpus.py`
- `tools/pipeline_map.sample.yaml`
- introduce `tools/pipeline_map.local.yaml` (gitignored) as the normal local default

**Definition of Done:**
- Running without a valid map exits with a clear error (no silent placeholder execution).

---

## P1 — Correctness, safety, and auditability

### P1.1 — Add hard limits to prevent accidental massive downloads
**What to change:**
- Add max-bytes limits per-run and per-target with explicit override flags.

**Where:**
- `acquire_worker.py` (and any strategy handlers)
- `tools/build_natural_corpus.py` CLI args

**Definition of Done:**
- A target exceeding `max_bytes_per_target` refuses to run unless `--allow-huge-downloads` is explicitly passed.

---

### P1.2 — Document and enforce RED routing
**What to change:**
- Ensure RED outputs are first-class in docs, catalogs, and ledgers.

**Where:**
- `docs/output_contract.md`
- catalog builder outputs (ensure red stats are visible)

**Definition of Done:**
- RED is visible in the final catalog summary and not silently dropped.

---

### P1.3 — Make yellow signoff behavior deterministic
**What to change:**
- If `require_yellow_signoff: true`, merge must exclude yellow unless signoff exists, and the “why” is surfaced.

**Where:**
- `yellow_screen_worker.py`
- `merge_worker.py`
- `tools/validate_repo.py` (validate signoff schema if present)

**Definition of Done:**
- When signoff required and missing, merge produces **0** yellow-included records and outputs a clear “pending review” summary.

---

## P2 — Maintainability and drift reduction

### P2.1 — Deduplicate shared code across pipelines
**What to change:**
- Move shared logic into a single importable package.

**Where:**
- New package: `collector_v2/` (name flexible)
- Pipelines become thin wrappers around shared workers and their configs.

**Definition of Done:**
- One bugfix in shared license normalization changes behavior across all pipelines without copying files.

---

### P2.2 — Centralize policies and schemas
**What to change:**
- Put defaults in one place and allow per-pipeline overrides only where necessary.

**Where:**
- `policies/license_map.yaml`, `policies/denylist.yaml`
- `schemas/field_schemas.yaml`

**Definition of Done:**
- A policy change is made once and affects all pipelines by default.

---

### P2.3 — Fix doc/changelog drift and remove dead references
Known drift items to fix:
- `docs/PIPELINE_V2_REWORK_PLAN.md` references a missing `repo_update_plan_dataset_collector_v2.md`
- `CHANGELOG.md` has an incorrect path `docs/docs/...`
- `CHANGELOG.md` mentions root `requirements.txt` though repo uses `.in/.lock`
- queue-root examples in `docs/yellow_review_workflow.md` are confusing

**Definition of Done:**
- No doc references missing files; changelog paths reflect reality.

---

### P2.4 — Make tests pipeline-agnostic and expand coverage
**What to change:**
- Tests should import from shared package, not a single pipeline.

**Definition of Done:**
- Add at least:
  - 1 validator test for each supported strategy type
  - 1 end-to-end fixture test that produces `combined/` shards

---

## P3 — Scalability and performance

### P3.1 — `merge_worker.py` dedupe is memory-bound
**What to change:**
- Replace in-memory `set()` with a bounded approach:
  - hash-prefix bucketing, or
  - external sort + unique, or
  - DuckDB/SQLite keyed dedupe

**Definition of Done:**
- Merge completes on large shard counts without unbounded RAM growth.

---

## PR plan (recommended PR slices)

### PR1 — Quick wins (clarity + safety)
- Stage naming consistency in docs
- Changelog/doc drift fixes
- Safe pipeline_map behavior (fail fast)
- Strategy registry skeleton + validator unknown-strategy errors

### PR2 — Core functionality: canonicalization + fixture runbook
- Canonical record schema definition
- Canonicalization stage (start with HF + HTTP)
- Update output contract
- Add end-to-end tiny fixture test

### PR3 — Validation completeness + preflight unification
- Fill registry for all supported strategies
- Make preflight read registry
- Add strategy-specific validator tests

### PR4 — Shared package refactor
- Move common IO/license/evidence logic into `collector_v2/`
- Update pipelines to import shared code
- Remove duplication

### PR5 — Scale improvements
- Merge dedupe redesign
- Ledger compression/batching improvements

---

## Runbook: verify end-to-end after each PR
Run these from repo root:

1) **Static checks**
```bash
python tools/validate_repo.py --repo-root .
python -m ruff check .
pytest -q
```

2) **Tiny end-to-end fixture**
- Run with a small fixture targets YAML containing:
  - 1 HTTP text target
  - 1 HF dataset target (tiny)
- Expect:
  - `raw/` has artifacts
  - `screened_yellow/**/shards/*.jsonl.gz` exists
  - `combined/**/shards/*.jsonl.gz` exists
  - `catalog/` summarizes counts + red/yellow/green routing

> Add this fixture to tests so it stays true forever.

---

## Notes
- This “clear version” intentionally repeats key decisions and “definition of done” so nothing is left to interpretation.
- Keep the prior plan as historical context, but treat this file as the *operational checklist*.

