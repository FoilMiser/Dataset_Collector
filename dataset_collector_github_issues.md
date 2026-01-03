# Dataset Collector Repo — PR-Sliced GitHub Issues

Copy/paste these issues into GitHub (one issue per PR). Each issue includes **why**, **scope**, **tasks**, **files**, and **acceptance criteria**.

---

## Issue 1 — Fix tool entrypoints + README accuracy (no behavioral changes)

**Type:** Bug / Docs  
**Priority:** P0

### Why
Running `python tools/*.py` from repo root can fail due to import path/module resolution. README also references a pipeline layout that doesn’t match the current repo.

### Scope
- Make `tools/preflight.py` + `tools/validate_repo.py` runnable both as scripts and modules.
- Update README to match the correct invocation + current pipeline layout.
- Remove or fix doc references to missing files.

### Tasks
- [ ] Add repo-root sys.path bootstrap (same pattern as `tools/build_natural_corpus.py`) to:
  - [ ] `tools/preflight.py`
  - [ ] `tools/validate_repo.py`
- [ ] Update `README.md`:
  - [ ] Use `python -m tools.preflight ...` and `python -m tools.validate_repo ...` as the recommended commands
  - [ ] Remove/replace pipeline layout text claiming `src/` and `configs/` if repo is flat
- [ ] Fix docs referencing missing in-repo plan file:
  - [ ] Either add the referenced plan file to `docs/`
  - [ ] Or update/remove the reference so it points to an existing file

### Files likely touched
- `tools/preflight.py`
- `tools/validate_repo.py`
- `README.md`
- `docs/PIPELINE_V2_REWORK_PLAN.md`
- `docs/` (add/rename a plan doc if needed)

### Acceptance criteria
- From repo root, all of these succeed:
  - [ ] `python -m tools.preflight --pipeline-map tools/pipeline_map.yaml`
  - [ ] `python tools/preflight.py --pipeline-map tools/pipeline_map.yaml`
  - [ ] `python -m tools.validate_repo --root .`
  - [ ] `python tools/validate_repo.py --root .`
- [ ] README commands work verbatim
- [ ] Docs contain no dead references to files that aren’t in the repo

---

## Issue 2 — Repo hygiene: remove caches + block pycache from PRs/zips

**Type:** Chore  
**Priority:** P1

### Why
`__pycache__/`, `*.pyc`, and `.pytest_cache/` are leaking into repo zips / artifacts and can cause noisy diffs and confusion.

### Scope
- Enhance cleanup tooling
- Add a CI guard to prevent cache artifacts from creeping back in

### Tasks
- [ ] Extend cleanup tool to remove:
  - [ ] `.pytest_cache/`
  - [ ] `__pycache__/`
  - [ ] `*.pyc`
- [ ] Add CI step that fails if cache artifacts exist after tests/lint:
  - [ ] search for `__pycache__`, `.pytest_cache`, `*.pyc` and fail job if found

### Files likely touched
- `tools/clean_repo_tree.py` (or equivalent cleaner)
- `.github/workflows/*` (add guard step)

### Acceptance criteria
- [ ] Running `python -m tools.clean_repo_tree --yes` removes caches in a test tree
- [ ] CI fails if `__pycache__` / `.pytest_cache` / `*.pyc` exist in workspace after pipeline runs

---

## Issue 3 — Canonicalization: YELLOW screening can ingest HF `save_to_disk` datasets

**Type:** Feature  
**Priority:** P0

### Why
`screen_yellow` currently only consumes `.jsonl/.jsonl.gz`. Many “acquired” YELLOW datasets are Hugging Face `save_to_disk()` directories, so they never get screened → never produce `screened_yellow` shards.

### Scope
- Extend `yellow_screen_worker.py` across all v2 pipelines to ingest HF-saved datasets and produce canonical `*.jsonl.gz` shards.

### Tasks
- [ ] In each `*_pipeline_v2/yellow_screen_worker.py`:
  - [ ] Detect HF saved datasets under `raw/yellow/**/hf_dataset` (and/or `split_*` dirs)
  - [ ] Load with `datasets.load_from_disk`
  - [ ] Extract canonical records and write shards to `screened_yellow/.../shards/*.jsonl.gz`
  - [ ] Fallback if no `text` field:
    - [ ] join string fields, else JSON-stringify row
  - [ ] Preserve existing deny/length logic + ledger outputs
- [ ] Add tests:
  - [ ] Create tiny `datasets.Dataset.from_dict`, save to temp raw/yellow hf path
  - [ ] Run one pipeline’s yellow screening in subprocess
  - [ ] Assert shards exist + records conform to schema

### Files likely touched
- `*_pipeline_v2/yellow_screen_worker.py` (bulk)
- `tests/*` (new tests)
- `docs/output_contract.md` (optional clarifying note)

### Acceptance criteria
- [ ] With HF `save_to_disk` present in `raw/yellow/...`, `screen_yellow --execute` generates:
  - [ ] `screened_yellow/**/shards/*.jsonl.gz`
  - [ ] ledger entries indicating processed record counts

---

## Issue 4 — Green ingestion: `merge` can consume HF `save_to_disk` datasets too

**Type:** Feature  
**Priority:** P0  
**Depends on:** Issue 3 (recommended, but not strictly required)

### Why
Even GREEN sources frequently land as HF saved datasets. `merge` currently only merges `.jsonl*`, so combined output can be empty despite successful acquisition.

### Scope
- Extend `merge_worker.py` across all v2 pipelines to ingest HF saved datasets in `raw/green/...` and emit canonical combined shards.

### Tasks
- [ ] In each `*_pipeline_v2/merge_worker.py`:
  - [ ] Continue supporting `.jsonl/.jsonl.gz` sources (existing)
  - [ ] Add ingestion path for HF saved datasets (`datasets.load_from_disk`)
  - [ ] Convert rows to canonical records and merge into `combined/**/shards/*.jsonl.gz`
  - [ ] For non-ingestible formats, write a skip ledger row with reason
- [ ] Update docs:
  - [ ] `docs/output_contract.md` to state `merge` consumes JSONL **and HF saved datasets**

### Files likely touched
- `*_pipeline_v2/merge_worker.py` (bulk)
- `docs/output_contract.md`

### Acceptance criteria
- [ ] Enable one GREEN HF target and run end-to-end: combined shards are produced
- [ ] Dedupe remains deterministic using `hash.content_sha256` (or current hash scheme)
- [ ] Non-ingestible formats produce explicit “skipped” ledger entries (not silent drops)

---

## Issue 5 — Add target-level canonicalize hints (minimal config, big clarity)

**Type:** Enhancement  
**Priority:** P1

### Why
HF datasets vary widely in column names (`prompt`, `question`, `instruction`, etc.). Heuristics alone will be brittle. Optional target hints prevent garbage extraction and empty outputs.

### Scope
- Support optional per-target `canonicalize` config:
  - `text_field_candidates`
  - `max_chars` (optional)
- Wire it into both YELLOW screening and GREEN merge.

### Tasks
- [ ] Extend parsing so targets can include:
  - [ ] `canonicalize.text_field_candidates`
  - [ ] `canonicalize.max_chars` (optional)
- [ ] Update `yellow_screen_worker.py` to honor:
  - [ ] `canonicalize.*` (in addition to any existing `yellow_screen.*`)
- [ ] Update `merge_worker.py` to honor:
  - [ ] `canonicalize.*` (same behavior as yellow)
- [ ] Add validator warnings:
  - [ ] If enabled target uses HF strategy and no `canonicalize.text_field_candidates`, emit a warning (not error)
- [ ] Add hints for a handful of high-value targets in targets YAMLs (start small)

### Files likely touched
- `*_pipeline_v2/*targets*.yaml` (selected)
- `*_pipeline_v2/yellow_screen_worker.py`
- `*_pipeline_v2/merge_worker.py`
- `tools/validate_repo.py`

### Acceptance criteria
- [ ] Validator warns (actionably) about enabled HF targets lacking canonicalize hints
- [ ] Targets with hints reliably produce readable canonical `text`

---

## Issue 6 — Merge dedupe scalability (avoid RAM cliff)

**Type:** Performance  
**Priority:** P2  
**Depends on:** Issue 4 (or implement in parallel)

### Why
Current merge dedupe keeps a Python `set()` of hashes, which can blow up memory on real corpora.

### Scope
Implement a bounded-memory dedupe strategy.

### Options (choose one)
- Hash prefix bucket partitioning (streaming per-bucket sets)
- SQLite/DuckDB keyed dedupe index
- Bloom filter + optional second pass verification

### Tasks
- [ ] Choose dedupe strategy and document tradeoffs
- [ ] Implement in `merge_worker.py` with same output determinism guarantees
- [ ] Add a stress-ish test (simulated many records) to prove no unbounded memory behavior
- [ ] Ensure ledger outputs still reflect dedupe stats correctly

### Files likely touched
- `*_pipeline_v2/merge_worker.py`
- `tests/*`
- `docs/output_contract.md` (optional)

### Acceptance criteria
- [ ] Merge completes on large simulated input without runaway memory
- [ ] Combined shards remain deterministic and valid
- [ ] Dedupe stats are recorded in ledgers

---

## Issue 7 — End-to-end local smoke test (CI proves the contract)

**Type:** Test  
**Priority:** P1  
**Depends on:** Issues 3–4 (recommended)

### Why
CI currently tests style + unit behavior, but not “does a pipeline produce canonical combined shards.”

### Scope
Add a minimal end-to-end test that:
- constructs tiny local raw inputs (no network)
- runs a pipeline stage chain
- asserts outputs exist and match schema

### Tasks
- [ ] Create a fixture that builds a temporary output tree with:
  - [ ] a tiny HF saved dataset under raw/green or raw/yellow
  - [ ] minimal manifests needed by workers
- [ ] Run stages (subprocess is fine):
  - [ ] `screen_yellow` (if testing yellow path)
  - [ ] `merge` (must test combined output)
- [ ] Assert:
  - [ ] `combined/**/shards/*.jsonl.gz` exists
  - [ ] record schema includes required keys (`text`, `source`, `hash`, etc.)

### Files likely touched
- `tests/test_end_to_end_pipeline_contract.py` (or similar)
- maybe one pipeline’s config or helper module if needed

### Acceptance criteria
- [ ] CI passes the smoke test on Windows + Ubuntu
- [ ] Smoke test does not require network access
- [ ] Smoke test fails loudly if combined output contract breaks

---

# Suggested merge order

1. **Issue 1** (entrypoints + README)  
2. **Issue 2** (hygiene guardrails)  
3. **Issue 3** (YELLOW HF → screened shards)  
4. **Issue 4** (GREEN HF → combined shards)  
5. **Issue 7** (E2E contract smoke test)  
6. **Issue 5** (canonicalize hints + validator warnings)  
7. **Issue 6** (scalable dedupe)

---

# Definition of Done (for the full series)

You’re done when:

- README commands work as written
- `screen_yellow` produces canonical shards for HF saved datasets
- `merge` produces combined shards for HF saved datasets
- `validate_repo` warns you when an enabled target won’t flow into combined
- CI prevents cache artifacts creeping back in
- CI has at least one local E2E smoke test proving the output contract
