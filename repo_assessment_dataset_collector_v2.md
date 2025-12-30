# Dataset Collector + License Screening Repo — Assessment

_Assessed from the uploaded `Dataset_Collector-main (8).zip`._

## Snapshot summary

**Overall:** this version looks **good and close to production-usable** for an ethical “GREEN / YELLOW / RED” corpus build.

What’s strongest is the **conservative compliance posture** and **auditability**: per-target license evidence snapshots, SPDX resolution with confidence, restriction phrase scans, denylist gating with severity, and a YELLOW signoff workflow. The on-disk layout is straightforward and reproducible (raw pools → screened_yellow shards → combined shards + ledgers + manifests).

I ran the repo’s own checks locally:
- `tools/validate_repo.py` ✅ passed
- `tools/preflight.py` ✅ passed

The main remaining issues are **documentation drift** (a referenced-but-missing plan doc; READMEs that describe a folder that the code no longer creates) and **repo hygiene** (committed `__pycache__/` and `.pyc`).


## What’s working really well

### 1) The v2 pipeline pattern is consistent across domains
Each `*_pipeline_v2` directory follows the same stage pattern and naming:

1. **Classify** (`pipeline_driver.py`): evidence snapshots + SPDX resolution + restriction/denylist checks; emits GREEN/YELLOW/RED queue JSONL.
2. **Acquire** (`acquire_worker.py`): downloads into `raw/{green|yellow}/{license_pool}/{target_id}/...` (dry-run by default).
3. **Screen YELLOW** (`yellow_screen_worker.py`): canonicalizes records, enforces “pitch if unclear,” writes shards and ledgers.
4. **Merge** (`merge_worker.py`): merges GREEN + screened YELLOW into `combined/{pool}/shards/...` with hash dedupe + index ledger.
5. **Catalog** (`catalog_builder.py`): summarizes counts/bytes/shards and points to ledgers/manifests.

That’s a defensible workflow for license-sensitive data collection.

### 2) Audit trail primitives are in the right place
You have the right “paper trail” surfaces:

- `_manifests/{target_id}/`: evidence snapshots + review signoffs.
- `_ledger/`: append-only, stage-oriented JSONL (pass/pitch ledgers, shard indexes).
- `_pitches/`: sampled pitched items for fast manual inspection.

### 3) Tooling / orchestration is pragmatic
- `tools/build_natural_corpus.py` + `tools/pipeline_map*.yaml` gives a single entrypoint for running “all pipelines” into a single destination root.
- `tools/patch_targets.py` makes targets portable (templated `globals.*_root` → concrete paths).
- `tools/validate_repo.py` and `tools/preflight.py` catch configuration mistakes early.


## High-priority fixes (recommended next)

### A) Remove committed build artifacts (`__pycache__`, `*.pyc`) and add a `.gitignore`
There are multiple `__pycache__/` directories and compiled `*.pyc` files inside pipeline folders. Those should never ship in the repo.

**Actions**
- Delete all `__pycache__/` directories and `*.pyc` files.
- Add a top-level `.gitignore` that ignores at least:
  - `__pycache__/`, `*.pyc`, `.venv/`, `.ipynb_checkpoints/`
  - dataset outputs: `raw/`, `screened_yellow/`, `combined/`, `_ledger/`, `_pitches/`, `_queues/`, `_catalogs/`, `_manifests/`, `_logs/`

### B) Fix or remove references to `PIPELINE_V2_REWORK_PLAN.md`
Many pipeline READMEs and `run_pipeline.sh` comments refer to `PIPELINE_V2_REWORK_PLAN.md`, but the file is missing.

**Actions**
- Add that document at repo root (or `docs/`) and update references, **or**
- Replace references with the pipeline-specific adaptation docs you do have, **or**
- Remove the references.

### C) Update READMEs to match the actual output contract
`tools/init_layout.py` creates `raw/`, `screened_yellow/`, and `combined/` (plus the `_...` folders). Many pipeline READMEs still document a `screened/` folder.

**Actions**
- Update every pipeline README “Directory layout” section to match `docs/output_contract.md` and `tools/init_layout.py`.
- Remove `screened/` references unless you intend to re-introduce it.

### D) Normalize `updated_utc` fields
Many YAML files have future-dated `updated_utc` (e.g., 2026) and inconsistent formats (date-only vs full ISO).

**Actions**
- Use real timestamps (or delete the field if it’s not being used).
- Standardize on ISO format: `YYYY-MM-DDTHH:MM:SSZ`.

### E) Root README “expected structure” is outdated
Root README says pipelines have `src/` and `configs/`, but the actual v2 pipelines are mostly flat (scripts + YAML in the pipeline directory).

**Actions**
- Update the root README to match the current on-disk structure.


## Medium-priority improvements

### 1) Clarify (or implement) a canonical “build/normalize” step
Right now, `merge_worker.py` only merges canonical JSONL records it can find under `raw/green/**.jsonl*` plus `screened_yellow/*/shards/*.jsonl*`. If a GREEN acquisition lands as zip/parquet/HF dataset caches, it won’t appear in `combined/` without an explicit conversion step.

**Options**
- Add a `green_screen_worker.py` analogous to `yellow_screen_worker.py`, or
- Add a generic `build_worker.py` that converts known formats into the canonical record schema, or
- Document clearly that only GREEN targets that already emit canonical JSONL will be merged.

### 2) Dependency story: align `environment.yml`, root requirements, and per-pipeline requirements
Your README correctly says to install per-pipeline `requirements.txt`, but `environment.yml` installs only the root `requirements.txt`, which is not a union of the pipeline requirements (many pipelines require `datasets`, `pyarrow`, `boto3`, etc.).

**Actions**
- Rename root requirements to something like `requirements-core.txt`, and add `requirements-all.txt` (union), or
- Update `environment.yml` to install the correct set for your intended “run all pipelines” path.

### 3) Gates: either enforce consistently or simplify
Targets carry a gates model (`default_gates`, `gates_override`, `gates_catalog`). Only some gates affect behavior today. Consider either implementing the rest (so “gates” means something) or trimming the mechanism to avoid implying controls that aren’t active.

### 4) Security hardening for Hugging Face dataset downloads
Some Hugging Face datasets require `trust_remote_code=True`. Even if it’s optional in the config, it’s a footgun.

**Recommendation**
- Default `trust_remote_code` to false.
- If a target needs it, force YELLOW + require signoff.

### 5) CI / smoke tests
Add a minimal CI job that runs:
- `python tools/validate_repo.py --root .`
- `python tools/preflight.py --repo-root . --pipeline-map tools/pipeline_map.yaml`

This will catch broken references, schema drift, and unsupported strategies early.

## Low-priority polish

- Add a top-level `LICENSE` file for the codebase (even if “all rights reserved”).
- Add a `CONTRIBUTING.md` that states “do not commit outputs” and how to add new targets safely.
- Consider extracting shared pipeline logic into a small internal module to reduce duplication (optional; keep duplication if it helps clarity and review).

## Quick fix order checklist

1. Delete `__pycache__/` + `*.pyc` and add `.gitignore`.
2. Fix the missing `PIPELINE_V2_REWORK_PLAN.md` references.
3. Update pipeline READMEs to match `docs/output_contract.md` (remove `screened/`).
4. Normalize or remove `updated_utc` fields.
5. Align dependency setup (`environment.yml` vs per-pipeline requirements).
6. Decide how GREEN non-JSONL sources become canonical records (build step vs documented limitation).
