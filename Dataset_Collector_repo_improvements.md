# Dataset Collector v2 — Improvements & Fixes (In-Depth)

_Last reviewed: 2025-12-30_

This document is a **repo-wide punch list** for turning the current Dataset Collector v2 repository into a:
- reliably runnable (Windows-first) dataset acquisition + license screening tool,
- reproducible (dependencies + outputs),
- maintainable (linting, testing, shared core),
- audit-friendly (license evidence, review workflow, manifests).

---

## Repo snapshot (what’s here today)

- Pipelines: **18** `*_pipeline_v2` directories
- Targets: **336** total targets across all `targets_*.yaml`
- Enabled: **194** targets enabled
- Enabled acquisition strategy mix (enabled targets):
  - `http`: 109
  - `huggingface_datasets`: 38
  - `git`: 37
  - `ftp`: 5
  - `s3_public`: 1
  - `zenodo`: 1
  - `s3_sync`: 1
  - `aws_requester_pays`: 1
  - `figshare`: 1
- CI: `pytest` passes; `tools/validate_repo.py` reports 0 errors/0 warnings; `tools/preflight.py` passes with warnings if AWS CLI is missing.

---

## Priority overview

### P0 (must-fix: affects “it runs” / “it works as documented”)
1. **`run_all.py` is currently broken** due to tool import layout (module imports fail).
2. **Jupyter notebook is broken** (`repo_root` never gets defined because of a placeholder `...`).
3. **Tool invocation inconsistency** (`validate_repo.py` uses `--root` while others use `--repo-root`) adds friction and creates “works in CI but not locally” confusion.

### P1 (high leverage: reproducibility + maintenance + correctness hardening)
4. Make dependency management **actually reproducible** (true lockfile strategy).
5. Expand Ruff/lint coverage to include **pipeline code**, not just `tools/` and `tests/`.
6. Align and standardize **schema versions & metadata** across pipeline YAMLs (`license_map`, `denylist`, `field_schemas`).
7. Tighten **license evidence snapshots + review workflow** so audits are straightforward.

### P2 (nice-to-have: scale, performance, polish)
8. Better download robustness (retry/backoff, checksum verification, resumable downloads, rate limiting).
9. Stronger test suite (strategy-level tests + orchestration tests).
10. Packaging + entrypoints (`dc-*` commands) for a smoother user experience.

---

# P0 — Must Fix

## 1) Fix `run_all.py` (currently crashes)

### Symptom
Running:
```bash
python run_all.py --help
```
crashes with:
`ModuleNotFoundError: No module named 'init_layout'`

### Root cause
- `run_all.py` imports `tools.build_natural_corpus`.
- `tools/build_natural_corpus.py` imports siblings as if it’s being executed as a script (e.g. `from init_layout import init_layout`).
- But when imported as a module (`from tools.build_natural_corpus import main`), those imports must be package-qualified (e.g. `from tools.init_layout import init_layout`).

### Recommended fix (keep both module import + script execution working)
Update **`tools/build_natural_corpus.py`** imports to package imports, and add a small bootstrap for script execution:

**A. Add bootstrap near top of `tools/build_natural_corpus.py`:**
```python
# Allow running as a script (python tools/build_natural_corpus.py) and as a module (python -m tools.build_natural_corpus)
if __package__ in (None, ""):
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
```

**B. Change imports in `tools/build_natural_corpus.py`:**
```python
from tools.init_layout import init_layout
from tools.patch_targets import patch_targets_yaml
from tools.preflight import run_preflight
```

### Acceptance criteria
- `python run_all.py --help` works.
- `python run_all.py --dest-root ".../Natural" --execute` works.
- `python tools/build_natural_corpus.py --help` still works (script path invocation).
- CI remains green.

---

## 2) Fix notebook: `dataset_collector_run_all_pipelines.ipynb` defines no `repo_root`

### Symptom
Notebook cell 2 contains a literal placeholder `...` and prints `repo_root` without defining it.

### Recommended fix
In **cell 2**, after `locate_repo_root`, add:

```python
repo_root = locate_repo_root(Path.cwd()).resolve()
os.chdir(repo_root)
print(f"Repo root detected: {repo_root}")
```

Remove the placeholder `...` line entirely.

### Acceptance criteria
- Running the notebook top-to-bottom correctly detects pipelines and runs validation/orchestration in dry-run mode without manual edits.

---

## 3) Standardize CLI args for repo tools (reduce confusion)

### Current state
- `tools/validate_repo.py` uses `--root`
- `tools/preflight.py` uses `--repo-root`

### Recommended fix
Pick **one** convention and apply across tools. Recommended: `--repo-root`.

- Update `tools/validate_repo.py` to accept `--repo-root` (keep `--root` as deprecated alias for one release if you want).
- Update notebook + docs accordingly.

### Acceptance criteria
- All repo-level tools accept `--repo-root`.
- Docs and examples use the same flag everywhere.

---

# P1 — High-Leverage Improvements

## 4) Make dependencies reproducible (true lockfile)

### Problem
- `requirements.lock` is used in CI, but it is not a “true lock” (some entries are version ranges).
- Pipelines also have `requirements.txt` files with ranges, meaning users can resolve to different environments over time.

### Recommended approach (simple, robust)
Adopt **pip-tools** (you already reference it) or **uv**; either is fine.

#### Option A (pip-tools)
1. Rename current `requirements.txt` → `requirements.in` (top-level inputs).
2. Generate lock:
```bash
pip-compile requirements.in --output-file requirements.lock --generate-hashes
```
3. For dev:
- create `requirements-dev.in` (e.g. `jupyterlab`, `ipykernel`, `pytest`, `ruff`, `pip-tools`)
- compile to `requirements-dev.lock`

#### Option B (uv)
- Use `uv pip compile` similarly and commit `requirements.lock` + `requirements-dev.lock`.

### Acceptance criteria
- Fresh checkout + `pip install -r requirements.lock` yields the same versions across machines.
- CI installs only locked files (no implicit upgrade drift).

---

## 5) Linting/formatting currently ignores pipeline code

### Current state
`pyproject.toml` config:
```toml
[tool.ruff]
include = ["run_all.py", "tools/**/*.py", "tests/**/*.py"]
```
So pipeline code can drift without being linted.

### Recommended fix
Expand to include all Python in repo, or explicitly include pipeline dirs:
```toml
include = ["*.py", "tools/**/*.py", "tests/**/*.py", "*_pipeline_v2/**/*.py"]
```

Also consider:
- `ruff format` (or keep formatting manual if you prefer minimal changes)
- `pre-commit` hooks (ruff + basic checks)

### Acceptance criteria
- `ruff check .` actually checks pipelines.
- Pipeline scripts stay consistent across all 18 directories.

---

## 6) Standardize YAML metadata and schema versions across pipelines

### Current drift
- `targets_*.yaml` are consistent (`schema_version: 0.8`) ✅
- `license_map.yaml` is mixed (`0.2` vs `0.3`) across pipelines
- `denylist.yaml` is mostly `0.2` but `cyber_pipeline_v2` uses `0.3`
- `field_schemas.yaml` varies wildly (`0.2`, `0.6`, `0.7`, `0.8`, `1.0`, `1.1`, `2.0`, and one with **no schema_version**)

This is not “wrong,” but it becomes hard to enforce tooling and prevents future automation.

### Recommended fix
Define a consistent envelope for YAML config files:
```yaml
schema_version: "X.Y"
updated_utc: "YYYY-MM-DD"
# optionally: owner, notes, references
```

For `field_schemas.yaml`, standardize a top-level key like:
```yaml
schema_version: "1.0"
updated_utc: "YYYY-MM-DD"
schemas:
  record_type_name:
    description: ...
    schema: { ...JSONSchema... }
```

Then update pipeline `field_schemas.yaml` to match.

### Add validation
Extend `tools/validate_repo.py` (or add `tools/validate_schemas.py`) to:
- verify presence of `schema_version` + `updated_utc`
- verify expected top-level keys exist
- optionally validate each JSONSchema is parseable

### Acceptance criteria
- All pipelines share the same metadata structure for companion YAMLs.
- Repo-level validator catches mismatches early.

---

## 7) Clarify and tighten the manual review workflow (YELLOW)

### What’s good already
- You have a clear green/yellow separation and “pitch if unclear” philosophy.
- There are `review_queue.py` scripts per pipeline.

### What’s missing / unclear
- A single, repo-level doc that explains:
  - what constitutes a **YELLOW** item,
  - where review queues land on disk,
  - how to mark reviewed items,
  - how a reviewed decision feeds back into `screen_yellow` / `merge` / `catalog`.

### Recommended fix
Add `docs/yellow_review_workflow.md` that defines:
- review states: `ALLOW`, `ALLOW_WITH_RESTRICTIONS`, `PITCH`
- required evidence artifacts for ALLOW decisions
- how to store reviewer notes (JSONL or YAML entries)
- how decisions are used by screening stage

Also consider a simple repo-level helper:
- `tools/review_queue.py` that dispatches into pipelines, so you don’t have to remember per-pipeline scripts.

### Acceptance criteria
- A new user can run: `classify → acquire_yellow → review → screen_yellow → merge → catalog` with no guesswork.

---

## 8) Fix minor doc/code drift around output paths

### Issue
Some pipeline scripts/docstrings still refer to older folder names like `queues/` and `manifests/`,
while the actual output contract uses `_queues/` and `_manifests/` (and the orchestrator writes `_queues` / `_catalogs` / `_logs`).

### Recommended fix
- Update docstrings in pipeline drivers/workers to match `docs/output_contract.md`
- Ensure default CLI args (if any) default to underscore paths when no targets YAML is provided.

### Acceptance criteria
- `docs/output_contract.md` is the source of truth and matches code behavior everywhere.

---

# P2 — Scale, Reliability, and Polish

## 9) Download robustness improvements (per strategy)

Even if you keep the “simple scripts per pipeline” pattern, there are a few robustness upgrades worth doing:

### For all strategies
- consistent retries with exponential backoff
- max file size / max total bytes per target (guardrails)
- checksum recording (sha256) for every downloaded file
- safer archive extraction (prevent zip-slip path traversal)
- consistent user-agent + polite rate limiting for HTTP

### HTTP
- support resumable downloads where possible
- enforce timeouts (connect/read)
- validate content-type when known

### Git
- shallow clones when possible
- allow pinning tags/commits in targets YAML

### HuggingFace datasets
- optional streaming mode
- record dataset revision + split + config deterministically

### S3 / requester pays
- clearly document required AWS CLI config and permissions
- add “dry-run list only” mode to verify access before downloading

---

## 10) Testing strategy expansion

Current tests focus on license primitives. Add tests for:

- `tools/patch_targets.py`:
  - Windows path patching
  - correct replacement of `/data/<domain>` placeholders
- `tools/build_natural_corpus.py`:
  - stage planning
  - pipeline filtering (`--pipelines`)
  - `--mode` behavior
- `tools/validate_repo.py`:
  - catches missing license evidence URLs, bad strategies, missing requirements
- “smoke” tests:
  - run classify stage on a tiny synthetic targets YAML (no network)

Add a test to import `run_all.py` to prevent the P0 regression.

---

## 11) Packaging + entrypoints (optional but nice)

If you want a cleaner UX:
- turn repo into a small package, e.g. `dataset_collector/` core + `tools/` thin CLIs
- expose:
  - `dc-validate`
  - `dc-preflight`
  - `dc-build`
  - `dc-patch-targets`

This also makes Windows usage easier (no path quirks).

---

# File-by-file punch list (quick index)

## Root
- `run_all.py`
  - **Fix** imports by making `tools/build_natural_corpus.py` module-import safe.
- `dataset_collector_run_all_pipelines.ipynb`
  - **Fix** cell 2 (`repo_root` assignment + remove placeholder).
- `pyproject.toml`
  - **Improve** ruff include to lint pipeline code too.
- `requirements.txt` / `requirements.lock` / `requirements-dev.txt`
  - **Improve** adopt a true lock strategy; add dev/test deps to dev lock.
- `README.md`
  - **Improve** ensure examples include the “right” invocation paths (`python -m ...` or script paths) and align with the final tool behavior.

## Tools
- `tools/build_natural_corpus.py`
  - **Fix** imports so module import works; add script bootstrap.
- `tools/validate_repo.py`
  - **Improve** standardize flag name (`--repo-root`) + optionally validate companion YAML envelopes.
- `tools/preflight.py`
  - **Improve** docs: explicitly list which targets require `aws`, `git`, etc; optionally add `--skip-external` or stronger `--strict` guidance.
- `tools/pipeline_map*.yaml`
  - **Improve** avoid hard-coded personal paths in the committed default; make them templates and document how to override.

## Pipelines (all `*_pipeline_v2`)
- `pipeline_driver.py` docstrings:
  - **Improve** align with output contract folders (`_queues`, `_manifests`, etc.)
- `field_schemas.yaml`
  - **Improve** unify envelope and schema versioning.
- `license_map.yaml` / `denylist.yaml`
  - **Improve** standardize schema versions and metadata keys.

---

# Suggested “minimal” implementation order (to avoid overengineering)

1. **Fix run_all import crash** (P0)
2. **Fix notebook repo_root cell** (P0)
3. **Standardize tool flags** (P0)
4. Expand Ruff to pipelines (P1)
5. Adopt true lockfiles (P1)
6. Standardize YAML envelopes (P1)
7. Improve YELLOW review docs (P1)

Everything else can wait until you start doing heavier runs and hit real-world edge cases.

---
