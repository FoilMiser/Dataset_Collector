# Dataset Collector & License Screening Repo — Issues and Recommended Updates

Date: 2026-01-06

This document lists **(1) every issue that needs fixing** (including run/CI blockers, security risks, and correctness problems) and **(2) every recommended update** (high-leverage improvements that are not strictly required but will make the repo safer, cleaner, and easier to maintain).

---

## 1. Issues to fix (numbered)

### 1.1 **BLOCKER: Companion YAML `schema_version` drift breaks validation and pipeline runs**
**What’s wrong**
Your JSON schemas expect `schema_version: "0.9"` for the following companion formats:
- `license_map.yaml`
- `denylist.yaml`
- `field_schemas.yaml`

However, almost every pipeline’s copies of these YAML files declare older (or otherwise different) `schema_version` values. This causes:
- Repo validation to fail (`tools.validate_yaml_schemas`)
- Pipeline startup to fail when configs are loaded via `read_yaml(..., schema_name=...)` with schema validation enabled

**Impact**
- CI fails.
- Most (likely all) pipelines crash on launch in environments that have schema validation installed (your requirements/CI do).

**Where**
Across all `*_pipeline_v2/` directories, the following files are affected:
- `*_pipeline_v2/license_map.yaml`
- `*_pipeline_v2/denylist.yaml`
- `*_pipeline_v2/field_schemas.yaml`

In total, **54 files** (18 pipelines × 3 companion files) are mismatched.

**Observed mismatches (current → expected)**
1. `3d_modeling_pipeline_v2`: `license_map 0.2`, `denylist 0.2`, `field_schemas 0.7` → **0.9**
2. `agri_circular_pipeline_v2`: `license_map 0.2`, `denylist 0.2`, `field_schemas 0.7` → **0.9**
3. `biology_pipeline_v2`: `license_map 0.2`, `denylist 0.2`, `field_schemas 0.7` → **0.9**
4. `chem_pipeline_v2`: `license_map 0.2`, `denylist 0.2`, `field_schemas 0.7` → **0.9**
5. `code_pipeline_v2`: `license_map 0.3`, `denylist 0.2`, `field_schemas 0.7` → **0.9**
6. `cyber_pipeline_v2`: `license_map 0.2`, `denylist 0.3`, `field_schemas 0.6` → **0.9**
7. `earth_pipeline_v2`: `license_map 0.2`, `denylist 0.1`, `field_schemas 0.6` → **0.9**
8. `econ_stats_decision_adaptation_pipeline_v2`: `license_map 0.2`, `denylist 0.2`, `field_schemas 0.8` → **0.9**
9. `engineering_pipeline_v2`: `license_map 0.2`, `denylist 0.2`, `field_schemas 0.7` → **0.9**
10. `kg_nav_pipeline_v2`: `license_map 0.3`, `denylist 0.2`, `field_schemas 0.6` → **0.9**
11. `logic_pipeline_v2`: `license_map 0.2`, `denylist 0.2`, `field_schemas 0.8` → **0.9**
12. `materials_science_pipeline_v2`: `license_map 0.2`, `denylist 0.2`, `field_schemas 0.7` → **0.9**
13. `math_pipeline_v2`: `license_map 2.0`, `denylist 0.1`, `field_schemas 1.0` → **0.9**
14. `metrology_pipeline_v2`: `license_map 0.2`, `denylist 1.1`, `field_schemas 0.6` → **0.9**
15. `nlp_pipeline_v2`: `license_map 0.3`, `denylist 0.2`, `field_schemas 0.6` → **0.9**
16. `physics_pipeline_v2`: `license_map 2.0`, `denylist 0.3`, `field_schemas 1.0` → **0.9**
17. `regcomp_pipeline_v2`: `license_map 0.2`, `denylist 0.2`, `field_schemas 0.7` → **0.9**
18. `safety_incident_pipeline_v2`: `license_map 0.2`, `denylist 0.2`, `field_schemas 0.7` → **0.9**

**How to fix (choose one approach)**
- **Approach A (recommended / fastest):**  
  Update every companion YAML to `schema_version: "0.9"` **if the actual structure already matches the 0.9 schema**.
- **Approach B (if those numbers were meant as “content versions”):**  
  Rename the YAML key:
  - from `schema_version` → `format_version` (or similar), and
  - adjust JSON schemas + loader logic to validate the new key, leaving content versioning elsewhere (e.g., per-schema `version:` fields).

**Verification**
```bash
python -m tools.validate_yaml_schemas --root .
python -m tools.validate_repo --root .
python -m tools.preflight --repo-root . --quiet
```

---

### 1.2 **HIGH: `tools.validate_repo` crashes instead of reporting all problems**
**What’s wrong**
`tools/validate_repo.py` calls `read_yaml(...)` and allows schema/parse exceptions (e.g., `ConfigValidationError`) to bubble out. This causes a hard crash at the first bad file, rather than producing a full report.

**Impact**
- Validation results are incomplete (you only learn about the first failure).
- This reduces the value of CI and makes local diagnosis slower.

**Where**
- `tools/validate_repo.py` (multiple YAML reads)

**How to fix**
1. Wrap each `read_yaml(...)` in `try/except` (`ConfigValidationError`, `YamlParseError`, etc.).
2. Append a structured error entry (file path, exception type, message).
3. Continue scanning remaining files/pipelines.

**Verification**
- Intentionally break one YAML and ensure `validate_repo` returns a JSON report listing errors (no traceback).

---

### 1.3 **HIGH: Potential secret leakage into manifests via recorded headers**
**What’s wrong**
Evidence request headers may be recorded into on-disk manifests/JSON, including sensitive values if they ever contain API tokens.

Examples of risky fields written to disk:
- `license_evidence_meta.json` (stores `headers_used`)
- `evaluation.json` (stores `evidence_headers_used`)

**Impact**
- Accidental persistence of secrets (API tokens, bearer auth, etc.) into `_manifests/`.
- Increased risk if manifests are committed, shared, or uploaded.

**Where**
- `collector_core/pipeline_driver_base.py` (evidence snapshot meta + evaluation output)

**How to fix**
Pick one (in order of preference):
1. **Do not store header values at all.** Store only a boolean and/or list of header names.
2. Store only **redacted** headers (e.g., `Authorization: <REDACTED>`) using your existing redaction helpers.

Add tests that assert secrets never appear in any manifest JSON.

**Verification**
- Run with `--evidence-header "Authorization: Bearer abc"` and confirm outputs contain no raw token.

---

### 1.4 **HIGH: HTTP resume can corrupt downloads if the server ignores `Range`**
**What’s wrong**
When resuming, the downloader appends to an existing partial file. If the server ignores the `Range` header and returns `200 OK` (full file), your code may append the entire file to the partial, producing a corrupted output.

**Impact**
- Silent corruption of acquired dataset files.
- Downstream parsing may fail or produce incorrect results.

**Where**
- `collector_core/acquire_strategies.py` (`_http_download_with_resume`)

**How to fix**
When `existing_size > 0` and you send `Range`:
1. Require HTTP **206 Partial Content** *or* a valid `Content-Range`.
2. If the response is `200`, either:
   - restart from scratch (truncate + re-download), or
   - fail loudly (recommended for correctness).

Also recommended:
- Download to `*.part` and atomic rename to final filename on success.

**Verification**
- Add an integration test where a mock server ignores `Range` and returns `200`. Ensure the code does not produce a corrupted file.

---

### 1.5 **MEDIUM: Expected-size verification is incorrectly gated behind `verify_sha256`**
**What’s wrong**
Size mismatch checks are performed only when `verify_sha256` is enabled. Size checks should be independent of hashing.

**Impact**
- Incorrectly “successful” downloads can pass even if truncated or oversized.
- More silent corruption.

**Where**
- `collector_core/acquire_strategies.py` (`_http_download_with_resume`)

**How to fix**
- Always validate `expected_size` when it is provided (even if sha256 is disabled).
- Improve error messages to report expected vs actual byte counts.

**Verification**
- Provide `expected_size` in a target and ensure mismatch fails even with hashing turned off.

---

### 1.6 **MEDIUM: `pip install .` won’t install runtime dependencies**
**What’s wrong**
Your `pyproject.toml` declares only a minimal dependency set (e.g., `PyYAML`), but running the repo requires additional packages (e.g., `requests`, `jsonschema`, and others).

**Impact**
- New environments using `pip install .` will fail at runtime.
- Confusing for contributors and automation.

**Where**
- `pyproject.toml` dependency declarations vs `requirements.in` / lock strategy

**How to fix (choose one)**
- **Option A (recommended):** fully declare runtime deps in `pyproject.toml` and keep constraints for CI reproducibility.
- **Option B:** clearly state in README: “this repo is not meant to be installed as a package; use requirements.in/lock.”

**Verification**
- In a clean venv: `pip install .` then run `python -m tools.preflight` successfully.

---

### 1.7 **MEDIUM: Git acquisition is not reproducible (no pinning to commit/tag)**
**What’s wrong**
Git targets can be cloned from a branch but cannot be pinned to a specific commit/tag. This can yield different dataset contents over time.

**Impact**
- Non-deterministic pipeline outputs.
- Harder audits and re-runs.

**Where**
- `collector_core/acquire_strategies.py` (`handle_git`)

**How to fix**
- Add optional fields in target config, e.g.:
  - `commit: <sha>`
  - `tag: <tag>`
- If provided, fetch and checkout that exact revision, then record the resolved SHA in manifests.

**Verification**
- Run the same target twice with a pinned commit; manifest should show the same SHA and identical content hashes.

---

### 1.8 **LOW/MEDIUM: Pipeline `requirements.txt` headers contain version drift**
**What’s wrong**
Many pipeline `requirements.txt` files contain header comments that are out of date (e.g., “v2.0.0” while core is “2.0.1”).

**Impact**
- Confusing for maintainers.
- Encourages accidental mismatch assumptions.

**Where**
- `*_pipeline_v2/requirements.txt` header comments

**How to fix**
- Update comments, or remove explicit version numbers from comments to avoid churn.

**Verification**
- Grep for old versions and ensure header comments match current release/tag conventions.

---

### 1.9 **CORRECTNESS RISK: License normalization uses broad substring matching**
**What’s wrong**
Your `resolve_spdx_with_confidence()` logic uses substring matching across normalized license text. This can misclassify licenses if a short token appears inside unrelated text (false positives).

**Impact**
- Wrong license classification (false GREEN/RED/YELLOW).
- Harder compliance audits.

**Where**
- License resolution / SPDX inference utilities (core classifier)

**How to fix**
- Use regex with word boundaries for short identifiers (e.g., `\bMIT\b`).
- Record “which rule matched” + a short excerpt to make classification auditable.
- (Optional) parse SPDX expressions when present.

**Verification**
- Add unit tests for “MIT” appearing inside unrelated strings; ensure it does not match incorrectly.

---

## 2. Recommended updates (numbered)

### 2.1 Centralize shared companion configs to prevent drift (highest leverage)
**What to do**
Create shared config files (single source of truth), for example:
- `configs/common/license_map.yaml`
- `configs/common/denylist.yaml`
- `configs/common/field_schemas.yaml`

Then point each pipeline to the shared versions via `companion_files:` (keeping only pipeline-specific overrides local).

**Why**
- Prevents future schema drift.
- One edit updates all pipelines.
- Easier review of compliance rules.

---

### 2.2 Make “license evidence changed” policy configurable
**What to do**
Add a policy option (e.g., in `license_map.yaml` or pipeline config), such as:
- `evidence_change_policy: raw | normalized | either`
- `cosmetic_change_policy: warn_only | treat_as_changed`

**Why**
- Your current behavior is very conservative and can create review churn.
- This lets you tune safety vs noise without rewriting logic.

---

### 2.3 Add stronger download integrity controls
**What to do**
- Always download to a temporary `*.part` file and atomic rename.
- Support optional `expected_sha256` per target (in addition to `expected_size`).
- Record final resolved URL, content-length, and sha256 in the acquisition manifest.

**Why**
- Makes downloads robust against partial writes, retries, and upstream changes.

---

### 2.4 Add a “manifest redaction sweep” test
**What to do**
Add a test that runs a minimal classification/acquisition flow and asserts that:
- no secret patterns appear in any written JSON (e.g., Authorization tokens)
- headers are redacted or absent

**Why**
- Prevents regressions and future accidental secret persistence.

---

### 2.5 Improve determinism for Git-based sources
**What to do**
- Support `commit` / `tag` pinning.
- Record the resolved commit SHA in `acquire_done.json`.

**Why**
- Makes dataset acquisition reproducible and auditable.

---

### 2.6 Reduce boilerplate entrypoints (optional cleanup)
**What to do**
Many pipelines include nearly identical wrappers (`review_queue.py`, `catalog_builder.py`, etc.).
Consider consolidating to a single CLI that takes `--pipeline-id` and discovers the pipeline directory.

**Why**
- Less duplication.
- Lower maintenance when adding global features.

---

### 2.7 Add validator rule: enabled targets may not use unsupported strategies
**What to do**
If any strategies are “TODO / not implemented,” make the validator fail when those are used in enabled targets.

**Why**
- Prevents runtime surprises.
- Keeps targets YAML honest.

---

## 3. “Done when” checklist (final verification)

Run these after applying fixes:
```bash
python -m tools.validate_yaml_schemas --root .
python -m tools.validate_repo --root .
python -m tools.preflight --repo-root . --quiet
ruff check .
yamllint .
pytest
```
