# Dataset Collector & License Screening Repo — Issue Checklist + Recommended Updates

This document converts the latest assessment into an actionable, **numbered checklist** of:

- **Every issue identified** (including CI/test blockers and compliance risks)
- **Exactly how to fix each issue**
- **Recommended updates** to make the pipeline more elegant and higher quality

---

## 1) `tools/preflight.py` can hard-crash when importing pipeline modules

**Problem**  
`tools/preflight.py` attempts to import each pipeline’s `acquire_worker.py` via `importlib`. If any pipeline has an import-time error (e.g., `NameError`, `SyntaxError`), preflight can crash because it only catches `RuntimeError` in at least one place.

**Why it matters**  
Preflight is intended to be a *reporting* tool. One broken pipeline should not prevent validation of all other pipelines.

**Fix**
- In `tools/preflight.py`, broaden exception handling around dynamic import calls:
  - Catch **`Exception`** and record a structured error entry (`pipeline`, `path`, `exception_type`, `message`).
  - Continue scanning other pipelines.

**Implementation notes**
- Keep traceback printing behind `--verbose` so CI output stays clean.
- Ensure preflight exits with non-zero status if *any* pipeline fails import/validation.

---

## 2) `Path` used before import in pipeline acquire workers (CI blocker)

**Files**
- `metrology_pipeline_v2/acquire_worker.py`
- `code_pipeline_v2/acquire_worker.py`
- `3d_modeling_pipeline_v2/acquire_worker.py`

**Problem**  
These files call `Path(...)` in `sys.path.insert(...)` before importing `Path` (`from pathlib import Path`). This raises `NameError` at import time.

**Fix**
- Move `from pathlib import Path` above any reference to `Path`.
- Prefer a safe “only patch sys.path when executed as a script” pattern:

```py
if __package__ in (None, ""):
    from pathlib import Path
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
```

**Quality upgrade (recommended)**
- Over time, remove sys.path patching entirely by running entrypoints via `python -m ...` and using real package imports.

---

## 3) Missing expected strategy aliases: `handle_http` and `handle_figshare` (tests/compat blocker)

**Problem**  
`collector_core.acquire_strategies` exports `handle_http_single`, `handle_http_multi`, and `handle_figshare_article`, but tests (and legacy calling code) expect `handle_http` and `handle_figshare`.

**Fix options**
- **Recommended:** Add backwards-compatible wrappers/aliases in `collector_core/acquire_strategies.py`:
  - `handle_http(...)` dispatches based on whether a target provides one URL or multiple URLs, or just alias it to the “most general” handler.
  - `handle_figshare = handle_figshare_article` (or a small wrapper that chooses article vs files if you support both).

**Why this is best**
- Keeps your public API stable and prevents future regressions when tests or pipelines reference older names.

---

## 4) Test writes invalid schema version: `schema_version: "1.0"` vs schema allows `"0.9"`

**Problem**  
`tests/test_preflight.py` generates a temporary `targets.yaml` with `schema_version: "1.0"`, while `schemas/targets.schema.json` only permits `"0.9"`. This fails schema validation.

**Fix**
- Update the test helper to write `schema_version: "0.9"`.

**Alternative (only if intentional)**
- If you are truly bumping schema versions, update `schemas/targets.schema.json` and the repo docs/configs accordingly.

---

## 5) `tools/preflight.py --quiet` flag suppresses warnings/info

**Status**  
`--quiet` now suppresses warning/info output while still printing errors and a final summary.

---

## 6) Compliance risk: “license evidence changed” can miss changes when text extraction fails

**Problem**  
When evidence normalization fails (PDF text extraction fails, etc.), `sha256_normalized_text` becomes `None`. With `evidence_change_policy: normalized`, the system can incorrectly treat evidence as unchanged / signoffs as still valid.

**Why it matters**  
This can silently reduce legal review rigor when it matters most (e.g., scanned PDFs, inaccessible PDFs, unusual HTML).

**Fix (recommended conservative behavior)**
- Always compute `sha256_raw_bytes`.
- If normalization fails, set:
  - `sha256_normalized_text = sha256_raw_bytes`
  - Record metadata: `normalized_hash_fallback = "raw_bytes"` and `text_extraction_failed = true`
- In signoff/staleness logic:
  - If `text_extraction_failed` is true, treat **raw hash mismatch** as sufficient to invalidate prior signoff (or at minimum require re-review).

**Immediate mitigation**
- Temporarily set `evidence_change_policy: either` in shared config (e.g., `configs/common/license_map.yaml`) until the fallback is implemented.

**Add tests (required)**
- Add a unit test that simulates extraction failure and verifies:
  - evidence change triggers stale/review state
  - signoff invalidation occurs when raw hash changes

---

## 7) `--max-bytes-per-target` parsed but not enforced (footgun)

**Problem**  
`Limits.max_bytes_per_target` exists in CLI parsing/config but is not enforced in download handlers.

**Why it matters**
- Users expect protection against runaway downloads; currently they don’t actually have it.

**Fix**
- Implement enforcement in strategy handlers:
  - For file-by-file strategies: stop downloading once cumulative bytes exceed limit; mark target failed/aborted with reason.
  - For bulk strategies (git/s3): post-check size and mark failed if oversized; ideally add per-file streaming enforcement where possible.

**Tests**
- Add a test that creates/downloads content exceeding limit and asserts the target is halted and labeled as oversized.

---

## 8) `run_target()` assumes handlers return a non-empty results list

**Problem**  
If a handler returns `[]`, code that expects `manifest["results"][0]` can raise `IndexError`.

**Fix**
- Normalize empty handler returns into a single standardized “no results / failed” record:
  - `status: "failed"`
  - `reason: "handler_returned_no_results"`

---

## 9) Threaded acquire mode produces non-deterministic ordering

**Problem**  
`ThreadPoolExecutor` + `as_completed()` yields completion order, causing summary JSON output to vary run-to-run.

**Fix**
- Capture `(index, target_id)` for each submitted future.
- Collect results into a list keyed by index and re-sort to the input order before writing `acquire_summary_*.json`.

**Benefit**
- Stable diffs, easier reviews, reproducible pipelines.

---

## 10) JSON outputs are not written atomically

**Problem**  
Writes to final output paths can leave partial files if interrupted.

**Fix**
- Write to a temp file `path + ".tmp"` then atomically `replace()` into place.

**Where to apply**
- Manifests, summaries, markers, evidence meta, any other “final” artifacts.

---

## 11) Unconditional `sys.path.insert(...)` across pipeline scripts (maintainability smell)

**Problem**  
Many pipeline scripts patch `sys.path` at import time. This makes packaging and tooling brittle.

**Fix (incremental)**
- Gate sys.path patching behind `if __package__ in (None, ""):` everywhere.

**Fix (best / elegant)**
- Convert pipelines to module execution (`python -m ...`) and rely on proper package imports, eliminating sys.path modifications.

---

## 12) Versioning drift: hardcoded version string in catalog builder

**Problem**  
A hardcoded `VERSION = "2.0"` can drift from `collector_core/__version__.py`.

**Fix**
- Replace hardcoded string with import from a single canonical version module.
- Ensure every output artifact includes:
  - `pipeline_version`
  - `schema_version` (where applicable)
  - `written_at_utc`

---

## 13) Docs mismatch: license map / companion files described inconsistently

**Problem**  
Docs imply per-pipeline config files, while the system uses shared configs and `companion_files`.

**Fix**
- Update docs to match reality:
  - Where shared configs live
  - How `targets_*.yaml` references companion files
  - What users should modify vs what pipelines inherit

---

## 14) Preflight imports modules just to learn strategy handler keys (fragile)

**Problem**  
Importing `acquire_worker.py` executes top-level code (optional deps/side effects), just to read handler mappings.

**Fix options**
- **Option A (recommended):** Use `ast` parsing to extract `STRATEGY_HANDLERS` keys without executing the module.
- **Option B:** Make handler list declarative in a YAML registry and validate against a single code registry.

---

## 15) SSRF protections exist for *evidence*, but not for *downloads*

**Problem**  
Download URLs in targets can point anywhere (including internal IPs) if targets are sourced externally.

**Fix (recommended)**
- Add an optional download URL validator mirroring evidence protections:
  - block non-global IPs (private/loopback/link-local/multicast/reserved/unspecified)
  - allow override via an explicit flag for trusted environments

---

## 16) Inconsistent application of limits across strategies

**Problem**  
`limit_files` and other limits are not enforced consistently; `max_bytes_per_target` is currently not enforced.

**Fix**
- Define a single “limits contract” and enforce via shared helpers:
  - per-target file count limit
  - per-target bytes limit
  - optional per-file bytes limit

---

## 17) Naming/readability friction in `tools/build_natural_corpus.py`

**Problem**  
`_normalize_stages()` is used generically and can be misleading.

**Fix**
- Rename to a generic helper (`_normalize_list_arg`) or split by use-case (`normalize_stages`, `normalize_pipelines`).

---

## 18) Archive/snapshot includes runtime artifacts (`__pycache__`, `.pytest_cache`)

**Problem**  
Even if `.gitignore` is correct, shipping cached artifacts in zips/releases is messy.

**Fix**
- Package from a clean tree (`git clean -fdx` before zipping; destructive) **or**
- Add explicit excludes in your zip/release script.

---

## 19) CLI surface area is spread across many near-identical scripts

**Problem**  
Each pipeline has multiple near-identical workers (acquire/merge/yellow screen), increasing maintenance.

**Recommended elegant refactor**
- Introduce a single CLI entrypoint:
  - `dc run --pipeline <name> --stage acquire|merge|yellow_screen ...`
- Pipelines become config + optional plugin hooks (postprocessors).

---

## 20) Strategy handler mapping duplicated across pipelines

**Problem**  
The same handler dict wiring is repeated in many pipeline `acquire_worker.py` scripts.

**Fix**
- Add `DEFAULT_STRATEGY_HANDLERS` in `collector_core.acquire_strategies`.
- Pipelines import and extend/override only what differs.

---

## 21) Missing unit tests for the “normalized hash fallback” edge case (compliance critical)

**Problem**  
The most legally-sensitive behavior is not tested.

**Fix**
- Add a test that ensures:
  - extraction failure triggers conservative behavior
  - raw hash mismatch invalidates signoff when normalized fallback is used
  - evidence change classification is correct under `normalized` and `either`

---

## 22) Missing unit tests for `--max-bytes-per-target` (once implemented)

**Problem**  
After enforcement is added, it’s easy to regress.

**Fix**
- Add tests for:
  - http multi download exceeding limit
  - git clone post-check exceeding limit
  - s3 sync post-check exceeding limit (if supported)

---

## 23) Catalog builder could provide stronger audit summaries

**Problem**  
Catalog aggregation is useful but could better support audits.

**Recommended updates**
- Add per-license-pool counts and bytes totals (GREEN/YELLOW/RED).
- Include counts per strategy (http/ftp/zenodo/etc.).
- Include “top N largest targets” and “top N most frequent licenses” summaries.

---

## 24) Make versioning and metadata consistent across all artifacts

**Problem**  
Different outputs include different metadata sets, and some version data is hardcoded.

**Fix**
- Centralize metadata creation in one helper and reuse everywhere:
  - `pipeline_version`
  - `schema_version`
  - `written_at_utc`
  - `git_commit` (optional)
  - `tool_versions` (optional)

---

## 25) Optional: dependency management elegance (`pyproject.toml` extras)

**Problem**  
Maintaining both `pyproject.toml` and lock-style requirements can create drift.

**Recommended approach**
- Put core deps in `[project.dependencies]`
- Put dev deps in `[project.optional-dependencies]` as `dev = [...]`
- Keep strict pins/constraints only if you truly need deterministic CI installs.

---

# “Do these first” (highest ROI / highest risk)

1. Fix `Path` import ordering in affected pipelines (**#2**).  
2. Make preflight resilient to import-time exceptions (**#1**).  
3. Restore `handle_http` and `handle_figshare` aliases (**#3**).  
4. Fix test schema version mismatch (**#4**).  
5. Fix evidence-change detection when normalization fails (**#6**).  
6. Implement or remove `--max-bytes-per-target` (**#7**).
