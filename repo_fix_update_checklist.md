# Dataset Collector + License Screening Repo — Fix & Update Checklist

> Goal: make the repo **CI-green**, and make license screening **conservative + auditable** (no accidental GREEN when evidence or terms change).

---

## How to use this checklist

- Items are ordered by **priority**.
- Each checklist item includes **what’s wrong**, **where**, and **how to fix**.
- When you're done, run the **verification commands** at the bottom.

---

## P0 — CI breakers / correctness bugs (must fix)

### [ ] 1) Fix gate-name mismatch: YAML uses `restriction_phrase_scan` / `manual_legal_review`, driver enforces `no_restrictions` / `manual_review`
**What’s wrong**
- Targets define default gates like:
  - `restriction_phrase_scan`
  - `manual_legal_review`
- But `collector_core/pipeline_driver_base.py::compute_effective_bucket()` only checks:
  - `no_restrictions`
  - `manual_review`
- Result: your intended “force YELLOW” gates may **silently do nothing**, and targets can become GREEN incorrectly.

**Where**
- `collector_core/pipeline_driver_base.py` → `compute_effective_bucket(...)`

**How to fix**
- Treat YAML gate names as canonical and support legacy aliases:
  - `restriction_phrase_scan` **OR** `no_restrictions`
  - `manual_legal_review` **OR** `manual_review`
- Suggested patch (conceptual):
  - Replace:
    - `if "no_restrictions" in gates and restriction_hits:`
    - `if "manual_review" in gates:`
  - With:
    - `if ("restriction_phrase_scan" in gates or "no_restrictions" in gates) and restriction_hits:`
    - `if ("manual_legal_review" in gates or "manual_review" in gates):`

**Bonus (recommended)**
- Add a `canonicalize_gates(gates: list[str]) -> list[str]` helper called right after `merge_gates(...)` so everything downstream sees consistent names.

---

### [ ] 2) Enforce the “license evidence changed” policy (force re-review; prevent stale approvals from promoting)
**Prudent policy (recommended)**
- If the saved license evidence changes, the target must be treated as **YELLOW until a human re-approves** against the new evidence snapshot.
- A review signoff should only be valid if it was issued for the **current evidence hash**.

**What’s wrong today**
- The driver computes `license_change_detected`, but it does **not** affect bucketing.
- `review_signoff.json` is not bound to an evidence hash, so an old approval can still apply after ToS/license changes.

**Where**
- Detection exists:
  - `collector_core/pipeline_driver_base.py` → `fetch_evidence()` sets `license_change_detected`
- Missing enforcement:
  - `collector_core/pipeline_driver_base.py` → `compute_effective_bucket(...)` / `resolve_effective_bucket(...)`
- Signoff writing:
  - `collector_core/review_queue.py`

**How to fix (minimum viable, recommended)**
1) **Bind signoff to the evidence hash**
   - When writing `review_signoff.json`, include:
     - `license_evidence_sha256` (the current evidence sha)
     - (optionally) `license_evidence_url`, `license_evidence_fetched_at_utc`
   - Implementation detail:
     - Read `license_evidence_meta.json` from the target manifest dir (if present) and copy `sha256`.

2) **Treat stale approvals as NOT approved**
   - After `fetch_evidence()` but before `resolve_effective_bucket(...)`, compute:
     - `current_sha = evidence.snapshot.get("sha256")`
     - `signoff_sha = ctx.signoff.get("license_evidence_sha256")`
   - If `current_sha` exists AND `signoff_sha` exists AND they differ:
     - downgrade the effective review state for this run:
       - `review_status = "pending"`
       - `promote_to = ""`
       - `review_required = True`

3) **Force YELLOW when evidence changed**
   - In `compute_effective_bucket(...)`:
     - if `evidence_snapshot.get("changed_from_previous")` is True:
       - set `bucket = "YELLOW"` (even if SPDX would allow GREEN)

**Acceptance criteria**
- If evidence changes, target appears in YELLOW queue until re-signed with matching `license_evidence_sha256`.

---

### [ ] 3) Preserve audit trail: don’t overwrite old evidence when it changes
**What’s wrong**
- `snapshot_evidence()` detects `changed_from_previous`, but then overwrites `license_evidence.ext`,
  erasing the previous evidence content.

**Where**
- `collector_core/pipeline_driver_base.py` → `snapshot_evidence()`

**How to fix**
- If evidence is changing and you’re about to overwrite the same path:
  - Rename the existing file first, e.g.:
    - `license_evidence.prev_<sha8>.html`
- Keep an audit record:
  - Either:
    - append to a `license_evidence_history.json` list
  - Or:
    - add a `history` array inside `license_evidence_meta.json` (store previous sha + filename + fetched_at)

**Acceptance criteria**
- After evidence changes, both old and new evidence files exist, and metadata records both.

---

### [ ] 4) Fix pytest collection failure: `kg_nav_pipeline_v2/acquire_worker.py` missing stable handler names
**What’s wrong**
- `tests/test_acquire_strategies.py` expects:
  - `handle_http`
  - `handle_figshare`
- But `kg_nav_pipeline_v2/acquire_worker.py` exports:
  - `handle_http_multi`
  - `handle_figshare_files`

**Where**
- `kg_nav_pipeline_v2/acquire_worker.py`

**How to fix**
- Add backwards-compatible aliases:
  - `handle_http = handle_http_multi`
  - `handle_figshare = handle_figshare_files`

**Acceptance criteria**
- `pytest -q` proceeds past collection and runs tests.

---

### [ ] 5) Fix `tools.validate_repo` failure: missing version import in 17 pipeline acquire workers
**What’s wrong**
- `python -m tools.validate_repo --repo-root .` fails with `missing_version_import` for 17 files.

**Where**
- Each file listed below must import and reference the version:
  - `agri_circular_pipeline_v2/acquire_worker.py`
  - `biology_pipeline_v2/acquire_worker.py`
  - `chem_pipeline_v2/acquire_worker.py`
  - `code_pipeline_v2/acquire_worker.py`
  - `cyber_pipeline_v2/acquire_worker.py`
  - `earth_pipeline_v2/acquire_worker.py`
  - `econ_stats_decision_adaptation_pipeline_v2/acquire_worker.py`
  - `engineering_pipeline_v2/acquire_worker.py`
  - `kg_nav_pipeline_v2/acquire_worker.py`
  - `logic_pipeline_v2/acquire_worker.py`
  - `materials_science_pipeline_v2/acquire_worker.py`
  - `math_pipeline_v2/acquire_worker.py`
  - `metrology_pipeline_v2/acquire_worker.py`
  - `nlp_pipeline_v2/acquire_worker.py`
  - `physics_pipeline_v2/acquire_worker.py`
  - `regcomp_pipeline_v2/acquire_worker.py`
  - `safety_incident_pipeline_v2/acquire_worker.py`

**How to fix**
- Near the top of each file:
  - `from collector_core.__version__ import __version__ as VERSION`
- Ensure it is “used” (so linters won’t complain), e.g.:
  - add to `__all__`, or
  - log it in `main()`, or
  - include in a `--version` CLI output if you have one.

**Acceptance criteria**
- `python -m tools.validate_repo --repo-root .` returns exit code 0.

---

## P1 — License screening robustness (high priority)

### [ ] 6) Implement PDF text extraction for restriction scanning
**What’s wrong**
- `extract_text_for_scanning()` returns an empty string for PDFs, so restriction scanning can miss “NoAI / NoTDM / no ML” clauses that appear in PDF terms.

**Where**
- `collector_core/pipeline_driver_base.py` → `extract_text_for_scanning()`

**How to fix**
- Use a lightweight extractor (example approach):
  - `pypdf.PdfReader` and concatenate text from first N pages (e.g., 1–5 pages).
- If extraction fails, be conservative:
  - If `restriction_phrase_scan` is enabled and evidence is PDF and text extraction fails → force YELLOW.

**Acceptance criteria**
- PDF evidence yields non-empty `evidence.text` when extractable.
- Restriction phrases in PDF terms get detected and push to YELLOW.

---

### [ ] 7) Add a “stale signoff” status to output rows / reports (visibility)
**What’s wrong**
- Even after you enforce hash binding, reviewers need visibility when a signoff is stale.

**Where**
- `collector_core/pipeline_driver_base.py` → `build_evaluation()` and `build_row()`

**How to fix**
- Add fields like:
  - `signoff_evidence_sha256`
  - `current_evidence_sha256`
  - `signoff_is_stale: true/false`
- Include it in dry-run reports / YELLOW queue rows if you want faster review.

---

### [ ] 8) Fix type annotation mismatch: `resolve_effective_bucket()` says gates is a dict, but it is a list
**What’s wrong**
- Signature says `gates: dict[str, Any]`, but you pass a `list[str]` from `merge_gates(...)`.

**Where**
- `collector_core/pipeline_driver_base.py` → `resolve_effective_bucket(...)`

**How to fix**
- Change the annotation to `gates: list[str]` (or `Sequence[str]`) to match reality.

---

## P2 — Lint / formatting / style updates (keep repo clean)

### [ ] 9) Fix ruff failures (imports + `Callable` import source)
**What’s wrong**
- `ruff check .` reports:
  - `I001` import sorting issues
  - `UP035` importing `Callable` from `typing` instead of `collections.abc`

**Where (files reported by ruff)**
- Import sorting (`I001`):
  - `3d_modeling_pipeline_v2/acquire_worker.py`
  - `3d_modeling_pipeline_v2/yellow_scrubber.py`
  - `agri_circular_pipeline_v2/yellow_scrubber.py`
  - `biology_pipeline_v2/yellow_scrubber.py`
  - `chem_pipeline_v2/yellow_scrubber.py`
  - `code_pipeline_v2/yellow_scrubber.py`
  - `collector_core/review_queue.py`
  - `cyber_pipeline_v2/yellow_scrubber.py`
  - `earth_pipeline_v2/yellow_scrubber.py`
  - `econ_stats_decision_adaptation_pipeline_v2/yellow_scrubber.py`
  - `engineering_pipeline_v2/yellow_scrubber.py`
  - `kg_nav_pipeline_v2/yellow_scrubber.py`
  - `logic_pipeline_v2/yellow_scrubber.py`
  - `materials_science_pipeline_v2/yellow_scrubber.py`
  - `math_pipeline_v2/yellow_scrubber.py`
  - `nlp_pipeline_v2/yellow_scrubber.py`
  - `physics_pipeline_v2/yellow_scrubber.py`
  - `regcomp_pipeline_v2/yellow_scrubber.py`
- `Callable` import (`UP035`):
  - `collector_core/acquire_strategies.py`
  - `metrology_pipeline_v2/acquire_worker.py`

**How to fix**
- Run:
  - `python -m ruff check . --fix`
- Manually update:
  - `from typing import Any, Callable` → `from typing import Any` and `from collections.abc import Callable`

**Acceptance criteria**
- `python -m ruff check .` returns 0.

---

## P3 — Docs + version consistency (nice-to-have but worth doing)

### [ ] 10) Fix docs mismatch: run instructions show `<dest_root>/<pipeline_name>/`, but pipeline_map writes `<dest_root>/<dest_folder>/`
**What’s wrong**
- `docs/run_instructions.md` claims outputs like:
  - `.../math_pipeline_v2/`
- But `tools/pipeline_map.sample.yaml` maps `math_pipeline_v2` → `dest_folder: "math"` (so the folder is `.../math/`).

**Where**
- `docs/run_instructions.md`
- `tools/pipeline_map.sample.yaml` (source of truth)
- (Optionally) `docs/output_contract.md` already uses `<dataset_root>/` language correctly.

**How to fix**
- Update examples in `docs/run_instructions.md` to use `dest_folder` paths:
  - Windows example: `E:\AI-Research\datasets\Natural\math\`
  - Linux example: `/data/Natural/math/`
- Add a short explanation:
  - “Dataset roots are created using `dest_folder` from pipeline_map.”

---

### [ ] 11) Update stale tool headers/docstrings that still say “v0.9”
**What’s wrong**
- Some scripts still claim v0.9 and mention old signoff schema versions.

**Where**
- `collector_core/review_queue.py` (header docstring)
- `collector_core/pmc_worker.py` (header docstring)

**How to fix**
- Update docstrings to reflect current tool versioning:
  - use `collector_core.__version__` and `__schema_version__` as the reference
- Update the signoff schema documentation to include the new evidence-hash binding fields.

---

## P4 — Make gates safer and less misleading (recommended)

### [ ] 12) Add gate validation: warn on unknown or unimplemented gates
**What’s wrong**
- Targets declare many gates in `gates_catalog`, but only a small subset is actually enforced in the driver.
- This can create a false sense of safety (“we gate on X”) when X is not implemented.

**Where**
- `collector_core/pipeline_driver_base.py` (`prepare_target_context` is a good spot), or
- `collector_core/config_validator.py` if you prefer schema-time validation.

**How to fix**
- Define an allowlist:
  - `SUPPORTED_GATES = {...}`
- For each target gate:
  - if not in `SUPPORTED_GATES`: emit a warning into `results.warnings` and/or fail in strict mode.
- Decide whether unknown gates should:
  - be warnings (default), or
  - hard errors when `--strict` is enabled.

---

### [ ] 13) Decide what to do with “planning gates” (emit manifest, attribution bundle, etc.)
**What’s wrong**
- Gates like `emit_training_manifest` / `emit_attribution_bundle` are defined in YAML but not enforced by the driver.

**Where**
- Target YAMLs (`default_gates`, `gates_catalog`)
- Downstream stage scripts (if you intend them to implement these)

**How to fix (pick a consistent approach)**
- **Option A (recommended):** implement these in the stage(s) that actually produce artifacts, and treat the gate as a switch.
- **Option B:** remove them from `default_gates` and keep them documented as “future work”.

---

## Verification commands (run after completing checklist)

From repo root:

```bash
python -m tools.validate_yaml_schemas --root .
python -m tools.preflight --repo-root .
python -m tools.validate_repo --repo-root .

python -m ruff check .
pytest -q
```

**Expected results**
- All commands exit code 0.
- Evidence change causes YELLOW and stale signoffs are ignored until renewed.
