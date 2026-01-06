# Dataset Collector repo: issues to fix + recommended updates

_Assessed from the uploaded archive `Dataset_Collector-main (18).zip` on 2026-01-06 (America/Phoenix context)._  
This document lists **(A) current issues that should be fixed** and **(B) recommended updates** with concrete, copy/paste-friendly steps.

---

## Legend

- **P0 (Blocker):** breaks validation/CI or makes the repo unusable in normal flows
- **P1 (High):** likely to break users, cause confusion, or create recurring cleanup work
- **P2 (Medium):** quality/maintainability/security improvements worth doing soon
- **P3 (Nice):** polish

---

# A) Issues that need fixing now

## 1) P0 — Invalid YAML in 9 pipeline target files (parsing fails)

### What’s happening
`tools.validate_repo` fails immediately because several `targets_*.yaml` files contain unquoted values like:

```yaml
resolve_license_spdx: Future work: Normalize license to SPDX ...
```

In YAML, a plain scalar containing `:` followed by a space must be **quoted** or expressed as a **block scalar**, otherwise parsing fails.

### Files affected
- `earth_pipeline_v2/targets_earth.yaml`
- `econ_stats_decision_adaptation_pipeline_v2/targets_econ_stats_decision_v2.yaml`
- `engineering_pipeline_v2/targets_engineering.yaml`
- `kg_nav_pipeline_v2/targets_kg_nav.yaml`
- `materials_science_pipeline_v2/targets_materials.yaml`
- `math_pipeline_v2/targets_math.yaml`
- `physics_pipeline_v2/targets_physics.yaml`
- `regcomp_pipeline_v2/targets_regcomp.yaml`
- `safety_incident_pipeline_v2/targets_safety_incident.yaml`

### How to fix (safe, consistent)
Replace each problematic line with one of the following patterns.

**Option A (recommended): folded block scalar**
```yaml
resolve_license_spdx: >
  Future work: Normalize license to SPDX via license_map.yaml rules; store evidence.
```

**Option B: quote the entire value (fine for one-liners)**
```yaml
resolve_license_spdx: "Future work: Normalize license to SPDX via license_map.yaml rules; store evidence."
```

### Verify
From repo root:

```bash
python -m tools.validate_repo --root .
python -m tools.validate_yaml_schemas --root .
python -m tools.preflight --repo-root . --quiet
```

### Prevent recurrence (do this as part of the fix)
Add a lint/check so that this never sneaks back in. See **B1** and **B2**.

---

## 2) P1 — Python cache artifacts are present in the repo snapshot (`__pycache__`, `*.pyc`, `.pytest_cache`)

### What’s happening
The archive contains Python runtime artifacts:
- multiple `__pycache__/` directories
- many `*.pyc` bytecode files
- `.pytest_cache/`

Even if `.gitignore` covers most of these, they should **not** ship in a repo snapshot, release, or PR branch.

### How to fix
**Clean now (cross-platform Python one-liner):**
```bash
python -c "from pathlib import Path; import shutil; root=Path('.'); [shutil.rmtree(p, ignore_errors=True) for p in root.rglob('__pycache__') if p.is_dir()]; [p.unlink(missing_ok=True) for p in root.rglob('*.pyc') if p.is_file()]; shutil.rmtree(root/'.pytest_cache', ignore_errors=True)"
```

**If any of these are tracked by git**, remove them from the index:
```bash
git rm -r --cached **/__pycache__ .pytest_cache || true
git rm --cached **/*.pyc || true
```

### Also fix `.gitignore`
`.gitignore` already ignores most cache artifacts, but **add Ruff cache too** (common future nuisance):

```gitignore
.ruff_cache/
```

### Verify
```bash
python -m tools.preflight --repo-root . --quiet
```

---

## 3) P1 — Developer environment parity: `environment.yml` / dev requirements don’t include core dev tools used by CI

### What’s happening
- CI explicitly installs `pytest`, `pytest-cov`, and `ruff`
- `requirements-dev.in` / `requirements-dev.constraints.txt` currently include **`pytest-cov`** but not **`pytest`** or **`ruff`**
- `environment.yml` installs `requirements-dev.constraints.txt`, so a user creating the conda env will **not** have pytest/ruff by default

That causes “works in CI, but not in the default dev env” friction.

### How to fix
1) Update `requirements-dev.in` to include the tools you expect contributors to run:

```text
-r requirements.in
jupyterlab
ipykernel
pytest
pytest-cov
ruff
```

2) Recompile pins (you already document using `uv pip compile` in the constraints headers):
```bash
uv pip compile requirements-dev.in -o requirements-dev.constraints.txt
```

3) Keep `environment.yml` as-is (it will now pull pytest/ruff via the updated constraints).

### Verify
```bash
python -m ruff check .
pytest
```

---

## 4) P1 — Integration test is effectively skipped unless `pytest_httpserver` is installed

### What’s happening
`tests/integration/test_full_pipeline.py` contains:

```py
pytest.importorskip("pytest_httpserver")
```

But `pytest_httpserver` isn’t in `requirements-dev.in`, so locally it’ll be skipped unless manually installed.

### How to fix
Add it to dev requirements:

```text
pytest_httpserver
```

Then recompile `requirements-dev.constraints.txt` as above.

### Optional (CI)
If you want CI to run the integration test consistently, ensure `pytest_httpserver` is installed in CI too (either through dev constraints or explicit install).

---

# B) Recommended updates (how to implement)

## B1) P1 — Add YAML linting / “colon-in-scalar” guardrail

### Why
The P0 YAML error is easy to reintroduce because it’s just prose inside config.

### Options
**Option A: yamllint in CI + pre-commit**
- Add `yamllint` to dev requirements
- Add `.yamllint` config
- CI step: `yamllint .`

**Option B: Extend your existing validators**
Add a quick scan in `tools.validate_repo` that flags patterns like:
- `^\s+\w+:\s+[^'"][^#]*:\s+` (a value that contains a `:` delimiter unquoted)

This gives you a targeted, repo-specific safety net.

---

## B2) P2 — Move `gates_catalog` prose out of YAML configs (or standardize quoting)

### Why
`gates_catalog` is helpful documentation, but prose inside YAML is fragile and can break parsing.

### Two good patterns
**Pattern A (recommended): move to docs**
- Move `gates_catalog` into `docs/pipeline_runtime_contract.md` or per-pipeline `README.md`
- Keep YAML for machine-consumed config only

**Pattern B: keep in YAML but enforce safe formatting**
- Require all `gates_catalog.*` values to be either quoted strings or block scalars (`>`)

---

## B3) P2 — Make “license evidence changed” detection less noisy (while staying strict)

### Current behavior (good default)
When evidence bytes change, force re-review (treat as review-required / YELLOW until signed off).

### Improvement
Store **two hashes**:
- `sha256_raw_bytes` (audit-strong)
- `sha256_normalized_text` (reduces false positives from timestamps / trackers)

Then define policy:
- If raw changes but normalized doesn’t: mark as **“cosmetic change”** and still require review, but can be fast-path
- If normalized changes: require full re-review

Implementation idea:
- Extract normalized text via the same pipeline used for review display (HTML → text; PDF → text)
- Strip obvious noise (timestamps, analytics querystrings, etc.)

---

## B4) P2 — Add a “tracked cache artifact” CI check (not just “present in workspace”)

### Why
Your CI currently deletes caches and then checks they’re gone.  
If cache artifacts were accidentally tracked, CI would still pass because it deleted them.

### How
Add this step **before** deleting anything:

```bash
python -c "import subprocess, re, sys; out=subprocess.check_output(['git','ls-files'], text=True).splitlines(); bad=[p for p in out if re.search(r'(__pycache__/|\.pyc$|\.pytest_cache/|\.ruff_cache/)', p)]; [print('Tracked cache artifact:', p) for p in bad]; sys.exit(1 if bad else 0)"
```

---

## B5) P2 — Add `pyproject.toml` extras for dev installs

### Why
Many contributors prefer:
```bash
pip install -e .[dev]
```

### How
In `pyproject.toml` add:

```toml
[project.optional-dependencies]
dev = [
  "pytest",
  "pytest-cov",
  "ruff",
  "pytest_httpserver",
  "jupyterlab",
  "ipykernel",
]
```

Then keep constraints files as your pinned “known good” path, but make the happy-path install simpler.

---

## B6) P2 — Clarify targets schema versioning (`schema_version: 0.8` vs core `__schema_version__: 0.9`)

### Why
- Targets YAMLs declare `schema_version: "0.8"`
- Core defines `__schema_version__ = "0.9"`
- CLI help text references “(v0.8)” in places, and “v0.9” elsewhere

That’s not breaking, but it’s confusing.

### How
Pick one:
- **Option A:** keep targets at 0.8; update core constant/help strings to match
- **Option B:** bump targets to 0.9 and enforce via JSON schema (enum/pattern)

Also consider enforcing `schema_version` in your JSON schema, so drift is caught early.

---

## B7) P2 — Make per-pipeline READMEs explicitly mention path patching / overrides

### Why
Per-pipeline READMEs show `/data/<domain>/...` roots.  
The orchestrator patches these to the destination root, but that detail is easy to miss.

### How
Add a short note near the “Directory layout” section:

- “Targets YAML defaults to `/data/...` for Linux conventions; the orchestrator patches to your `--dest-root`.”
- “If running standalone, pass `--dataset-root` and/or use `tools/patch_targets.py`.”

---

## B8) P2 — Add guardrails on evidence downloads (size/time caps)

### Why
A “license evidence” URL can unexpectedly point to huge PDFs/binaries, causing:
- long runtimes
- disk bloat
- memory pressure

### How
In the evidence fetcher:
- set a max response size (e.g., 10–25MB) and fail gracefully to “needs manual evidence”
- set timeouts and retry policy
- log the final URL + content-type + size for audit

---

## B9) P3 — Improve dedupe provenance during merge

### Why
Current dedupe is generally “keep first, drop later.”  
That can discard useful provenance (alternative URLs, mirrors, etc.).

### How
When duplicates are found:
- append a bounded `provenance.duplicates[]` record to the kept item
- or merge distinct `source_urls` up to N entries
- always log the dedupe event into `_ledger`

---

# C) Quick “done when…” checklist

- [ ] All 9 targets YAMLs parse (no `mapping values are not allowed here`)
- [ ] `python -m tools.validate_repo --root .` passes in CI and locally
- [ ] No `__pycache__`, `*.pyc`, `.pytest_cache` committed or shipped
- [ ] Dev env installs pytest + ruff (via `requirements-dev` and/or extras)
- [ ] Integration test can run with `pytest_httpserver` installed
- [ ] (Optional) yamllint / pre-commit added to prevent YAML regressions

---

## Notes / scope limits

This review is based on static inspection + running repo validators in the archive environment.  
It does not execute real dataset downloads (network disabled), so resolver correctness against live endpoints should still be tested in your normal runtime environment.
