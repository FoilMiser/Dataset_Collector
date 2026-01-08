# Dataset Collector repo — optimal fix & update checklist (v2)

Generated: 2026-01-07

This repo’s **overall structure is strong**: v2 pipeline layout is consistent, schemas validate, and `tools/preflight` + `tools/validate_yaml_schemas` pass. The remaining problems are mostly **CI blockers** (syntax error + test drift + contract mismatches), plus a few policy/ergonomics improvements.

---

## 0) Fix-first (CI blockers)

### 0.1 Fix `tools/generate_pipeline.py` syntax error (blocks compile + ruff)
**Confirmed issue**
- `python -m compileall` fails at `tools/generate_pipeline.py` because docstrings are embedded inside `f"""..."""` templates, prematurely terminating the string.

**Optimal fix**
- Use **triple single quotes** for outer templates (`f'''...'''`) so inner `"""docstrings"""` render correctly.
- Apply this in:
  - `render_acquire_worker()`
  - `render_yellow_screen_worker()`
  - Any other generator template containing nested docstrings

**Validation**
- `python -m compileall -q .` succeeds
- CI `ruff check .` runs normally

---

### 0.2 Fix `tests/test_acquire_strategies.py` importing the wrong module
**Confirmed issue**
- The test imports `kg_nav_pipeline_v2/acquire_worker.py` but expects implementation symbols that only exist in `collector_core.acquire_strategies`.
- This causes many `AttributeError`s.

**Optimal fix**
- Update the test to import the real implementation:

```py
import collector_core.acquire_strategies as aw
```

**Why this is optimal**
- Keeps pipeline adapters thin (as intended)
- Tests the real shared code path used across pipelines

**Validation**
- `pytest -q tests/test_acquire_strategies.py` no longer fails on missing attributes

---

### 0.3 Add retry/backoff to `_http_download_with_resume()` (test expects it)
**Confirmed issue**
- `_http_download_with_resume()` currently performs a single request.
- Tests expect retry behavior using `RetryConfig`.

**Optimal fix**
- Wrap the download logic in a retry loop using `ctx.retry`:
  - `ctx.retry.max_attempts`
  - exponential backoff using `ctx.retry.backoff_base` capped by `ctx.retry.backoff_max`
  - use `time.sleep()` (tests monkeypatch it)

**Validation**
- transient failures retry and succeed
- `pytest -q tests/test_acquire_strategies.py` passes retry test

---

### 0.4 Update schema versions in `test_regcomp_license_denylist_enforcement.py`
**Confirmed issue**
- Test writes:
  - `license_map.schema_version: 0.3`
  - `denylist.schema_version: 0.2`
- But schemas require `0.9`.

**Optimal fix**
- Change both schema_version values to `"0.9"` (strings)
- Keep structure otherwise unchanged

**Validation**
- test passes under schema validation

---

### 0.5 Fix `collector_core/pipeline_cli.py` to ignore leading `--`
**Confirmed issue**
- `tools/run_minimal_dry_run.sh` calls:

```bash
python -m collector_core.pipeline_cli -- --pipeline-id regcomp catalog-builder ...
```

- CLI forwards args directly, so `--` breaks parsing.

**Optimal fix**
- Strip `--` if present before parsing:

```py
passthrough = list(args.args)
if passthrough and passthrough[0] == "--":
    passthrough = passthrough[1:]
```

**Also update script**
- Remove the `--` in `tools/run_minimal_dry_run.sh` (cleaner + avoids other wrappers breaking)

---

### 0.6 Add missing license profiles to `configs/common/license_map.yaml`
**Confirmed issue**
- `tools.validate_repo` warns that these are unknown profiles:
  - `quarantine` (cyber targets)
  - `public_domain` (engineering targets)

**Optimal fix**
Add these to `profiles:` in `configs/common/license_map.yaml`:

```yaml
profiles:
  permissive:
    default_bucket: "GREEN"
  public_domain:
    default_bucket: "GREEN"
  copyleft:
    default_bucket: "YELLOW"
  record_level:
    default_bucket: "YELLOW"
  unknown:
    default_bucket: "YELLOW"
  quarantine:
    default_bucket: "YELLOW"
  deny:
    default_bucket: "RED"
```

**Validation**
- `python -m tools.validate_repo` returns 0 warnings

---

### 0.7 Adjust `.yamllint` to prevent GH workflow false positives
**Likely CI break**
Default yamllint rules often flag:
- missing `---`
- `truthy` check on keys like `on:` in `.github/workflows`

**Optimal fix**
Update `.yamllint`:

```yaml
extends: default
rules:
  line-length:
    max: 160
  document-start: disable
  truthy:
    check-keys: false
```

**Validation**
- `yamllint .` passes without forcing repo-wide YAML rewrites

---

## 1) Fix-next (high value improvements)

### 1.1 Standardize import bootstrapping for pipeline scripts
Some pipeline scripts insert repo root into `sys.path`, others rely on `PYTHONPATH`.
This is inconsistent and makes direct execution brittle.

**Optimal fix**
- Add the same `sys.path.insert(...)` bootstrap to all pipeline entry scripts:
  - `pipeline_driver.py`
  - `acquire_worker.py`
  - `merge_worker.py`
  - `catalog_builder.py`
  - etc.

---

### 1.2 Use the same retry policy across all network calls
You’ll have retry in `_http_download_with_resume`, but metadata endpoints (Zenodo, Figshare) and evidence fetch are also flaky.

**Optimal fix**
- Add a small internal helper (e.g., `_with_retries(fn)`) and reuse it for API calls
- Only retry timeouts/transport errors and 5xx — never retry 4xx

---

## 2) Documentation updates (small, high leverage)

### 2.1 Document canonical install flow once
The README contains multiple install snippets.

**Optimal fix**
- Make “Reproducible installs” canonical
- Keep other snippets short and point to it

### 2.2 Document license profile vocabulary explicitly
Add a short section listing supported `license_profile` values:
- permissive, public_domain, copyleft, record_level, unknown, quarantine, deny

---

## 3) Done checklist (copy/paste)
- [ ] Fix `tools/generate_pipeline.py` quoting so it compiles.
- [ ] Update acquire strategy tests to import `collector_core.acquire_strategies`.
- [ ] Add retry/backoff in `_http_download_with_resume()` using `ctx.retry`.
- [ ] Update schema_version literals in regcomp denylist/license tests to `"0.9"`.
- [ ] Strip leading `--` in `pipeline_cli.py` and remove it from minimal dry run script.
- [ ] Add `public_domain` and `quarantine` to `license_map.yaml` profiles.
- [ ] Tune `.yamllint` (disable doc-start, don’t truthy-check keys).
- [ ] Standardize sys.path bootstrap across pipeline entry scripts.
