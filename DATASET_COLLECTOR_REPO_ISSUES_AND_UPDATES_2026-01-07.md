# Dataset Collector & License Screening Repo — Issues + Recommended Updates

Date: 2026-01-07

This is a full review of the repo snapshot you shared (the ZIP). It includes:

- **Issues**: what’s wrong, why it matters, where it is, and how to fix it.
- **Recommended updates**: high-leverage improvements that make the repo safer/easier to maintain.

---

## What I checked (actual runs)

From repo root (`/mnt/data/dc_repo/Dataset_Collector-main`):

- ✅ `python -m tools.validate_yaml_schemas --root .` → **YAML schema validation succeeded**
- ✅ `python -m tools.validate_repo --repo-root . --strict` → **0 errors / 0 warnings**
- ✅ `python -m tools.preflight --repo-root . --quiet` → **Preflight checks passed**
- ⚠️ `pytest -q` → **fails during collection** here because `datasets` isn’t installed (`ModuleNotFoundError: No module named 'datasets'`).

---

## Severity

- **P0** Must fix (misleading/broken)
- **P1** High (correctness/security/compliance risk)
- **P2** Medium (automation/reliability/DX)
- **P3** Low (polish/maintainability)

---

# 1) Issues (and how to fix each one)

## 1. P0 — The ZIP includes local build artifacts (`__pycache__/` + `*.pyc`)
**What’s wrong**: The archive contains compiled artifacts in many folders.

**Why it matters**: Noisy snapshots, confusing diffs/reviews, and occasional “stale bytecode” weirdness.

**Where**: Widespread (`*_pipeline_v2/__pycache__`, `collector_core/__pycache__`, `tools/__pycache__`, tests, `*.pyc`).

**Fix**:
```bash
python -m tools.clean_repo_tree --repo-root . --yes
```

**Verify**: Re-run the command and confirm it prints “No cleanup targets found.”

---

## 2. P0 — `REPO_ISSUES_AND_UPDATES.md` is stale and contradicts the repo state
**What’s wrong**: That doc claims big blockers (schema drift, validator crash paths), but the repo now validates cleanly in strict mode.

**Why it matters**: Contributors (and future-you) will chase problems that no longer exist.

**Where**: `REPO_ISSUES_AND_UPDATES.md`

**Fix** (pick one):
- Replace it with an up-to-date “current status + current issues”, **or**
- Move it to `docs/archive/` and label it as historical.

**Verify**: The replacement doc should include a “validated on” section with the exact commands above.

---

## 3. P1 — Denylist domain scanning misses download URLs (because it scans a JSON blob)
**What’s wrong**:
- Domain scanning is applied to `license_evidence_url` (good), **and** `download_blob` (bad).
- `download_blob` is `json.dumps(download_cfg)`, not a URL → `urlparse()` returns no domain → domain denylist doesn’t apply to downloads.

**Why it matters**: You can block evidence domains but still download from a blocked domain.

**Where**: `collector_core/pipeline_driver_base.py`
- `prepare_target_context()` builds `download_blob` from JSON.
- `denylist_hits()` domain matching runs on `url_fields = ["license_evidence_url", "download_blob"]`.

**Fix**:
- Extract URLs from the structured `download` config (`download.url`, `download.urls[]`, and any other URL fields your strategies use).
- Run domain denylist checks against those extracted URLs.

**Verify**: Unit test: target with `download: {url: "https://blocked.example.com/file"}` should trigger a domain deny hit.

---

## 4. P1 — Domain matching uses substring semantics (false positives)
**What’s wrong**: Domain rules currently match with substring checks (e.g., `target_domain in src_domain`).

**Why it matters**: Denylists need to be predictable; substring matches cause surprising blocks/escapes.

**Where**: `collector_core/pipeline_driver_base.py` (`denylist_hits()`)

**Fix**:
1) Change `extract_domain()` to return `urlparse(url).hostname` (not `netloc`).
2) Use boundary-safe matching:
```py
host == target or host.endswith('.' + target)
```

**Verify**: Tests that `example.com` matches `sub.example.com` but `gov` does **not** match `notgov.com`.

---

## 5. P1 — Evidence snapshots are non-atomic and “verification” checks existence, not integrity
**What’s wrong**:
- Evidence is written directly to `license_evidence.<ext>`.
- `evidence_files_verified` only checks that files exist, not that hashes match expected bytes.

**Why it matters**: Evidence integrity is part of your compliance story; partial writes can create silent corruption.

**Where**: `collector_core/pipeline_driver_base.py` (`snapshot_evidence()`)

**Fix**:
- Write to `license_evidence.<ext>.part`, compute hash, then `replace()` into place.
- Verify the final file’s hash matches the digest computed from downloaded bytes.

**Verify**: Unit test simulating write failure should yield `status=error` and never claim success.

---

## 6. P1 — Evidence extension drift can leave multiple “current” evidence files
**What’s wrong**: The “current evidence filename” depends on `Content-Type`. If a server changes content-type, you can end up with both `license_evidence.html` and `license_evidence.pdf` (or `.txt/.json`) as “current”. Future runs then pick the first existing extension in a fixed order.

**Why it matters**: Confusing history and possible comparisons against the wrong prior file.

**Where**: `collector_core/pipeline_driver_base.py` (`snapshot_evidence()`, `find_existing_evidence()`)

**Fix options**:
- Make the current evidence filename **canonical** (e.g., `license_evidence.bin` + store `content_type` in meta), **or**
- Before writing new evidence, delete/rename other `license_evidence.*` siblings (excluding `license_evidence.prev_*`).

**Verify**: Test: existing `.html`, new content-type resolves to `.pdf`; after snapshot, there should be exactly one “current” evidence file.

---

## 7. P1 — SSRF-style hardening: evidence URLs have no private-network guards
**What’s wrong**: Evidence fetching will request any configured URL; there are no explicit guards against `localhost`, RFC1918 private ranges, link-local, or redirects into private IPs.

**Why it matters**: Low-cost safety hardening that prevents “oops” mistakes and reduces blast radius if configs are ever compromised.

**Where**: `collector_core/pipeline_driver_base.py` (`fetch_url_with_retry()`)

**Fix**:
- Allow only `http/https`.
- Resolve host → IP and block private/link-local/loopback ranges.
- Re-validate on redirects.
- Optional escape hatch flag: `--allow-private-evidence-hosts`.

**Verify**: Tests that `http://127.0.0.1/...` is rejected and redirects to private IPs are rejected.

---

## 8. P2 — Acquire workers don’t fail the process when target downloads fail
**What’s wrong**: `run_acquire_worker()` writes a summary and exits 0, even if individual targets returned `status=error`.

**Why it matters**: `tools/build_natural_corpus.py` uses `subprocess.run(..., check=True)`. If acquire exits 0, orchestration continues and later stages run on incomplete data.

**Where**: `collector_core/acquire_strategies.py` (`run_acquire_worker()`)

**Fix**:
- Add `--fail-on-error/--strict`.
- When executing (`--execute`), exit non-zero if any target errored.

**Verify**: Intentionally break one target and confirm the worker exits 1 under strict mode.

---

## 9. P2 — Core code imports from `tools` (layering / packaging risk)
**What’s wrong**: `collector_core/merge.py` imports `tools.output_contract`.

**Why it matters**: `tools/` is “CLI scripts” by convention. Depending on it from core runtime code is brittle if you ever package/relocate parts of the repo.

**Where**: `collector_core/merge.py`

**Fix**:
- Move output-contract helpers into `collector_core/` (or a `collector_shared/` module).
- Have `tools/` import from that shared location.

---

## 10. P2 — Tests fail to collect without Hugging Face `datasets`
**What’s wrong**: Several tests import `datasets` at module import time, so `pytest` can’t even collect unless HF `datasets` is installed.

**Where**: multiple tests under `tests/` (errors occurred on `from datasets import Dataset` and in `collector_core/merge.py`).

**Fix**:
- For tests that truly need HF datasets: `pytest.importorskip("datasets")` or mark as `integration` and skip by default.
- Keep unit tests runnable in a minimal environment.

**Verify**: `pytest -q` should run at least unit tests without requiring HF.

---

## 11. P3 — Minor CLI ergonomics: `--enable-resume` is redundant
**What’s wrong**: `--enable-resume` is `store_true` but defaults True; you effectively only need `--no-resume`.

**Fix (optional)**: Use `argparse.BooleanOptionalAction` for `--resume/--no-resume`, or keep only `--no-resume` and document resume as default.

---

# 2) Recommended updates

## A. Reduce companion-file drift across pipelines
You already have `configs/common/`. Make those the single source of truth and have pipelines reference them via `companion_files:` instead of copying.

## B. Add `pre-commit` (ruff + formatting + yamllint)
Suggested hooks: `ruff check`, `ruff format`, `yamllint`, whitespace/EOF fixes.

## C. Enforce formatting in CI
Add `ruff format --check .` so formatting doesn’t drift.

## D. Add Dependabot
Keep GitHub Actions and pinned dependency constraints up to date automatically.

## E. Add a tiny “smoke pipeline” fixture for CI
A micro targets file (1 green, 1 yellow, 1 denylist) that exercises classify/acquire/merge/catalog in dry-run.

## F. Add stage summaries that are easy to consume
In each stage summary JSON: counts, and a `failed_targets` list with `{id, error}`.

## G. Add a pipeline skeleton generator
Most pipeline wrappers are nearly identical; a generator prevents subtle drift and accelerates new domains.

## H. Add a “release zip” helper
Even if you don’t use `git archive`, provide `tools/make_release_zip.py` that copies the repo to a temp dir, runs cleanup, and zips with exclusions.

---

# Suggested implementation order

1) Update/archive `REPO_ISSUES_AND_UPDATES.md`
2) Fix denylist domain scanning (extract download URLs + boundary-safe matching)
3) Harden evidence snapshot (atomic write + integrity verify + extension drift handling)
4) Add strict failure mode for acquire workers (non-zero exit on errors)
5) Add evidence URL private-network guards
6) Fix layering (move output contract out of `tools/`)
7) Improve tests (unit vs integration split)
8) Add pre-commit + CI format check + Dependabot

---

## Quick commands

Clean artifacts before zipping:
```bash
python -m tools.clean_repo_tree --repo-root . --yes
```

Validate configs:
```bash
python -m tools.validate_yaml_schemas --root .
python -m tools.validate_repo --repo-root . --strict
python -m tools.preflight --repo-root . --quiet
```

Run tests (full dev env):
```bash
pip install -r requirements-dev.constraints.txt
pytest -q
```
