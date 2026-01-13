# Dataset Collector repo — A‑Grade Patch Checklist (Concrete file‑by‑file diffs)

This is a **concrete, implementable** checklist (rename/move/delete exact files; update exact config blocks; edit specific functions) to make the repo **more elegant, more robust, and "A‑grade"** while keeping the current architecture intact.

> Scope note: Items are prioritized (P0→P3). P0 are critical security fixes. P1 eliminates correctness/error handling issues. P2 improves code quality and developer UX. P3 items are test coverage and documentation polish.

---

## ✅ Previously Completed Items (v1.0)

<details>
<summary>Click to expand completed items from previous audit</summary>

### P0 — Stop path/packaging footguns ✅ DONE
- [x] **P0.1**: Removed duplicate `tools/` package at repo root, moved shell script to `scripts/`
- [x] **P0.2**: Added 14 console scripts to `pyproject.toml`
- [x] **P0.3**: Updated CI to install package and use console scripts, removed PYTHONPATH injection
- [x] **P0.4**: Deleted root-level `schemas/`, updated fallback path, cleaned up test conftest.py

### P1.2-P1.4 — Correctness fixes ✅ DONE
- [x] **P1.2**: Fixed missing `re` import in `pmc_worker.py`
- [x] **P1.3**: Fixed `_get_default_handlers()` bug in `acquire_strategies.py`
- [x] **P1.4**: Added return type annotations to `pmc_worker.py`, `acquire_strategies.py`, `near_duplicate.py`

### P2 — Developer UX ✅ DONE
- [x] **P2.1**: Updated `README.md` and `docs/quickstart.md` to use new CLI commands
- [x] **P2.2**: Updated `.pre-commit-config.yaml` to self-install the package

### P1.1 — Yellow screen entrypoint duplication → one dispatcher path ✅ DONE
- [x] **P1.1A-F**: Unified yellow screen dispatch, removed 5 duplicate wrapper modules

### P3.1-P3.4 — Pipeline unification, contracts, mypy, tests ✅ DONE
- [x] **P3.1**: Unified pipeline sources of truth (YAML-driven config)
- [x] **P3.2**: Output contracts enforced in CI
- [x] **P3.3**: Expanded mypy to `src/tools`
- [x] **P3.4**: Added high-impact test coverage

</details>

---

## ✅ Completed Items (v2.0 — Security & Quality Audit)

### P0 — Critical Security Fixes ✅ DONE

#### P0.1 — FTP Command Injection ✅
- [x] **P0.1A**: Sanitize filenames in `src/collector_core/acquire/strategies/ftp.py:73`
  ```python
  # BEFORE (vulnerable):
  ftp.retrbinary(f"RETR {fname}", f.write)

  # AFTER (safe):
  # Validate fname contains no control characters or path traversal
  if not _is_safe_filename(fname):
      raise ValueError(f"Unsafe filename from FTP server: {fname!r}")
  ftp.retrbinary(f"RETR {fname}", f.write)
  ```
- [x] **P0.1B**: Add `_is_safe_filename()` helper that rejects filenames containing: newlines, carriage returns, null bytes, `..`, absolute paths

#### P0.2 — Torrent/Magnet Command Injection ✅
- [x] **P0.2A**: Validate magnet link format in `src/collector_core/acquire/strategies/torrent.py:82`
  ```python
  # BEFORE (vulnerable):
  log = run_cmd(["aria2c", "--seed-time=0", "-d", str(out_dir), magnet])

  # AFTER (safe):
  if not _is_valid_magnet(magnet):
      return [{"status": "error", "error": "Invalid magnet link format"}]
  log = run_cmd(["aria2c", "--seed-time=0", "-d", str(out_dir), magnet])
  ```
- [x] **P0.2B**: Add `_is_valid_magnet()` that validates `magnet:?xt=urn:` prefix and rejects shell metacharacters

#### P0.3 — S3 Parameter Injection ✅
- [x] **P0.3A**: Whitelist allowed AWS CLI parameters in `src/collector_core/acquire/strategies/s3.py:96-97`
  ```python
  # Add validation before command construction:
  ALLOWED_REQUEST_PAYER_VALUES = {"requester", ""}
  if download.get("request_payer") and str(download["request_payer"]) not in ALLOWED_REQUEST_PAYER_VALUES:
      raise ValueError(f"Invalid request_payer value: {download['request_payer']}")

  # Validate extra_args against whitelist
  ALLOWED_EXTRA_ARGS = {"--no-sign-request", "--region", "--endpoint-url"}
  for arg in extra_args:
      if not any(str(arg).startswith(allowed) for allowed in ALLOWED_EXTRA_ARGS):
          raise ValueError(f"Disallowed S3 extra arg: {arg}")
  ```

#### P0.4 — Zenodo SSRF Prevention ✅
- [x] **P0.4A**: Validate `record_id` and `doi` in `src/collector_core/acquire/strategies/zenodo.py:87-89`
  ```python
  # Add validation:
  import re
  RECORD_ID_PATTERN = re.compile(r"^\d+$")
  DOI_PATTERN = re.compile(r"^10\.\d{4,}/[^\s]+$")

  if record_id and not RECORD_ID_PATTERN.match(str(record_id)):
      raise ValueError(f"Invalid Zenodo record_id: {record_id}")
  if doi and not DOI_PATTERN.match(str(doi)):
      raise ValueError(f"Invalid DOI format: {doi}")
  ```

#### P0.5 — GitHub Token Security ✅
- [x] **P0.5A**: Remove plaintext token file support in `src/collector_core/acquire/strategies/github_release.py:100-104`
  ```python
  # BEFORE:
  token_file = Path.home() / ".github_token"
  if not token and token_file.exists():
      token = token_file.read_text().strip()

  # AFTER: Remove file-based token, only use env var
  # Add warning if token found in download config
  if download.get("github_token"):
      logger.warning("github_token in config is deprecated; use GITHUB_TOKEN env var")
  ```
- [x] **P0.5B**: Deprecated github_token config option, only use GITHUB_TOKEN env var

#### P0.6 — Path Traversal in YAML Include ✅
- [x] **P0.6A**: Add symlink check in `src/collector_core/config_validator.py:112`
  ```python
  include_path = (base_dir / include_path).resolve()
  # Add: Verify resolved path is within allowed directory
  if not include_path.is_relative_to(base_dir.resolve()):
      raise ValueError(f"Include path escapes base directory: {include_path}")
  if include_path.is_symlink():
      raise ValueError(f"Symlinks not allowed in includes: {include_path}")
  ```

---

### P1 — Error Handling & Correctness Fixes ✅ DONE

#### P1.1 — Replace Broad Exception Catches (18 instances) ✅
- [x] **P1.1A**: `src/collector_core/stability.py:12` — Catch `AttributeError` instead of `Exception`
- [x] **P1.1B**: `src/collector_core/policy_snapshot.py:22` — Catch `(subprocess.SubprocessError, FileNotFoundError)` instead of `Exception`
- [x] **P1.1C**: `src/collector_core/denylist_matcher.py:32` — Catch `ValueError` instead of `Exception`
- [x] **P1.1D**: `src/collector_core/sharding.py:442` — Log error before swallowing, catch `OSError`
- [x] **P1.1E**: `src/collector_core/pipeline_driver_base.py:451,489` — Catch specific URL parsing exceptions
- [x] **P1.1F**: `src/collector_core/yellow_scrubber_base.py:458` — Catch `requests.RequestException`
- [x] **P1.1G**: `src/collector_core/pmc_worker.py:212` — Catch `(tarfile.TarError, zlib.error)`
- [x] **P1.1H**: `src/collector_core/review_queue.py:115,126` — Catch `(json.JSONDecodeError, OSError)`
- [x] **P1.1I**: `src/collector_core/queue/emission.py:19` — Catch `ValueError` instead of `Exception`
- [x] **P1.1J**: `src/collector_core/observability.py` — Replace 8 broad catches with specific OTEL exceptions

#### P1.2 — Add Missing Error Handling (8 instances) ✅
- [x] **P1.2A**: `src/collector_core/acquire/strategies/figshare.py:79` — Wrap `resp.json()` in try/except
  ```python
  try:
      meta = resp.json()
  except json.JSONDecodeError as e:
      return [{"status": "error", "error": f"Invalid JSON from Figshare API: {e}"}]
  ```
- [x] **P1.2B**: `src/collector_core/acquire/strategies/zenodo.py:111` — Same pattern
- [x] **P1.2C**: `src/collector_core/acquire/strategies/github_release.py:138` — Same pattern
- [x] **P1.2D**: `src/collector_core/catalog_builder.py:38-42` — Handle `FileNotFoundError` in `file_stats()`
- [x] **P1.2E**: `src/collector_core/utils/io.py:38-41` — Wrap zstd stream creation in try/except
- [x] **P1.2F**: `src/collector_core/checkpoint.py:41` — Handle `json.JSONDecodeError` in `load_checkpoint()`
- [x] **P1.2G**: `src/collector_core/evidence/fetching.py:578` — Handle `OSError` on rename
- [x] **P1.2H**: `src/collector_core/decision_bundle.py:241` — Handle file read/JSON errors

#### P1.3 — Fix Race Conditions (4 instances) ✅
- [x] **P1.3A**: `src/collector_core/sharding.py:447` — Ensure file is flushed before atomic rename
  ```python
  # In __exit__, before replace():
  if self._wrapper is not None:
      self._wrapper.flush()
  if self._file is not None:
      self._file.flush()
      os.fsync(self._file.fileno())  # Ensure data on disk
  ```
- [x] **P1.3B**: `src/collector_core/utils/io.py:28-31` — Add fsync before `write_json()` replace
- [x] **P1.3C**: `src/collector_core/merge/__init__.py:602` — Add fsync before atomic rename
- [x] **P1.3D**: `src/collector_core/evidence/fetching.py:578-589` — Added try/except for atomic operation

#### P1.4 — Fix Missing Null Checks (7 instances) ✅
- [x] **P1.4A**: `src/collector_core/acquire/strategies/figshare.py:174` — Check if `f` is a dict before `.get()`
- [x] **P1.4B**: `src/collector_core/acquire/strategies/zenodo.py:116` — Fix unsafe `[0]` access on potentially empty list
  ```python
  # BEFORE:
  files = data.get("files", []) or data.get("hits", {}).get("hits", [{}])[0].get("files", [])

  # AFTER:
  files = data.get("files", [])
  if not files:
      hits = data.get("hits", {}).get("hits", [])
      if hits:
          files = hits[0].get("files", [])
  ```
- [x] **P1.4C**: `src/collector_core/archive_safety.py:205` — Check `member.file_size` for None
- [x] **P1.4D**: `src/collector_core/yellow_scrubber_base.py:260` — Add JSON decode error handling
- [x] **P1.4E-G**: Similar checks in `decision_bundle.py`, `catalog_builder.py`, `checkpoint.py`

---

## ✅ Completed Items (v3.0 — Code Quality)

### P2 — Code Quality Improvements ✅ PARTIAL

#### P2.1 — Eliminate Duplicate Code ✅ DONE
- [x] **P2.1A**: Extracted `normalize_download()` to `src/collector_core/utils/download.py`
  - Updated `src/tools/validate_repo.py` to import from new location
  - Updated `src/collector_core/acquire/strategies/http.py` to delegate to new location
  - Updated `src/collector_core/acquire/strategies/git.py` to import from new location
  - Updated `src/tools/preflight.py` to import from new location
  - Updated `src/collector_core/acquire/strategies/s3.py` to import from new location
  - Updated `src/collector_core/acquire/strategies/torrent.py` to import from new location
  - Updated `src/collector_core/acquire/strategies/zenodo.py` to import from new location

- [x] **P2.1B**: Extracted `run_cmd()` to `src/collector_core/utils/subprocess.py`
  - Removed duplicate from `src/collector_core/acquire/strategies/git.py`
  - Removed duplicate from `src/collector_core/acquire/strategies/s3.py`
  - Removed duplicate from `src/collector_core/acquire/strategies/torrent.py`

- [x] **P2.1C**: Added `md5_file()` to `src/collector_core/utils/hash.py`
  - `sha256_file()` already existed
  - Removed duplicate `md5_file()` from `src/collector_core/acquire/strategies/zenodo.py`

#### P2.2 — Refactor Long Functions 🔲 PENDING
- [ ] **P2.2A**: Split `run_pmc_worker()` (247 lines) in `src/collector_core/pmc_worker.py:385`
  - Extract `_process_batch()`, `_handle_article()`, `_write_outputs()`
- [ ] **P2.2B**: Split `process_target()` (231 lines) in `src/collector_core/yellow/base.py:221`
  - Extract `_validate_target()`, `_apply_screening()`, `_write_results()`
- [ ] **P2.2C**: Split `run_preflight()` (214 lines) in `src/tools/preflight.py:55`
  - Extract `_check_targets()`, `_check_strategies()`, `_generate_report()`

#### P2.3 — Consolidate Domain Implementations ✅ DONE
- [x] **P2.3A**: Created `src/collector_core/yellow/domains/base.py` with:
  - Default `filter_record()` implementation delegating to `standard_filter()`
  - Default `transform_record()` implementation delegating to `standard_transform()`
  - Common utilities: `extract_text()`, `detect_pii()`, `calculate_quality_score()`
  - Re-exports of `DomainContext`, `FilterDecision`, `standard_filter`, `standard_transform`
- [x] **P2.3B**: Domain modules can now optionally import from base (backwards compatible)

#### P2.4 — Standardize CLI Arguments ✅ DONE
- [x] **P2.4A**: Added `--targets` argument in `src/collector_core/acquire/worker.py`
- [x] **P2.4B**: Kept `--targets-yaml` as deprecated alias
- [x] **P2.4C**: Added deprecation warning when `--targets-yaml` is used

#### P2.5 — Clean Up Deprecated Module ✅ ALREADY DONE
- [x] **P2.5A**: Deprecation warning already present in `src/collector_core/acquire_strategies.py`
- [x] **P2.5B**: Module already uses lazy imports that emit deprecation warnings
- [x] **P2.5C**: Migration guide exists at `docs/migration_guide.md`

---

### P3 — Test Coverage & Documentation ✅ PARTIAL

#### P3.1 — Add Tests for Untested Modules ✅ PARTIAL
- [ ] **P3.1A**: Create `tests/test_network_utils.py` (95 LOC untested)
  - Test `_is_retryable_http_exception()` with 429, 403, 5xx, timeouts
  - Test `_with_retries()` exponential backoff behavior
  - Test retry count limits

- [ ] **P3.1B**: Create `tests/test_observability.py` (447 LOC untested)
  - Test `_setup_otel_tracing()` initialization
  - Test metric recording functions
  - Test fallback behavior when OTEL unavailable

- [ ] **P3.1C**: Create `tests/test_policy_override.py` (305 LOC untested)
  - Test `PolicyOverride.is_active()` with expiration edge cases
  - Test `PolicyOverride.matches_rule()` pattern matching
  - Test `apply_override_to_decision()` RED→YELLOW, FORCE_GREEN transformations

- [x] **P3.1D**: Created `tests/test_decision_bundle.py` with comprehensive tests:
  - Tests for `to_dict()` serialization
  - Tests for `from_dict()` deserialization with missing fields
  - Tests for nested data structures
  - Tests for save/load roundtrip
  - Tests for bundle_from_denylist_hits

- [x] **P3.1E**: Created `tests/test_denylist_matcher.py` with comprehensive tests:
  - Tests for `extract_domain()` with malformed URLs
  - Tests for `_domain_matches()` subdomain logic
  - Tests for `denylist_hits()` with regex, substring, domain patterns
  - Tests for publisher pattern matching

- [ ] **P3.1F**: Create `tests/test_evidence_policy.py` (290 LOC untested)
  - Test evidence fetching and validation

#### P3.2 — Add Tests for Untested Pipelines 🔲 PENDING
- [ ] **P3.2A**: Create `tests/test_domain_screeners/test_agri_circular_screener.py`
- [ ] **P3.2B**: Create `tests/test_domain_screeners/test_earth_screener.py`
- [ ] **P3.2C**: Create `tests/test_domain_screeners/test_econ_screener.py`
- [ ] **P3.2D**: Create `tests/test_domain_screeners/test_engineering_screener.py`

#### P3.3 — Add Error Path Tests 🔲 PENDING
- [ ] **P3.3A**: Add `pytest.raises` tests to `tests/test_merge_shard.py`
- [ ] **P3.3B**: Add error path tests to `tests/test_merge_contract.py`
- [ ] **P3.3C**: Add error path tests to `tests/test_pipeline_driver_classification.py`
- [ ] **P3.3D**: Target: Increase error path coverage from 6.6% to >30%

#### P3.4 — Fix Documentation Issues ✅ PARTIAL
- [ ] **P3.4A**: Implement or remove `DC_PROFILE` system
  - Option 1: Implement profile loading in `src/collector_core/dc_cli.py`
  - Option 2: Remove `configs/profiles/` and all documentation references

- [x] **P3.4B**: Updated `docs/environment-variables.md` with defaults:
  - Added default values column to all tables
  - Added Observability section with OTEL variables
  - Added HF_TOKEN and AWS credentials

- [ ] **P3.4C**: Resolve requirements file confusion
  - Update `docs/run_instructions.md:51` to use `pipelines/requirements/<domain>.txt`
  - Add deprecation notice to `*_pipeline_v2/requirements.txt` files

- [ ] **P3.4D**: Document JSON schema validation
  - Add section to `docs/configuration.md` explaining schema validation
  - Document `dc-validate-yaml-schemas` command

- [x] **P3.4E**: Created `docs/cli-reference.md` documenting all 22 console scripts:
  - Main commands (dc, dc-pipeline, dc-review, dc-catalog)
  - Validation commands (dc-preflight, dc-validate-repo, etc.)
  - Maintenance commands (dc-sync-wrappers, dc-clean-repo-tree, etc.)
  - Common options and exit codes

---

## "Done when" checklist (definition of A‑grade v3.0)

### Security ✅ COMPLETE
- [x] No command injection vulnerabilities in download strategies (P0.1-P0.3)
- [x] No SSRF risks in API URL construction (P0.4)
- [x] No plaintext credential storage (P0.5)
- [x] No path traversal in config loading (P0.6)

### Error Handling ✅ COMPLETE
- [x] No broad `except Exception:` catches without specific handling (P1.1)
- [x] All external API calls have JSON decode error handling (P1.2)
- [x] All atomic file operations use fsync before rename (P1.3)
- [x] All index/key accesses have null checks (P1.4)

### Code Quality ✅ MOSTLY COMPLETE
- [x] No duplicate utility functions across modules (P2.1) — Consolidated to utils/
- [ ] No functions exceeding 150 lines (P2.2) — 3 long functions remain
- [x] Domain implementations share common base (P2.3) — Created domains/base.py
- [x] CLI arguments are consistent across all workers (P2.4) — Standardized to --targets

### Test Coverage 🔲 PARTIAL
- [x] Key untested modules now have test files (P3.1D, P3.1E) — 2/6 done
- [ ] All 19 pipelines have screener tests (P3.2) — Pending
- [ ] Error path coverage >30% (P3.3) — Pending

### Documentation ✅ MOSTLY COMPLETE
- [x] All environment variables documented with defaults (P3.4B) — Updated
- [x] All CLI commands documented (P3.4E) — Created cli-reference.md
- [ ] No references to deprecated file locations (P3.4C) — Pending

---

## Priority Summary

| Priority | Category | Item Count | Effort | Impact |
|----------|----------|------------|--------|--------|
| **P0** | Security | 6 items | Medium | Critical — prevents exploits |
| **P1** | Error Handling | 4 categories, 37 fixes | Medium | High — prevents crashes |
| **P2** | Code Quality | 5 categories | High | Medium — maintainability |
| **P3** | Tests & Docs | 4 categories | High | Medium — reliability |

### Recommended Implementation Order

1. **Week 1**: P0 Security fixes (all 6 items)
2. **Week 2**: P1.1-P1.2 Exception handling (26 fixes)
3. **Week 3**: P1.3-P1.4 Race conditions and null checks (11 fixes)
4. **Week 4**: P2.1 Duplicate code elimination
5. **Week 5**: P3.1 Critical test coverage (6 test files)
6. **Ongoing**: P2.2-P2.5, P3.2-P3.4

---

## Files to Create

| File | Purpose |
|------|---------|
| `src/collector_core/utils/download.py` | Consolidated `normalize_download()` |
| `src/collector_core/utils/subprocess.py` | Consolidated `run_cmd()` |
| `src/collector_core/yellow/domains/base.py` | Base domain implementation |
| `tests/test_network_utils.py` | Network utility tests |
| `tests/test_observability.py` | Observability tests |
| `tests/test_policy_override.py` | Policy override tests |
| `tests/test_decision_bundle.py` | Decision bundle tests |
| `tests/test_denylist_matcher.py` | Denylist matcher tests |
| `tests/test_evidence_policy.py` | Evidence policy tests |
| `tests/test_domain_screeners/test_agri_circular_screener.py` | Pipeline test |
| `tests/test_domain_screeners/test_earth_screener.py` | Pipeline test |
| `tests/test_domain_screeners/test_econ_screener.py` | Pipeline test |
| `tests/test_domain_screeners/test_engineering_screener.py` | Pipeline test |
| `docs/cli-reference.md` | CLI documentation |
| `docs/migration.md` | Migration guide for deprecated APIs |

## Files to Modify

| File | Changes |
|------|---------|
| `src/collector_core/acquire/strategies/ftp.py` | P0.1 — Filename sanitization |
| `src/collector_core/acquire/strategies/torrent.py` | P0.2 — Magnet validation |
| `src/collector_core/acquire/strategies/s3.py` | P0.3 — Parameter whitelist |
| `src/collector_core/acquire/strategies/zenodo.py` | P0.4 — Input validation |
| `src/collector_core/acquire/strategies/github_release.py` | P0.5 — Remove file token |
| `src/collector_core/config_validator.py` | P0.6 — Path traversal check |
| `src/collector_core/acquire/strategies/figshare.py` | P1.2A, P1.4A — Error handling |
| `src/collector_core/sharding.py` | P1.1D, P1.3A — Exception handling, fsync |
| `src/collector_core/utils/io.py` | P1.2E, P1.3B — Error handling, locking |
| `src/collector_core/acquire/worker.py` | P2.4A — CLI argument rename |
| `docs/run_instructions.md` | P2.4B, P3.4C — Consistency fixes |
| `docs/environment-variables.md` | P3.4B — Add defaults |
