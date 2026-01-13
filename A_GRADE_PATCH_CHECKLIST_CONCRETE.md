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

## 🔲 Pending Items (v2.0 — Security & Quality Audit)

### P0 — Critical Security Fixes

#### P0.1 — FTP Command Injection
- [ ] **P0.1A**: Sanitize filenames in `src/collector_core/acquire/strategies/ftp.py:73`
  ```python
  # BEFORE (vulnerable):
  ftp.retrbinary(f"RETR {fname}", f.write)

  # AFTER (safe):
  # Validate fname contains no control characters or path traversal
  if not _is_safe_filename(fname):
      raise ValueError(f"Unsafe filename from FTP server: {fname!r}")
  ftp.retrbinary(f"RETR {fname}", f.write)
  ```
- [ ] **P0.1B**: Add `_is_safe_filename()` helper that rejects filenames containing: newlines, carriage returns, null bytes, `..`, absolute paths

#### P0.2 — Torrent/Magnet Command Injection
- [ ] **P0.2A**: Validate magnet link format in `src/collector_core/acquire/strategies/torrent.py:82`
  ```python
  # BEFORE (vulnerable):
  log = run_cmd(["aria2c", "--seed-time=0", "-d", str(out_dir), magnet])

  # AFTER (safe):
  if not _is_valid_magnet(magnet):
      return [{"status": "error", "error": "Invalid magnet link format"}]
  log = run_cmd(["aria2c", "--seed-time=0", "-d", str(out_dir), magnet])
  ```
- [ ] **P0.2B**: Add `_is_valid_magnet()` that validates `magnet:?xt=urn:` prefix and rejects shell metacharacters

#### P0.3 — S3 Parameter Injection
- [ ] **P0.3A**: Whitelist allowed AWS CLI parameters in `src/collector_core/acquire/strategies/s3.py:96-97`
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

#### P0.4 — Zenodo SSRF Prevention
- [ ] **P0.4A**: Validate `record_id` and `doi` in `src/collector_core/acquire/strategies/zenodo.py:87-89`
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

#### P0.5 — GitHub Token Security
- [ ] **P0.5A**: Remove plaintext token file support in `src/collector_core/acquire/strategies/github_release.py:100-104`
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
- [ ] **P0.5B**: Add documentation for secure token handling via `gh auth` or credential helpers

#### P0.6 — Path Traversal in YAML Include
- [ ] **P0.6A**: Add symlink check in `src/collector_core/config_validator.py:112`
  ```python
  include_path = (base_dir / include_path).resolve()
  # Add: Verify resolved path is within allowed directory
  if not include_path.is_relative_to(base_dir.resolve()):
      raise ValueError(f"Include path escapes base directory: {include_path}")
  if include_path.is_symlink():
      raise ValueError(f"Symlinks not allowed in includes: {include_path}")
  ```

---

### P1 — Error Handling & Correctness Fixes

#### P1.1 — Replace Broad Exception Catches (18 instances)
- [ ] **P1.1A**: `src/collector_core/stability.py:12` — Catch `AttributeError` instead of `Exception`
- [ ] **P1.1B**: `src/collector_core/policy_snapshot.py:22` — Catch `(subprocess.SubprocessError, FileNotFoundError)` instead of `Exception`
- [ ] **P1.1C**: `src/collector_core/denylist_matcher.py:32` — Catch `ValueError` instead of `Exception`
- [ ] **P1.1D**: `src/collector_core/sharding.py:442` — Log error before swallowing, catch `OSError`
- [ ] **P1.1E**: `src/collector_core/pipeline_driver_base.py:451,489` — Catch specific URL parsing exceptions
- [ ] **P1.1F**: `src/collector_core/yellow_scrubber_base.py:458` — Catch `requests.RequestException`
- [ ] **P1.1G**: `src/collector_core/pmc_worker.py:212` — Catch `(tarfile.TarError, zlib.error)`
- [ ] **P1.1H**: `src/collector_core/review_queue.py:115,126` — Catch `(json.JSONDecodeError, OSError)`
- [ ] **P1.1I**: `src/collector_core/queue/emission.py:19` — Catch `ValueError` instead of `Exception`
- [ ] **P1.1J**: `src/collector_core/observability.py` — Replace 8 broad catches with specific OTEL exceptions

#### P1.2 — Add Missing Error Handling (8 instances)
- [ ] **P1.2A**: `src/collector_core/acquire/strategies/figshare.py:79` — Wrap `resp.json()` in try/except
  ```python
  try:
      meta = resp.json()
  except json.JSONDecodeError as e:
      return [{"status": "error", "error": f"Invalid JSON from Figshare API: {e}"}]
  ```
- [ ] **P1.2B**: `src/collector_core/acquire/strategies/zenodo.py:111` — Same pattern
- [ ] **P1.2C**: `src/collector_core/acquire/strategies/github_release.py:138` — Same pattern
- [ ] **P1.2D**: `src/collector_core/catalog_builder.py:38-42` — Handle `FileNotFoundError` in `file_stats()`
- [ ] **P1.2E**: `src/collector_core/utils/io.py:38-41` — Wrap zstd stream creation in try/except
- [ ] **P1.2F**: `src/collector_core/checkpoint.py:41` — Handle `json.JSONDecodeError` in `load_checkpoint()`
- [ ] **P1.2G**: `src/collector_core/evidence/fetching.py:578` — Handle `OSError` on rename
- [ ] **P1.2H**: `src/collector_core/decision_bundle.py:241` — Handle file read/JSON errors

#### P1.3 — Fix Race Conditions (4 instances)
- [ ] **P1.3A**: `src/collector_core/sharding.py:447` — Ensure file is flushed before atomic rename
  ```python
  # In __exit__, before replace():
  if self._wrapper is not None:
      self._wrapper.flush()
  if self._file is not None:
      self._file.flush()
      os.fsync(self._file.fileno())  # Ensure data on disk
  ```
- [ ] **P1.3B**: `src/collector_core/utils/io.py:28-31` — Add file locking for `write_json()`
- [ ] **P1.3C**: `src/collector_core/merge/__init__.py:602` — Add fsync before atomic rename
- [ ] **P1.3D**: `src/collector_core/evidence/fetching.py:578-589` — Use a single atomic operation

#### P1.4 — Fix Missing Null Checks (7 instances)
- [ ] **P1.4A**: `src/collector_core/acquire/strategies/figshare.py:174` — Check if `f` is a dict before `.get()`
- [ ] **P1.4B**: `src/collector_core/acquire/strategies/zenodo.py:116` — Fix unsafe `[0]` access on potentially empty list
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
- [ ] **P1.4C**: `src/collector_core/archive_safety.py:205` — Check `member.file_size` for None
- [ ] **P1.4D**: `src/collector_core/yellow_scrubber_base.py:260` — Add file existence check before JSON load
- [ ] **P1.4E-G**: Similar checks in `decision_bundle.py`, `catalog_builder.py`, `checkpoint.py`

---

### P2 — Code Quality Improvements

#### P2.1 — Eliminate Duplicate Code
- [ ] **P2.1A**: Extract `normalize_download()` to `src/collector_core/utils/download.py`
  - Remove duplicate from `src/tools/validate_repo.py:34`
  - Remove duplicate from `src/collector_core/acquire_strategies.py:170`
  - Remove duplicate from `src/collector_core/acquire/strategies/http.py:41`
  - Remove duplicate from `src/collector_core/acquire/strategies/git.py:19`
  - Remove duplicate from `src/tools/preflight.py:21`
  - Import from new location in all files

- [ ] **P2.1B**: Extract `run_cmd()` to `src/collector_core/utils/subprocess.py`
  - Consolidate 4 duplicate implementations

- [ ] **P2.1C**: Extract `sha256_file()` and `md5_file()` to `src/collector_core/utils/hash.py`
  - Update all 3 locations to import from utils

#### P2.2 — Refactor Long Functions
- [ ] **P2.2A**: Split `run_pmc_worker()` (247 lines) in `src/collector_core/pmc_worker.py:385`
  - Extract `_process_batch()`, `_handle_article()`, `_write_outputs()`
- [ ] **P2.2B**: Split `process_target()` (231 lines) in `src/collector_core/yellow/base.py:221`
  - Extract `_validate_target()`, `_apply_screening()`, `_write_results()`
- [ ] **P2.2C**: Split `run_preflight()` (214 lines) in `src/tools/preflight.py:55`
  - Extract `_check_targets()`, `_check_strategies()`, `_generate_report()`

#### P2.3 — Consolidate Domain Implementations
- [ ] **P2.3A**: Create `src/collector_core/yellow/domains/base.py` with default implementations
  ```python
  def filter_record(record: dict[str, Any], config: dict[str, Any]) -> bool:
      """Default filter - include all records."""
      return True

  def transform_record(record: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
      """Default transform - return record unchanged."""
      return record
  ```
- [ ] **P2.3B**: Update all 9 domain modules to inherit/import from base:
  - `yellow/domains/code.py`, `biology.py`, `econ.py`, `nlp.py`, `kg_nav.py`, `safety.py`, `cyber.py`, `standard.py`, `chem.py`

#### P2.4 — Standardize CLI Arguments
- [ ] **P2.4A**: Rename `--targets-yaml` to `--targets` in `src/collector_core/acquire/worker.py:411`
- [ ] **P2.4B**: Update `docs/run_instructions.md` to use consistent `--targets` everywhere
- [ ] **P2.4C**: Add deprecation warning for `--targets-yaml` with message to use `--targets`

#### P2.5 — Clean Up Deprecated Module
- [ ] **P2.5A**: Add `DeprecationWarning` to `src/collector_core/acquire_strategies.py` module docstring
- [ ] **P2.5B**: Update all internal imports to use new locations
- [ ] **P2.5C**: Add migration guide to `docs/migration.md`

---

### P3 — Test Coverage & Documentation

#### P3.1 — Add Tests for Untested Modules (Critical)
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

- [ ] **P3.1D**: Create `tests/test_decision_bundle.py` (351 LOC untested)
  - Test `to_dict()` serialization
  - Test `from_dict()` deserialization with missing fields
  - Test nested data structures

- [ ] **P3.1E**: Create `tests/test_denylist_matcher.py` (248 LOC untested)
  - Test `extract_domain()` with malformed URLs
  - Test `_domain_matches()` subdomain logic
  - Test `denylist_hits()` regex patterns

- [ ] **P3.1F**: Create `tests/test_evidence_policy.py` (290 LOC untested)
  - Test evidence fetching and validation

#### P3.2 — Add Tests for Untested Pipelines
- [ ] **P3.2A**: Create `tests/test_domain_screeners/test_agri_circular_screener.py`
- [ ] **P3.2B**: Create `tests/test_domain_screeners/test_earth_screener.py`
- [ ] **P3.2C**: Create `tests/test_domain_screeners/test_econ_screener.py`
- [ ] **P3.2D**: Create `tests/test_domain_screeners/test_engineering_screener.py`

#### P3.3 — Add Error Path Tests
- [ ] **P3.3A**: Add `pytest.raises` tests to `tests/test_merge_shard.py`
- [ ] **P3.3B**: Add error path tests to `tests/test_merge_contract.py`
- [ ] **P3.3C**: Add error path tests to `tests/test_pipeline_driver_classification.py`
- [ ] **P3.3D**: Target: Increase error path coverage from 6.6% to >30%

#### P3.4 — Fix Documentation Issues
- [ ] **P3.4A**: Implement or remove `DC_PROFILE` system
  - Option 1: Implement profile loading in `src/collector_core/dc_cli.py`
  - Option 2: Remove `configs/profiles/` and all documentation references

- [ ] **P3.4B**: Document environment variable defaults in `docs/environment-variables.md`
  ```markdown
  | Variable | Default | Description |
  |----------|---------|-------------|
  | PIPELINE_RETRY_MAX | 3 | Maximum retry attempts |
  | PIPELINE_RETRY_BACKOFF | 2.0 | Backoff multiplier in seconds |
  | OTEL_EXPORTER_OTLP_ENDPOINT | (none) | OpenTelemetry collector endpoint |
  ```

- [ ] **P3.4C**: Resolve requirements file confusion
  - Update `docs/run_instructions.md:51` to use `pipelines/requirements/<domain>.txt`
  - Add deprecation notice to `*_pipeline_v2/requirements.txt` files

- [ ] **P3.4D**: Document JSON schema validation
  - Add section to `docs/configuration.md` explaining schema validation
  - Document `dc-validate-yaml-schemas` command

- [ ] **P3.4E**: Document CLI commands
  - Add `docs/cli-reference.md` covering all 20 console scripts in `pyproject.toml`

---

## "Done when" checklist (definition of A‑grade v2.0)

### Security
- [ ] No command injection vulnerabilities in download strategies (P0.1-P0.3)
- [ ] No SSRF risks in API URL construction (P0.4)
- [ ] No plaintext credential storage (P0.5)
- [ ] No path traversal in config loading (P0.6)

### Error Handling
- [ ] No broad `except Exception:` catches without specific handling (P1.1)
- [ ] All external API calls have JSON decode error handling (P1.2)
- [ ] All atomic file operations use fsync before rename (P1.3)
- [ ] All index/key accesses have null checks (P1.4)

### Code Quality
- [ ] No duplicate utility functions across modules (P2.1)
- [ ] No functions exceeding 150 lines (P2.2)
- [ ] Domain implementations share common base (P2.3)
- [ ] CLI arguments are consistent across all workers (P2.4)

### Test Coverage
- [ ] All modules with >100 LOC have dedicated test files (P3.1)
- [ ] All 19 pipelines have screener tests (P3.2)
- [ ] Error path coverage >30% (P3.3)

### Documentation
- [ ] All environment variables documented with defaults (P3.4B)
- [ ] All CLI commands documented (P3.4E)
- [ ] No references to deprecated file locations (P3.4C)

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
