# Bug Scan Report - Dataset Collector Repository
**Generated**: 2026-01-15
**Revised**: 2026-01-15 (after verification scan)
**Updated**: 2026-01-15 (all bugs fixed)
**Scan Type**: Comprehensive Repository Audit
**Scope**: Logic Bugs, Concurrency Issues, Security Vulnerabilities, Data Integrity, Code Quality
**Status**: ✅ **ALL 12 BUGS FIXED** - 7 Previous Bugs Verified Fixed

---

## EXECUTIVE SUMMARY

This report documents a thorough scan of the Dataset_Collector repository identifying issues and verifying previous fixes.

**Key Findings**:
- All 7 previously reported critical/high-priority bugs have been fixed
- **✅ ALL 12 NEW BUGS FIXED** (2 critical, 3 high, 7 medium)
- ✅ 2 critical bugs in core utility functions fixed - entire pipeline now protected
- No security vulnerabilities found
- Strong architecture with all data integrity issues resolved

---

## BUGS REQUIRING FIXES

### Priority 1: CRITICAL (Fix Immediately)

---

### BUG-010: Non-Atomic Write in Core write_jsonl() Utility ✅ FIXED
**Location**: `src/collector_core/utils/io.py:72-77`
**Severity**: CRITICAL
**Type**: Data Corruption Risk
**Priority**: Immediate - affects entire pipeline
**Status**: ✅ **FIXED** - Atomic writes implemented

#### Description
The core `write_jsonl()` utility function writes directly to the final path without using atomic writes. This function is used throughout the entire pipeline for writing critical JSONL data including combined indexes, screening results, queue files, and more.

#### Current Problematic Code
```python
def write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    """Write records to JSONL file (supports .gz/.zst)."""
    ensure_dir(path.parent)
    with _open_text(path, "wt") as f:  # DIRECT WRITE - NON-ATOMIC!
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
```

#### Impact
- **Pipeline-Wide**: This utility is used across all pipelines
- **Data Corruption**: Any interruption during write corrupts critical data files
- **Cascade Failures**: Corrupted JSONL files cause downstream pipeline failures
- **Inconsistency**: `write_json()` in same file correctly uses atomic writes

#### Recommended Fix
```python
def write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    """Write records to JSONL file (supports .gz/.zst) atomically."""
    ensure_dir(path.parent)
    # For compressed files, write to temp then rename
    if path.suffix in (".gz", ".zst"):
        tmp_path = path.with_suffix(path.suffix + ".tmp")
    else:
        tmp_path = path.with_suffix(".tmp")

    with _open_text(tmp_path, "wt") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
    tmp_path.replace(path)
```

---

### BUG-011: Non-Atomic Write in Core write_jsonl_gz() Utility ✅ FIXED
**Location**: `src/collector_core/utils/io.py:100-108`
**Severity**: CRITICAL
**Type**: Data Corruption Risk
**Priority**: Immediate - affects merge operations
**Status**: ✅ **FIXED** - Atomic writes implemented

#### Description
The `write_jsonl_gz()` utility writes compressed JSONL data directly to the final path. This function is used for writing data shards during merge operations.

#### Current Problematic Code
```python
def write_jsonl_gz(path: Path, rows: Iterable[dict[str, Any]]) -> tuple[int, int]:
    """Write rows to gzipped JSONL file, return (count, bytes)."""
    ensure_dir(path.parent)
    count = 0
    with gzip.open(path, "wt", encoding="utf-8") as f:  # DIRECT WRITE!
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
            count += 1
    return count, path.stat().st_size
```

#### Recommended Fix
```python
def write_jsonl_gz(path: Path, rows: Iterable[dict[str, Any]]) -> tuple[int, int]:
    """Write rows to gzipped JSONL file atomically, return (count, bytes)."""
    ensure_dir(path.parent)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    count = 0
    with gzip.open(tmp_path, "wt", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
            count += 1
    tmp_path.replace(path)
    return count, path.stat().st_size
```

---

### Priority 2: HIGH (Fix This Week)

---

### BUG-008: Non-Atomic Write in CSV Export ✅ FIXED
**Location**: `src/collector_core/review_queue.py:330-336`
**Severity**: HIGH
**Type**: Data Corruption Risk
**Status**: ✅ **FIXED** - Atomic writes implemented

#### Description
The `cmd_export()` function implements atomic writes for JSON exports but **not** for CSV exports.

#### Current Problematic Code
```python
# Lines 323-329: JSON export uses atomic write
tmp_path = Path(f"{out_path}.tmp")
tmp_path.write_text(json.dumps(reviewed, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
tmp_path.replace(out_path)  # Atomic rename

# Lines 330-336: CSV export does NOT use atomic write
elif fmt == "csv":
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as f:  # NON-ATOMIC!
        if reviewed:
            writer = csv.DictWriter(f, fieldnames=reviewed[0].keys())
            writer.writeheader()
            writer.writerows(reviewed)
```

#### Recommended Fix
```python
elif fmt == "csv":
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = out_path.with_suffix(out_path.suffix + ".tmp")
    with tmp_path.open("w", newline="", encoding="utf-8") as f:
        if reviewed:
            writer = csv.DictWriter(f, fieldnames=reviewed[0].keys())
            writer.writeheader()
            writer.writerows(reviewed)
    tmp_path.replace(out_path)
```

---

### BUG-009: Non-Atomic Write in PMC Allowlist Generation ✅ FIXED
**Location**: `src/collector_core/yellow_scrubber_base.py:659-663`
**Severity**: HIGH
**Type**: Data Corruption Risk
**Status**: ✅ **FIXED** - Atomic writes implemented

#### Description
The `plan_pmc_allowlist()` function writes the `pmc_allowlist.jsonl` file directly without using atomic writes.

#### Current Problematic Code
```python
allow_path = out_dir / "pmc_allowlist.jsonl"
with allow_path.open("w", encoding="utf-8") as f:  # NON-ATOMIC!
    for rr in allow_rows:
        f.write(json.dumps(rr, ensure_ascii=False) + "\n")
plan["allowlist_path"] = str(allow_path)
```

#### Recommended Fix
```python
allow_path = out_dir / "pmc_allowlist.jsonl"
tmp_path = allow_path.with_suffix(".tmp")
with tmp_path.open("w", encoding="utf-8") as f:
    for rr in allow_rows:
        f.write(json.dumps(rr, ensure_ascii=False) + "\n")
tmp_path.replace(allow_path)
plan["allowlist_path"] = str(allow_path)
```

---

### BUG-012: Unsafe Empty List Access in Acquire Worker ✅ FIXED
**Location**: `src/collector_core/acquire/worker.py:270-274`
**Severity**: HIGH
**Type**: Runtime Crash
**Status**: ✅ **FIXED** - Empty list check added

#### Description
The acquire worker accesses `manifest["results"][0]` without checking if the results list is empty. If `any()` returns False because the list is empty, the subsequent index access crashes.

#### Current Problematic Code
```python
status = (
    "ok"
    if any(r.get("status") == "ok" for r in manifest["results"])
    else manifest["results"][0].get("status", "error")  # CRASH IF EMPTY!
)
```

#### Impact
- **Pipeline Crash**: IndexError when results list is empty
- **Data Loss**: Interrupts acquire stage, may leave partial state

#### Recommended Fix
```python
results = manifest["results"]
if any(r.get("status") == "ok" for r in results):
    status = "ok"
elif results:
    status = results[0].get("status", "error")
else:
    status = "error"
```

---

### Priority 3: MEDIUM (Fix This Sprint)

---

### BUG-013: Non-Atomic Write in mesh_worker.py ✅ FIXED
**Location**: `3d_modeling_pipeline_v2/mesh_worker.py:142-146`
**Severity**: MEDIUM
**Type**: Data Corruption Risk
**Status**: ✅ **FIXED** - Atomic writes implemented

#### Description
Local `write_jsonl()` function in mesh_worker writes directly to final path.

#### Current Problematic Code
```python
def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8") as f:  # NON-ATOMIC!
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
```

#### Recommended Fix
Use atomic write pattern or import from `collector_core.utils.io` after BUG-010 is fixed.

---

### BUG-014: Non-Atomic Cache Write in PMC Worker ✅ FIXED
**Location**: `src/collector_core/pmc_worker.py:171-173`
**Severity**: MEDIUM
**Type**: Data Corruption / Cache Poisoning
**Status**: ✅ **FIXED** - Atomic writes implemented

#### Description
PMC cache files are written directly without atomic pattern. Corrupted cache files will be reused on retry.

#### Current Problematic Code
```python
if cache_dir:
    ensure_dir(cache_dir)
    cache_path.write_bytes(content)  # NON-ATOMIC!
```

#### Impact
- **Cache Corruption**: Partial writes create invalid cache entries
- **Error Propagation**: Corrupted cache is reused, spreading bad data

#### Recommended Fix
```python
if cache_dir:
    ensure_dir(cache_dir)
    tmp_path = cache_path.with_suffix(".tmp")
    tmp_path.write_bytes(content)
    tmp_path.replace(cache_path)
```

---

### BUG-015: Non-Atomic HTML/XML Response Write ✅ FIXED
**Location**: `3d_modeling_pipeline_v2/acquire_plugin.py:165-166`
**Severity**: MEDIUM
**Type**: Data Corruption Risk
**Status**: ✅ **FIXED** - Atomic writes implemented

#### Description
HTML and XML responses are written directly while JSON responses use atomic writes (inconsistent).

#### Current Problematic Code
```python
if "json" in content_type:
    tmp_path = Path(f"{dest}.tmp")
    tmp_path.write_text(json.dumps(resp.json(), indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    tmp_path.replace(dest)  # JSON is atomic
elif "html" in content_type or "xml" in content_type:
    dest.write_text(resp.text, encoding="utf-8")  # HTML/XML is NOT atomic!
else:
    dest.write_bytes(resp.content)  # Binary is NOT atomic!
```

#### Recommended Fix
Apply atomic write pattern to all content types consistently.

---

### BUG-016: Non-Atomic Binary Response Write ✅ FIXED
**Location**: `3d_modeling_pipeline_v2/acquire_plugin.py:167-168`
**Severity**: MEDIUM
**Type**: Data Corruption Risk
**Status**: ✅ **FIXED** - Atomic writes implemented

#### Description
Binary responses written directly without atomic pattern.

---

### BUG-017: Non-Atomic ToS Snapshot Write + Silent Exception ✅ FIXED
**Location**: `3d_modeling_pipeline_v2/acquire_plugin.py:331-337`
**Severity**: MEDIUM
**Type**: Data Corruption + Silent Failure
**Status**: ✅ **FIXED** - Atomic writes and logging implemented

#### Description
ToS snapshot has two issues: non-atomic write AND silent exception handling.

#### Current Problematic Code
```python
def snapshot_tos(url: str) -> None:
    try:
        resp = requests.get(url, timeout=15)
        fname = catalog_dir / f"tos_{safe_name(urlparse(url).netloc)}.html"
        fname.write_text(resp.text, encoding="utf-8")  # NON-ATOMIC!
    except Exception:
        pass  # SILENT EXCEPTION!
```

#### Impact
- **Data Corruption**: Partial ToS files on interruption
- **Hidden Failures**: ToS fetch failures completely hidden, impossible to debug

#### Recommended Fix
```python
def snapshot_tos(url: str) -> None:
    try:
        resp = requests.get(url, timeout=15)
        fname = catalog_dir / f"tos_{safe_name(urlparse(url).netloc)}.html"
        tmp_path = fname.with_suffix(".tmp")
        tmp_path.write_text(resp.text, encoding="utf-8")
        tmp_path.replace(fname)
    except Exception as e:
        logger.debug("Failed to snapshot ToS from %s: %s", url, e)
```

---

### BUG-018: Non-Atomic Web Crawl Content Write ✅ FIXED
**Location**: `3d_modeling_pipeline_v2/acquire_plugin.py:379-381`
**Severity**: MEDIUM
**Type**: Data Corruption Risk
**Status**: ✅ **FIXED** - Atomic writes implemented

#### Description
Web crawl downloaded content written directly without atomic pattern.

#### Current Problematic Code
```python
dest = out_dir / safe_name(path_part)
ensure_dir(dest.parent)
dest.write_bytes(resp.content)  # NON-ATOMIC!
```

---

### BUG-019: Silent Exception in robots_allows() ✅ FIXED
**Location**: `3d_modeling_pipeline_v2/acquire_plugin.py:328-329`
**Severity**: LOW
**Type**: Silent Failure
**Status**: ✅ **FIXED** - Debug logging added

#### Description
Exception in robots.txt parsing silently returns True (allow), potentially masking issues.

#### Current Code
```python
except Exception:
    return True  # Silently assumes allowed
```

#### Recommendation
Add logging for debugging purposes.

---

## VERIFIED FIXES FROM PREVIOUS BUG REPORT

All 7 bugs from the original BUG_REPORT.md have been properly fixed and verified:

| Bug ID | Description | Location | Status |
|--------|-------------|----------|--------|
| BUG-001 | Infinite Loop with Zero Refill Rate | `rate_limit.py` | FIXED |
| BUG-002 | Non-Atomic Write in Decision Bundle | `decision_bundle.py:228` | FIXED |
| BUG-003 | Non-Atomic Write in Dry Run Report | `pipeline_driver_base.py:632` | FIXED |
| BUG-004 | TOCTOU Race in Evidence Rotation | `evidence/fetching.py:577-584` | FIXED |
| BUG-005 | Missing Rate Limiter Validation | `rate_limit.py:67-108` | FIXED |
| BUG-006 | Silent Exceptions in PMC Worker | `acquire_plugin.py:186-187` | FIXED |
| BUG-007 | Non-Atomic Writes in Secondary Files | Multiple files | FIXED |

---

## COMPREHENSIVE BUG SUMMARY

### By Severity
| Severity | Count | Status |
|----------|-------|--------|
| **Critical** | 2 | ✅ BUG-010, BUG-011 **FIXED** |
| **High** | 3 | ✅ BUG-008, BUG-009, BUG-012 **FIXED** |
| **Medium** | 6 | ✅ BUG-013 through BUG-018 **FIXED** |
| **Low** | 1 | ✅ BUG-019 **FIXED** |
| **TOTAL** | **12** | ✅ **ALL BUGS FIXED** |

### By Category
| Category | Count | Bug IDs |
|----------|-------|---------|
| Non-Atomic Writes (Core) | 2 | BUG-010, BUG-011 |
| Non-Atomic Writes (Other) | 7 | BUG-008, BUG-009, BUG-013-018 |
| Unsafe List Access | 1 | BUG-012 |
| Silent Exceptions | 2 | BUG-017, BUG-019 |

---

## ACTION ITEMS & EXECUTION PLAN

### Priority 1: Critical (Immediate - Today/Tomorrow)

#### Task 1.1: Fix BUG-010 and BUG-011 (Core Utilities)
**Files**: `src/collector_core/utils/io.py`
**Impact**: Fixes affect entire pipeline

**Steps**:
1. Update `write_jsonl()` to use atomic write pattern
2. Update `write_jsonl_gz()` to use atomic write pattern
3. Run full test suite to verify no regressions
4. Commit: "Fix BUG-010, BUG-011: Atomic writes in core JSONL utilities"

---

### Priority 2: High (This Week)

#### Task 2.1: Fix BUG-008 (CSV Export)
**File**: `src/collector_core/review_queue.py`

#### Task 2.2: Fix BUG-009 (PMC Allowlist)
**File**: `src/collector_core/yellow_scrubber_base.py`

#### Task 2.3: Fix BUG-012 (Unsafe List Access)
**File**: `src/collector_core/acquire/worker.py`

---

### Priority 3: Medium (This Sprint)

#### Task 3.1: Fix BUG-013 (mesh_worker.py)
**File**: `3d_modeling_pipeline_v2/mesh_worker.py`

#### Task 3.2: Fix BUG-014 (PMC Cache)
**File**: `src/collector_core/pmc_worker.py`

#### Task 3.3: Fix BUG-015 through BUG-018 (acquire_plugin.py)
**File**: `3d_modeling_pipeline_v2/acquire_plugin.py`

---

## SUCCESS CRITERIA

### Definition of Done
- [x] All 12 bugs fixed with atomic write patterns or proper guards ✅
- [ ] Unit tests added for each fix
- [ ] No regressions in existing tests
- [ ] Code review approved
- [ ] Merged to main branch

### Testing Checklist
- [x] `write_jsonl()` atomic write verified ✅
- [x] `write_jsonl_gz()` atomic write verified ✅
- [x] CSV export atomic write verified ✅
- [x] PMC allowlist atomic write verified ✅
- [x] Empty results list handled gracefully ✅
- [x] All acquire_plugin.py writes are atomic ✅
- [x] Silent exceptions replaced with logging ✅

---

## APPENDIX: Atomic Write Pattern Reference

**Standard Pattern** (use consistently):
```python
# For text files
tmp_path = target_path.with_suffix(".tmp")
tmp_path.write_text(content, encoding="utf-8")
tmp_path.replace(target_path)

# For binary files
tmp_path = target_path.with_suffix(".tmp")
tmp_path.write_bytes(data)
tmp_path.replace(target_path)

# For streaming writes
tmp_path = target_path.with_suffix(".tmp")
with tmp_path.open("w", encoding="utf-8") as f:
    for line in data:
        f.write(line)
tmp_path.replace(target_path)

# For compressed files
tmp_path = target_path.with_suffix(target_path.suffix + ".tmp")
with gzip.open(tmp_path, "wt", encoding="utf-8") as f:
    for line in data:
        f.write(line)
tmp_path.replace(target_path)
```

---

**Report Generated By**: Claude Code Repository Scanner
**Date**: 2026-01-15
**Revision**: 2.0 (comprehensive scan with verification)

---

**End of Bug Scan Report**
