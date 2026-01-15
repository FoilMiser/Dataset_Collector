# Bug Report - Dataset Collector Repository
**Generated**: 2026-01-15
**Analysis Type**: Comprehensive Bug Screening
**Scope**: Logic, Concurrency, Resource Management, Edge Cases, Data Integrity

---

## 🔴 CRITICAL BUGS

### BUG-001: Infinite Loop with Zero Refill Rate
**Location**: `src/collector_core/rate_limit.py:191`
**Severity**: CRITICAL
**Type**: Logic Bug / Infinite Loop

**Description**:
If `refill_rate` is set to 0, the `acquire()` method will enter an infinite loop. Line 191 checks `if self.refill_rate > 0` and defaults to waiting 1 second if it's 0, but the `_refill()` method (line 159) calculates `added = elapsed * self.refill_rate`, which will always be 0 when refill_rate is 0. This means tokens will never refill, and the while loop will run forever.

**Code**:
```python
# Line 191
wait_time = needed / self.refill_rate if self.refill_rate > 0 else 1.0
```

**Impact**:
- Deadlock/hang if configuration mistakenly sets refill_rate to 0
- No timeout or escape mechanism
- Process becomes unresponsive

**Recommendation**:
Add validation in `RateLimiterConfig.from_dict()` and `RateLimiter.__post_init__()` to reject refill_rate <= 0:
```python
if refill_rate <= 0:
    raise ValueError(f"refill_rate must be positive, got {refill_rate}")
```

---

### BUG-002: Non-Atomic Write Causing Potential Data Corruption
**Location**: `src/collector_core/decision_bundle.py:228`
**Severity**: CRITICAL
**Type**: Data Corruption Risk

**Description**:
The `save_decision_bundle()` function writes directly to the target file without using a temporary file + atomic rename pattern. If the process is interrupted (crash, kill signal, power loss) during the write, the file will be partially written and corrupted.

**Code**:
```python
# Line 228
output_path.write_text(json.dumps(bundle.to_dict(), indent=2))
```

**Impact**:
- Corrupted decision bundle files cannot be loaded
- Loss of audit trail for routing decisions
- Breaks pipeline resumability
- Violates Issue 4.2 requirement for complete audit trails

**Recommendation**:
Use atomic write pattern (already exists in the codebase):
```python
tmp_path = output_path.with_suffix(".tmp")
tmp_path.write_text(json.dumps(bundle.to_dict(), indent=2))
tmp_path.replace(output_path)
```

---

### BUG-003: Non-Atomic Write in Dry Run Report
**Location**: `src/collector_core/pipeline_driver_base.py:632`
**Severity**: HIGH
**Type**: Data Corruption Risk

**Description**:
The dry run report write is not atomic and could be corrupted if interrupted.

**Code**:
```python
# Line 632
report_path.write_text(report, encoding="utf-8")
```

**Impact**:
- Corrupted dry run reports
- Less severe than decision bundles (can be re-generated)
- Still violates best practices

**Recommendation**:
Use atomic write pattern as in BUG-002.

---

### BUG-004: TOCTOU Race Condition in Evidence File Rotation
**Location**: `src/collector_core/evidence/fetching.py:577-584`
**Severity**: HIGH
**Type**: Race Condition / Data Loss Risk

**Description**:
Lines 577-582 check if `prev_path` exists in a loop to find a non-existing path, then rename the existing evidence file to `prev_path`. However, between the `exists()` check and the `rename()` call, another process could create a file at `prev_path`, causing the rename to fail. The OSError is caught but silently ignored with `pass`, and execution continues, potentially overwriting the original evidence file without preserving the old version.

**Code**:
```python
# Lines 577-584
while prev_path.exists():
    prev_path = manifest_dir / f"{prev_prefix}_{counter}{prev_ext}"
    counter += 1
# P1.2G: Handle OSError on rename
try:
    existing_path.rename(prev_path)
except OSError:
    pass  # Ignore rename failures, proceed with new file
```

**Impact**:
- Loss of previous evidence file
- Breaks evidence change detection (Issue 4.3)
- Corrupted evidence history
- Race condition in multi-process scenarios

**Recommendation**:
1. Use file locking or atomic operations
2. Log the error instead of silent `pass`
3. Consider failing the operation if rename fails to preserve evidence integrity
4. Use a more robust naming scheme (timestamp + random suffix)

---

## 🟠 HIGH PRIORITY BUGS

### BUG-005: Missing Validation for Rate Limiter Configuration
**Location**: `src/collector_core/rate_limit.py:67-108`
**Severity**: HIGH
**Type**: Configuration Validation Bug

**Description**:
The `RateLimiterConfig.from_dict()` method does not validate that `capacity` and `refill_rate` are positive numbers. Negative or zero values would cause unexpected behavior:
- `capacity <= 0`: Tokens can never be acquired
- `refill_rate <= 0`: Infinite loop (see BUG-001)
- `initial_tokens < 0`: Negative token bucket

Additionally, line 94 has a bug: `if d.get("initial_tokens")` treats `initial_tokens=0` as falsy, converting it to None instead of 0.

**Code**:
```python
# Lines 80-94 (no validation)
capacity = float(d.get("capacity", 60.0))
refill_rate = float(d.get("refill_rate", 1.0))
initial_tokens = float(d["initial_tokens"]) if d.get("initial_tokens") else None  # BUG: 0 becomes None
```

**Impact**:
- Invalid configurations cause runtime failures
- Difficult to debug (errors occur far from configuration)
- `initial_tokens=0` incorrectly becomes None

**Recommendation**:
Add validation:
```python
if capacity <= 0:
    raise ValueError(f"capacity must be positive, got {capacity}")
if refill_rate <= 0:
    raise ValueError(f"refill_rate must be positive, got {refill_rate}")

# Fix initial_tokens handling
initial_tokens = float(d["initial_tokens"]) if "initial_tokens" in d else None
if initial_tokens is not None and initial_tokens < 0:
    raise ValueError(f"initial_tokens must be non-negative, got {initial_tokens}")
```

---

## 🟡 MEDIUM PRIORITY BUGS

### BUG-006: Unreachable Code in PMC Worker
**Location**: `3d_modeling_pipeline_v2/acquire_plugin.py:186-187`
**Severity**: MEDIUM
**Type**: Logic Bug (Unreachable Code)

**Description**:
There's a bare `except Exception: pass` that catches and silently ignores all exceptions when parsing JSON responses. This makes debugging API issues difficult.

**Code**:
```python
# Lines 186-187
except Exception:
    pass
```

**Impact**:
- Silent failures in API response parsing
- Difficult to debug API changes
- Could mask serious errors

**Recommendation**:
Add logging:
```python
except Exception as e:
    logger.debug("Failed to parse API response as JSON: %s", e)
```

---

## 📊 BUG SUMMARY

| Priority | Count | Categories |
|----------|-------|-----------|
| CRITICAL | 4 | Infinite Loop, Data Corruption (2x), Race Condition |
| HIGH | 1 | Configuration Validation |
| MEDIUM | 1 | Silent Failures |
| **TOTAL** | **6** | |

---

## 🎯 RECOMMENDED FIX PRIORITY

### Immediate (This Week)
1. **BUG-001**: Add refill_rate validation (prevents infinite loops)
2. **BUG-002**: Fix decision_bundle.py atomic write (prevents data corruption)
3. **BUG-005**: Add rate limiter config validation (prevents runtime errors)

### Short Term (This Sprint)
4. **BUG-004**: Fix evidence rotation race condition (prevents data loss)
5. **BUG-003**: Fix pipeline_driver_base.py atomic write (best practice)

### Medium Term (Next Month)
6. **BUG-006**: Add logging to silent exception handlers (improves debugging)

---

## 🔍 BUGS NOT FOUND (✅ Code is Safe)

The following potential bug categories were screened and **no issues found**:

✅ **Off-by-one errors**: All array/list accesses are properly guarded
✅ **Null pointer dereferences**: Proper None checks throughout
✅ **Resource leaks**: All file handles use context managers
✅ **Lock leaks**: Proper lock cleanup in finally blocks
✅ **Division by zero**: Proper checks before division operations
✅ **Empty list access**: All list[0] accesses are guarded with `if list:` checks
✅ **Missing imports**: All imports are present and correct

---

## 📝 TESTING RECOMMENDATIONS

To prevent these bugs from recurring:

1. **Add unit tests for edge cases**:
   - Rate limiter with refill_rate=0
   - Rate limiter with capacity=0
   - Rate limiter with initial_tokens=0 vs None
   - Concurrent evidence file writes

2. **Add integration tests**:
   - Process interruption during file writes
   - Concurrent pipeline execution

3. **Add property-based tests** (hypothesis):
   - Rate limiter configurations (all valid positive values should work)
   - File write atomicity under failures

4. **Add static analysis**:
   - Detect non-atomic writes (could be a custom linter rule)
   - Detect missing validation in config loading

---

## 🔒 SECURITY IMPACT

None of these bugs directly introduce security vulnerabilities (no injection, authentication bypass, etc.), but:

- **BUG-002** and **BUG-004** could be exploited in a DOS attack by triggering crashes during writes
- **BUG-001** is a DOS vector if attacker can control configuration

All bugs are **availability** and **integrity** issues, not confidentiality issues.

---

## 📚 REFERENCES

- Atomic file writes: https://docs.python.org/3/library/pathlib.html#pathlib.Path.replace
- TOCTOU: https://en.wikipedia.org/wiki/Time-of-check_to_time-of-use
- Token bucket algorithm: https://en.wikipedia.org/wiki/Token_bucket
- Python file locking: https://docs.python.org/3/library/fcntl.html

---

**End of Bug Report**
