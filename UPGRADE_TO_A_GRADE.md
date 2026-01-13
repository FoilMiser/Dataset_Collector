# Dataset Collector: A-Grade Upgrade Plan

## Status Overview

**Progress: ~85% Complete**

Most A-grade requirements have been implemented. This document tracks remaining work.

---

## ✅ Completed Phases (Summary)

### Phase 1: Technical Debt Elimination ✅
- **1.1** Deprecated pipeline wrappers removed (~2,500 lines deleted)
- **1.2** Broken `src/schemas` symlink fixed
- **1.3** HTTP strategies consolidated into `http_base.py` (~800 lines reduced)
- Migration helper at `src/collector_core/migration.py`

### Phase 2: Feature Implementation ✅
- **2.1** Domain-specific yellow screeners implemented:
  - `chem.py` - CAS number validation, controlled substance detection, quality scoring
  - `biology.py` - Biosecurity screening, gene/protein ID extraction, sequence detection
  - `code.py` - License detection (SPDX), secret scanning, malware patterns, quality metrics
  - `cyber.py`, `econ.py`, `kg_nav.py`, `nlp.py`, `safety.py` - All with domain-specific logic
- **2.2** Near-duplicate detection: `src/collector_core/checks/near_duplicate.py`
  - MinHash LSH with datasketch backend + pure Python fallback
- **2.3** Content checks: `src/collector_core/checks/implementations/`
  - `distribution_statement.py`, `language_detect.py`, `license_validate.py`
  - `schema_validate.py`, `toxicity_scan.py`

### Phase 4: Production Hardening ✅
- **4.1** Metrics dashboard: `src/collector_core/metrics/dashboard.py`
  - Prometheus export, HTML reports, JSON output
- **4.2** Checkpoint/resume: `src/collector_core/checkpoint.py`
- **4.3** Schema version enforcement: `src/collector_core/schema_version.py`

### Phase 5: Documentation ✅
- **5.1** Sphinx API docs: `docs/api/`, `docs/conf.py`
- **5.2** Guides: `docs/guide/custom_screeners.rst`, `docs/guide/content_checks.rst`
- **5.3** Notebooks: `notebooks/` (5 Jupyter notebooks)

---

## ⬜ Pending Tasks

### Phase 3: Type Safety & Testing

#### 3.1 Verify Type Coverage
Run mypy strict mode and fix any errors:

```bash
mypy src/collector_core --strict
```

**Priority files if errors occur:**
1. `src/collector_core/utils/io.py`
2. `src/collector_core/utils/paths.py`
3. `src/collector_core/pipeline_driver_base.py`
4. `src/collector_core/acquire/strategies/http.py`
5. `src/collector_core/merge/__init__.py`
6. `src/collector_core/yellow/base.py`

#### 3.2 Achieve 90%+ Test Coverage

```bash
pytest --cov=collector_core --cov-report=html --cov-fail-under=90
```

**Tests needed for new modules:**
- `tests/test_domain_screeners/test_chem_screener.py`
- `tests/test_domain_screeners/test_bio_screener.py`
- `tests/test_domain_screeners/test_code_screener.py`
- `tests/test_near_duplicate.py` (expand coverage)
- `tests/test_content_checks/` (for each check)

#### 3.3 Add Property-Based Tests

Add hypothesis-based tests for core algorithms:

```python
"""Example: tests/test_near_duplicate_properties.py"""
from hypothesis import given, strategies as st
from collector_core.checks.near_duplicate import create_detector

class TestNearDuplicateProperties:
    @given(st.text(min_size=50, max_size=1000))
    def test_self_similarity(self, text: str) -> None:
        """Any text should be highly similar to itself."""
        if len(text.split()) < 5:
            return
        detector = create_detector(threshold=0.5)
        detector.add("original", text)
        result = detector.query(text)
        assert result.is_duplicate
        assert result.similarity > 0.95

    @given(st.text(min_size=50), st.text(min_size=50))
    def test_symmetry(self, text1: str, text2: str) -> None:
        """Similarity should be approximately symmetric."""
        if len(text1.split()) < 5 or len(text2.split()) < 5:
            return
        d1 = create_detector()
        d1.add("doc1", text1)
        r1 = d1.query(text2)

        d2 = create_detector()
        d2.add("doc2", text2)
        r2 = d2.query(text1)

        assert abs(r1.similarity - r2.similarity) < 0.15
```

### CI Validation Scripts

Ensure CI runs all validation:

```bash
# Add to CI pipeline
mypy src/collector_core --strict
ruff check .
ruff format --check .
pytest --cov=collector_core --cov-report=html --cov-fail-under=90
pytest -m integration
python -m tools.validate_yaml_schemas --root .
cd docs && make html
```

---

## Acceptance Criteria Checklist

### Phase 1: Technical Debt
- [x] Deprecated wrappers removed
- [x] Broken symlink fixed
- [x] HTTP strategies consolidated
- [ ] CI passes with no deprecated imports

### Phase 2: Features
- [x] All domain screeners implemented with real logic
- [x] Near-duplicate detection working
- [x] At least 5 content checks implemented
- [x] Integration with merge stage complete

### Phase 3: Quality
- [ ] `mypy --strict` passes
- [ ] Test coverage ≥ 90%
- [ ] Property-based tests for core algorithms
- [x] Integration test suite complete

### Phase 4: Production
- [x] Metrics collection and export
- [x] Checkpoint/resume support
- [x] Schema version enforcement
- [ ] CI validation scripts

### Phase 5: Documentation
- [x] API reference generated
- [x] All guides complete
- [x] Example notebooks working
- [x] CLI reference complete

---

## Final Validation

Run the complete validation suite:

```bash
# Type checking
mypy src/collector_core --strict

# Linting
ruff check .
ruff format --check .

# Tests with coverage
pytest --cov=collector_core --cov-report=html --cov-fail-under=90

# Schema validation
python -m tools.validate_yaml_schemas --root .

# Preflight check
python -m tools.preflight --repo-root .

# Integration tests
pytest -m integration

# Documentation build
cd docs && make html
```

**All commands must pass for A-grade status.**
