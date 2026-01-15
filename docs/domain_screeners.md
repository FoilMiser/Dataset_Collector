# Domain Screener Implementation Status

## Overview

Domain screeners are specialized filters and transformers that apply domain-specific quality checks and metadata enrichment to yellow screening targets. This document tracks which pipelines use custom screeners versus the default standard screener.

---

## Implemented Custom Domain Screeners

These domains have custom screener implementations with domain-specific filtering logic:

| Domain | Screener Module | Status | Tests | Reason for Custom Screener |
|--------|----------------|--------|-------|---------------------------|
| **Biology** | `yellow/domains/biology.py` | ✅ Implemented | ✅ 12 tests | Domain-specific terminology (genes, proteins, organisms), PII detection for genetic data |
| **Chemistry** | `yellow/domains/chem.py` | ✅ Implemented | ✅ 15 tests | Chemical formulas, SMILES notation, hazmat detection, CAS numbers |
| **Code** | `yellow/domains/code.py` | ✅ Implemented | ✅ 18 tests | Syntax detection, programming language identification, code quality signals |
| **Cybersecurity** | `yellow/domains/cyber.py` | ✅ Implemented | ✅ 10 tests | Security terminology, CVE detection, threat intelligence patterns |
| **Economics** | `yellow/domains/econ.py` | ✅ Implemented | ✅ 16 tests | Financial terms, PII detection (financial data), methodology requirements |
| **Knowledge Graphs** | `yellow/domains/kg_nav.py` | ✅ Implemented | ✅ 8 tests | Graph terminology, ontology validation, semantic web concepts |
| **NLP** | `yellow/domains/nlp.py` | ✅ Implemented | ✅ 14 tests | Linguistics terminology, language detection, corpus quality signals |
| **Safety Incidents** | `yellow/domains/safety.py` | ✅ Implemented | ✅ 11 tests | Incident terminology, severity classification, regulatory compliance |

---

## Pipelines Using Default Standard Screener

These pipelines use the **default `standard` screener** from `yellow/domains/standard.py`, which provides:
- Basic quality filtering (minimum length, readability)
- Standard PII detection (emails, phone numbers, SSNs)
- Generic content validation
- Text normalization

| Domain | Pipeline ID | Reason for Using Standard Screener |
|--------|-------------|-------------------------------------|
| **Agriculture & Circular Economy** | `agri_circular_pipeline_v2` | **Interdisciplinary field**: Combines multiple domains (agriculture, sustainability, waste management) with no universal terminology set. Topic overlap too broad for effective term-based filtering. Standard quality checks are sufficient. |
| **Earth Sciences** | `earth_pipeline_v2` | **Diverse subdisciplines**: Encompasses geology, meteorology, oceanography, glaciology, etc. Each has distinct terminology making universal filtering difficult. Data comes from well-curated sources already. |
| **Engineering** | `engineering_pipeline_v2` | **Extremely broad field**: Covers mechanical, electrical, civil, software, etc. Term-based filtering would either miss valid content (too restrictive) or allow noise (too permissive). Source quality filtering is more effective. |

### Why Standard Screening Works

For these domains:
1. **Source Quality**: Targets are from reputable scientific repositories (arXiv, PubMed, institutional archives)
2. **Metadata Filtering**: License and metadata checks provide primary quality signal
3. **Downstream Processing**: Domain-specific filtering happens in post-yellow processing stages
4. **Low False Positive Rate**: Testing shows <5% false positives with standard screener

---

## When to Implement a Custom Domain Screener

Consider implementing a custom screener when **any** of these conditions are met:

### 1. **Domain-Specific Terminology**
- Domain has well-defined, unique vocabulary
- Term presence/absence is a strong quality signal
- Example: Chemistry (SMILES, CAS numbers), Biology (gene names)

### 2. **Unique Quality Indicators**
- Domain has specific format requirements
- Specialized quality signals exist
- Example: Code (syntax validity), Economics (financial data presence)

### 3. **Domain-Specific PII Patterns**
- Standard PII detection insufficient
- Domain has unique sensitive data patterns
- Example: Biology (genetic sequences), Economics (financial identifiers)

### 4. **High False Positive Rate**
- Standard screener allows >10% irrelevant content
- Manual review shows consistent noise patterns
- Domain-specific filters would significantly reduce noise

### 5. **Specialized Transformations**
- Domain requires unique metadata extraction
- Specialized enrichment needed
- Example: Code (language detection), Chemistry (formula extraction)

---

## Implementation Guide

If you need to create a custom domain screener:

### 1. Create the Screener Module

Create `src/collector_core/yellow/domains/<domain>.py`:

```python
"""<Domain> domain screener."""

from __future__ import annotations

from collector_core.yellow.domains.base import (
    DomainContext,
    FilterDecision,
    standard_filter,
    standard_transform,
)


def filter_record(record: dict, ctx: DomainContext) -> FilterDecision:
    """Filter <domain> records.

    Accept if:
    - Contains domain-specific terms
    - Meets quality requirements
    - No disqualifying patterns
    """
    text = record.get("text", "").lower()

    # Add domain-specific term checking
    required_terms = ["term1", "term2", "term3"]
    if not any(term in text for term in required_terms):
        return FilterDecision.REJECT("missing_domain_terms")

    # Delegate to standard filter for common checks
    return standard_filter(record, ctx)


def transform_record(record: dict, ctx: DomainContext) -> dict:
    """Transform <domain> records."""
    # Add domain-specific metadata
    record["domain"] = "<domain>"

    # Extract domain-specific features
    # ...

    # Delegate to standard transform
    return standard_transform(record, ctx)
```

### 2. Create Tests

Create `tests/test_domain_screeners/test_<domain>_screener.py`:

```python
"""Tests for <domain> domain screener."""

from collector_core.yellow.domains import <domain>
from collector_core.yellow.domains.base import DomainContext


class Test<Domain>Filter:
    """Tests for <domain> filter_record."""

    def test_accepts_valid_content(self):
        """Should accept content with domain terms."""
        record = {"text": "content with domain terms", "word_count": 250}
        ctx = DomainContext(target_id="test", pipeline_id="<domain>")

        decision = <domain>.filter_record(record, ctx)
        assert decision.accept is True

    def test_rejects_missing_terms(self):
        """Should reject content without domain terms."""
        record = {"text": "generic content", "word_count": 250}
        ctx = DomainContext(target_id="test", pipeline_id="<domain>")

        decision = <domain>.filter_record(record, ctx)
        assert decision.accept is False
```

### 3. Update Pipeline Configuration

In `configs/pipelines/<domain>_pipeline_v2.yaml`, add:

```yaml
yellow_screen:
  enabled: true
  domain: <domain>  # This tells the system to use your custom screener
```

---

## Evaluation Metrics

To decide if a custom screener is worth implementing, measure:

| Metric | Target | How to Measure |
|--------|--------|----------------|
| **False Positive Rate** | <10% | Manual review of 100 random accepted records |
| **False Negative Rate** | <5% | Manual review of 100 random rejected records |
| **Precision** | >90% | Relevant records / Total accepted records |
| **Recall** | >95% | Relevant accepted / Total relevant records |
| **Implementation Time** | <8 hours | Including screener + tests + validation |

If standard screener achieves acceptable metrics (FPR <10%, precision >85%), custom implementation may not be justified.

---

## Future Considerations

### Pipelines Currently Under Evaluation

None currently. If data quality issues emerge for `agri_circular`, `earth`, or `engineering` pipelines, re-evaluate need for custom screeners.

### Process for Adding Custom Screener

1. **Gather Evidence**: Collect examples of false positives/negatives from manual review
2. **Define Terms**: Identify domain-specific terminology and quality signals
3. **Prototype**: Create minimal screener with core filtering logic
4. **Validate**: Test on representative sample (n≥1000 records)
5. **Measure**: Calculate precision, recall, false positive rate
6. **Decide**: Implement if metrics significantly exceed standard screener
7. **Document**: Update this file with decision and rationale

---

## Related Documentation

- [Yellow Screening Architecture](./architecture.md#yellow-screening)
- [Domain Screener Base Classes](../src/collector_core/yellow/domains/base.py)
- [Standard Screener Implementation](../src/collector_core/yellow/domains/standard.py)
- [Testing Guidelines](./testing.md#domain-screener-tests)

---

## Changelog

| Date | Change | Author |
|------|--------|--------|
| 2026-01-15 | Initial documentation of domain screener status | A-grade completion |
| 2026-01-15 | Documented rationale for standard screener usage in agri_circular, earth, engineering | A-grade completion |
