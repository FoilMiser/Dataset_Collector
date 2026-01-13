"""Content checks package for Dataset Collector.

This package provides content validation and filtering checks that can be
applied during the yellow screening and merge stages.

Available checks:
- near_duplicate: Near-duplicate detection using MinHash LSH
- language_detect: Language detection for text content
- license_validate: License validation and compatibility checking
- schema_validate: Schema validation for structured data
- toxicity_scan: Toxicity and harmful content detection
- distribution_statement: Distribution statement extraction
"""

from collector_core.checks.near_duplicate import (
    NearDuplicateDetector,
    DuplicateResult,
    DetectorStats,
    create_detector,
)

__all__ = [
    "NearDuplicateDetector",
    "DuplicateResult",
    "DetectorStats",
    "create_detector",
]
