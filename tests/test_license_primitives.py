from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from collector_core.pipeline_driver_base import (  # noqa: E402
    contains_any,
    denylist_hits,
    load_license_map,
    resolve_spdx_with_confidence,
    spdx_bucket,
)

LICENSE_MAP_PATH = REPO_ROOT / "regcomp_pipeline_v2" / "license_map.yaml"


@pytest.fixture(scope="module")
def license_map():
    return load_license_map(LICENSE_MAP_PATH)


def test_resolve_spdx_with_confidence_uses_explicit_hint(license_map):
    spdx, confidence, reason = resolve_spdx_with_confidence(
        license_map,
        evidence_text="",
        spdx_hint="MIT",
    )
    assert spdx == "MIT"
    assert confidence == 0.95
    assert reason == "explicit SPDX hint"


def test_resolve_spdx_with_confidence_matches_rules(license_map):
    spdx, confidence, reason = resolve_spdx_with_confidence(
        license_map,
        evidence_text="Licensed under Creative Commons Attribution 4.0 terms.",
        spdx_hint="UNKNOWN",
    )
    assert spdx == "CC-BY-4.0"
    assert confidence >= 0.6
    assert "normalized via rule match" in reason
    assert "spdx=CC-BY-4.0" in reason
    assert "excerpt=" in reason


def test_resolve_spdx_with_confidence_avoids_embedded_short_tokens(license_map):
    spdx, confidence, reason = resolve_spdx_with_confidence(
        license_map,
        evidence_text="Users may permit access to the dataset.",
        spdx_hint="UNKNOWN",
    )
    assert spdx == "UNKNOWN"
    assert confidence == 0.2
    assert reason == "no confident match"


def test_spdx_bucket_respects_allow_and_denies(license_map):
    assert spdx_bucket(license_map, "CC-BY-4.0") == "GREEN"
    assert spdx_bucket(license_map, "CC-BY-NC-4.0") == "RED"
    assert spdx_bucket(license_map, "UNKNOWN") == "YELLOW"


def test_restriction_phrase_scanning(license_map):
    hits = contains_any("This dataset has no ai training permitted.", license_map.restriction_phrases)
    assert "no ai" in hits


def test_denylist_hits_cover_patterns_domains_publishers():
    denylist = {
        "patterns": [
            {
                "type": "substring",
                "value": "Example",
                "fields": ["name"],
                "severity": "hard_red",
                "reason": "bad actor",
            }
        ],
        "domain_patterns": [
            {
                "domain": "restricted.com",
                "severity": "force_yellow",
            }
        ],
        "publisher_patterns": [
            {
                "publisher": "Blocked Publishing",
                "severity": "hard_red",
            }
        ],
    }
    hay = {
        "name": "Example Source",
        "license_evidence_url": "https://sub.restricted.com/terms",
        "publisher": "Blocked Publishing",
    }
    hits = denylist_hits(denylist, hay)
    hit_types = {hit["type"] for hit in hits}
    assert {"substring", "domain", "publisher"}.issubset(hit_types)
