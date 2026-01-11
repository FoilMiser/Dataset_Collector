#!/usr/bin/env python3
"""
yellow_scrubber.py (v2.0)

Thin wrapper that delegates to the spec-driven yellow review helper.
"""
from __future__ import annotations
from collector_core.pipeline_spec import get_pipeline_spec  # noqa: E402
from collector_core.yellow_review_helpers import make_main  # noqa: E402

DOMAIN = "metrology"

if __name__ == "__main__":
    spec = get_pipeline_spec(DOMAIN)
    assert spec is not None, f"Unknown domain: {DOMAIN}"
    make_main(domain_name=spec.name, domain_prefix=spec.prefix, targets_yaml_name=spec.targets_yaml)
