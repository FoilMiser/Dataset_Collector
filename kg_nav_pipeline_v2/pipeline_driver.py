#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from collector_core.__version__ import __version__ as VERSION
from collector_core.pipeline_driver_base import (
    BasePipelineDriver,
)


class KgNavPipelineDriver(BasePipelineDriver):
    DOMAIN = 'kg_nav'
    PIPELINE_VERSION = VERSION
    TARGETS_LABEL = 'targets_kg_nav.yaml'
    USER_AGENT = 'kg-nav-pipeline'
    ROUTING_KEYS = ['kg_routing', 'math_routing']
    ROUTING_CONFIDENCE_KEYS = ['kg_routing']
    DEFAULT_ROUTING = {'subject': 'kg_nav', 'granularity': 'target'}

if __name__ == "__main__":
    KgNavPipelineDriver.main()
