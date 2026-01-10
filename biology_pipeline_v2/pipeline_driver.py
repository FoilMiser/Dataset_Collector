#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from collector_core.__version__ import __version__ as VERSION
from collector_core.pipeline_driver_base import (
    BasePipelineDriver,
    RoutingBlockSpec,
)


class BiologyPipelineDriver(BasePipelineDriver):
    DOMAIN = 'biology'
    PIPELINE_VERSION = VERSION
    TARGETS_LABEL = 'targets_biology.yaml'
    USER_AGENT = 'bio-corpus-pipeline'
    ROUTING_KEYS = ['bio_routing', 'math_routing']
    ROUTING_CONFIDENCE_KEYS = ['bio_routing']
    DEFAULT_ROUTING = {'subject': 'biology', 'granularity': 'target'}
    ROUTING_BLOCKS = [
        RoutingBlockSpec(name='bio_routing', sources=['bio_routing'], mode='raw'),
    ]

if __name__ == "__main__":
    BiologyPipelineDriver.main()
