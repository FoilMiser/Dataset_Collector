#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from collector_core.__version__ import __version__ as VERSION
from collector_core.pipeline_driver_base import (
    BasePipelineDriver,
    RoutingBlockSpec,
)


class EngineeringPipelineDriver(BasePipelineDriver):
    DOMAIN = 'engineering'
    PIPELINE_VERSION = VERSION
    TARGETS_LABEL = 'targets_engineering.yaml'
    USER_AGENT = 'engineering-corpus-pipeline'
    ROUTING_KEYS = ['engineering_routing']
    DEFAULT_ROUTING = {'subject': 'engineering', 'granularity': 'target'}
    ROUTING_BLOCKS = [
        RoutingBlockSpec(name='engineering_routing', sources=['engineering_routing'], mode='subset'),
    ]

if __name__ == "__main__":
    EngineeringPipelineDriver.main()
