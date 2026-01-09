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


class ThreeDModelingPipelineDriver(BasePipelineDriver):
    DOMAIN = '3d_modeling'
    PIPELINE_VERSION = VERSION
    TARGETS_LABEL = 'targets_3d.yaml'
    USER_AGENT = '3d-modeling-pipeline'
    ROUTING_KEYS = ['three_d_routing']
    DEFAULT_ROUTING = {'subject': '3d', 'granularity': 'target'}
    ROUTING_BLOCKS = [
        RoutingBlockSpec(name='three_d_routing', sources=['three_d_routing'], mode='subset'),
    ]

if __name__ == "__main__":
    ThreeDModelingPipelineDriver.main()
