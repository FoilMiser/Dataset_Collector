#!/usr/bin/env python3
from __future__ import annotations

from typing import Any

from collector_core.pipeline_driver_base import (
    BasePipelineDriver,
    RoutingBlockSpec,
)


class AgriCircularPipelineDriver(BasePipelineDriver):
    DOMAIN = 'agri_circular'
    TARGETS_LABEL = 'targets_agri_circular.yaml'
    USER_AGENT = 'agri-circular-pipeline'
    ROUTING_KEYS = ['agri_routing', 'math_routing']
    DEFAULT_ROUTING = {'subject': 'agri_circular', 'granularity': 'target'}
    ROUTING_BLOCKS = [
        RoutingBlockSpec(name='agri_routing', sources=['agri_routing', 'math_routing'], mode='subset'),
    ]

if __name__ == "__main__":
    AgriCircularPipelineDriver.main()
