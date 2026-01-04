#!/usr/bin/env python3
from __future__ import annotations

from typing import Any

from collector_core.pipeline_driver_base import (
    BasePipelineDriver,
    RoutingBlockSpec,
)


class EngineeringPipelineDriver(BasePipelineDriver):
    DOMAIN = 'engineering'
    TARGETS_LABEL = 'targets_engineering.yaml'
    USER_AGENT = 'engineering-corpus-pipeline'
    ROUTING_KEYS = ['engineering_routing']
    DEFAULT_ROUTING = {'subject': 'engineering', 'granularity': 'target'}
    ROUTING_BLOCKS = [
        RoutingBlockSpec(name='engineering_routing', sources=['engineering_routing'], mode='subset'),
    ]

if __name__ == "__main__":
    EngineeringPipelineDriver.main()
