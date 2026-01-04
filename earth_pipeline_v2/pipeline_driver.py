#!/usr/bin/env python3
from __future__ import annotations

from typing import Any

from collector_core.pipeline_driver_base import (
    BasePipelineDriver,
    RoutingBlockSpec,
)


class EarthPipelineDriver(BasePipelineDriver):
    DOMAIN = 'earth'
    TARGETS_LABEL = 'targets_earth.yaml'
    USER_AGENT = 'earth-corpus-pipeline'
    ROUTING_KEYS = ['earth_routing', 'math_routing']
    DEFAULT_ROUTING = {'subject': 'earth', 'granularity': 'target'}

if __name__ == "__main__":
    EarthPipelineDriver.main()
