#!/usr/bin/env python3
from __future__ import annotations

from typing import Any

from collector_core.pipeline_driver_base import (
    BasePipelineDriver,
    RoutingBlockSpec,
    coerce_int,
)


class CyberPipelineDriver(BasePipelineDriver):
    DOMAIN = 'cyber'
    TARGETS_LABEL = 'targets_cyber.yaml'
    USER_AGENT = 'cyber-corpus-pipeline'
    ROUTING_KEYS = ['cyber_routing', 'math_routing']
    DEFAULT_ROUTING = {'subject': 'cyber', 'granularity': 'target'}

if __name__ == "__main__":
    CyberPipelineDriver.main()
