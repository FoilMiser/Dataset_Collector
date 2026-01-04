#!/usr/bin/env python3
from __future__ import annotations

from typing import Any

from collector_core.pipeline_driver_base import (
    BasePipelineDriver,
    RoutingBlockSpec,
    coerce_int,
)


class KgNavPipelineDriver(BasePipelineDriver):
    DOMAIN = 'kg_nav'
    TARGETS_LABEL = 'targets_kg_nav.yaml'
    USER_AGENT = 'kg-nav-pipeline'
    ROUTING_KEYS = ['kg_routing', 'math_routing']
    ROUTING_CONFIDENCE_KEYS = ['kg_routing']
    DEFAULT_ROUTING = {'subject': 'kg_nav', 'granularity': 'target'}

if __name__ == "__main__":
    KgNavPipelineDriver.main()
