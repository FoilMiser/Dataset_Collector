#!/usr/bin/env python3
from __future__ import annotations

from typing import Any

from collector_core.pipeline_driver_base import (
    BasePipelineDriver,
    RoutingBlockSpec,
)


class CodePipelineDriver(BasePipelineDriver):
    DOMAIN = 'code'
    TARGETS_LABEL = 'targets_code.yaml'
    USER_AGENT = 'code-corpus-pipeline'
    ROUTING_KEYS = ['code_routing', 'math_routing']
    ROUTING_CONFIDENCE_KEYS = ['code_routing', 'math_routing']
    DEFAULT_ROUTING = {'subject': 'code', 'domain': 'multi', 'category': 'misc', 'level': 5, 'granularity': 'target'}
    ROUTING_BLOCKS = [
        RoutingBlockSpec(name='math_routing', sources=['math_routing'], mode='subset'),
        RoutingBlockSpec(name='code_routing', sources=['code_routing'], mode='subset'),
    ]

if __name__ == "__main__":
    CodePipelineDriver.main()
