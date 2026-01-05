#!/usr/bin/env python3
from __future__ import annotations

from collector_core.pipeline_driver_base import (
    BasePipelineDriver,
    RoutingBlockSpec,
)


class LogicPipelineDriver(BasePipelineDriver):
    DOMAIN = 'logic'
    TARGETS_LABEL = 'targets_logic.yaml'
    USER_AGENT = 'logic-corpus-pipeline'
    ROUTING_KEYS = ['logic_routing']
    DEFAULT_ROUTING = {'subject': 'logic', 'granularity': 'target'}
    ROUTING_BLOCKS = [
        RoutingBlockSpec(name='logic_routing', sources=['logic_routing'], mode='subset'),
    ]

if __name__ == "__main__":
    LogicPipelineDriver.main()
