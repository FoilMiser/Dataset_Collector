#!/usr/bin/env python3
from __future__ import annotations

from collector_core.pipeline_driver_base import (
    BasePipelineDriver,
    RoutingBlockSpec,
)


class BiologyPipelineDriver(BasePipelineDriver):
    DOMAIN = 'biology'
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
