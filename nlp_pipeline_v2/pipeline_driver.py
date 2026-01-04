#!/usr/bin/env python3
from __future__ import annotations

from typing import Any

from collector_core.pipeline_driver_base import (
    BasePipelineDriver,
    RoutingBlockSpec,
    coerce_int,
)


class NlpPipelineDriver(BasePipelineDriver):
    DOMAIN = 'nlp'
    TARGETS_LABEL = 'targets_nlp.yaml'
    USER_AGENT = 'nlp-corpus-pipeline'
    ROUTING_KEYS = ['nlp_routing']
    ROUTING_CONFIDENCE_KEYS = ['nlp_routing']
    DEFAULT_ROUTING = {'subject': 'nlp', 'domain': 'misc', 'category': 'misc', 'granularity': 'target'}
    ROUTING_BLOCKS = [
        RoutingBlockSpec(name='nlp_routing', sources=['nlp_routing'], mode='subset'),
    ]

if __name__ == "__main__":
    NlpPipelineDriver.main()
