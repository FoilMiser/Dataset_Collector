#!/usr/bin/env python3
from __future__ import annotations

from collector_core.__version__ import __version__ as VERSION
from collector_core.pipeline_driver_base import (
    BasePipelineDriver,
    RoutingBlockSpec,
)


class MaterialsSciencePipelineDriver(BasePipelineDriver):
    DOMAIN = 'materials_science'
    PIPELINE_VERSION = VERSION
    TARGETS_LABEL = 'targets_materials.yaml'
    USER_AGENT = 'materials-corpus-pipeline'
    ROUTING_KEYS = ['materials_routing', 'math_routing']
    ROUTING_CONFIDENCE_KEYS = ['materials_routing']
    DEFAULT_ROUTING = {'subject': 'materials_science', 'granularity': 'target'}
    ROUTING_BLOCKS = [
        RoutingBlockSpec(name='materials_routing', sources=['materials_routing', 'math_routing'], mode='subset'),
    ]

if __name__ == "__main__":
    MaterialsSciencePipelineDriver.main()
