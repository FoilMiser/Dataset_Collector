#!/usr/bin/env python3
from __future__ import annotations

from collector_core.__version__ import __version__ as VERSION
from collector_core.pipeline_driver_base import (
    BasePipelineDriver,
    RoutingBlockSpec,
)


class ChemPipelineDriver(BasePipelineDriver):
    DOMAIN = 'chem'
    PIPELINE_VERSION = VERSION
    TARGETS_LABEL = 'targets_chem.yaml'
    USER_AGENT = 'chem-corpus-pipeline'
    ROUTING_KEYS = ['chem_routing', 'math_routing']
    ROUTING_CONFIDENCE_KEYS = ['chem_routing']
    DEFAULT_ROUTING = {'subject': 'chem', 'domain': 'misc', 'category': 'misc', 'level': 5, 'granularity': 'target'}
    ROUTING_BLOCKS = [
        RoutingBlockSpec(name='math_routing', sources=['math_routing'], mode='subset'),
        RoutingBlockSpec(name='chem_routing', sources=['chem_routing'], mode='subset'),
    ]

if __name__ == "__main__":
    ChemPipelineDriver.main()
