#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from collector_core.__version__ import __version__ as VERSION
from collector_core.pipeline_driver_base import (
    BasePipelineDriver,
    RoutingBlockSpec,
)


class CodePipelineDriver(BasePipelineDriver):
    DOMAIN = 'code'
    PIPELINE_VERSION = VERSION
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
