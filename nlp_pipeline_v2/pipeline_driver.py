#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from collector_core.__version__ import __version__ as VERSION
from collector_core.pipeline_driver_base import (
    BasePipelineDriver,
    RoutingBlockSpec,
)


class NlpPipelineDriver(BasePipelineDriver):
    DOMAIN = 'nlp'
    PIPELINE_VERSION = VERSION
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
