#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from collector_core.__version__ import __version__ as VERSION
from collector_core.pipeline_driver_base import (
    BasePipelineDriver,
)


class CyberPipelineDriver(BasePipelineDriver):
    DOMAIN = 'cyber'
    PIPELINE_VERSION = VERSION
    TARGETS_LABEL = 'targets_cyber.yaml'
    USER_AGENT = 'cyber-corpus-pipeline'
    ROUTING_KEYS = ['cyber_routing', 'math_routing']
    DEFAULT_ROUTING = {'subject': 'cyber', 'granularity': 'target'}

if __name__ == "__main__":
    CyberPipelineDriver.main()
