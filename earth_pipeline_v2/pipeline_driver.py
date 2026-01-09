#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from collector_core.__version__ import __version__ as VERSION
from collector_core.pipeline_driver_base import (
    BasePipelineDriver,
)


class EarthPipelineDriver(BasePipelineDriver):
    DOMAIN = 'earth'
    PIPELINE_VERSION = VERSION
    TARGETS_LABEL = 'targets_earth.yaml'
    USER_AGENT = 'earth-corpus-pipeline'
    ROUTING_KEYS = ['earth_routing', 'math_routing']
    DEFAULT_ROUTING = {'subject': 'earth', 'granularity': 'target'}

if __name__ == "__main__":
    EarthPipelineDriver.main()
