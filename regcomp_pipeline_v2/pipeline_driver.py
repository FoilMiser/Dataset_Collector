#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from collector_core.__version__ import __version__ as VERSION
from collector_core.pipeline_driver_base import (
    BasePipelineDriver,
)


class RegcompPipelineDriver(BasePipelineDriver):
    DOMAIN = 'regcomp'
    PIPELINE_VERSION = VERSION
    TARGETS_LABEL = 'targets_regcomp.yaml'
    USER_AGENT = 'regcomp-corpus-pipeline'
    ROUTING_KEYS = ['regcomp_routing']
    DEFAULT_ROUTING = {'subject': 'regcomp', 'granularity': 'target'}

if __name__ == "__main__":
    RegcompPipelineDriver.main()
