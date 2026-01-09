#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from collector_core.__version__ import __version__ as VERSION
from collector_core.pipeline_driver_base import (
    BasePipelineDriver,
)


class EconStatsDecisionAdaptationPipelineDriver(BasePipelineDriver):
    DOMAIN = 'econ'
    PIPELINE_VERSION = VERSION
    TARGETS_LABEL = 'targets_econ_stats_decision_v2.yaml'
    USER_AGENT = 'econ-stats-corpus'
    ROUTING_KEYS = ['econ_routing', 'math_routing']
    DEFAULT_ROUTING = {'subject': 'econ', 'granularity': 'target'}
    INCLUDE_ROUTING_DICT_IN_ROW = True

if __name__ == "__main__":
    EconStatsDecisionAdaptationPipelineDriver.main()
