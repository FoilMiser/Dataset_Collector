#!/usr/bin/env python3
from __future__ import annotations

from typing import Any

from collector_core.pipeline_driver_base import (
    BasePipelineDriver,
    RoutingBlockSpec,
    coerce_int,
)


class RegcompPipelineDriver(BasePipelineDriver):
    DOMAIN = 'regcomp'
    TARGETS_LABEL = 'targets_regcomp.yaml'
    USER_AGENT = 'regcomp-corpus-pipeline'
    ROUTING_KEYS = ['regcomp_routing']
    DEFAULT_ROUTING = {'subject': 'regcomp', 'granularity': 'target'}

if __name__ == "__main__":
    RegcompPipelineDriver.main()
