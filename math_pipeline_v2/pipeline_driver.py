#!/usr/bin/env python3
from __future__ import annotations

from typing import Any

from collector_core.__version__ import __version__ as VERSION
from collector_core.pipeline_driver_base import (
    BasePipelineDriver,
    RoutingBlockSpec,
)


class MathPipelineDriver(BasePipelineDriver):
    DOMAIN = 'math'
    PIPELINE_VERSION = VERSION
    TARGETS_LABEL = 'targets_math.yaml'
    USER_AGENT = 'math-corpus-pipeline'
    ROUTING_KEYS = ['math_routing']
    DEFAULT_ROUTING = {'subject': 'math', 'granularity': 'target'}
    ROUTING_BLOCKS = [
        RoutingBlockSpec(name='math_routing', sources=['math_routing'], mode='subset'),
    ]

    def build_row_extras(self, target: dict[str, Any], routing: dict[str, Any]) -> dict[str, Any]:
        mr = target.get("math_routing", {}) or {}
        return {
            "math_domain": mr.get("domain"),
            "math_category": mr.get("category"),
            "difficulty_level": mr.get("level"),
            "math_granularity": mr.get("granularity"),
        }


if __name__ == "__main__":
    MathPipelineDriver.main()
