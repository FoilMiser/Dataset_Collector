#!/usr/bin/env python3
from __future__ import annotations

from typing import Any

from collector_core.pipeline_driver_base import (
    BasePipelineDriver,
    RoutingBlockSpec,
)


class MetrologyPipelineDriver(BasePipelineDriver):
    DOMAIN = 'metrology'
    TARGETS_LABEL = 'targets_metrology.yaml'
    USER_AGENT = 'metrology-corpus-pipeline'
    ROUTING_KEYS = ['metrology_routing', 'math_routing']
    DEFAULT_ROUTING = {'subject': 'metrology', 'granularity': 'target'}
    ROUTING_BLOCKS = [
        RoutingBlockSpec(name='math_routing', sources=['math_routing'], mode='subset'),
    ]
    INCLUDE_ROUTING_DICT_IN_ROW = True

    def build_row_extras(self, target: dict[str, Any], routing: dict[str, Any]) -> dict[str, Any]:
        mr = target.get("math_routing", {}) or {}
        return {
            "publisher": target.get("publisher"),
            "math_domain": mr.get("domain"),
            "math_category": mr.get("category"),
            "difficulty_level": mr.get("level"),
            "math_granularity": mr.get("granularity"),
        }


if __name__ == "__main__":
    MetrologyPipelineDriver.main()
