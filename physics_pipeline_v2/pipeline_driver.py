#!/usr/bin/env python3
from __future__ import annotations

from typing import Any

from collector_core.pipeline_driver_base import (
    BasePipelineDriver,
    RoutingBlockSpec,
    coerce_int,
)


class PhysicsPipelineDriver(BasePipelineDriver):
    DOMAIN = 'physics'
    TARGETS_LABEL = 'targets_physics.yaml'
    USER_AGENT = 'physics-corpus-pipeline'
    ROUTING_KEYS = ['physics_routing', 'math_routing']
    DEFAULT_ROUTING = {'subject': 'physics', 'granularity': 'target'}
    ROUTING_BLOCKS = [
        RoutingBlockSpec(name='math_routing', sources=['math_routing'], mode='subset'),
        RoutingBlockSpec(name='physics_routing', sources=['physics_routing'], mode='subset'),
    ]

    def build_row_extras(self, target: dict[str, Any], routing: dict[str, Any]) -> dict[str, Any]:
        mr = target.get("math_routing", {}) or {}
        pr = target.get("physics_routing", {}) or {}
        return {
            "math_domain": mr.get("domain"),
            "math_category": mr.get("category"),
            "difficulty_level": mr.get("level"),
            "math_granularity": mr.get("granularity"),
            "physics_domain": pr.get("domain"),
            "physics_category": pr.get("category"),
            "physics_level": coerce_int(pr.get("level")),
            "physics_granularity": pr.get("granularity"),
        }


if __name__ == "__main__":
    PhysicsPipelineDriver.main()
