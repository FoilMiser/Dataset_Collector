#!/usr/bin/env python3
from __future__ import annotations

from typing import Any

from collector_core.pipeline_driver_base import (
    BasePipelineDriver,
    RoutingBlockSpec,
)


class SafetyIncidentPipelineDriver(BasePipelineDriver):
    DOMAIN = 'safety_incident'
    TARGETS_LABEL = 'targets_safety_incident.yaml'
    USER_AGENT = 'safety-incident-pipeline'
    ROUTING_KEYS = ['safety_routing']
    DEFAULT_ROUTING = {'subject': 'safety_incident', 'granularity': 'target'}
    ROUTING_BLOCKS = [
        RoutingBlockSpec(name='safety_routing', sources=['safety_routing'], mode='raw'),
    ]

if __name__ == "__main__":
    SafetyIncidentPipelineDriver.main()
