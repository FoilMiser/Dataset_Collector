"""Metrics collection and reporting for Dataset Collector.

This module provides pipeline run metrics collection, Prometheus export,
and simple dashboard generation.
"""

from collector_core.metrics.dashboard import (
    MetricsCollector,
    PipelineMetrics,
    export_prometheus,
    generate_html_dashboard,
)

__all__ = [
    "MetricsCollector",
    "PipelineMetrics",
    "export_prometheus",
    "generate_html_dashboard",
]
