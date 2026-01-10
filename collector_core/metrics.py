"""
collector_core/metrics.py

Metrics collection for pipeline operations.
Provides a simple, lightweight interface for tracking operational metrics.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

from collector_core.utils import utc_now


@dataclass
class Counter:
    """A simple counter metric."""

    name: str
    value: int = 0
    labels: dict[str, str] = field(default_factory=dict)

    def inc(self, amount: int = 1) -> None:
        """Increment the counter."""
        self.value += amount

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "name": self.name,
            "value": self.value,
            "labels": self.labels,
        }


@dataclass
class Gauge:
    """A gauge metric (can go up or down)."""

    name: str
    value: float = 0.0
    labels: dict[str, str] = field(default_factory=dict)

    def set(self, value: float) -> None:
        """Set the gauge value."""
        self.value = value

    def inc(self, amount: float = 1.0) -> None:
        """Increment the gauge."""
        self.value += amount

    def dec(self, amount: float = 1.0) -> None:
        """Decrement the gauge."""
        self.value -= amount

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "name": self.name,
            "value": self.value,
            "labels": self.labels,
        }


@dataclass
class Timer:
    """A timer metric for measuring durations."""

    name: str
    start_time: float | None = None
    duration_ms: float | None = None
    labels: dict[str, str] = field(default_factory=dict)

    def start(self) -> Timer:
        """Start the timer."""
        self.start_time = time.time()
        return self

    def stop(self) -> float:
        """Stop the timer and return duration in milliseconds."""
        if self.start_time is None:
            raise RuntimeError("Timer was not started")
        self.duration_ms = (time.time() - self.start_time) * 1000
        return self.duration_ms

    def __enter__(self) -> Timer:
        return self.start()

    def __exit__(self, *args: Any) -> None:
        self.stop()

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "name": self.name,
            "duration_ms": self.duration_ms,
            "labels": self.labels,
        }


class MetricsCollector:
    """Collects and manages metrics for a pipeline run."""

    def __init__(self, pipeline_id: str):
        self.pipeline_id = pipeline_id
        self.start_time = utc_now()
        self.counters: dict[str, Counter] = {}
        self.gauges: dict[str, Gauge] = {}
        self.timers: list[Timer] = []
        self.events: list[dict[str, Any]] = []

    def counter(self, name: str, **labels: str) -> Counter:
        """Get or create a counter."""
        key = f"{name}:{sorted(labels.items())}"
        if key not in self.counters:
            self.counters[key] = Counter(name=name, labels=labels)
        return self.counters[key]

    def gauge(self, name: str, **labels: str) -> Gauge:
        """Get or create a gauge."""
        key = f"{name}:{sorted(labels.items())}"
        if key not in self.gauges:
            self.gauges[key] = Gauge(name=name, labels=labels)
        return self.gauges[key]

    def timer(self, name: str, **labels: str) -> Timer:
        """Create a new timer."""
        t = Timer(name=name, labels=labels)
        self.timers.append(t)
        return t

    def event(self, name: str, **data: Any) -> None:
        """Record an event."""
        self.events.append(
            {
                "name": name,
                "timestamp": utc_now(),
                **data,
            }
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert all metrics to dictionary."""
        return {
            "pipeline_id": self.pipeline_id,
            "start_time": self.start_time,
            "end_time": utc_now(),
            "counters": [c.to_dict() for c in self.counters.values()],
            "gauges": [g.to_dict() for g in self.gauges.values()],
            "timers": [t.to_dict() for t in self.timers if t.duration_ms is not None],
            "events": self.events,
        }

    def summary(self) -> dict[str, Any]:
        """Get a summary of key metrics."""
        counter_summary = {c.name: c.value for c in self.counters.values()}
        return {
            "pipeline_id": self.pipeline_id,
            "counters": counter_summary,
            "timer_count": len([t for t in self.timers if t.duration_ms]),
            "event_count": len(self.events),
        }


# Global metrics collector (optional singleton pattern)
_current_collector: MetricsCollector | None = None


def get_collector() -> MetricsCollector | None:
    """Get the current metrics collector."""
    return _current_collector


def set_collector(collector: MetricsCollector) -> None:
    """Set the current metrics collector."""
    global _current_collector
    _current_collector = collector


def clear_collector() -> None:
    """Clear the current metrics collector."""
    global _current_collector
    _current_collector = None


# Convenience functions
def inc_counter(name: str, amount: int = 1, **labels: str) -> None:
    """Increment a counter on the current collector."""
    collector = get_collector()
    if collector:
        collector.counter(name, **labels).inc(amount)


def set_gauge(name: str, value: float, **labels: str) -> None:
    """Set a gauge on the current collector."""
    collector = get_collector()
    if collector:
        collector.gauge(name, **labels).set(value)


def record_event(name: str, **data: Any) -> None:
    """Record an event on the current collector."""
    collector = get_collector()
    if collector:
        collector.event(name, **data)
