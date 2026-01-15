"""
collector_core/observability.py

Unified observability module providing OpenTelemetry tracing and Prometheus metrics.
Optional dependencies - falls back gracefully if not installed.

Usage:
    from collector_core.observability import get_tracer, get_meter, start_metrics_server

    # Tracing
    tracer = get_tracer("my_component")
    with tracer.start_as_current_span("operation_name") as span:
        span.set_attribute("key", "value")
        # ... do work ...

    # Metrics
    meter = get_meter("my_component")
    counter = meter.create_counter("requests", description="Total requests")
    counter.add(1, {"status": "success"})
"""

from __future__ import annotations

import atexit
import logging
import os
from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

from collector_core.dependencies import _try_import

logger = logging.getLogger(__name__)

# Optional imports for OpenTelemetry
_otel_trace = _try_import("opentelemetry.trace")
_otel_sdk_trace = _try_import("opentelemetry.sdk.trace")
_otel_sdk_resources = _try_import("opentelemetry.sdk.resources")
_otel_exporter_otlp = _try_import("opentelemetry.exporter.otlp.proto.grpc.trace_exporter")
_otel_exporter_console = _try_import("opentelemetry.sdk.trace.export")

# Optional imports for Prometheus
_prometheus_client = _try_import("prometheus_client")

if TYPE_CHECKING:
    from opentelemetry.trace import Tracer

# Module state
_tracer_provider: Any = None
_meter_provider: Any = None
_metrics_server_started = False


def _get_service_name() -> str:
    """Get the service name from environment or default."""
    return os.environ.get("OTEL_SERVICE_NAME", "dataset-collector")


def _get_service_version() -> str:
    """Get the service version."""
    try:
        from collector_core.__version__ import __version__
        return __version__
    except ImportError:
        return "unknown"


def _setup_otel_tracing() -> Any | None:
    """Initialize OpenTelemetry tracing if available."""
    global _tracer_provider

    if _tracer_provider is not None:
        return _tracer_provider

    if _otel_trace is None or _otel_sdk_trace is None:
        return None

    try:
        from opentelemetry import trace
        from opentelemetry.sdk.resources import SERVICE_NAME, SERVICE_VERSION, Resource
        from opentelemetry.sdk.trace import TracerProvider

        resource = Resource.create({
            SERVICE_NAME: _get_service_name(),
            SERVICE_VERSION: _get_service_version(),
        })

        _tracer_provider = TracerProvider(resource=resource)
        trace.set_tracer_provider(_tracer_provider)

        # Add OTLP exporter if endpoint is configured
        otlp_endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT")
        if otlp_endpoint and _otel_exporter_otlp:
            try:
                from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
                from opentelemetry.sdk.trace.export import BatchSpanProcessor

                exporter = OTLPSpanExporter(endpoint=otlp_endpoint)
                _tracer_provider.add_span_processor(BatchSpanProcessor(exporter))
                logger.info(f"OpenTelemetry tracing enabled, exporting to {otlp_endpoint}")
            except (ImportError, OSError, RuntimeError) as e:
                # P1.1J: Catch specific OTEL exporter setup errors
                logger.warning(f"Failed to setup OTLP exporter: {e}")

        return _tracer_provider
    except (ImportError, AttributeError, RuntimeError) as e:
        # P1.1J: Catch specific OTEL initialization errors
        logger.debug(f"Failed to initialize OpenTelemetry tracing: {e}")
        return None


class NoOpSpan:
    """No-op span when OpenTelemetry is not available."""

    def __enter__(self) -> NoOpSpan:
        return self

    def __exit__(self, *args: Any) -> None:
        pass

    def set_attribute(self, key: str, value: Any) -> None:
        pass

    def set_attributes(self, attributes: dict[str, Any]) -> None:
        pass

    def add_event(self, name: str, attributes: dict[str, Any] | None = None) -> None:
        pass

    def record_exception(self, exception: Exception) -> None:
        pass

    def set_status(self, status: Any) -> None:
        pass


class NoOpTracer:
    """No-op tracer when OpenTelemetry is not available."""

    def start_as_current_span(
        self, name: str, **kwargs: Any
    ) -> NoOpSpan:
        return NoOpSpan()

    @contextmanager
    def start_span(self, name: str, **kwargs: Any) -> Iterator[NoOpSpan]:
        yield NoOpSpan()


def get_tracer(name: str = "dataset-collector") -> Tracer | NoOpTracer:
    """Get a tracer instance.

    Returns a real OpenTelemetry tracer if available, otherwise a no-op tracer.
    """
    _setup_otel_tracing()

    if _otel_trace is not None and _tracer_provider is not None:
        return _otel_trace.get_tracer(name, _get_service_version())
    return NoOpTracer()


# Prometheus metrics support
_prometheus_registry: Any = None
_prometheus_counters: dict[str, Any] = {}
_prometheus_gauges: dict[str, Any] = {}
_prometheus_histograms: dict[str, Any] = {}


def _get_prometheus_registry() -> Any:
    """Get or create the Prometheus registry."""
    global _prometheus_registry

    if _prometheus_registry is not None:
        return _prometheus_registry

    if _prometheus_client is None:
        return None

    try:
        from prometheus_client import CollectorRegistry
        _prometheus_registry = CollectorRegistry()
        return _prometheus_registry
    except (ImportError, RuntimeError):
        # P1.1J: Catch specific Prometheus initialization errors
        return None


def prometheus_counter(
    name: str,
    description: str = "",
    labelnames: tuple[str, ...] = (),
) -> Any:
    """Create or get a Prometheus counter."""
    if _prometheus_client is None:
        return None

    registry = _get_prometheus_registry()
    if registry is None:
        return None

    key = f"counter:{name}"
    if key not in _prometheus_counters:
        try:
            from prometheus_client import Counter
            _prometheus_counters[key] = Counter(
                name, description, labelnames=labelnames, registry=registry
            )
        except (ImportError, ValueError) as e:
            # P1.1J: Catch specific Prometheus metric creation errors
            logger.debug(f"Failed to create Prometheus counter {name}: {e}")
            return None
    return _prometheus_counters[key]


def prometheus_gauge(
    name: str,
    description: str = "",
    labelnames: tuple[str, ...] = (),
) -> Any:
    """Create or get a Prometheus gauge."""
    if _prometheus_client is None:
        return None

    registry = _get_prometheus_registry()
    if registry is None:
        return None

    key = f"gauge:{name}"
    if key not in _prometheus_gauges:
        try:
            from prometheus_client import Gauge
            _prometheus_gauges[key] = Gauge(
                name, description, labelnames=labelnames, registry=registry
            )
        except (ImportError, ValueError) as e:
            # P1.1J: Catch specific Prometheus metric creation errors
            logger.debug(f"Failed to create Prometheus gauge {name}: {e}")
            return None
    return _prometheus_gauges[key]


def prometheus_histogram(
    name: str,
    description: str = "",
    labelnames: tuple[str, ...] = (),
    buckets: tuple[float, ...] | None = None,
) -> Any:
    """Create or get a Prometheus histogram."""
    if _prometheus_client is None:
        return None

    registry = _get_prometheus_registry()
    if registry is None:
        return None

    key = f"histogram:{name}"
    if key not in _prometheus_histograms:
        try:
            from prometheus_client import Histogram
            kwargs: dict[str, Any] = {
                "name": name,
                "documentation": description,
                "labelnames": labelnames,
                "registry": registry,
            }
            if buckets:
                kwargs["buckets"] = buckets
            _prometheus_histograms[key] = Histogram(**kwargs)
        except (ImportError, ValueError) as e:
            # P1.1J: Catch specific Prometheus metric creation errors
            logger.debug(f"Failed to create Prometheus histogram {name}: {e}")
            return None
    return _prometheus_histograms[key]


def start_metrics_server(port: int = 9090) -> bool:
    """Start a Prometheus metrics HTTP server.

    Args:
        port: Port to listen on (default 9090).

    Returns:
        True if server started successfully, False otherwise.
    """
    global _metrics_server_started

    if _metrics_server_started:
        logger.debug("Metrics server already started")
        return True

    if _prometheus_client is None:
        logger.debug("prometheus_client not installed, metrics server disabled")
        return False

    registry = _get_prometheus_registry()
    if registry is None:
        return False

    try:
        from prometheus_client import start_http_server
        start_http_server(port, registry=registry)
        _metrics_server_started = True
        logger.info(f"Prometheus metrics server started on port {port}")
        return True
    except (ImportError, OSError, RuntimeError) as e:
        # P1.1J: Catch specific server startup errors
        logger.warning(f"Failed to start metrics server: {e}")
        return False


# Pre-defined metrics for pipeline operations
def _setup_pipeline_metrics() -> None:
    """Setup common pipeline metrics."""
    prometheus_counter(
        "dc_targets_processed_total",
        "Total number of targets processed",
        labelnames=("pipeline", "status"),
    )
    prometheus_counter(
        "dc_files_downloaded_total",
        "Total number of files downloaded",
        labelnames=("pipeline", "strategy"),
    )
    prometheus_counter(
        "dc_bytes_downloaded_total",
        "Total bytes downloaded",
        labelnames=("pipeline",),
    )
    prometheus_histogram(
        "dc_download_duration_seconds",
        "Download duration in seconds",
        labelnames=("pipeline", "strategy"),
        buckets=(0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0),
    )
    prometheus_gauge(
        "dc_pipeline_active",
        "Whether a pipeline is currently active",
        labelnames=("pipeline",),
    )
    prometheus_counter(
        "dc_errors_total",
        "Total number of errors",
        labelnames=("pipeline", "error_type"),
    )


# Helper functions for recording metrics
def record_target_processed(pipeline: str, status: str) -> None:
    """Record a target being processed."""
    counter = _prometheus_counters.get("counter:dc_targets_processed_total")
    if counter:
        counter.labels(pipeline=pipeline, status=status).inc()


def record_file_downloaded(pipeline: str, strategy: str) -> None:
    """Record a file being downloaded."""
    counter = _prometheus_counters.get("counter:dc_files_downloaded_total")
    if counter:
        counter.labels(pipeline=pipeline, strategy=strategy).inc()


def record_bytes_downloaded(pipeline: str, bytes_count: int) -> None:
    """Record bytes downloaded."""
    counter = _prometheus_counters.get("counter:dc_bytes_downloaded_total")
    if counter:
        counter.labels(pipeline=pipeline).inc(bytes_count)


def record_download_duration(pipeline: str, strategy: str, duration_seconds: float) -> None:
    """Record download duration."""
    histogram = _prometheus_histograms.get("histogram:dc_download_duration_seconds")
    if histogram:
        histogram.labels(pipeline=pipeline, strategy=strategy).observe(duration_seconds)


def set_pipeline_active(pipeline: str, active: bool) -> None:
    """Set whether a pipeline is active."""
    gauge = _prometheus_gauges.get("gauge:dc_pipeline_active")
    if gauge:
        gauge.labels(pipeline=pipeline).set(1 if active else 0)


def record_error(pipeline: str, error_type: str) -> None:
    """Record an error."""
    counter = _prometheus_counters.get("counter:dc_errors_total")
    if counter:
        counter.labels(pipeline=pipeline, error_type=error_type).inc()


@contextmanager
def traced_operation(
    name: str,
    tracer: Tracer | NoOpTracer | None = None,
    attributes: dict[str, Any] | None = None,
) -> Iterator[Any]:
    """Context manager for tracing an operation.

    Args:
        name: Name of the operation/span.
        tracer: Optional tracer instance. If None, uses default tracer.
        attributes: Optional attributes to set on the span.

    Yields:
        The span object.
    """
    if tracer is None:
        tracer = get_tracer()

    with tracer.start_as_current_span(name) as span:
        if attributes:
            span.set_attributes(attributes)
        try:
            yield span
        except Exception as e:
            span.record_exception(e)
            raise


def init_observability(
    enable_tracing: bool = True,
    enable_metrics: bool = True,
    metrics_port: int = 9090,
) -> None:
    """Initialize observability features.

    Args:
        enable_tracing: Whether to enable OpenTelemetry tracing.
        enable_metrics: Whether to enable Prometheus metrics.
        metrics_port: Port for the Prometheus metrics server.
    """
    if enable_tracing:
        _setup_otel_tracing()

    if enable_metrics:
        _setup_pipeline_metrics()
        if os.environ.get("DC_METRICS_SERVER", "").lower() in ("1", "true", "yes"):
            start_metrics_server(metrics_port)


def shutdown_observability() -> None:
    """Shutdown observability features gracefully."""
    global _tracer_provider

    if _tracer_provider is not None:
        try:
            _tracer_provider.shutdown()
        except (AttributeError, RuntimeError, OSError) as e:
            # P1.1J: Catch specific OTEL shutdown errors
            logger.debug(f"Error shutting down tracer provider: {e}")
        _tracer_provider = None


# Register shutdown on exit
atexit.register(shutdown_observability)
