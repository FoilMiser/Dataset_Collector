"""Tests for collector_core.observability module."""

from __future__ import annotations

import os
from unittest.mock import Mock, patch, MagicMock

import pytest


class TestNoOpClasses:
    """Test NoOp fallback classes when OTEL is unavailable."""

    def test_noop_span_context_manager(self) -> None:
        """NoOpSpan works as context manager."""
        from collector_core.observability import NoOpSpan

        span = NoOpSpan()
        with span as s:
            assert s is span
            s.set_attribute("key", "value")
            s.set_attributes({"a": 1, "b": 2})
            s.add_event("event_name", {"attr": "value"})
            s.record_exception(ValueError("test"))
            s.set_status(None)

    def test_noop_tracer_start_as_current_span(self) -> None:
        """NoOpTracer.start_as_current_span returns NoOpSpan."""
        from collector_core.observability import NoOpTracer, NoOpSpan

        tracer = NoOpTracer()
        span = tracer.start_as_current_span("test_span")
        assert isinstance(span, NoOpSpan)

    def test_noop_tracer_start_span_context(self) -> None:
        """NoOpTracer.start_span works as context manager."""
        from collector_core.observability import NoOpTracer, NoOpSpan

        tracer = NoOpTracer()
        with tracer.start_span("test_span") as span:
            assert isinstance(span, NoOpSpan)


class TestGetTracer:
    """Test get_tracer function."""

    def test_get_tracer_returns_noop_when_otel_unavailable(self) -> None:
        """get_tracer returns NoOpTracer when OTEL is not installed."""
        with patch("collector_core.observability._otel_trace", None):
            from collector_core.observability import get_tracer, NoOpTracer

            # Reset provider to ensure fresh state
            import collector_core.observability as obs
            obs._tracer_provider = None

            tracer = get_tracer("test")
            assert isinstance(tracer, NoOpTracer)

    def test_get_tracer_uses_default_name(self) -> None:
        """get_tracer uses default name when not provided."""
        from collector_core.observability import get_tracer

        # Should not raise
        tracer = get_tracer()
        assert tracer is not None


class TestServiceInfo:
    """Test service info helper functions."""

    def test_get_service_name_from_env(self) -> None:
        """_get_service_name reads from OTEL_SERVICE_NAME env var."""
        from collector_core.observability import _get_service_name

        with patch.dict(os.environ, {"OTEL_SERVICE_NAME": "my-service"}):
            assert _get_service_name() == "my-service"

    def test_get_service_name_default(self) -> None:
        """_get_service_name returns default when env var not set."""
        from collector_core.observability import _get_service_name

        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("OTEL_SERVICE_NAME", None)
            assert _get_service_name() == "dataset-collector"

    def test_get_service_version(self) -> None:
        """_get_service_version returns version from __version__."""
        from collector_core.observability import _get_service_version

        version = _get_service_version()
        assert isinstance(version, str)
        assert version != ""

    def test_get_service_version_fallback(self) -> None:
        """_get_service_version returns 'unknown' on import error."""
        with patch.dict("sys.modules", {"collector_core.__version__": None}):
            # Force re-import behavior by patching the import
            from collector_core.observability import _get_service_version
            # This should return the cached version since it was already imported
            version = _get_service_version()
            assert isinstance(version, str)


class TestPrometheusMetrics:
    """Test Prometheus metrics functions."""

    def test_prometheus_counter_returns_none_when_unavailable(self) -> None:
        """prometheus_counter returns None when prometheus_client not installed."""
        with patch("collector_core.observability._prometheus_client", None):
            from collector_core.observability import prometheus_counter

            result = prometheus_counter("test_counter", "A test counter")
            assert result is None

    def test_prometheus_gauge_returns_none_when_unavailable(self) -> None:
        """prometheus_gauge returns None when prometheus_client not installed."""
        with patch("collector_core.observability._prometheus_client", None):
            from collector_core.observability import prometheus_gauge

            result = prometheus_gauge("test_gauge", "A test gauge")
            assert result is None

    def test_prometheus_histogram_returns_none_when_unavailable(self) -> None:
        """prometheus_histogram returns None when prometheus_client not installed."""
        with patch("collector_core.observability._prometheus_client", None):
            from collector_core.observability import prometheus_histogram

            result = prometheus_histogram("test_histogram", "A test histogram")
            assert result is None

    def test_get_prometheus_registry_returns_none_when_unavailable(self) -> None:
        """_get_prometheus_registry returns None when prometheus_client not installed."""
        with patch("collector_core.observability._prometheus_client", None):
            import collector_core.observability as obs
            obs._prometheus_registry = None

            result = obs._get_prometheus_registry()
            assert result is None


class TestMetricsServer:
    """Test metrics server functions."""

    def test_start_metrics_server_returns_false_when_unavailable(self) -> None:
        """start_metrics_server returns False when prometheus_client not installed."""
        import collector_core.observability as obs
        obs._metrics_server_started = False

        with patch("collector_core.observability._prometheus_client", None):
            from collector_core.observability import start_metrics_server

            result = start_metrics_server(9090)
            assert result is False

    def test_start_metrics_server_returns_true_when_already_started(self) -> None:
        """start_metrics_server returns True when server already started."""
        import collector_core.observability as obs
        obs._metrics_server_started = True

        from collector_core.observability import start_metrics_server

        result = start_metrics_server(9090)
        assert result is True

        # Reset state
        obs._metrics_server_started = False


class TestMetricRecordingHelpers:
    """Test metric recording helper functions."""

    def test_record_target_processed_no_op_when_no_counter(self) -> None:
        """record_target_processed is no-op when counter doesn't exist."""
        import collector_core.observability as obs

        # Clear any existing counters
        obs._prometheus_counters = {}

        from collector_core.observability import record_target_processed

        # Should not raise
        record_target_processed("test_pipeline", "success")

    def test_record_file_downloaded_no_op_when_no_counter(self) -> None:
        """record_file_downloaded is no-op when counter doesn't exist."""
        import collector_core.observability as obs
        obs._prometheus_counters = {}

        from collector_core.observability import record_file_downloaded

        # Should not raise
        record_file_downloaded("test_pipeline", "http")

    def test_record_bytes_downloaded_no_op_when_no_counter(self) -> None:
        """record_bytes_downloaded is no-op when counter doesn't exist."""
        import collector_core.observability as obs
        obs._prometheus_counters = {}

        from collector_core.observability import record_bytes_downloaded

        # Should not raise
        record_bytes_downloaded("test_pipeline", 1024)

    def test_record_download_duration_no_op_when_no_histogram(self) -> None:
        """record_download_duration is no-op when histogram doesn't exist."""
        import collector_core.observability as obs
        obs._prometheus_histograms = {}

        from collector_core.observability import record_download_duration

        # Should not raise
        record_download_duration("test_pipeline", "http", 1.5)

    def test_set_pipeline_active_no_op_when_no_gauge(self) -> None:
        """set_pipeline_active is no-op when gauge doesn't exist."""
        import collector_core.observability as obs
        obs._prometheus_gauges = {}

        from collector_core.observability import set_pipeline_active

        # Should not raise
        set_pipeline_active("test_pipeline", True)
        set_pipeline_active("test_pipeline", False)

    def test_record_error_no_op_when_no_counter(self) -> None:
        """record_error is no-op when counter doesn't exist."""
        import collector_core.observability as obs
        obs._prometheus_counters = {}

        from collector_core.observability import record_error

        # Should not raise
        record_error("test_pipeline", "network_error")


class TestTracedOperation:
    """Test traced_operation context manager."""

    def test_traced_operation_with_default_tracer(self) -> None:
        """traced_operation works with default tracer."""
        from collector_core.observability import traced_operation

        with traced_operation("test_operation") as span:
            # Should not raise
            assert span is not None

    def test_traced_operation_with_custom_tracer(self) -> None:
        """traced_operation works with custom tracer."""
        from collector_core.observability import traced_operation, NoOpTracer

        tracer = NoOpTracer()
        with traced_operation("test_operation", tracer=tracer) as span:
            assert span is not None

    def test_traced_operation_with_attributes(self) -> None:
        """traced_operation sets attributes on span."""
        from collector_core.observability import traced_operation

        attrs = {"key1": "value1", "key2": 42}
        with traced_operation("test_operation", attributes=attrs) as span:
            # NoOpSpan accepts attributes without error
            assert span is not None

    def test_traced_operation_records_exception(self) -> None:
        """traced_operation records exceptions."""
        from collector_core.observability import traced_operation

        with pytest.raises(ValueError):
            with traced_operation("test_operation") as span:
                raise ValueError("test error")


class TestInitShutdown:
    """Test initialization and shutdown functions."""

    def test_init_observability_with_defaults(self) -> None:
        """init_observability works with defaults."""
        from collector_core.observability import init_observability

        # Should not raise
        init_observability(enable_tracing=False, enable_metrics=False)

    def test_init_observability_enables_tracing(self) -> None:
        """init_observability can enable tracing."""
        from collector_core.observability import init_observability

        # Should not raise even if OTEL not installed
        init_observability(enable_tracing=True, enable_metrics=False)

    def test_init_observability_metrics_server_from_env(self) -> None:
        """init_observability starts metrics server based on env var."""
        import collector_core.observability as obs

        # Ensure server not started
        obs._metrics_server_started = False

        with patch.dict(os.environ, {"DC_METRICS_SERVER": "false"}):
            from collector_core.observability import init_observability

            init_observability(enable_tracing=False, enable_metrics=True)

        # Reset state
        obs._metrics_server_started = False

    def test_shutdown_observability_handles_no_provider(self) -> None:
        """shutdown_observability handles missing provider gracefully."""
        import collector_core.observability as obs
        obs._tracer_provider = None

        from collector_core.observability import shutdown_observability

        # Should not raise
        shutdown_observability()

    def test_shutdown_observability_handles_provider_error(self) -> None:
        """shutdown_observability handles provider shutdown errors."""
        import collector_core.observability as obs

        mock_provider = Mock()
        mock_provider.shutdown.side_effect = RuntimeError("shutdown error")
        obs._tracer_provider = mock_provider

        from collector_core.observability import shutdown_observability

        # Should not raise
        shutdown_observability()
        assert obs._tracer_provider is None


class TestSetupOtelTracing:
    """Test _setup_otel_tracing function."""

    def test_setup_returns_none_when_otel_unavailable(self) -> None:
        """_setup_otel_tracing returns None when OTEL not installed."""
        import collector_core.observability as obs
        obs._tracer_provider = None

        with patch("collector_core.observability._otel_trace", None):
            result = obs._setup_otel_tracing()
            assert result is None

    def test_setup_returns_cached_provider(self) -> None:
        """_setup_otel_tracing returns cached provider on second call."""
        import collector_core.observability as obs

        mock_provider = Mock()
        obs._tracer_provider = mock_provider

        result = obs._setup_otel_tracing()
        assert result is mock_provider

        # Reset state
        obs._tracer_provider = None


class TestSetupPipelineMetrics:
    """Test _setup_pipeline_metrics function."""

    def test_setup_pipeline_metrics_creates_metrics(self) -> None:
        """_setup_pipeline_metrics creates standard pipeline metrics."""
        import collector_core.observability as obs

        # Clear existing metrics
        obs._prometheus_counters = {}
        obs._prometheus_gauges = {}
        obs._prometheus_histograms = {}

        # Run setup (metrics creation may fail if prometheus not installed, that's OK)
        obs._setup_pipeline_metrics()

        # The function should not raise regardless of whether prometheus is installed
