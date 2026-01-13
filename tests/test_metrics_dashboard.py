"""
Tests for metrics dashboard and collection module.

Issue 4.1 (v3.0): Tests for pipeline metrics collection.
"""

from __future__ import annotations

import json
import pytest
from pathlib import Path
from collector_core.metrics import (
    MetricsCollector,
    PipelineMetrics,
    export_prometheus,
    generate_html_dashboard,
)


class TestPipelineMetrics:
    """Tests for PipelineMetrics dataclass."""

    def test_creation(self) -> None:
        """PipelineMetrics can be created."""
        metrics = PipelineMetrics(
            pipeline_id="math",
            stage="acquire",
            records_processed=1000,
            records_passed=950,
            records_filtered=50,
        )

        assert metrics.pipeline_id == "math"
        assert metrics.stage == "acquire"
        assert metrics.records_processed == 1000

    def test_defaults(self) -> None:
        """Default values are sensible."""
        metrics = PipelineMetrics(pipeline_id="test", stage="test")

        assert metrics.started_at == ""
        assert metrics.finished_at == ""
        assert metrics.duration_seconds == 0.0
        assert metrics.records_processed == 0
        assert metrics.errors == []
        assert metrics.custom_metrics == {}

    def test_custom_metrics(self) -> None:
        """Custom metrics can be added."""
        metrics = PipelineMetrics(
            pipeline_id="test",
            stage="test",
            custom_metrics={"cache_hits": 500, "api_calls": 100},
        )

        assert metrics.custom_metrics["cache_hits"] == 500
        assert metrics.custom_metrics["api_calls"] == 100


class TestMetricsCollector:
    """Tests for MetricsCollector class."""

    def test_track_context(self) -> None:
        """Track context manager works."""
        collector = MetricsCollector()

        with collector.track("math", "acquire") as metrics:
            metrics.records_processed = 1000
            metrics.records_passed = 950

        summary = collector.get_summary()
        assert summary["total_runs"] == 1
        assert summary["total_records_processed"] == 1000

    def test_track_timing(self) -> None:
        """Track records timing."""
        collector = MetricsCollector()

        with collector.track("math", "acquire") as metrics:
            pass  # Just measure overhead

        # Check that timing was recorded
        assert len(collector._runs) == 1
        run = collector._runs[0]
        assert run.started_at != ""
        assert run.finished_at != ""
        assert run.duration_seconds >= 0

    def test_track_error(self) -> None:
        """Track records errors."""
        collector = MetricsCollector()

        with pytest.raises(ValueError):
            with collector.track("math", "acquire") as metrics:
                raise ValueError("Test error")

        # Error should be recorded
        run = collector._runs[0]
        assert len(run.errors) == 1
        assert "Test error" in run.errors[0]

    def test_add_run(self) -> None:
        """Add run manually."""
        collector = MetricsCollector()
        metrics = PipelineMetrics(
            pipeline_id="test",
            stage="test",
            records_processed=100,
        )

        collector.add_run(metrics)

        assert len(collector._runs) == 1

    def test_get_summary_empty(self) -> None:
        """Empty collector returns minimal summary."""
        collector = MetricsCollector()

        summary = collector.get_summary()

        assert summary["total_runs"] == 0

    def test_get_summary_aggregates(self) -> None:
        """Summary aggregates across runs."""
        collector = MetricsCollector()

        with collector.track("math", "acquire") as m:
            m.records_processed = 1000
            m.records_passed = 800
            m.records_filtered = 200

        with collector.track("physics", "acquire") as m:
            m.records_processed = 500
            m.records_passed = 450
            m.records_filtered = 50

        summary = collector.get_summary()

        assert summary["total_runs"] == 2
        assert summary["total_records_processed"] == 1500
        assert summary["total_records_passed"] == 1250
        assert summary["total_records_filtered"] == 250

    def test_pass_rate(self) -> None:
        """Pass rate is calculated correctly."""
        collector = MetricsCollector()

        with collector.track("test", "test") as m:
            m.records_processed = 100
            m.records_passed = 75

        summary = collector.get_summary()

        assert summary["pass_rate"] == 0.75

    def test_clear(self) -> None:
        """Clear removes all runs."""
        collector = MetricsCollector()

        with collector.track("test", "test") as m:
            m.records_processed = 100

        collector.clear()

        assert len(collector._runs) == 0
        summary = collector.get_summary()
        assert summary["total_runs"] == 0

    def test_multiple_pipelines(self) -> None:
        """Tracks multiple pipelines."""
        collector = MetricsCollector()

        with collector.track("math", "acquire"):
            pass
        with collector.track("physics", "merge"):
            pass
        with collector.track("chem", "yellow_screen"):
            pass

        summary = collector.get_summary()

        assert "math" in summary["pipelines"]
        assert "physics" in summary["pipelines"]
        assert "chem" in summary["pipelines"]

    def test_multiple_stages(self) -> None:
        """Tracks multiple stages."""
        collector = MetricsCollector()

        with collector.track("math", "acquire"):
            pass
        with collector.track("math", "merge"):
            pass
        with collector.track("math", "yellow_screen"):
            pass

        summary = collector.get_summary()

        assert "acquire" in summary["stages"]
        assert "merge" in summary["stages"]
        assert "yellow_screen" in summary["stages"]


class TestExportJson:
    """Tests for JSON export."""

    def test_export_json(self, tmp_path: Path) -> None:
        """Export to JSON file."""
        collector = MetricsCollector()

        with collector.track("test", "test") as m:
            m.records_processed = 100
            m.records_passed = 90

        export_path = tmp_path / "metrics.json"
        collector.export_json(export_path)

        assert export_path.exists()
        data = json.loads(export_path.read_text())

        assert "exported_at" in data
        assert "summary" in data
        assert "runs" in data
        assert len(data["runs"]) == 1

    def test_export_json_content(self, tmp_path: Path) -> None:
        """Export JSON has correct content."""
        collector = MetricsCollector()

        with collector.track("math", "acquire") as m:
            m.records_processed = 1000
            m.bytes_processed = 1024 * 1024
            m.custom_metrics = {"api_calls": 50}

        export_path = tmp_path / "metrics.json"
        collector.export_json(export_path)

        data = json.loads(export_path.read_text())
        run = data["runs"][0]

        assert run["pipeline_id"] == "math"
        assert run["stage"] == "acquire"
        assert run["records_processed"] == 1000
        assert run["bytes_processed"] == 1024 * 1024
        assert run["custom_metrics"]["api_calls"] == 50


class TestExportPrometheus:
    """Tests for Prometheus export."""

    def test_export_prometheus(self) -> None:
        """Export to Prometheus format."""
        collector = MetricsCollector()

        with collector.track("test", "test") as m:
            m.records_processed = 1000
            m.records_passed = 900
            m.records_filtered = 100

        output = export_prometheus(collector)

        assert "dc_records_processed_total 1000" in output
        assert "dc_records_passed_total 900" in output
        assert "dc_records_filtered_total 100" in output

    def test_prometheus_format(self) -> None:
        """Prometheus output has correct format."""
        collector = MetricsCollector()

        with collector.track("test", "test") as m:
            m.records_processed = 100

        output = export_prometheus(collector)

        # Should have HELP and TYPE comments
        assert "# HELP" in output
        assert "# TYPE" in output
        assert "counter" in output or "gauge" in output

    def test_prometheus_pass_rate(self) -> None:
        """Prometheus includes pass rate."""
        collector = MetricsCollector()

        with collector.track("test", "test") as m:
            m.records_processed = 100
            m.records_passed = 80

        output = export_prometheus(collector)

        assert "dc_pass_rate" in output
        assert "0.8" in output


class TestGenerateHtmlDashboard:
    """Tests for HTML dashboard generation."""

    def test_generate_html(self) -> None:
        """Generate HTML dashboard."""
        collector = MetricsCollector()

        with collector.track("test", "test") as m:
            m.records_processed = 100

        html = generate_html_dashboard(collector)

        assert "<!DOCTYPE html>" in html
        assert "<html>" in html
        assert "Dataset Collector" in html

    def test_html_contains_metrics(self) -> None:
        """HTML contains metric values."""
        collector = MetricsCollector()

        with collector.track("math", "acquire") as m:
            m.records_processed = 1000
            m.records_passed = 950

        html = generate_html_dashboard(collector)

        assert "math" in html
        assert "acquire" in html
        assert "1,000" in html or "1000" in html
        assert "950" in html

    def test_html_table(self) -> None:
        """HTML includes run table."""
        collector = MetricsCollector()

        with collector.track("test", "test"):
            pass

        html = generate_html_dashboard(collector)

        assert "<table>" in html
        assert "<th>" in html
        assert "</table>" in html

    def test_html_styling(self) -> None:
        """HTML includes styling."""
        collector = MetricsCollector()

        with collector.track("test", "test"):
            pass

        html = generate_html_dashboard(collector)

        assert "<style>" in html
        assert "</style>" in html


class TestEdgeCases:
    """Edge case tests."""

    def test_zero_records(self) -> None:
        """Zero records doesn't cause division by zero."""
        collector = MetricsCollector()

        with collector.track("test", "test") as m:
            m.records_processed = 0
            m.records_passed = 0

        summary = collector.get_summary()

        # Should not raise, pass_rate handles zero division
        assert summary["pass_rate"] == 0.0

    def test_many_runs(self) -> None:
        """Many runs are handled."""
        collector = MetricsCollector()

        for i in range(100):
            with collector.track(f"pipeline{i}", "test") as m:
                m.records_processed = i

        summary = collector.get_summary()

        assert summary["total_runs"] == 100
        assert summary["total_records_processed"] == sum(range(100))

    def test_bytes_processed(self) -> None:
        """Bytes processed is tracked."""
        collector = MetricsCollector()

        with collector.track("test", "test") as m:
            m.bytes_processed = 1024 * 1024 * 100  # 100 MB

        assert collector._runs[0].bytes_processed == 1024 * 1024 * 100
