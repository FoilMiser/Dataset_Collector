"""Metrics dashboard and collection utilities."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


@dataclass
class PipelineMetrics:
    """Metrics for a single pipeline run.
    
    Attributes:
        pipeline_id: Pipeline identifier
        stage: Pipeline stage (acquire, yellow_screen, merge, etc.)
        started_at: Start timestamp (UTC)
        finished_at: End timestamp (UTC)
        duration_seconds: Total duration
        records_processed: Number of records processed
        records_passed: Number of records that passed
        records_filtered: Number of records filtered
        bytes_processed: Total bytes processed
        errors: List of error messages
        custom_metrics: Additional custom metrics
    """
    pipeline_id: str
    stage: str
    started_at: str = ""
    finished_at: str = ""
    duration_seconds: float = 0.0
    records_processed: int = 0
    records_passed: int = 0
    records_filtered: int = 0
    bytes_processed: int = 0
    errors: list[str] = field(default_factory=list)
    custom_metrics: dict[str, Any] = field(default_factory=dict)


class MetricsCollector:
    """Collects and aggregates pipeline metrics.
    
    Example:
        collector = MetricsCollector()
        
        with collector.track("math", "acquire") as metrics:
            # Run pipeline stage
            metrics.records_processed = 1000
            metrics.records_passed = 950
        
        # Export metrics
        collector.export_json(Path("metrics.json"))
    """
    
    def __init__(self) -> None:
        self._runs: list[PipelineMetrics] = []
        self._start_time: float = 0.0
        self._current: PipelineMetrics | None = None
    
    def track(self, pipeline_id: str, stage: str) -> "_MetricsContext":
        """Create a context manager for tracking metrics.
        
        Args:
            pipeline_id: Pipeline identifier
            stage: Pipeline stage name
            
        Returns:
            Context manager that yields PipelineMetrics
        """
        return _MetricsContext(self, pipeline_id, stage)
    
    def add_run(self, metrics: PipelineMetrics) -> None:
        """Add a completed run's metrics."""
        self._runs.append(metrics)
    
    def get_summary(self) -> dict[str, Any]:
        """Get summary statistics across all runs."""
        if not self._runs:
            return {"total_runs": 0}
        
        total_processed = sum(r.records_processed for r in self._runs)
        total_passed = sum(r.records_passed for r in self._runs)
        total_filtered = sum(r.records_filtered for r in self._runs)
        total_duration = sum(r.duration_seconds for r in self._runs)
        total_errors = sum(len(r.errors) for r in self._runs)
        
        return {
            "total_runs": len(self._runs),
            "total_records_processed": total_processed,
            "total_records_passed": total_passed,
            "total_records_filtered": total_filtered,
            "total_duration_seconds": total_duration,
            "total_errors": total_errors,
            "pass_rate": total_passed / max(1, total_processed),
            "avg_duration_seconds": total_duration / len(self._runs),
            "pipelines": list(set(r.pipeline_id for r in self._runs)),
            "stages": list(set(r.stage for r in self._runs)),
        }
    
    def export_json(self, path: Path) -> None:
        """Export metrics to JSON file."""
        data = {
            "exported_at": datetime.now(timezone.utc).isoformat(),
            "summary": self.get_summary(),
            "runs": [
                {
                    "pipeline_id": r.pipeline_id,
                    "stage": r.stage,
                    "started_at": r.started_at,
                    "finished_at": r.finished_at,
                    "duration_seconds": r.duration_seconds,
                    "records_processed": r.records_processed,
                    "records_passed": r.records_passed,
                    "records_filtered": r.records_filtered,
                    "bytes_processed": r.bytes_processed,
                    "errors": r.errors,
                    "custom_metrics": r.custom_metrics,
                }
                for r in self._runs
            ],
        }
        path.write_text(json.dumps(data, indent=2))
    
    def clear(self) -> None:
        """Clear all collected metrics."""
        self._runs.clear()


class _MetricsContext:
    """Context manager for tracking pipeline metrics."""
    
    def __init__(self, collector: MetricsCollector, pipeline_id: str, stage: str):
        self._collector = collector
        self._metrics = PipelineMetrics(pipeline_id=pipeline_id, stage=stage)
    
    def __enter__(self) -> PipelineMetrics:
        self._start = time.perf_counter()
        self._metrics.started_at = datetime.now(timezone.utc).isoformat()
        return self._metrics
    
    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self._metrics.finished_at = datetime.now(timezone.utc).isoformat()
        self._metrics.duration_seconds = time.perf_counter() - self._start
        
        if exc_val:
            self._metrics.errors.append(str(exc_val))
        
        self._collector.add_run(self._metrics)


def export_prometheus(collector: MetricsCollector) -> str:
    """Export metrics in Prometheus format.
    
    Args:
        collector: MetricsCollector with run data
        
    Returns:
        Prometheus-formatted metrics string
    """
    lines = []
    summary = collector.get_summary()
    
    # Counter metrics
    lines.append("# HELP dc_records_processed_total Total records processed")
    lines.append("# TYPE dc_records_processed_total counter")
    lines.append(f"dc_records_processed_total {summary.get('total_records_processed', 0)}")
    
    lines.append("# HELP dc_records_passed_total Total records passed")
    lines.append("# TYPE dc_records_passed_total counter")
    lines.append(f"dc_records_passed_total {summary.get('total_records_passed', 0)}")
    
    lines.append("# HELP dc_records_filtered_total Total records filtered")
    lines.append("# TYPE dc_records_filtered_total counter")
    lines.append(f"dc_records_filtered_total {summary.get('total_records_filtered', 0)}")
    
    lines.append("# HELP dc_errors_total Total errors encountered")
    lines.append("# TYPE dc_errors_total counter")
    lines.append(f"dc_errors_total {summary.get('total_errors', 0)}")
    
    # Gauge metrics
    lines.append("# HELP dc_pass_rate Current pass rate")
    lines.append("# TYPE dc_pass_rate gauge")
    lines.append(f"dc_pass_rate {summary.get('pass_rate', 0):.4f}")
    
    lines.append("# HELP dc_pipeline_runs_total Total pipeline runs")
    lines.append("# TYPE dc_pipeline_runs_total counter")
    lines.append(f"dc_pipeline_runs_total {summary.get('total_runs', 0)}")
    
    return "\n".join(lines)


def generate_html_dashboard(collector: MetricsCollector) -> str:
    """Generate a simple HTML dashboard.
    
    Args:
        collector: MetricsCollector with run data
        
    Returns:
        HTML string for dashboard
    """
    summary = collector.get_summary()
    
    html = f"""<!DOCTYPE html>
<html>
<head>
    <title>Dataset Collector Metrics Dashboard</title>
    <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; margin: 40px; }}
        .metric {{ display: inline-block; margin: 20px; padding: 20px; background: #f5f5f5; border-radius: 8px; }}
        .metric-value {{ font-size: 2em; font-weight: bold; color: #333; }}
        .metric-label {{ color: #666; margin-top: 5px; }}
        .success {{ color: #28a745; }}
        .warning {{ color: #ffc107; }}
        .error {{ color: #dc3545; }}
        table {{ border-collapse: collapse; width: 100%; margin-top: 20px; }}
        th, td {{ border: 1px solid #ddd; padding: 12px; text-align: left; }}
        th {{ background: #f8f9fa; }}
    </style>
</head>
<body>
    <h1>Dataset Collector Metrics</h1>
    <p>Generated: {datetime.now(timezone.utc).isoformat()}</p>
    
    <div class="metrics-summary">
        <div class="metric">
            <div class="metric-value">{summary.get('total_runs', 0)}</div>
            <div class="metric-label">Pipeline Runs</div>
        </div>
        <div class="metric">
            <div class="metric-value">{summary.get('total_records_processed', 0):,}</div>
            <div class="metric-label">Records Processed</div>
        </div>
        <div class="metric">
            <div class="metric-value success">{summary.get('total_records_passed', 0):,}</div>
            <div class="metric-label">Records Passed</div>
        </div>
        <div class="metric">
            <div class="metric-value warning">{summary.get('total_records_filtered', 0):,}</div>
            <div class="metric-label">Records Filtered</div>
        </div>
        <div class="metric">
            <div class="metric-value">{summary.get('pass_rate', 0):.1%}</div>
            <div class="metric-label">Pass Rate</div>
        </div>
        <div class="metric">
            <div class="metric-value error">{summary.get('total_errors', 0)}</div>
            <div class="metric-label">Errors</div>
        </div>
    </div>
    
    <h2>Pipeline Runs</h2>
    <table>
        <tr>
            <th>Pipeline</th>
            <th>Stage</th>
            <th>Duration</th>
            <th>Processed</th>
            <th>Passed</th>
            <th>Filtered</th>
            <th>Errors</th>
        </tr>
"""
    
    for run in collector._runs:
        html += f"""        <tr>
            <td>{run.pipeline_id}</td>
            <td>{run.stage}</td>
            <td>{run.duration_seconds:.2f}s</td>
            <td>{run.records_processed:,}</td>
            <td>{run.records_passed:,}</td>
            <td>{run.records_filtered:,}</td>
            <td>{len(run.errors)}</td>
        </tr>
"""
    
    html += """    </table>
</body>
</html>"""
    
    return html


__all__ = [
    "MetricsCollector",
    "PipelineMetrics",
    "export_prometheus",
    "generate_html_dashboard",
]
