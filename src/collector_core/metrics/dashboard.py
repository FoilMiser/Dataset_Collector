"""Metrics dashboard helpers for pipeline runs."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from collector_core.utils.io import read_json, write_json
from collector_core.utils.paths import ensure_dir

METRICS_FILENAME = "metrics.json"


@dataclass(frozen=True)
class AggregatedMetrics:
    total_runs: int
    pipelines: dict[str, dict[str, Any]]
    totals: dict[str, int]
    bytes: dict[str, int]
    latest_run_at_utc: str | None


def _parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def collect_run_metrics(ledger_root: Path) -> list[dict[str, Any]]:
    """Collect metrics.json payloads from a ledger root."""
    metrics: list[dict[str, Any]] = []
    if not ledger_root.exists():
        return metrics
    for path in sorted(ledger_root.glob(f"*/{METRICS_FILENAME}")):
        metrics.append(read_json(path))
    return metrics


def aggregate_run_metrics(metrics: Iterable[dict[str, Any]]) -> AggregatedMetrics:
    totals: dict[str, int] = defaultdict(int)
    bytes_totals: dict[str, int] = defaultdict(int)
    pipeline_totals: dict[str, dict[str, Any]] = {}
    latest: datetime | None = None

    metrics_list = list(metrics)
    for entry in metrics_list:
        counts = entry.get("counts") or {}
        for key, value in counts.items():
            try:
                totals[key] += int(value)
            except (TypeError, ValueError):
                continue
        bytes_blob = entry.get("bytes") or {}
        for key, value in bytes_blob.items():
            try:
                bytes_totals[key] += int(value)
            except (TypeError, ValueError):
                continue
        pipeline_id = str(entry.get("pipeline_id") or "unknown")
        pipeline_stats = pipeline_totals.setdefault(
            pipeline_id,
            {
                "runs": 0,
                "counts": defaultdict(int),
                "bytes": defaultdict(int),
                "latest_run_at_utc": None,
            },
        )
        pipeline_stats["runs"] += 1
        for key, value in counts.items():
            try:
                pipeline_stats["counts"][key] += int(value)
            except (TypeError, ValueError):
                continue
        for key, value in bytes_blob.items():
            try:
                pipeline_stats["bytes"][key] += int(value)
            except (TypeError, ValueError):
                continue
        ended_at = entry.get("ended_at_utc")
        ts = _parse_timestamp(str(ended_at) if ended_at else None)
        if ts and (latest is None or ts > latest):
            latest = ts
        if ts:
            pipeline_stats["latest_run_at_utc"] = ts.isoformat()

    pipelines_serialized: dict[str, dict[str, Any]] = {}
    for pipeline_id, stats in pipeline_totals.items():
        pipelines_serialized[pipeline_id] = {
            "runs": stats["runs"],
            "counts": dict(stats["counts"]),
            "bytes": dict(stats["bytes"]),
            "latest_run_at_utc": stats["latest_run_at_utc"],
        }

    return AggregatedMetrics(
        total_runs=len(metrics_list),
        pipelines=pipelines_serialized,
        totals=dict(totals),
        bytes=dict(bytes_totals),
        latest_run_at_utc=latest.isoformat() if latest else None,
    )


def render_prometheus(metrics: Iterable[dict[str, Any]]) -> str:
    """Render metrics.json payloads in Prometheus text format."""
    lines = ["# TYPE pipeline_run_info gauge"]
    for entry in metrics:
        pipeline_id = str(entry.get("pipeline_id") or "unknown")
        run_id = str(entry.get("run_id") or "unknown")
        lines.append(
            f"pipeline_run_info{{pipeline_id=\"{pipeline_id}\",run_id=\"{run_id}\"}} 1"
        )
        counts = entry.get("counts") or {}
        for key, value in counts.items():
            lines.append(
                "pipeline_run_count{"
                f"pipeline_id=\"{pipeline_id}\",metric=\"{key}\",run_id=\"{run_id}\""
                f"}} {value}"
            )
        bytes_blob = entry.get("bytes") or {}
        for key, value in bytes_blob.items():
            lines.append(
                "pipeline_run_bytes{"
                f"pipeline_id=\"{pipeline_id}\",metric=\"{key}\",run_id=\"{run_id}\""
                f"}} {value}"
            )
        timings = entry.get("timings_ms") or {}
        for key, value in timings.items():
            lines.append(
                "pipeline_run_timing_ms{"
                f"pipeline_id=\"{pipeline_id}\",metric=\"{key}\",run_id=\"{run_id}\""
                f"}} {value}"
            )
    return "\n".join(lines) + "\n"


def render_html_report(summary: AggregatedMetrics, runs: Iterable[dict[str, Any]]) -> str:
    rows = []
    for run in runs:
        counts = run.get("counts") or {}
        rows.append(
            "<tr>"
            f"<td>{run.get('run_id','')}</td>"
            f"<td>{run.get('pipeline_id','')}</td>"
            f"<td>{run.get('started_at_utc','')}</td>"
            f"<td>{run.get('ended_at_utc','')}</td>"
            f"<td>{counts.get('targets_total','')}</td>"
            f"<td>{counts.get('queued_green','')}</td>"
            f"<td>{counts.get('queued_yellow','')}</td>"
            f"<td>{counts.get('queued_red','')}</td>"
            "</tr>"
        )
    pipelines_html = "".join(
        f"<li><strong>{pipeline_id}</strong>: {stats['runs']} runs</li>"
        for pipeline_id, stats in summary.pipelines.items()
    )
    return (
        "<!doctype html>"
        "<html lang=\"en\">"
        "<head><meta charset=\"utf-8\"><title>Pipeline Metrics</title>"
        "<style>body{font-family:Arial,sans-serif}table{border-collapse:collapse;width:100%}"
        "th,td{border:1px solid #ccc;padding:6px;text-align:left}"
        "th{background:#f3f3f3}</style></head>"
        "<body>"
        "<h1>Pipeline Metrics Dashboard</h1>"
        f"<p>Total runs: {summary.total_runs}</p>"
        f"<p>Latest run: {summary.latest_run_at_utc or 'n/a'}</p>"
        "<h2>Pipelines</h2>"
        f"<ul>{pipelines_html}</ul>"
        "<h2>Runs</h2>"
        "<table>"
        "<thead><tr><th>Run ID</th><th>Pipeline</th><th>Started</th><th>Ended</th>"
        "<th>Targets</th><th>Green</th><th>Yellow</th><th>Red</th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody>"
        "</table>"
        "</body></html>"
    )


def write_dashboard(ledger_root: Path, output_dir: Path) -> dict[str, Any]:
    """Write JSON/HTML/Prometheus outputs for metrics dashboard."""
    runs = collect_run_metrics(ledger_root)
    summary = aggregate_run_metrics(runs)
    payload = {
        "summary": {
            "total_runs": summary.total_runs,
            "pipelines": summary.pipelines,
            "totals": summary.totals,
            "bytes": summary.bytes,
            "latest_run_at_utc": summary.latest_run_at_utc,
        },
        "runs": runs,
    }
    ensure_dir(output_dir)
    write_json(output_dir / "metrics_dashboard.json", payload)
    (output_dir / "metrics_dashboard.html").write_text(
        render_html_report(summary, runs),
        encoding="utf-8",
    )
    (output_dir / "metrics.prom").write_text(
        render_prometheus(runs),
        encoding="utf-8",
    )
    return payload
