from __future__ import annotations

import logging
from pathlib import Path

from collector_core.utils import write_jsonl

if False:  # pragma: no cover - type checking
    from collector_core.pipeline_driver_base import ClassificationResult

logger = logging.getLogger(__name__)


def sort_queue_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    def sort_key(row: dict[str, object]) -> tuple[int, str]:
        p = row.get("priority", None)
        try:
            pi = int(p) if p is not None else -999999
        except Exception:
            logger.debug("Failed to parse priority %r for row %s", p, row.get("id", ""))
            pi = -999999
        return (-pi, str(row.get("id", "")))

    return sorted(rows, key=sort_key)


def emit_queues(queues_root: Path, results: "ClassificationResult") -> None:
    results.green_rows = sort_queue_rows(results.green_rows)
    results.yellow_rows = sort_queue_rows(results.yellow_rows)
    results.red_rows = sort_queue_rows(results.red_rows)
    write_jsonl(queues_root / "green_download.jsonl", results.green_rows)
    write_jsonl(queues_root / "yellow_pipeline.jsonl", results.yellow_rows)
    write_jsonl(queues_root / "red_rejected.jsonl", results.red_rows)
