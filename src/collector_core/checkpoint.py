"""Checkpoint management for pipeline runs."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from collector_core.utils.io import read_json, write_json
from collector_core.utils.logging import utc_now
from collector_core.utils.paths import ensure_dir

DEFAULT_CHECKPOINT_FILENAME = "pipeline_checkpoint.json"


@dataclass
class CheckpointState:
    run_id: str
    pipeline_id: str
    created_at_utc: str
    updated_at_utc: str
    completed_targets: list[str] = field(default_factory=list)
    counts: dict[str, int] = field(default_factory=dict)

    def record_target(self, target_id: str, bucket: str | None = None) -> None:
        if target_id not in self.completed_targets:
            self.completed_targets.append(target_id)
        if bucket:
            self.counts[bucket] = int(self.counts.get(bucket, 0)) + 1
        self.updated_at_utc = utc_now()


def checkpoint_path(checkpoint_dir: Path, pipeline_id: str) -> Path:
    safe_id = pipeline_id.replace("/", "_")
    return checkpoint_dir / safe_id / DEFAULT_CHECKPOINT_FILENAME


def load_checkpoint(path: Path) -> CheckpointState | None:
    if not path.exists():
        return None
    # P1.2F: Handle JSON decode errors when loading checkpoint
    try:
        payload = read_json(path)
    except (json.JSONDecodeError, OSError):
        return None
    return CheckpointState(
        run_id=str(payload.get("run_id") or ""),
        pipeline_id=str(payload.get("pipeline_id") or ""),
        created_at_utc=str(payload.get("created_at_utc") or utc_now()),
        updated_at_utc=str(payload.get("updated_at_utc") or utc_now()),
        completed_targets=list(payload.get("completed_targets") or []),
        counts={k: int(v) for k, v in (payload.get("counts") or {}).items()},
    )


def save_checkpoint(path: Path, state: CheckpointState) -> None:
    ensure_dir(path.parent)
    payload: dict[str, Any] = {
        "run_id": state.run_id,
        "pipeline_id": state.pipeline_id,
        "created_at_utc": state.created_at_utc,
        "updated_at_utc": state.updated_at_utc,
        "completed_targets": state.completed_targets,
        "counts": state.counts,
    }
    write_json(path, payload)


def init_checkpoint(path: Path, *, pipeline_id: str, run_id: str) -> CheckpointState:
    now = utc_now()
    state = CheckpointState(
        run_id=run_id,
        pipeline_id=pipeline_id,
        created_at_utc=now,
        updated_at_utc=now,
    )
    save_checkpoint(path, state)
    return state


def cleanup_checkpoint(path: Path) -> None:
    if path.exists():
        path.unlink()
