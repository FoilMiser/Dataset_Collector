from __future__ import annotations

import dataclasses
import shutil
from pathlib import Path
from typing import Any


def _as_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def limit_violation(
    *,
    target_id: str,
    limit_type: str,
    limit: int | None,
    observed: int | None,
    file_label: str | None = None,
) -> dict[str, Any]:
    payload = {
        "status": "error",
        "error": "limit_exceeded",
        "limit_type": limit_type,
        "limit": limit,
        "observed": observed,
        "target_id": target_id,
    }
    if file_label:
        payload["file"] = file_label
    payload["message"] = (
        f"Limit exceeded ({limit_type}) for target {target_id}. limit={limit} observed={observed}."
    )
    return payload


@dataclasses.dataclass
class TargetLimitEnforcer:
    target_id: str
    limit_files: int | None
    max_bytes_per_target: int | None
    max_bytes_per_file: int | None
    files_seen: int = 0
    bytes_seen: int = 0

    def start_file(self, file_label: str | None = None) -> dict[str, Any] | None:
        if self.limit_files is not None and self.files_seen >= self.limit_files:
            return limit_violation(
                target_id=self.target_id,
                limit_type="files_per_target",
                limit=self.limit_files,
                observed=self.files_seen,
                file_label=file_label,
            )
        self.files_seen += 1
        return None

    def check_remaining_bytes(self, file_label: str | None = None) -> dict[str, Any] | None:
        if self.max_bytes_per_target is not None and self.bytes_seen >= self.max_bytes_per_target:
            return limit_violation(
                target_id=self.target_id,
                limit_type="bytes_per_target",
                limit=self.max_bytes_per_target,
                observed=self.bytes_seen,
                file_label=file_label,
            )
        return None

    def check_size_hint(self, size_bytes: int | None, file_label: str | None = None) -> dict[str, Any] | None:
        if size_bytes is None:
            return None
        if self.max_bytes_per_file is not None and size_bytes > self.max_bytes_per_file:
            return limit_violation(
                target_id=self.target_id,
                limit_type="bytes_per_file",
                limit=self.max_bytes_per_file,
                observed=size_bytes,
                file_label=file_label,
            )
        if self.max_bytes_per_target is not None and self.bytes_seen + size_bytes > self.max_bytes_per_target:
            return limit_violation(
                target_id=self.target_id,
                limit_type="bytes_per_target",
                limit=self.max_bytes_per_target,
                observed=self.bytes_seen + size_bytes,
                file_label=file_label,
            )
        return None

    def record_bytes(self, size_bytes: int | None, file_label: str | None = None) -> dict[str, Any] | None:
        if size_bytes is None:
            return None
        self.bytes_seen += size_bytes
        if self.max_bytes_per_file is not None and size_bytes > self.max_bytes_per_file:
            return limit_violation(
                target_id=self.target_id,
                limit_type="bytes_per_file",
                limit=self.max_bytes_per_file,
                observed=size_bytes,
                file_label=file_label,
            )
        if self.max_bytes_per_target is not None and self.bytes_seen > self.max_bytes_per_target:
            return limit_violation(
                target_id=self.target_id,
                limit_type="bytes_per_target",
                limit=self.max_bytes_per_target,
                observed=self.bytes_seen,
                file_label=file_label,
            )
        return None


def build_target_limit_enforcer(
    *,
    target_id: str,
    limit_files: int | None,
    max_bytes_per_target: int | None,
    download: dict[str, Any] | None,
) -> TargetLimitEnforcer:
    download_cfg = download or {}
    max_bytes_per_file = _as_int(download_cfg.get("max_bytes_per_file"))
    return TargetLimitEnforcer(
        target_id=target_id,
        limit_files=_as_int(limit_files),
        max_bytes_per_target=_as_int(max_bytes_per_target),
        max_bytes_per_file=max_bytes_per_file,
    )


def path_bytes(path: Path) -> int:
    if path.is_file():
        return path.stat().st_size
    total = 0
    for child in path.rglob("*"):
        if child.is_file():
            total += child.stat().st_size
    return total


def resolve_result_bytes(result: dict[str, Any], path: Path | None) -> int | None:
    for key in ("content_length", "bytes"):
        if key in result:
            try:
                return int(result[key])
            except (TypeError, ValueError):
                return None
    if path and path.exists():
        return path_bytes(path)
    return None


def cleanup_path(path: Path) -> None:
    if not path.exists():
        return
    if path.is_file():
        path.unlink(missing_ok=True)
    else:
        shutil.rmtree(path, ignore_errors=True)
