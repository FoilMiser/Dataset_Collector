from __future__ import annotations
from pathlib import Path

import dataclasses
import threading
import shutil
from typing import Any

# Default allowed content types for downloads
DEFAULT_ALLOWED_CONTENT_TYPES: frozenset[str] = frozenset(
    {
        # Text formats
        "text/plain",
        "text/csv",
        "text/html",
        "text/xml",
        "application/json",
        "application/xml",
        "application/x-yaml",
        # Archive formats
        "application/zip",
        "application/gzip",
        "application/x-gzip",
        "application/x-tar",
        "application/x-bzip2",
        "application/x-xz",
        "application/x-7z-compressed",
        # Data formats
        "application/pdf",
        "application/octet-stream",  # Generic binary
        # Dataset formats
        "application/x-hdf5",
        "application/x-netcdf",
        "application/parquet",
        # Images (for datasets with image data)
        "image/png",
        "image/jpeg",
        "image/gif",
        "image/webp",
        "image/tiff",
    }
)

# Blocked content types that should never be downloaded
BLOCKED_CONTENT_TYPES: frozenset[str] = frozenset(
    {
        "text/html",  # Often indicates error pages when expecting data
        "application/javascript",
        "application/x-javascript",
        "text/javascript",
    }
)

# Default download size limits (in bytes)
DEFAULT_MAX_BYTES_PER_FILE: int = 10 * 1024 * 1024 * 1024  # 10 GB
DEFAULT_MAX_BYTES_PER_TARGET: int = 100 * 1024 * 1024 * 1024  # 100 GB
DEFAULT_MAX_FILES_PER_TARGET: int = 10000


def validate_content_type(
    content_type: str | None,
    allowed: frozenset[str] | None = None,
    blocked: frozenset[str] | None = None,
) -> tuple[bool, str | None]:
    """
    Validate that a content type is acceptable.

    Args:
        content_type: The Content-Type header value
        allowed: Set of allowed content types (None = allow all except blocked)
        blocked: Set of blocked content types

    Returns:
        (is_valid, reason) tuple
    """
    if content_type is None:
        return True, None  # No content type is acceptable

    # Normalize: strip parameters (e.g., "text/html; charset=utf-8" -> "text/html")
    normalized = content_type.split(";")[0].strip().lower()

    # Check blocked list
    if blocked and normalized in blocked:
        return False, f"blocked_content_type:{normalized}"

    # If allowed list is specified, check it
    if allowed and normalized not in allowed:
        return False, f"unexpected_content_type:{normalized}"

    return True, None


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
    run_budget: "RunByteBudget | None" = None
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
        if self.run_budget:
            limit_error = self.run_budget.check_remaining(
                target_id=self.target_id, file_label=file_label
            )
            if limit_error:
                return limit_error
        if self.max_bytes_per_target is not None and self.bytes_seen >= self.max_bytes_per_target:
            return limit_violation(
                target_id=self.target_id,
                limit_type="bytes_per_target",
                limit=self.max_bytes_per_target,
                observed=self.bytes_seen,
                file_label=file_label,
            )
        return None

    def check_size_hint(
        self, size_bytes: int | None, file_label: str | None = None
    ) -> dict[str, Any] | None:
        if size_bytes is None:
            return None
        if self.run_budget:
            limit_error = self.run_budget.check_size_hint(
                target_id=self.target_id, size_bytes=size_bytes, file_label=file_label
            )
            if limit_error:
                return limit_error
        if self.max_bytes_per_file is not None and size_bytes > self.max_bytes_per_file:
            return limit_violation(
                target_id=self.target_id,
                limit_type="bytes_per_file",
                limit=self.max_bytes_per_file,
                observed=size_bytes,
                file_label=file_label,
            )
        if (
            self.max_bytes_per_target is not None
            and self.bytes_seen + size_bytes > self.max_bytes_per_target
        ):
            return limit_violation(
                target_id=self.target_id,
                limit_type="bytes_per_target",
                limit=self.max_bytes_per_target,
                observed=self.bytes_seen + size_bytes,
                file_label=file_label,
            )
        return None

    def record_bytes(
        self, size_bytes: int | None, file_label: str | None = None
    ) -> dict[str, Any] | None:
        if size_bytes is None:
            return None
        self.bytes_seen += size_bytes
        run_budget_error = None
        if self.run_budget:
            run_budget_error = self.run_budget.record_bytes(
                target_id=self.target_id, size_bytes=size_bytes, file_label=file_label
            )
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
        if run_budget_error:
            return run_budget_error
        return None


def build_target_limit_enforcer(
    *,
    target_id: str,
    limit_files: int | None,
    max_bytes_per_target: int | None,
    download: dict[str, Any] | None,
    run_budget: "RunByteBudget | None" = None,
) -> TargetLimitEnforcer:
    download_cfg = download or {}
    max_bytes_per_file = _as_int(download_cfg.get("max_bytes_per_file"))
    download_max_bytes = _as_int(download_cfg.get("max_bytes"))
    effective_max_bytes = _as_int(max_bytes_per_target)
    if download_max_bytes is not None:
        if effective_max_bytes is None:
            effective_max_bytes = download_max_bytes
        else:
            effective_max_bytes = min(effective_max_bytes, download_max_bytes)
    return TargetLimitEnforcer(
        target_id=target_id,
        limit_files=_as_int(limit_files),
        max_bytes_per_target=effective_max_bytes,
        max_bytes_per_file=max_bytes_per_file,
        run_budget=run_budget,
    )


@dataclasses.dataclass
class RunByteBudget:
    limit: int
    bytes_seen: int = 0
    lock: threading.Lock = dataclasses.field(default_factory=threading.Lock)

    def exhausted(self) -> bool:
        with self.lock:
            return self.bytes_seen >= self.limit

    def check_remaining(
        self, *, target_id: str, file_label: str | None = None
    ) -> dict[str, Any] | None:
        with self.lock:
            if self.bytes_seen >= self.limit:
                return limit_violation(
                    target_id=target_id,
                    limit_type="run_byte_budget",
                    limit=self.limit,
                    observed=self.bytes_seen,
                    file_label=file_label,
                )
        return None

    def check_size_hint(
        self, *, target_id: str, size_bytes: int, file_label: str | None = None
    ) -> dict[str, Any] | None:
        with self.lock:
            projected = self.bytes_seen + size_bytes
            if projected > self.limit:
                return limit_violation(
                    target_id=target_id,
                    limit_type="run_byte_budget",
                    limit=self.limit,
                    observed=projected,
                    file_label=file_label,
                )
        return None

    def record_bytes(
        self, *, target_id: str, size_bytes: int, file_label: str | None = None
    ) -> dict[str, Any] | None:
        with self.lock:
            self.bytes_seen += size_bytes
            if self.bytes_seen > self.limit:
                return limit_violation(
                    target_id=target_id,
                    limit_type="run_byte_budget",
                    limit=self.limit,
                    observed=self.bytes_seen,
                    file_label=file_label,
                )
        return None


def build_run_budget(limit: Any) -> RunByteBudget | None:
    budget = _as_int(limit)
    if budget is None:
        return None
    return RunByteBudget(limit=budget)


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
