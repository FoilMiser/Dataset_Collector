"""Deterministic sharding module for EPIC 7.2.

This module provides deterministic shard assignment, stage resumption,
and atomic shard writing capabilities for the Dataset Collector pipeline.

Features:
- Deterministic shard assignment based on target_id hash (stable across runs)
- Stage resumption tracking via completion markers
- Atomic shard writing (write to .tmp then rename)
- Parallel worker coordination via file locking
"""

from __future__ import annotations

import dataclasses
import fcntl
import hashlib
import json
import logging
import os
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from collector_core.stability import stable_api
from collector_core.utils.paths import ensure_dir

logger = logging.getLogger("collector_core.sharding")

# Completion marker suffix for shard files
COMPLETION_MARKER_SUFFIX = ".complete"

# Lock file suffix for shard coordination
LOCK_FILE_SUFFIX = ".lock"

# Default lock acquisition timeout in seconds
DEFAULT_LOCK_TIMEOUT = 300.0

# Temporary file suffix for atomic writes
TMP_SUFFIX = ".tmp"


@stable_api
@dataclasses.dataclass(frozen=True)
class ShardConfig:
    """Configuration for deterministic sharding.

    Attributes:
        base_dir: Base directory for shard files.
        prefix: Prefix for shard file names.
        num_shards: Total number of shards to distribute data across.
        compression: Compression format ('none', 'gzip', 'zstd').
        extension: File extension without compression suffix (default: 'jsonl').
    """

    base_dir: Path
    prefix: str
    num_shards: int
    compression: str = "none"
    extension: str = "jsonl"

    def __post_init__(self) -> None:
        if self.num_shards < 1:
            raise ValueError("num_shards must be at least 1")


@stable_api
def stable_hash_int(value: str) -> int:
    """Compute a stable integer hash from a string.

    Uses SHA-256 for deterministic, stable hashing across Python versions
    and runs. Returns a positive integer derived from the first 8 bytes.

    Args:
        value: String value to hash.

    Returns:
        Positive integer hash value.
    """
    digest = hashlib.sha256(value.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], byteorder="big")


@stable_api
def compute_shard_index(target_id: str, num_shards: int) -> int:
    """Compute deterministic shard index for a target_id.

    Args:
        target_id: Unique identifier for the target.
        num_shards: Total number of shards.

    Returns:
        Shard index in range [0, num_shards).
    """
    if num_shards < 1:
        raise ValueError("num_shards must be at least 1")
    return stable_hash_int(target_id) % num_shards


@stable_api
def get_shard_filename(
    shard_index: int,
    prefix: str,
    extension: str = "jsonl",
    compression: str = "none",
) -> str:
    """Generate a stable shard filename.

    Args:
        shard_index: Zero-based shard index.
        prefix: Prefix for the filename.
        extension: Base file extension.
        compression: Compression type ('none', 'gzip', 'zstd').

    Returns:
        Shard filename with appropriate extension.
    """
    if compression == "gzip":
        suffix = f"{extension}.gz"
    elif compression in ("zstd", "zst"):
        suffix = f"{extension}.zst"
    else:
        suffix = extension
    return f"{prefix}_{shard_index:05d}.{suffix}"


@stable_api
def get_shard_path(target_id: str, config: ShardConfig) -> Path:
    """Get the deterministic shard path for a target_id.

    The shard path is computed using a stable hash of the target_id,
    ensuring the same target always maps to the same shard across runs.

    Args:
        target_id: Unique identifier for the target.
        config: Shard configuration.

    Returns:
        Path to the shard file for this target.
    """
    shard_index = compute_shard_index(target_id, config.num_shards)
    filename = get_shard_filename(
        shard_index=shard_index,
        prefix=config.prefix,
        extension=config.extension,
        compression=config.compression,
    )
    return config.base_dir / filename


@stable_api
def get_completion_marker_path(shard_path: Path) -> Path:
    """Get the completion marker path for a shard.

    Args:
        shard_path: Path to the shard file.

    Returns:
        Path to the completion marker file.
    """
    return shard_path.with_suffix(shard_path.suffix + COMPLETION_MARKER_SUFFIX)


@stable_api
def get_lock_file_path(shard_path: Path) -> Path:
    """Get the lock file path for a shard.

    Args:
        shard_path: Path to the shard file.

    Returns:
        Path to the lock file.
    """
    return shard_path.with_suffix(shard_path.suffix + LOCK_FILE_SUFFIX)


@stable_api
def get_tmp_path(shard_path: Path) -> Path:
    """Get the temporary file path for atomic writes.

    Args:
        shard_path: Path to the final shard file.

    Returns:
        Path to the temporary file.
    """
    return shard_path.with_suffix(shard_path.suffix + TMP_SUFFIX)


@stable_api
def is_shard_complete(shard_path: Path) -> bool:
    """Check if a shard has been marked as complete.

    A shard is considered complete if both the shard file and its
    completion marker exist.

    Args:
        shard_path: Path to the shard file.

    Returns:
        True if the shard is complete, False otherwise.
    """
    if not shard_path.exists():
        return False
    marker_path = get_completion_marker_path(shard_path)
    return marker_path.exists()


@stable_api
def mark_shard_complete(shard_path: Path, metadata: dict[str, Any] | None = None) -> None:
    """Mark a shard as complete by creating a completion marker.

    The completion marker contains metadata about when the shard was
    completed and optionally additional information.

    Args:
        shard_path: Path to the shard file.
        metadata: Optional metadata to store in the completion marker.

    Raises:
        FileNotFoundError: If the shard file does not exist.
    """
    if not shard_path.exists():
        raise FileNotFoundError(f"Cannot mark non-existent shard as complete: {shard_path}")

    marker_path = get_completion_marker_path(shard_path)
    marker_data = {
        "shard_path": str(shard_path),
        "completed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "shard_size_bytes": shard_path.stat().st_size,
    }
    if metadata:
        marker_data["metadata"] = metadata

    # Write marker atomically
    tmp_marker = marker_path.with_suffix(marker_path.suffix + TMP_SUFFIX)
    ensure_dir(marker_path.parent)
    tmp_marker.write_text(json.dumps(marker_data, indent=2) + "\n", encoding="utf-8")
    tmp_marker.replace(marker_path)
    logger.debug("Marked shard complete: %s", shard_path)


@stable_api
def remove_completion_marker(shard_path: Path) -> bool:
    """Remove the completion marker for a shard.

    Args:
        shard_path: Path to the shard file.

    Returns:
        True if marker was removed, False if it did not exist.
    """
    marker_path = get_completion_marker_path(shard_path)
    if marker_path.exists():
        marker_path.unlink()
        logger.debug("Removed completion marker: %s", marker_path)
        return True
    return False


@stable_api
@contextmanager
def shard_lock(
    shard_path: Path,
    timeout: float = DEFAULT_LOCK_TIMEOUT,
    blocking: bool = True,
) -> Iterator[bool]:
    """Context manager for acquiring an exclusive lock on a shard.

    Uses fcntl.flock for portable file locking on Unix systems.
    The lock is automatically released when the context exits.

    Args:
        shard_path: Path to the shard file to lock.
        timeout: Maximum time to wait for lock acquisition in seconds.
        blocking: If True, block until lock is acquired or timeout.
                  If False, return immediately with False if lock unavailable.

    Yields:
        True if lock was acquired, False if lock acquisition failed
        (only possible when blocking=False or timeout exceeded).

    Example:
        with shard_lock(shard_path) as acquired:
            if acquired:
                # Safe to write to shard
                write_shard(shard_path, data)
    """
    lock_path = get_lock_file_path(shard_path)
    ensure_dir(lock_path.parent)

    lock_fd = None
    acquired = False

    try:
        # Open or create the lock file
        lock_fd = os.open(str(lock_path), os.O_RDWR | os.O_CREAT, 0o644)

        if not blocking:
            # Non-blocking attempt
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                acquired = True
            except (BlockingIOError, OSError):
                acquired = False
        else:
            # Blocking with timeout using polling
            start_time = time.monotonic()
            poll_interval = 0.1

            while True:
                try:
                    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    acquired = True
                    break
                except (BlockingIOError, OSError):
                    elapsed = time.monotonic() - start_time
                    if elapsed >= timeout:
                        logger.warning(
                            "Lock acquisition timeout after %.1fs for %s",
                            timeout,
                            shard_path,
                        )
                        acquired = False
                        break
                    time.sleep(min(poll_interval, timeout - elapsed))
                    poll_interval = min(poll_interval * 1.5, 1.0)  # Exponential backoff

        if acquired:
            logger.debug("Acquired lock for shard: %s", shard_path)

        yield acquired

    finally:
        if lock_fd is not None:
            if acquired:
                try:
                    fcntl.flock(lock_fd, fcntl.LOCK_UN)
                    logger.debug("Released lock for shard: %s", shard_path)
                except OSError:
                    pass
            os.close(lock_fd)


@stable_api
class AtomicShardWriter:
    """Context manager for atomic shard writing.

    Writes to a temporary file first, then atomically renames to the
    final destination. This prevents partial/corrupted shards from
    interruptions.

    Example:
        with AtomicShardWriter(shard_path) as writer:
            for record in records:
                writer.write_line(json.dumps(record))
        # File is atomically moved to shard_path on successful exit
    """

    def __init__(
        self,
        shard_path: Path,
        compression: str = "none",
        auto_complete: bool = True,
    ) -> None:
        """Initialize the atomic shard writer.

        Args:
            shard_path: Final path for the shard file.
            compression: Compression type ('none', 'gzip', 'zstd').
            auto_complete: If True, mark shard complete after successful write.
        """
        self.shard_path = shard_path
        self.tmp_path = get_tmp_path(shard_path)
        self.compression = compression
        self.auto_complete = auto_complete
        self._file: Any = None
        self._wrapper: Any = None
        self._record_count = 0
        self._bytes_written = 0

    def __enter__(self) -> AtomicShardWriter:
        ensure_dir(self.shard_path.parent)

        # Clean up any stale temp file
        if self.tmp_path.exists():
            self.tmp_path.unlink()

        if self.compression == "gzip":
            import gzip

            self._file = gzip.open(self.tmp_path, "wt", encoding="utf-8")
        elif self.compression in ("zstd", "zst"):
            import io

            import zstandard as zstd

            self._file = self.tmp_path.open("wb")
            self._wrapper = zstd.ZstdCompressor().stream_writer(self._file)
        else:
            self._file = self.tmp_path.open("w", encoding="utf-8")

        return self

    def write_line(self, line: str) -> None:
        """Write a single line to the shard.

        Args:
            line: Line to write (newline will be added).
        """
        content = line + "\n"
        if self._wrapper is not None:
            # zstd compression
            encoded = content.encode("utf-8")
            self._wrapper.write(encoded)
            self._bytes_written += len(encoded)
        else:
            self._file.write(content)
            self._bytes_written += len(content.encode("utf-8"))
        self._record_count += 1

    def write_record(self, record: dict[str, Any]) -> None:
        """Write a JSON record to the shard.

        Args:
            record: Dictionary to serialize as JSON and write.
        """
        self.write_line(json.dumps(record, ensure_ascii=False))

    @property
    def record_count(self) -> int:
        """Number of records written so far."""
        return self._record_count

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        try:
            if self._wrapper is not None:
                self._wrapper.close()
            if self._file is not None:
                self._file.close()
        except Exception:
            pass

        if exc_type is None:
            # Success - atomically move temp to final
            self.tmp_path.replace(self.shard_path)
            logger.debug(
                "Atomically wrote shard: %s (%d records)",
                self.shard_path,
                self._record_count,
            )

            if self.auto_complete:
                mark_shard_complete(
                    self.shard_path,
                    metadata={
                        "record_count": self._record_count,
                        "compression": self.compression,
                    },
                )
        else:
            # Failure - clean up temp file
            if self.tmp_path.exists():
                self.tmp_path.unlink()
            logger.warning(
                "Shard write failed, cleaned up temp file: %s",
                self.tmp_path,
            )


@stable_api
@dataclasses.dataclass
class ShardState:
    """State of a single shard.

    Attributes:
        shard_index: Zero-based index of this shard.
        shard_path: Path to the shard file.
        is_complete: Whether the shard has been marked complete.
        target_ids: Set of target_ids assigned to this shard.
    """

    shard_index: int
    shard_path: Path
    is_complete: bool = False
    target_ids: set[str] = dataclasses.field(default_factory=set)


@stable_api
class StageResumption:
    """Tracks completed shards for resumable stage execution.

    This class enables deterministic resumption of interrupted pipeline
    stages by tracking which shards have been completed and which
    target_ids have been processed.

    Example:
        config = ShardConfig(base_dir=Path("output"), prefix="stage1", num_shards=10)
        resumption = StageResumption(config)

        for target_id in targets:
            if resumption.is_target_processed(target_id):
                continue  # Skip already processed targets

            shard_path = resumption.get_shard_for_target(target_id)
            with shard_lock(shard_path) as acquired:
                if acquired and not resumption.is_shard_complete(shard_path):
                    # Process and write shard
                    ...
    """

    def __init__(self, config: ShardConfig) -> None:
        """Initialize stage resumption tracker.

        Args:
            config: Shard configuration for this stage.
        """
        self.config = config
        self._shard_states: dict[int, ShardState] = {}
        self._processed_targets: set[str] = set()
        self._scan_existing_shards()

    def _scan_existing_shards(self) -> None:
        """Scan base directory for existing shards and their completion status."""
        if not self.config.base_dir.exists():
            return

        for shard_idx in range(self.config.num_shards):
            filename = get_shard_filename(
                shard_index=shard_idx,
                prefix=self.config.prefix,
                extension=self.config.extension,
                compression=self.config.compression,
            )
            shard_path = self.config.base_dir / filename
            is_complete = is_shard_complete(shard_path)

            self._shard_states[shard_idx] = ShardState(
                shard_index=shard_idx,
                shard_path=shard_path,
                is_complete=is_complete,
            )

            if is_complete:
                logger.debug("Found complete shard: %s", shard_path)

    def get_shard_for_target(self, target_id: str) -> Path:
        """Get the shard path for a target_id.

        Args:
            target_id: Unique identifier for the target.

        Returns:
            Path to the shard file for this target.
        """
        return get_shard_path(target_id, self.config)

    def get_shard_index_for_target(self, target_id: str) -> int:
        """Get the shard index for a target_id.

        Args:
            target_id: Unique identifier for the target.

        Returns:
            Shard index for this target.
        """
        return compute_shard_index(target_id, self.config.num_shards)

    def is_target_processed(self, target_id: str) -> bool:
        """Check if a target has been processed (its shard is complete).

        Args:
            target_id: Unique identifier for the target.

        Returns:
            True if the target's shard is complete.
        """
        shard_idx = self.get_shard_index_for_target(target_id)
        state = self._shard_states.get(shard_idx)
        return state is not None and state.is_complete

    def is_shard_complete_by_index(self, shard_index: int) -> bool:
        """Check if a shard is complete by its index.

        Args:
            shard_index: Zero-based shard index.

        Returns:
            True if the shard is complete.
        """
        state = self._shard_states.get(shard_index)
        return state is not None and state.is_complete

    def get_incomplete_shard_indices(self) -> list[int]:
        """Get list of incomplete shard indices.

        Returns:
            List of shard indices that are not yet complete.
        """
        incomplete = []
        for shard_idx in range(self.config.num_shards):
            if not self.is_shard_complete_by_index(shard_idx):
                incomplete.append(shard_idx)
        return incomplete

    def get_complete_shard_indices(self) -> list[int]:
        """Get list of complete shard indices.

        Returns:
            List of shard indices that are complete.
        """
        complete = []
        for shard_idx in range(self.config.num_shards):
            if self.is_shard_complete_by_index(shard_idx):
                complete.append(shard_idx)
        return complete

    def mark_target_processed(self, target_id: str) -> None:
        """Mark a target as processed.

        Note: This only tracks in-memory. Use mark_shard_complete()
        for persistent completion tracking.

        Args:
            target_id: Unique identifier for the target.
        """
        self._processed_targets.add(target_id)

    def mark_shard_complete_by_index(
        self,
        shard_index: int,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Mark a shard as complete by its index.

        Args:
            shard_index: Zero-based shard index.
            metadata: Optional metadata for the completion marker.
        """
        filename = get_shard_filename(
            shard_index=shard_index,
            prefix=self.config.prefix,
            extension=self.config.extension,
            compression=self.config.compression,
        )
        shard_path = self.config.base_dir / filename
        mark_shard_complete(shard_path, metadata)

        # Update internal state
        if shard_index in self._shard_states:
            self._shard_states[shard_index].is_complete = True
        else:
            self._shard_states[shard_index] = ShardState(
                shard_index=shard_index,
                shard_path=shard_path,
                is_complete=True,
            )

    def refresh_state(self) -> None:
        """Refresh shard states by re-scanning the filesystem.

        Useful when multiple workers are processing shards in parallel.
        """
        self._shard_states.clear()
        self._scan_existing_shards()

    def get_progress_summary(self) -> dict[str, Any]:
        """Get a summary of sharding progress.

        Returns:
            Dictionary with progress statistics.
        """
        complete = self.get_complete_shard_indices()
        incomplete = self.get_incomplete_shard_indices()

        return {
            "total_shards": self.config.num_shards,
            "complete_shards": len(complete),
            "incomplete_shards": len(incomplete),
            "progress_pct": 100.0 * len(complete) / self.config.num_shards
            if self.config.num_shards > 0
            else 0.0,
            "complete_indices": complete,
            "incomplete_indices": incomplete,
        }

    def group_targets_by_shard(
        self,
        target_ids: list[str],
    ) -> dict[int, list[str]]:
        """Group target_ids by their assigned shard index.

        Args:
            target_ids: List of target identifiers.

        Returns:
            Dictionary mapping shard indices to lists of target_ids.
        """
        groups: dict[int, list[str]] = {}
        for target_id in target_ids:
            shard_idx = self.get_shard_index_for_target(target_id)
            if shard_idx not in groups:
                groups[shard_idx] = []
            groups[shard_idx].append(target_id)
        return groups

    def filter_unprocessed_targets(self, target_ids: list[str]) -> list[str]:
        """Filter to only targets whose shards are incomplete.

        Args:
            target_ids: List of target identifiers.

        Returns:
            List of target_ids whose shards are not yet complete.
        """
        return [tid for tid in target_ids if not self.is_target_processed(tid)]


@stable_api
def atomic_write_shard(
    shard_path: Path,
    records: list[dict[str, Any]],
    compression: str = "none",
    auto_complete: bool = True,
) -> int:
    """Write records to a shard atomically.

    Convenience function for writing a complete shard in one operation.

    Args:
        shard_path: Path to the shard file.
        records: List of records to write.
        compression: Compression type ('none', 'gzip', 'zstd').
        auto_complete: If True, mark shard complete after write.

    Returns:
        Number of records written.
    """
    with AtomicShardWriter(shard_path, compression, auto_complete) as writer:
        for record in records:
            writer.write_record(record)
        return writer.record_count


@stable_api
def process_shard_with_lock(
    shard_path: Path,
    records: list[dict[str, Any]],
    compression: str = "none",
    timeout: float = DEFAULT_LOCK_TIMEOUT,
    skip_if_complete: bool = True,
) -> tuple[bool, int]:
    """Process and write a shard with exclusive locking.

    This function provides the full atomic+locked shard write pattern
    suitable for parallel worker scenarios.

    Args:
        shard_path: Path to the shard file.
        records: List of records to write.
        compression: Compression type ('none', 'gzip', 'zstd').
        timeout: Lock acquisition timeout in seconds.
        skip_if_complete: If True, skip writing if shard already complete.

    Returns:
        Tuple of (success, records_written).
        success is False if lock acquisition failed or shard was skipped.
    """
    if skip_if_complete and is_shard_complete(shard_path):
        logger.debug("Skipping already complete shard: %s", shard_path)
        return (False, 0)

    with shard_lock(shard_path, timeout=timeout) as acquired:
        if not acquired:
            logger.warning("Failed to acquire lock for shard: %s", shard_path)
            return (False, 0)

        # Re-check completion after acquiring lock (another worker may have finished)
        if skip_if_complete and is_shard_complete(shard_path):
            logger.debug("Shard completed by another worker: %s", shard_path)
            return (False, 0)

        count = atomic_write_shard(shard_path, records, compression, auto_complete=True)
        return (True, count)
