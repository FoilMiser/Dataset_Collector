"""Checkpoint and resume support for long-running pipeline operations.

This module provides checkpoint saving during long operations and the ability
to resume from a checkpoint on restart.

Example:
    checkpoint_mgr = CheckpointManager(Path("/data/_checkpoints"))
    
    # Start or resume operation
    state = checkpoint_mgr.load("my-pipeline-acquire")
    
    for i, item in enumerate(items):
        if i < state.get("last_processed_index", 0):
            continue  # Skip already processed
        
        process(item)
        
        # Save checkpoint periodically
        if i % 100 == 0:
            checkpoint_mgr.save("my-pipeline-acquire", {
                "last_processed_index": i,
                "processed_count": state.get("processed_count", 0) + 1,
            })
    
    # Clean up on completion
    checkpoint_mgr.cleanup("my-pipeline-acquire")
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


@dataclass
class CheckpointState:
    """Represents checkpoint state for a pipeline operation.
    
    Attributes:
        operation_id: Unique identifier for the operation
        created_at: When checkpoint was created
        updated_at: Last update time
        data: Custom state data
        version: Checkpoint format version
    """
    operation_id: str
    created_at: str = ""
    updated_at: str = ""
    data: dict[str, Any] = field(default_factory=dict)
    version: int = 1


class CheckpointManager:
    """Manages checkpoints for pipeline operations.
    
    Features:
    - Atomic checkpoint writes (write to temp, then rename)
    - Automatic checkpoint aging and cleanup
    - Lock file support for concurrent access prevention
    
    Args:
        checkpoint_dir: Directory for storing checkpoints
        max_age_hours: Maximum age before checkpoint is considered stale
    """
    
    def __init__(self, checkpoint_dir: Path, max_age_hours: float = 24.0):
        self.checkpoint_dir = checkpoint_dir
        self.max_age_hours = max_age_hours
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
    
    def _checkpoint_path(self, operation_id: str) -> Path:
        """Get path for checkpoint file."""
        safe_id = operation_id.replace("/", "_").replace("\\", "_")
        return self.checkpoint_dir / f"{safe_id}.checkpoint.json"
    
    def _lock_path(self, operation_id: str) -> Path:
        """Get path for lock file."""
        safe_id = operation_id.replace("/", "_").replace("\\", "_")
        return self.checkpoint_dir / f"{safe_id}.lock"
    
    def exists(self, operation_id: str) -> bool:
        """Check if checkpoint exists for operation."""
        return self._checkpoint_path(operation_id).exists()
    
    def load(self, operation_id: str) -> dict[str, Any]:
        """Load checkpoint state for operation.
        
        Args:
            operation_id: Operation identifier
            
        Returns:
            Checkpoint data dict, empty if no checkpoint exists
        """
        path = self._checkpoint_path(operation_id)
        
        if not path.exists():
            return {}
        
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            
            # Check age
            updated = data.get("updated_at", "")
            if updated:
                updated_dt = datetime.fromisoformat(updated.replace("Z", "+00:00"))
                age_hours = (datetime.now(timezone.utc) - updated_dt).total_seconds() / 3600
                
                if age_hours > self.max_age_hours:
                    # Checkpoint is stale
                    return {"_stale": True, "_age_hours": age_hours, **data.get("data", {})}
            
            return data.get("data", {})
            
        except (json.JSONDecodeError, KeyError, ValueError):
            return {}
    
    def save(
        self,
        operation_id: str,
        data: dict[str, Any],
        *,
        atomic: bool = True,
    ) -> None:
        """Save checkpoint state.
        
        Args:
            operation_id: Operation identifier
            data: State data to save
            atomic: Use atomic write (temp file + rename)
        """
        path = self._checkpoint_path(operation_id)
        now = datetime.now(timezone.utc).isoformat()
        
        # Load existing to preserve created_at
        existing = {}
        if path.exists():
            try:
                existing = json.loads(path.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, ValueError):
                pass
        
        checkpoint = {
            "version": 1,
            "operation_id": operation_id,
            "created_at": existing.get("created_at", now),
            "updated_at": now,
            "data": data,
        }
        
        content = json.dumps(checkpoint, indent=2)
        
        if atomic:
            # Write to temp file, then rename
            temp_path = path.with_suffix(".tmp")
            temp_path.write_text(content, encoding="utf-8")
            temp_path.rename(path)
        else:
            path.write_text(content, encoding="utf-8")
    
    def cleanup(self, operation_id: str) -> bool:
        """Remove checkpoint after successful completion.
        
        Args:
            operation_id: Operation identifier
            
        Returns:
            True if checkpoint was removed
        """
        path = self._checkpoint_path(operation_id)
        lock_path = self._lock_path(operation_id)
        
        removed = False
        
        if path.exists():
            path.unlink()
            removed = True
        
        if lock_path.exists():
            lock_path.unlink()
        
        return removed
    
    def acquire_lock(self, operation_id: str, timeout: float = 10.0) -> bool:
        """Acquire lock for operation.
        
        Args:
            operation_id: Operation identifier
            timeout: Maximum time to wait for lock
            
        Returns:
            True if lock acquired
        """
        lock_path = self._lock_path(operation_id)
        start = time.time()
        
        while time.time() - start < timeout:
            try:
                # Try to create lock file exclusively
                fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.write(fd, f"{os.getpid()}".encode())
                os.close(fd)
                return True
            except FileExistsError:
                # Lock exists, check if stale
                try:
                    mtime = lock_path.stat().st_mtime
                    if time.time() - mtime > 3600:  # 1 hour stale
                        lock_path.unlink()
                        continue
                except FileNotFoundError:
                    continue
                
                time.sleep(0.1)
        
        return False
    
    def release_lock(self, operation_id: str) -> None:
        """Release lock for operation."""
        lock_path = self._lock_path(operation_id)
        if lock_path.exists():
            lock_path.unlink()
    
    def list_checkpoints(self) -> list[str]:
        """List all checkpoint operation IDs."""
        checkpoints = []
        for path in self.checkpoint_dir.glob("*.checkpoint.json"):
            op_id = path.stem.replace(".checkpoint", "")
            checkpoints.append(op_id)
        return checkpoints
    
    def cleanup_stale(self) -> int:
        """Remove all stale checkpoints.
        
        Returns:
            Number of checkpoints removed
        """
        removed = 0
        now = datetime.now(timezone.utc)
        
        for op_id in self.list_checkpoints():
            path = self._checkpoint_path(op_id)
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                updated = data.get("updated_at", "")
                if updated:
                    updated_dt = datetime.fromisoformat(updated.replace("Z", "+00:00"))
                    age_hours = (now - updated_dt).total_seconds() / 3600
                    
                    if age_hours > self.max_age_hours:
                        self.cleanup(op_id)
                        removed += 1
            except (json.JSONDecodeError, ValueError, FileNotFoundError):
                pass
        
        return removed


def checkpoint_context(
    checkpoint_mgr: CheckpointManager,
    operation_id: str,
) -> "_CheckpointContext":
    """Context manager for checkpoint-protected operations.
    
    Example:
        with checkpoint_context(mgr, "my-op") as ctx:
            for item in items:
                if ctx.should_skip(item.id):
                    continue
                process(item)
                ctx.mark_processed(item.id)
    """
    return _CheckpointContext(checkpoint_mgr, operation_id)


class _CheckpointContext:
    """Context manager for checkpoint-protected operations."""
    
    def __init__(self, mgr: CheckpointManager, operation_id: str):
        self._mgr = mgr
        self._operation_id = operation_id
        self._state: dict[str, Any] = {}
        self._processed: set[str] = set()
        self._save_interval = 100
        self._op_count = 0
    
    def __enter__(self) -> "_CheckpointContext":
        self._state = self._mgr.load(self._operation_id)
        self._processed = set(self._state.get("processed_ids", []))
        return self
    
    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if exc_val is None:
            # Success - cleanup checkpoint
            self._mgr.cleanup(self._operation_id)
        else:
            # Error - save final state
            self._save()
    
    def should_skip(self, item_id: str) -> bool:
        """Check if item was already processed."""
        return item_id in self._processed
    
    def mark_processed(self, item_id: str) -> None:
        """Mark item as processed."""
        self._processed.add(item_id)
        self._op_count += 1
        
        if self._op_count % self._save_interval == 0:
            self._save()
    
    def _save(self) -> None:
        """Save current state."""
        self._state["processed_ids"] = list(self._processed)
        self._state["processed_count"] = len(self._processed)
        self._mgr.save(self._operation_id, self._state)


__all__ = [
    "CheckpointManager",
    "CheckpointState",
    "checkpoint_context",
]
