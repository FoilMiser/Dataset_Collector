"""
Tests for checkpoint and resume support module.

Issue 4.2 (v3.0): Tests for checkpoint/resume functionality.
"""

from __future__ import annotations

import json
import time
import pytest
from pathlib import Path
from datetime import datetime, timezone, timedelta
from collector_core.checkpoint import (
    CheckpointManager,
    CheckpointState,
    checkpoint_context,
)


class TestCheckpointManager:
    """Tests for CheckpointManager class."""

    def test_save_and_load(self, tmp_path: Path) -> None:
        """Checkpoint can be saved and loaded."""
        mgr = CheckpointManager(tmp_path)

        mgr.save("test-op", {"last_index": 100, "processed": 500})
        data = mgr.load("test-op")

        assert data["last_index"] == 100
        assert data["processed"] == 500

    def test_load_nonexistent(self, tmp_path: Path) -> None:
        """Loading nonexistent checkpoint returns empty dict."""
        mgr = CheckpointManager(tmp_path)

        data = mgr.load("nonexistent")

        assert data == {}

    def test_exists(self, tmp_path: Path) -> None:
        """Exists check works correctly."""
        mgr = CheckpointManager(tmp_path)

        assert mgr.exists("test-op") is False

        mgr.save("test-op", {"key": "value"})

        assert mgr.exists("test-op") is True

    def test_cleanup(self, tmp_path: Path) -> None:
        """Cleanup removes checkpoint."""
        mgr = CheckpointManager(tmp_path)
        mgr.save("test-op", {"key": "value"})

        assert mgr.exists("test-op") is True

        removed = mgr.cleanup("test-op")

        assert removed is True
        assert mgr.exists("test-op") is False

    def test_cleanup_nonexistent(self, tmp_path: Path) -> None:
        """Cleanup of nonexistent returns False."""
        mgr = CheckpointManager(tmp_path)

        removed = mgr.cleanup("nonexistent")

        assert removed is False

    def test_list_checkpoints(self, tmp_path: Path) -> None:
        """List all checkpoints."""
        mgr = CheckpointManager(tmp_path)
        mgr.save("op1", {"a": 1})
        mgr.save("op2", {"b": 2})
        mgr.save("op3", {"c": 3})

        checkpoints = mgr.list_checkpoints()

        assert len(checkpoints) == 3
        assert "op1" in checkpoints
        assert "op2" in checkpoints
        assert "op3" in checkpoints

    def test_atomic_write(self, tmp_path: Path) -> None:
        """Atomic write creates checkpoint correctly."""
        mgr = CheckpointManager(tmp_path)

        mgr.save("test-op", {"key": "value"}, atomic=True)

        # Verify no temp file remains
        temp_files = list(tmp_path.glob("*.tmp"))
        assert len(temp_files) == 0

        # Verify data is correct
        data = mgr.load("test-op")
        assert data["key"] == "value"

    def test_preserves_created_at(self, tmp_path: Path) -> None:
        """Updated saves preserve created_at timestamp."""
        mgr = CheckpointManager(tmp_path)

        mgr.save("test-op", {"v": 1})
        checkpoint_path = tmp_path / "test-op.checkpoint.json"
        first_data = json.loads(checkpoint_path.read_text())
        created_at = first_data["created_at"]

        # Save again
        mgr.save("test-op", {"v": 2})
        second_data = json.loads(checkpoint_path.read_text())

        assert second_data["created_at"] == created_at
        assert second_data["data"]["v"] == 2

    def test_stale_checkpoint_flag(self, tmp_path: Path) -> None:
        """Stale checkpoints are flagged."""
        mgr = CheckpointManager(tmp_path, max_age_hours=0.001)  # Very short

        mgr.save("test-op", {"key": "value"})
        time.sleep(0.01)  # Wait to exceed max_age

        data = mgr.load("test-op")

        assert data.get("_stale") is True
        assert "_age_hours" in data

    def test_safe_operation_id(self, tmp_path: Path) -> None:
        """Operation IDs with special chars are handled."""
        mgr = CheckpointManager(tmp_path)

        # IDs with slashes/backslashes
        mgr.save("path/to/op", {"key": "value"})

        # Should create file with sanitized name
        files = list(tmp_path.glob("*.checkpoint.json"))
        assert len(files) == 1
        assert "/" not in files[0].name

        # Can still load
        data = mgr.load("path/to/op")
        assert data["key"] == "value"


class TestCheckpointLocking:
    """Tests for checkpoint locking."""

    def test_acquire_lock(self, tmp_path: Path) -> None:
        """Lock can be acquired."""
        mgr = CheckpointManager(tmp_path)

        acquired = mgr.acquire_lock("test-op", timeout=1.0)

        assert acquired is True

        # Lock file exists
        lock_files = list(tmp_path.glob("*.lock"))
        assert len(lock_files) == 1

        mgr.release_lock("test-op")

    def test_release_lock(self, tmp_path: Path) -> None:
        """Lock can be released."""
        mgr = CheckpointManager(tmp_path)
        mgr.acquire_lock("test-op")

        mgr.release_lock("test-op")

        lock_files = list(tmp_path.glob("*.lock"))
        assert len(lock_files) == 0

    def test_lock_timeout(self, tmp_path: Path) -> None:
        """Lock times out when already held."""
        mgr = CheckpointManager(tmp_path)
        mgr.acquire_lock("test-op")

        # Try to acquire same lock
        acquired = mgr.acquire_lock("test-op", timeout=0.1)

        assert acquired is False

        mgr.release_lock("test-op")


class TestCheckpointCleanup:
    """Tests for checkpoint cleanup."""

    def test_cleanup_stale(self, tmp_path: Path) -> None:
        """Stale checkpoints are cleaned up."""
        mgr = CheckpointManager(tmp_path, max_age_hours=0.001)

        mgr.save("op1", {"a": 1})
        mgr.save("op2", {"b": 2})
        time.sleep(0.01)

        removed = mgr.cleanup_stale()

        assert removed == 2
        assert len(mgr.list_checkpoints()) == 0


class TestCheckpointContext:
    """Tests for checkpoint_context context manager."""

    def test_context_success(self, tmp_path: Path) -> None:
        """Successful completion cleans up checkpoint."""
        mgr = CheckpointManager(tmp_path)

        with checkpoint_context(mgr, "test-op") as ctx:
            ctx.mark_processed("item1")
            ctx.mark_processed("item2")

        # Checkpoint should be cleaned up
        assert mgr.exists("test-op") is False

    def test_context_failure(self, tmp_path: Path) -> None:
        """Failed context saves checkpoint."""
        mgr = CheckpointManager(tmp_path)

        with pytest.raises(ValueError):
            with checkpoint_context(mgr, "test-op") as ctx:
                ctx.mark_processed("item1")
                raise ValueError("Test error")

        # Checkpoint should exist
        assert mgr.exists("test-op") is True
        data = mgr.load("test-op")
        assert "item1" in data.get("processed_ids", [])

    def test_context_skip_processed(self, tmp_path: Path) -> None:
        """Previously processed items are skipped."""
        mgr = CheckpointManager(tmp_path)

        # Simulate previous run
        mgr.save("test-op", {"processed_ids": ["item1", "item2"]})

        with checkpoint_context(mgr, "test-op") as ctx:
            assert ctx.should_skip("item1") is True
            assert ctx.should_skip("item2") is True
            assert ctx.should_skip("item3") is False

    def test_context_incremental(self, tmp_path: Path) -> None:
        """Context tracks incremental progress."""
        mgr = CheckpointManager(tmp_path)

        with checkpoint_context(mgr, "test-op") as ctx:
            for i in range(10):
                ctx.mark_processed(f"item{i}")

        # After success, checkpoint is cleaned
        assert mgr.exists("test-op") is False


class TestCheckpointState:
    """Tests for CheckpointState dataclass."""

    def test_state_creation(self) -> None:
        """State can be created."""
        state = CheckpointState(
            operation_id="test-op",
            created_at="2024-01-01T00:00:00Z",
            updated_at="2024-01-01T01:00:00Z",
            data={"key": "value"},
        )

        assert state.operation_id == "test-op"
        assert state.data == {"key": "value"}
        assert state.version == 1

    def test_state_defaults(self) -> None:
        """State has sensible defaults."""
        state = CheckpointState(operation_id="test")

        assert state.created_at == ""
        assert state.updated_at == ""
        assert state.data == {}
        assert state.version == 1


class TestEdgeCases:
    """Edge case tests."""

    def test_corrupted_checkpoint(self, tmp_path: Path) -> None:
        """Corrupted checkpoint returns empty dict."""
        mgr = CheckpointManager(tmp_path)

        # Write invalid JSON
        checkpoint_file = tmp_path / "test-op.checkpoint.json"
        checkpoint_file.write_text("not valid json{{{")

        data = mgr.load("test-op")

        assert data == {}

    def test_missing_data_field(self, tmp_path: Path) -> None:
        """Missing data field returns empty dict."""
        mgr = CheckpointManager(tmp_path)

        # Write valid JSON but missing data field
        checkpoint_file = tmp_path / "test-op.checkpoint.json"
        checkpoint_file.write_text('{"version": 1, "operation_id": "test"}')

        data = mgr.load("test-op")

        assert data == {}

    def test_concurrent_saves(self, tmp_path: Path) -> None:
        """Multiple saves don't corrupt data."""
        mgr = CheckpointManager(tmp_path)

        for i in range(100):
            mgr.save("test-op", {"iteration": i})

        data = mgr.load("test-op")
        assert data["iteration"] == 99
