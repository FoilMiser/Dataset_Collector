"""Tests for checkpoint module roundtrip behavior.

These tests verify that checkpoints can be saved and loaded correctly,
ensuring resume functionality works as expected.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from collector_core.checkpoint import (
    CheckpointState,
    checkpoint_path,
    cleanup_checkpoint,
    init_checkpoint,
    load_checkpoint,
    save_checkpoint,
)


class TestCheckpointRoundtrip:
    """Tests for checkpoint save/load roundtrip."""

    def test_init_checkpoint_creates_file(self, tmp_path: Path) -> None:
        """init_checkpoint should create a checkpoint file."""
        path = tmp_path / "checkpoint.json"
        state = init_checkpoint(path, pipeline_id="test_pipeline", run_id="run_001")

        assert path.exists()
        assert state.pipeline_id == "test_pipeline"
        assert state.run_id == "run_001"
        assert state.completed_targets == []
        assert state.counts == {}

    def test_save_and_load_checkpoint_preserves_data(self, tmp_path: Path) -> None:
        """save_checkpoint and load_checkpoint should preserve all data."""
        path = tmp_path / "checkpoint.json"

        # Create and save a checkpoint
        state = CheckpointState(
            run_id="run_123",
            pipeline_id="physics_pipeline",
            created_at_utc="2024-01-01T00:00:00Z",
            updated_at_utc="2024-01-01T01:00:00Z",
            completed_targets=["target_a", "target_b"],
            counts={"processed": 100, "skipped": 5},
        )
        save_checkpoint(path, state)

        # Load and verify
        loaded = load_checkpoint(path)
        assert loaded is not None
        assert loaded.run_id == "run_123"
        assert loaded.pipeline_id == "physics_pipeline"
        assert loaded.created_at_utc == "2024-01-01T00:00:00Z"
        assert loaded.updated_at_utc == "2024-01-01T01:00:00Z"
        assert loaded.completed_targets == ["target_a", "target_b"]
        assert loaded.counts == {"processed": 100, "skipped": 5}

    def test_load_checkpoint_returns_none_for_missing_file(self, tmp_path: Path) -> None:
        """load_checkpoint should return None for non-existent file."""
        path = tmp_path / "nonexistent_checkpoint.json"
        assert load_checkpoint(path) is None

    def test_record_target_adds_to_completed_targets(self, tmp_path: Path) -> None:
        """record_target should add target to completed list."""
        path = tmp_path / "checkpoint.json"
        state = init_checkpoint(path, pipeline_id="test", run_id="run_001")

        state.record_target("target_1")
        state.record_target("target_2")
        state.record_target("target_1")  # Duplicate, should not add

        assert state.completed_targets == ["target_1", "target_2"]

    def test_record_target_increments_bucket_counts(self, tmp_path: Path) -> None:
        """record_target should increment bucket counts."""
        path = tmp_path / "checkpoint.json"
        state = init_checkpoint(path, pipeline_id="test", run_id="run_001")

        state.record_target("t1", bucket="success")
        state.record_target("t2", bucket="success")
        state.record_target("t3", bucket="failed")

        assert state.counts == {"success": 2, "failed": 1}

    def test_checkpoint_path_generates_safe_path(self) -> None:
        """checkpoint_path should handle special characters in pipeline_id."""
        checkpoint_dir = Path("/tmp/checkpoints")

        # Normal pipeline ID
        path = checkpoint_path(checkpoint_dir, "physics_pipeline_v2")
        assert "physics_pipeline_v2" in str(path)
        assert "pipeline_checkpoint.json" in str(path)

        # Pipeline ID with slashes (should be sanitized)
        path = checkpoint_path(checkpoint_dir, "org/repo/pipeline")
        assert "org_repo_pipeline" in str(path)

    def test_cleanup_checkpoint_removes_file(self, tmp_path: Path) -> None:
        """cleanup_checkpoint should remove the checkpoint file."""
        path = tmp_path / "checkpoint.json"
        init_checkpoint(path, pipeline_id="test", run_id="run_001")
        assert path.exists()

        cleanup_checkpoint(path)
        assert not path.exists()

    def test_cleanup_checkpoint_handles_missing_file(self, tmp_path: Path) -> None:
        """cleanup_checkpoint should not error on missing file."""
        path = tmp_path / "nonexistent_checkpoint.json"
        cleanup_checkpoint(path)  # Should not raise


class TestCheckpointResume:
    """Tests for resume functionality using checkpoints."""

    def test_resume_skips_completed_targets(self, tmp_path: Path) -> None:
        """Resume should skip targets already in completed_targets."""
        path = tmp_path / "checkpoint.json"

        # Simulate first run
        state = init_checkpoint(path, pipeline_id="test", run_id="run_001")
        state.record_target("target_a")
        state.record_target("target_b")
        save_checkpoint(path, state)

        # Simulate resume
        resumed_state = load_checkpoint(path)
        assert resumed_state is not None

        all_targets = ["target_a", "target_b", "target_c", "target_d"]
        pending_targets = [t for t in all_targets if t not in resumed_state.completed_targets]

        assert pending_targets == ["target_c", "target_d"]

    def test_idempotent_processing_with_checkpoint(self, tmp_path: Path) -> None:
        """Processing should be idempotent with proper checkpoint usage."""
        path = tmp_path / "checkpoint.json"

        # First run
        state = init_checkpoint(path, pipeline_id="test", run_id="run_001")
        targets = ["t1", "t2", "t3"]
        processed = []

        for t in targets:
            if t not in state.completed_targets:
                processed.append(t)
                state.record_target(t)
        save_checkpoint(path, state)

        assert processed == ["t1", "t2", "t3"]
        assert state.completed_targets == ["t1", "t2", "t3"]

        # Second run (resume) - should process nothing
        state2 = load_checkpoint(path)
        assert state2 is not None
        processed2 = []

        for t in targets:
            if t not in state2.completed_targets:
                processed2.append(t)
                state2.record_target(t)

        assert processed2 == []
        assert state2.completed_targets == ["t1", "t2", "t3"]

    def test_partial_run_can_resume(self, tmp_path: Path) -> None:
        """Partial run should be resumable from checkpoint."""
        path = tmp_path / "checkpoint.json"

        # First run - partial (simulating interruption after target_b)
        state = init_checkpoint(path, pipeline_id="test", run_id="run_001")
        state.record_target("target_a")
        state.record_target("target_b")
        save_checkpoint(path, state)

        # Resume run
        state2 = load_checkpoint(path)
        assert state2 is not None
        assert state2.completed_targets == ["target_a", "target_b"]

        # Continue processing
        state2.record_target("target_c")
        state2.record_target("target_d")
        save_checkpoint(path, state2)

        # Final verification
        final_state = load_checkpoint(path)
        assert final_state is not None
        assert final_state.completed_targets == ["target_a", "target_b", "target_c", "target_d"]
