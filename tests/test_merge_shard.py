from __future__ import annotations

from collector_core.merge.shard import Sharder, sharding_cfg
from collector_core.merge.types import ShardingConfig
from collector_core.utils import read_jsonl


def test_sharder_add_and_flush(tmp_path) -> None:
    cfg = ShardingConfig(max_records_per_shard=2, compression="gzip", prefix="combined")
    sharder = Sharder(tmp_path, cfg)

    path, flushed = sharder.add({"text": "a"})
    assert path is None
    assert flushed == []

    path, flushed = sharder.add({"text": "b"})
    assert path is not None
    assert path.exists()
    assert len(flushed) == 2

    records = list(read_jsonl(path))
    assert [row["text"] for row in records] == ["a", "b"]


def test_sharding_cfg_defaults() -> None:
    cfg = {"globals": {"sharding": {"max_records_per_shard": 10, "compression": "none"}}}
    resolved = sharding_cfg(cfg)
    assert resolved.max_records_per_shard == 10
    assert resolved.compression == "none"
    assert resolved.prefix == "combined"


def test_sharder_flush_empty_returns_none(tmp_path) -> None:
    """Test that flushing an empty sharder returns None."""
    cfg = ShardingConfig(max_records_per_shard=10, compression="gzip", prefix="test")
    sharder = Sharder(tmp_path, cfg)

    path, flushed = sharder.flush()

    assert path is None
    assert flushed == []


def test_sharder_flush_partial_shard(tmp_path) -> None:
    """Test flushing a partial shard that hasn't reached max records."""
    cfg = ShardingConfig(max_records_per_shard=10, compression="gzip", prefix="test")
    sharder = Sharder(tmp_path, cfg)

    # Add fewer records than max
    sharder.add({"text": "record1"})
    sharder.add({"text": "record2"})

    # Manually flush partial shard
    path, flushed = sharder.flush()

    assert path is not None
    assert path.exists()
    assert len(flushed) == 2


def test_sharder_no_compression(tmp_path) -> None:
    """Test sharder with no compression."""
    cfg = ShardingConfig(max_records_per_shard=2, compression="none", prefix="plain")
    sharder = Sharder(tmp_path, cfg)

    sharder.add({"text": "a"})
    path, flushed = sharder.add({"text": "b"})

    assert path is not None
    assert path.suffix == ".jsonl"
    assert path.exists()


def test_sharder_increments_shard_index(tmp_path) -> None:
    """Test that shard index increments after each flush."""
    cfg = ShardingConfig(max_records_per_shard=1, compression="gzip", prefix="test")
    sharder = Sharder(tmp_path, cfg)

    # First shard
    path1, _ = sharder.add({"text": "first"})
    assert "00000" in path1.name

    # Second shard
    path2, _ = sharder.add({"text": "second"})
    assert "00001" in path2.name


def test_sharding_cfg_empty_config() -> None:
    """Test sharding_cfg with empty config returns defaults."""
    cfg = {}
    resolved = sharding_cfg(cfg)
    assert resolved.max_records_per_shard == 50000
    assert resolved.compression == "gzip"


def test_sharding_cfg_missing_sharding_section() -> None:
    """Test sharding_cfg with globals but no sharding section."""
    cfg = {"globals": {"other": "value"}}
    resolved = sharding_cfg(cfg)
    assert resolved.max_records_per_shard == 50000
    assert resolved.compression == "gzip"
