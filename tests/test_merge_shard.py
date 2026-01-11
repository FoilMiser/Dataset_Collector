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
