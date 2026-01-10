from __future__ import annotations

from collector_core import merge


def test_build_target_meta_and_canon() -> None:
    cfg = {
        "globals": {"canonicalize": {"text_field_candidates": ["title"]}},
        "targets": [
            {"id": "a", "download": {"dataset_id": "ds1", "config": "cfg1"}},
            {"id": "b", "dataset_id": "ds2"},
        ],
    }
    meta = merge.build_target_meta(cfg)
    assert meta["a"]["dataset_id"] == "ds1"
    assert meta["a"]["config"] == "cfg1"
    assert meta["b"]["dataset_id"] == "ds2"

    target_canon, default_canon = merge.build_target_canon(cfg)
    assert target_canon["a"][0] == ["title"]
    assert default_canon[0] == ["title"]


def test_get_sharder_reuses_instance(tmp_path) -> None:
    roots = merge.Roots(
        raw_root=tmp_path / "raw",
        screened_root=tmp_path / "screened",
        combined_root=tmp_path / "combined",
        ledger_root=tmp_path / "ledger",
    )
    state = merge.MergeState(
        summary={"written": 0, "deduped": 0, "skipped": 0, "shards": []},
        dedupe=merge.DedupeIndex(roots.ledger_root / "dedupe.sqlite"),
        shard_cfg=merge.ShardingConfig(
            max_records_per_shard=10, compression="gzip", prefix="combined"
        ),
        pool_sharders={},
        target_meta={},
        pipeline_id="test",
        execute=False,
        progress=False,
        progress_interval=10000,
    )
    sharder_a = merge.get_sharder("permissive", roots, state)
    sharder_b = merge.get_sharder("permissive", roots, state)
    assert sharder_a is sharder_b
    assert sharder_a.base_dir.exists()
    state.dedupe.close()


def test_handle_record_writes_index_and_shard(tmp_path) -> None:
    roots = merge.Roots(
        raw_root=tmp_path / "raw",
        screened_root=tmp_path / "screened",
        combined_root=tmp_path / "combined",
        ledger_root=tmp_path / "ledger",
    )
    state = merge.MergeState(
        summary={"written": 0, "deduped": 0, "skipped": 0, "shards": []},
        dedupe=merge.DedupeIndex(roots.ledger_root / "dedupe.sqlite"),
        shard_cfg=merge.ShardingConfig(
            max_records_per_shard=10, compression="gzip", prefix="combined"
        ),
        pool_sharders={},
        target_meta={},
        pipeline_id="test",
        execute=True,
        progress=False,
        progress_interval=10000,
    )
    record = {"text": "hello", "source": {"target_id": "t1", "license_profile": "permissive"}}
    merge.handle_record(record, "green", None, roots, state, target_id="t1", pool_hint="permissive")
    merge.finalize_shards(state)
    state.dedupe.close()

    assert state.summary["written"] == 1
    shard_paths = list((roots.combined_root / "permissive" / "shards").glob("*.jsonl*"))
    assert shard_paths
    index_rows = list(merge.read_jsonl(roots.ledger_root / "combined_index.jsonl"))
    assert index_rows and index_rows[0]["license_pool"] == "permissive"
