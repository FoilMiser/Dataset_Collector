from __future__ import annotations

from datasets import Dataset

from collector_core.merge.hf import is_hf_dataset_dir, iter_hf_dataset_dirs, iter_hf_inputs


def test_iter_hf_dataset_dirs_and_inputs(tmp_path) -> None:
    target_dir = tmp_path / "target"
    target_dir.mkdir()
    dataset_dir = target_dir / "hf_dataset"

    dataset = Dataset.from_dict({"text": ["hello"]})
    dataset.save_to_disk(str(dataset_dir))

    assert is_hf_dataset_dir(dataset_dir)

    hf_dirs = iter_hf_dataset_dirs(target_dir)
    assert dataset_dir in hf_dirs

    items = list(iter_hf_inputs(hf_dirs, target_id="t1", pool="permissive"))
    assert len(items) == 1
    assert items[0].raw["text"] == "hello"
    assert items[0].raw["split"] == "train"
