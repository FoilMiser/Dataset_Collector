from __future__ import annotations

from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any

from datasets import DatasetDict, load_from_disk

from collector_core.merge.types import GreenInput, GreenSkip
from collector_core.stability import stable_api


@stable_api
def is_hf_dataset_dir(path: Path) -> bool:
    if not path.is_dir():
        return False
    markers = ("dataset_info.json", "state.json", "dataset_dict.json")
    return any((path / marker).exists() for marker in markers)


@stable_api
def iter_hf_dataset_dirs(target_dir: Path) -> list[Path]:
    candidates: list[Path] = []
    if is_hf_dataset_dir(target_dir):
        candidates.append(target_dir)
    for marker in ("dataset_info.json", "state.json", "dataset_dict.json"):
        for fp in target_dir.rglob(marker):
            candidates.append(fp.parent)
    for pattern in ("hf_dataset", "split_*"):
        candidates.extend([p for p in target_dir.rglob(pattern) if p.is_dir()])
    seen: set[Path] = set()
    ordered: list[Path] = []
    for path in sorted(candidates):
        resolved = path.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        ordered.append(path)
    return ordered


@stable_api
def iter_hf_inputs(
    dataset_dirs: Iterable[Path],
    *,
    target_id: str,
    pool: str,
) -> Iterator[GreenInput | GreenSkip]:
    for ds_path in dataset_dirs:
        try:
            dataset_obj = load_from_disk(str(ds_path))
        except Exception as exc:
            yield GreenSkip(
                target_id,
                pool,
                ds_path,
                "hf_dataset",
                "hf_load_failed",
                detail={"error": str(exc)},
            )
            continue
        if isinstance(dataset_obj, DatasetDict):
            for split_name in sorted(dataset_obj.keys()):
                dataset = dataset_obj[split_name]
                for raw in dataset:
                    row: dict[str, Any] = dict(raw)
                    row.setdefault("split", split_name)
                    yield GreenInput(row, target_id, pool, ds_path, "hf_dataset")
        else:
            for raw in dataset_obj:
                row = dict(raw)
                row.setdefault("split", "train")
                yield GreenInput(row, target_id, pool, ds_path, "hf_dataset")
