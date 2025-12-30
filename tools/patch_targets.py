from __future__ import annotations

import argparse
from collections.abc import Iterable
from pathlib import Path, PurePath, PureWindowsPath

import yaml


def _path_to_posix(path: PurePath) -> str:
    return path.as_posix()


def _queue_filename(entry: dict[str, object]) -> str | None:
    path_value = entry.get("path")
    if isinstance(path_value, str) and path_value:
        if ":" in path_value or "\\" in path_value:
            return PureWindowsPath(path_value).name
        return Path(path_value).name
    filename = entry.get("filename")
    if isinstance(filename, str) and filename:
        return filename
    entry_id = entry.get("id")
    if isinstance(entry_id, str) and entry_id:
        return f"{entry_id}.jsonl"
    return None


def patch_targets_yaml(
    targets_path: Path,
    dataset_root: PurePath,
    output_path: Path,
) -> Path:
    cfg = yaml.safe_load(targets_path.read_text(encoding="utf-8")) or {}
    globals_cfg = cfg.get("globals", {}) or {}

    roots = {
        "raw_root": dataset_root / "raw",
        "screened_yellow_root": dataset_root / "screened_yellow",
        "combined_root": dataset_root / "combined",
        "ledger_root": dataset_root / "_ledger",
        "pitches_root": dataset_root / "_pitches",
        "manifests_root": dataset_root / "_manifests",
        "queues_root": dataset_root / "_queues",
        "catalogs_root": dataset_root / "_catalogs",
        "logs_root": dataset_root / "_logs",
    }

    for key, root in roots.items():
        globals_cfg[key] = _path_to_posix(root)

    cfg["globals"] = globals_cfg

    queues_cfg = cfg.get("queues", {}) or {}
    emit = queues_cfg.get("emit", []) or []
    queues_root = roots["queues_root"]
    for entry in emit:
        if not isinstance(entry, dict):
            continue
        filename = _queue_filename(entry)
        if not filename:
            continue
        entry["path"] = _path_to_posix(queues_root / filename)
    queues_cfg["emit"] = emit
    cfg["queues"] = queues_cfg

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        yaml.safe_dump(cfg, sort_keys=False, default_flow_style=False),
        encoding="utf-8",
    )
    return output_path


def _parse_dataset_root(value: str) -> PurePath:
    if ":" in value or "\\" in value:
        return PureWindowsPath(value)
    return PurePath(value)


def main(argv: Iterable[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Patch pipeline targets YAML for Windows roots.")
    ap.add_argument("--targets", required=True, help="Path to the original targets YAML")
    ap.add_argument("--dataset-root", required=True, help="Destination dataset root")
    ap.add_argument("--output", required=True, help="Path for patched YAML")
    args = ap.parse_args(argv)

    targets_path = Path(args.targets).expanduser().resolve()
    dataset_root = _parse_dataset_root(args.dataset_root)
    output_path = Path(args.output).expanduser().resolve()

    patch_targets_yaml(targets_path, dataset_root, output_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
