from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable

POOLS = ("permissive", "copyleft", "quarantine")


def init_layout(dataset_root: Path) -> None:
    base_dirs = [
        "raw",
        "screened_yellow",
        "combined",
        "_queues",
        "_logs",
        "_catalogs",
        "_ledger",
        "_pitches",
        "_manifests",
    ]
    for directory in base_dirs:
        (dataset_root / directory).mkdir(parents=True, exist_ok=True)

    for pool in POOLS:
        (dataset_root / "raw" / "green" / pool).mkdir(parents=True, exist_ok=True)
        (dataset_root / "raw" / "yellow" / pool).mkdir(parents=True, exist_ok=True)
        (dataset_root / "screened_yellow" / pool / "shards").mkdir(parents=True, exist_ok=True)
        (dataset_root / "combined" / pool / "shards").mkdir(parents=True, exist_ok=True)


def main(argv: Iterable[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Initialize the Natural corpus directory layout.")
    ap.add_argument("--dataset-root", required=True, help="Dataset root folder for a domain")
    args = ap.parse_args(argv)

    dataset_root = Path(args.dataset_root).expanduser().resolve()
    init_layout(dataset_root)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
