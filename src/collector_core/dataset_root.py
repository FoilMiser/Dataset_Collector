from __future__ import annotations

import os
from collections.abc import Iterable
from pathlib import Path


def resolve_dataset_root(explicit: str | None = None) -> Path | None:
    value = explicit or os.getenv("DATASET_ROOT") or os.getenv("DATASET_COLLECTOR_ROOT")
    if not value:
        return None
    return Path(value).expanduser().resolve()


def ensure_data_root_allowed(paths: Iterable[Path], allow_data_root: bool) -> None:
    if allow_data_root:
        return
    for path in paths:
        resolved = path.expanduser().resolve()
        if resolved.is_absolute() and resolved.parts[:2] == ("/", "data"):
            raise ValueError(
                "Refusing to use /data without explicit opt-in. "
                "Pass --allow-data-root or set an explicit dataset/root override."
            )
