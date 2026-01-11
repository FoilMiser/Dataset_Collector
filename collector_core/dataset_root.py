from __future__ import annotations

import os
from pathlib import Path


def resolve_dataset_root(explicit: str | None = None) -> Path | None:
    value = explicit or os.getenv("DATASET_ROOT") or os.getenv("DATASET_COLLECTOR_ROOT")
    if not value:
        return None
    return Path(value).expanduser().resolve()
