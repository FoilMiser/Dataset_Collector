from __future__ import annotations

import hashlib
import json
import logging
import re
from pathlib import Path
from typing import Any

logger = logging.getLogger("collector_core.utils")


def sha256_bytes(data: bytes) -> str:
    """Compute SHA-256 hash of bytes."""
    return hashlib.sha256(data).hexdigest()


def sha256_text(text: str) -> str:
    """Compute SHA-256 hash of normalized text."""
    normalized = re.sub(r"\s+", " ", (text or "").strip())
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def sha256_file(path: Path) -> str | None:
    """Compute SHA-256 hash of a file. Returns None on error."""
    try:
        h = hashlib.sha256()
        with path.open("rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                h.update(chunk)
        return h.hexdigest()
    except Exception:
        logger.warning("Failed to compute SHA-256 hash for %s", path, exc_info=True)
        return None


def stable_json_hash(value: Any) -> str:
    """Compute a stable SHA-256 hash for JSON-serializable values."""
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return sha256_bytes(payload.encode("utf-8"))


def stable_unit_interval(key: str) -> float:
    """Return a stable [0, 1) float derived from a string key."""
    digest = hashlib.sha256(key.encode()).digest()
    return (int.from_bytes(digest[:8], "big") % 1_000_000) / 1_000_000.0
