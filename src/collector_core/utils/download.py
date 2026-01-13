"""Download configuration normalization utilities.

This module provides the canonical implementation of download configuration
normalization, used across all acquisition strategies.
"""

from __future__ import annotations

from typing import Any

from collector_core.stability import stable_api


@stable_api
def normalize_download(download: dict[str, Any]) -> dict[str, Any]:
    """Normalize download configuration by merging nested config.

    This function performs two normalizations:
    1. Merges any nested 'config' dict into the parent download dict
    2. Normalizes Zenodo-specific record ID aliases (record -> record_id)

    Args:
        download: Raw download configuration dictionary

    Returns:
        Normalized download configuration with merged config

    Example:
        >>> normalize_download({"url": "...", "config": {"timeout": 30}})
        {"url": "...", "timeout": 30}
    """
    d = dict(download or {})
    cfg = d.get("config")

    if isinstance(cfg, dict):
        merged = dict(cfg)
        merged.update({k: v for k, v in d.items() if k != "config"})
        d = merged

    # Handle Zenodo-specific record ID aliases
    if d.get("strategy") == "zenodo":
        if not d.get("record_id") and d.get("record"):
            d["record_id"] = d["record"]
        if not d.get("record_id") and isinstance(d.get("record_ids"), list) and d["record_ids"]:
            d["record_id"] = d["record_ids"][0]

    return d
