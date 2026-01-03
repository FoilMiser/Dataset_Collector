from __future__ import annotations

from collections.abc import Iterable
from typing import Any


def normalize_download(download: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(download or {})
    cfg = normalized.get("config")
    if isinstance(cfg, dict):
        merged = dict(cfg)
        merged.update({k: v for k, v in normalized.items() if k != "config"})
        normalized = merged

    if normalized.get("strategy") == "zenodo":
        if not normalized.get("record_id") and normalized.get("record"):
            normalized["record_id"] = normalized.get("record")
        if not normalized.get("record_id") and isinstance(normalized.get("record_ids"), list):
            record_ids = normalized.get("record_ids") or []
            if record_ids:
                normalized["record_id"] = record_ids[0]

    return normalized


STRATEGY_REGISTRY: dict[str, dict[str, Any]] = {
    "http": {
        "required_any": [{"url", "urls", "base_url"}],
        "external_tools": [],
    },
    "ftp": {
        "required_any": [{"url", "urls", "base_url"}],
        "external_tools": [],
    },
    "git": {
        "required_any": [{"repo", "repo_url", "url"}],
        "external_tools": ["git"],
    },
    "huggingface_datasets": {
        "required_all": {"dataset_id"},
        "external_tools": [],
    },
    "zenodo": {
        "required_any": [{"record_id", "doi", "url"}],
        "external_tools": [],
    },
    "dataverse": {
        "required_any": [{"persistent_id", "url"}],
        "external_tools": [],
    },
    "figshare": {
        "required_any": [{"article_id", "id"}],
        "external_tools": [],
    },
    "github_release": {
        "required_any": [{"repo", "repository", "owner"}],
        "external_tools": [],
    },
    "api": {
        "required_all": {"base_url"},
        "external_tools": [],
    },
    "web_crawl": {
        "required_all": {"seed_urls"},
        "external_tools": [],
    },
    "s3_public": {
        "required_all": {"bucket"},
        "external_tools": [],
    },
    "s3_sync": {
        "required_all": {"urls"},
        "external_tools": ["aws"],
    },
    "aws_requester_pays": {
        "required_all": {"bucket", "key"},
        "external_tools": ["aws"],
    },
    "torrent": {
        "required_any": [{"magnet", "torrent"}],
        "external_tools": ["aria2c"],
    },
}


def _has_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, Iterable) and not isinstance(value, (str, bytes, dict)):
        return bool(list(value))
    if isinstance(value, dict):
        return bool(value)
    return True


def get_required_keys(strategy: str) -> tuple[list[set[str]], set[str]]:
    entry = STRATEGY_REGISTRY.get(strategy, {})
    required_any = [set(group) for group in entry.get("required_any", [])]
    required_all = set(entry.get("required_all", set()))
    return required_any, required_all


def get_external_tools_for_strategy(strategy: str) -> list[str]:
    entry = STRATEGY_REGISTRY.get(strategy, {})
    return list(entry.get("external_tools", []))


def validate_download_config(download: dict[str, Any], strategy: str) -> list[str]:
    errors: list[str] = []
    strategy = (strategy or "").lower()
    entry = STRATEGY_REGISTRY.get(strategy)
    if not entry:
        errors.append(f"unknown strategy '{strategy}'")
        return errors

    required_any, required_all = get_required_keys(strategy)
    for key in required_all:
        if not _has_value(download.get(key)):
            errors.append(f"download.{key} required for {strategy} strategy")

    for group in required_any:
        if not any(_has_value(download.get(key)) for key in group):
            keys = "/".join(sorted(group))
            errors.append(f"download.{keys} required for {strategy} strategy")

    if strategy == "github_release":
        repo = download.get("repo") or download.get("repository")
        owner = download.get("owner")
        if owner and not repo:
            errors.append("download.repo/repository required when download.owner is set for github_release strategy")

    return errors
