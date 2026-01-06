from __future__ import annotations

from collections.abc import Iterable
from typing import Any

StrategySpec = dict[str, Any]

STRATEGY_REGISTRY: dict[str, StrategySpec] = {
    "http": {
        "status": "supported",
        "required": [
            {
                "keys": ("url", "urls", "base_url"),
                "message": "download.url(s) or base_url required for http strategy",
            }
        ],
        "external_tools": [],
    },
    "ftp": {
        "status": "supported",
        "required": [
            {
                "keys": ("url", "urls", "base_url"),
                "message": "download.url(s) or base_url required for ftp strategy",
            }
        ],
        "external_tools": [],
    },
    "git": {
        "status": "supported",
        "required": [
            {
                "keys": ("repo", "repo_url", "url"),
                "message": "download.repo/repo_url/url required for git strategy",
            }
        ],
        "external_tools": ["git"],
    },
    "huggingface_datasets": {
        "status": "supported",
        "required": [
            {
                "keys": ("dataset_id",),
                "message": "download.dataset_id required for huggingface_datasets strategy",
            }
        ],
        "external_tools": [],
    },
    "zenodo": {
        "status": "supported",
        "required": [
            {
                "keys": ("record_id", "doi", "url"),
                "message": "download.record_id/doi/url required for zenodo strategy",
            }
        ],
        "external_tools": [],
    },
    "dataverse": {
        "status": "supported",
        "required": [
            {
                "keys": ("persistent_id", "url"),
                "message": "download.persistent_id/url required for dataverse strategy",
            }
        ],
        "external_tools": [],
    },
    "figshare": {
        "status": "supported",
        "required": [
            {
                "keys": ("article_id", "article_url"),
                "message": "download.article_id/article_url required for figshare strategy",
            }
        ],
        "external_tools": [],
    },
    "github_release": {
        "status": "supported",
        "required": [
            {
                "keys": ("repo", "repository"),
                "message": "download.repo/repository required for github_release strategy",
            }
        ],
        "external_tools": [],
    },
    "s3_public": {
        "status": "supported",
        "required": [
            {
                "keys": ("bucket",),
                "message": "download.bucket required for s3_public strategy",
            }
        ],
        "external_tools": [],
    },
    "s3_sync": {
        "status": "supported",
        "required": [
            {
                "keys": ("urls",),
                "message": "download.urls required for s3_sync strategy",
            }
        ],
        "external_tools": ["aws"],
    },
    "aws_requester_pays": {
        "status": "supported",
        "required": [
            {
                "keys": ("bucket",),
                "message": "download.bucket required for aws_requester_pays strategy",
            },
            {
                "keys": ("key",),
                "message": "download.key required for aws_requester_pays strategy",
            },
        ],
        "external_tools": ["aws"],
    },
    "torrent": {
        "status": "supported",
        "required": [
            {
                "keys": ("magnet", "torrent"),
                "message": "download.magnet/torrent required for torrent strategy",
            }
        ],
        "external_tools": ["aria2c"],
    },
    "api": {
        "status": "supported",
        "required": [
            {
                "keys": ("base_url",),
                "message": "download.base_url required for api strategy",
            }
        ],
        "external_tools": [],
    },
    "web_crawl": {
        "status": "supported",
        "required": [
            {
                "keys": ("seed_urls",),
                "message": "download.seed_urls required for web_crawl strategy",
            }
        ],
        "external_tools": [],
    },
}


def iter_registry_strategies() -> Iterable[str]:
    return STRATEGY_REGISTRY.keys()


def get_strategy_spec(strategy: str) -> StrategySpec | None:
    return STRATEGY_REGISTRY.get((strategy or "").strip().lower())


def get_external_tools(strategy: str) -> list[str]:
    spec = get_strategy_spec(strategy)
    if not spec:
        return []
    return list(spec.get("external_tools") or [])


def _has_value(download: dict[str, Any], key: str) -> bool:
    value = download.get(key)
    if isinstance(value, list):
        return bool(value)
    return bool(value)


def _validate_github_release(download: dict[str, Any]) -> list[str]:
    repo = download.get("repo") or download.get("repository")
    owner = download.get("owner")
    if not repo:
        return ["download.repo/repository required for github_release strategy"]
    if "/" not in str(repo) and not owner:
        return ["download.owner required when repo is not in 'owner/repo' form for github_release strategy"]
    return []


def get_strategy_requirement_errors(download: dict[str, Any], strategy: str) -> list[str]:
    strategy = (strategy or "").strip().lower()
    spec = get_strategy_spec(strategy)
    if not spec:
        return []
    if strategy == "github_release":
        return _validate_github_release(download)
    errors: list[str] = []
    for requirement in spec.get("required", []):
        keys = requirement.get("keys", ())
        if not any(_has_value(download, key) for key in keys):
            errors.append(requirement.get("message", "missing required download config"))
    return errors
