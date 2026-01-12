"""Registry for acquisition strategy handlers."""

from __future__ import annotations

from collector_core.acquire.strategies import (
    dataverse,
    figshare,
    ftp,
    git,
    github_release,
    hf,
    http,
    s3,
    torrent,
    zenodo,
)
from collector_core.acquire.context import StrategyHandler


def build_default_handlers(
    *,
    http_handler: str = "multi",
    figshare_variant: str | None = None,
    github_release_repo: str | None = None,
    extra_handlers: list[str] | None = None,
) -> dict[str, StrategyHandler]:
    handlers: dict[str, StrategyHandler] = {
        "http": http.resolve_http_handler(http_handler),
        "ftp": ftp.get_handler(),
        "git": git.get_handler(),
        "zenodo": zenodo.get_handler(),
        "dataverse": dataverse.get_handler(),
        "huggingface_datasets": hf.get_handler(),
    }

    if figshare_variant:
        handlers["figshare"] = figshare.resolve_figshare_handler(figshare_variant)

    if github_release_repo:
        handlers["github_release"] = github_release.resolve_handler(github_release_repo)

    if extra_handlers:
        for name in extra_handlers:
            if name == "s3_sync":
                handlers[name] = s3.get_sync_handler()
            elif name == "aws_requester_pays":
                handlers[name] = s3.get_requester_pays_handler()
            elif name == "torrent":
                handlers[name] = torrent.get_handler()

    return handlers
