from __future__ import annotations

import collector_core.acquire_strategies as aw
from collector_core.acquire.strategies import (
    dataverse,
    figshare,
    ftp,
    git,
    hf,
    http,
    registry,
    s3,
    torrent,
    zenodo,
)


def test_strategy_modules_resolve_handlers() -> None:
    assert http.resolve_http_handler("single") is aw.handle_http_single
    assert http.resolve_http_handler("multi") is aw.handle_http_multi
    assert ftp.get_handler() is aw.handle_ftp
    assert git.get_handler() is aw.handle_git
    assert zenodo.get_handler() is aw.handle_zenodo
    assert dataverse.get_handler() is aw.handle_dataverse
    assert hf.get_handler() is aw.handle_hf_datasets
    assert figshare.resolve_figshare_handler("article") is aw.handle_figshare_article
    assert figshare.resolve_figshare_handler("files") is aw.handle_figshare_files
    assert s3.get_sync_handler() is aw.handle_s3_sync
    assert s3.get_requester_pays_handler() is aw.handle_aws_requester_pays
    assert torrent.get_handler() is aw.handle_torrent


def test_registry_builds_default_handlers() -> None:
    handlers = registry.build_default_handlers()

    assert handlers["http"] is aw.handle_http_multi
    assert handlers["ftp"] is aw.handle_ftp
    assert handlers["git"] is aw.handle_git
    assert handlers["zenodo"] is aw.handle_zenodo
    assert handlers["dataverse"] is aw.handle_dataverse
    assert handlers["huggingface_datasets"] is aw.handle_hf_datasets


def test_registry_supports_variants_and_extras() -> None:
    handlers = registry.build_default_handlers(
        http_handler="single",
        figshare_variant="files",
        github_release_repo="openai/example",
        extra_handlers=["s3_sync", "aws_requester_pays", "torrent"],
    )

    assert handlers["http"] is aw.handle_http_single
    assert handlers["figshare"] is aw.handle_figshare_files
    assert handlers["s3_sync"] is aw.handle_s3_sync
    assert handlers["aws_requester_pays"] is aw.handle_aws_requester_pays
    assert handlers["torrent"] is aw.handle_torrent
    assert "github_release" in handlers
    assert callable(handlers["github_release"])
