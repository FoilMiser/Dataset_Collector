"""Figshare acquisition strategy handlers."""

from collector_core.acquire_strategies import (
    StrategyHandler,
    handle_figshare_article,
    handle_figshare_files,
)


def resolve_figshare_handler(variant: str) -> StrategyHandler:
    if variant == "files":
        return handle_figshare_files
    return handle_figshare_article
