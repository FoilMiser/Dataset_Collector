"""Torrent acquisition strategy handlers."""

from collector_core.acquire_strategies import StrategyHandler, handle_torrent


def get_handler() -> StrategyHandler:
    return handle_torrent
