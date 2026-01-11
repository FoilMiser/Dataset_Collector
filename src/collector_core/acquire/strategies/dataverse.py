"""Dataverse acquisition strategy handlers."""

from collector_core.acquire_strategies import StrategyHandler, handle_dataverse


def get_handler() -> StrategyHandler:
    return handle_dataverse
