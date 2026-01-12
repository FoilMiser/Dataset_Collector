"""Dataverse acquisition strategy handlers."""

from collector_core.acquire.context import StrategyHandler
from collector_core.acquire_strategies import handle_dataverse


def get_handler() -> StrategyHandler:
    return handle_dataverse
