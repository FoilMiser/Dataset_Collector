"""Git acquisition strategy handlers."""

from collector_core.acquire.context import StrategyHandler
from collector_core.acquire_strategies import handle_git


def get_handler() -> StrategyHandler:
    return handle_git
