"""Git acquisition strategy handlers."""

from collector_core.acquire_strategies import StrategyHandler, handle_git


def get_handler() -> StrategyHandler:
    return handle_git
