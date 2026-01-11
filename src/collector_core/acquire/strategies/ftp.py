"""FTP acquisition strategy handlers."""

from collector_core.acquire_strategies import StrategyHandler, handle_ftp


def get_handler() -> StrategyHandler:
    return handle_ftp
