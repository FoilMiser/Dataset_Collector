"""HTTP acquisition strategy handlers."""

from collector_core.acquire.context import StrategyHandler
from collector_core.acquire_strategies import handle_http_multi, handle_http_single


def resolve_http_handler(variant: str = "multi") -> StrategyHandler:
    if variant == "single":
        return handle_http_single
    return handle_http_multi
