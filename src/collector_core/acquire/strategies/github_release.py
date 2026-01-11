"""GitHub release acquisition strategy handlers."""

from collector_core.acquire_strategies import StrategyHandler, make_github_release_handler


def resolve_handler(repo: str) -> StrategyHandler:
    return make_github_release_handler(repo)
