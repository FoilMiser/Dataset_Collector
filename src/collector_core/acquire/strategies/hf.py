"""Hugging Face acquisition strategy handlers."""

from collector_core.acquire_strategies import StrategyHandler, handle_hf_datasets


def get_handler() -> StrategyHandler:
    return handle_hf_datasets
