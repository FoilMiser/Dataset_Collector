"""S3 acquisition strategy handlers."""

from collector_core.acquire_strategies import StrategyHandler, handle_aws_requester_pays, handle_s3_sync


def get_sync_handler() -> StrategyHandler:
    return handle_s3_sync


def get_requester_pays_handler() -> StrategyHandler:
    return handle_aws_requester_pays
