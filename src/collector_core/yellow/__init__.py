from __future__ import annotations

from collector_core.yellow.base import (
    DomainContext,
    FilterDecision,
    canonical_record,
    iter_hf_dataset_dirs,
    iter_raw_files,
    process_target,
    record_pitch,
    run_yellow_screen,
    standard_filter,
    standard_transform,
)
from collector_core.yellow.dispatcher import (
    clear_domain_cache,
    create_domain_runner,
    dispatch_yellow_screen,
    get_domain,
    is_domain_available,
    list_domain_aliases,
    list_domains,
    register_domain,
    unregister_domain,
)

__all__ = [
    # Base module exports
    "DomainContext",
    "FilterDecision",
    "canonical_record",
    "iter_hf_dataset_dirs",
    "iter_raw_files",
    "process_target",
    "record_pitch",
    "run_yellow_screen",
    "standard_filter",
    "standard_transform",
    # Dispatcher exports
    "clear_domain_cache",
    "create_domain_runner",
    "dispatch_yellow_screen",
    "get_domain",
    "is_domain_available",
    "list_domain_aliases",
    "list_domains",
    "register_domain",
    "unregister_domain",
]
