"""Shared utility functions for the Dataset Collector."""

from collector_core.utils.hash import (
    sha256_bytes,
    sha256_file,
    sha256_text,
    stable_json_hash,
    stable_unit_interval,
)
from collector_core.utils.io import (
    append_jsonl,
    read_json,
    read_jsonl,
    read_jsonl_list,
    read_yaml,
    write_json,
    write_jsonl,
    write_jsonl_gz,
)
from collector_core.utils.logging import log_context, log_event, utc_now
from collector_core.utils.paths import (
    DEFAULT_MAX_ARCHIVE_BYTES,
    DEFAULT_MAX_ARCHIVE_FILES,
    ensure_dir,
    ensure_under_root,
    safe_filename,
    safe_join,
    validate_tar_archive,
    validate_zip_archive,
)
from collector_core.utils.text import (
    coerce_int,
    contains_any,
    lower,
    normalize_whitespace,
    safe_text,
)

__all__ = [
    "utc_now",
    "ensure_dir",
    "ensure_under_root",
    "safe_join",
    "DEFAULT_MAX_ARCHIVE_FILES",
    "DEFAULT_MAX_ARCHIVE_BYTES",
    "sha256_bytes",
    "sha256_text",
    "sha256_file",
    "stable_json_hash",
    "stable_unit_interval",
    "normalize_whitespace",
    "lower",
    "safe_text",
    "read_yaml",
    "read_json",
    "write_json",
    "read_jsonl",
    "read_jsonl_list",
    "write_jsonl",
    "write_jsonl_gz",
    "append_jsonl",
    "safe_filename",
    "validate_tar_archive",
    "validate_zip_archive",
    "contains_any",
    "coerce_int",
    "log_event",
    "log_context",
]
