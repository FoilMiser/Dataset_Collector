from __future__ import annotations

import time
from typing import Any

from collector_core.__version__ import __schema_version__ as SCHEMA_VERSION
from collector_core.__version__ import __version__ as PIPELINE_VERSION


def _utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def build_artifact_metadata(
    *,
    pipeline_version: str | None = None,
    schema_version: str | None = None,
    written_at_utc: str | None = None,
    git_commit: str | None = None,
    tool_versions: dict[str, str] | None = None,
) -> dict[str, Any]:
    metadata = {
        "pipeline_version": pipeline_version or PIPELINE_VERSION,
        "schema_version": schema_version or SCHEMA_VERSION,
        "written_at_utc": written_at_utc or _utc_now(),
    }
    if git_commit:
        metadata["git_commit"] = git_commit
    if tool_versions:
        metadata["tool_versions"] = tool_versions
    return metadata
