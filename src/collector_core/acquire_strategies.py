"""DEPRECATED: acquire_strategies module - compat shim only.

This module is deprecated and will be removed in v4.0. Please migrate to:
- collector_core.acquire.context for AcquireContext and related types
- collector_core.acquire.strategies.* for individual strategy handlers
- collector_core.acquire.strategies.registry for build_default_handlers()

Migration timeline:
- v3.0: Deprecation warnings added
- v4.0: Module will be removed

Example migration:
    # Old:
    from collector_core.acquire_strategies import handle_http, AcquireContext, run_acquire_worker

    # New:
    from collector_core.acquire.context import AcquireContext
    from collector_core.acquire.strategies.http import handle_http_multi as handle_http
    from collector_core.acquire.worker import run_acquire_worker
"""

from __future__ import annotations

import warnings
from collections.abc import ItemsView, KeysView, ValuesView
from typing import TYPE_CHECKING, Any

# Import for backward compatibility (tests may monkeypatch these)
from collector_core.dependencies import _try_import

requests = _try_import("requests")
FTP = _try_import("ftplib", "FTP")

# Re-export context types - these are the canonical location now
from collector_core.acquire.context import (
    AcquireContext,
    InternalMirrorAllowlist,
    Limits,
    PostProcessor,
    RetryConfig,
    Roots,
    RootsDefaults,
    RunMode,
    StrategyHandler,
)

# Re-export utility functions that are used elsewhere
from collector_core.acquire_limits import (
    build_run_budget,
    build_target_limit_enforcer,
    cleanup_path,
    resolve_result_bytes,
)

if TYPE_CHECKING:
    from pathlib import Path

# Deprecation warning configuration
_DEPRECATION_WARNING = (
    "collector_core.acquire_strategies is deprecated; "
    "use collector_core.acquire.context for context types "
    "and collector_core.acquire.strategies.* for handlers. "
    "This module will be removed in v4.0."
)

_deprecation_warned = False


def _emit_deprecation_warning() -> None:
    """Emit deprecation warning once per session."""
    global _deprecation_warned
    if not _deprecation_warned:
        warnings.warn(_DEPRECATION_WARNING, DeprecationWarning, stacklevel=3)
        _deprecation_warned = True


# ============================================================================
# Re-exports from new strategy modules (emit deprecation warning on use)
# ============================================================================


def _lazy_import_http():
    """Lazy import HTTP strategy module."""
    _emit_deprecation_warning()
    from collector_core.acquire.strategies import http
    return http


def _lazy_import_ftp():
    """Lazy import FTP strategy module."""
    _emit_deprecation_warning()
    from collector_core.acquire.strategies import ftp
    return ftp


def _lazy_import_git():
    """Lazy import git strategy module."""
    _emit_deprecation_warning()
    from collector_core.acquire.strategies import git
    return git


def _lazy_import_zenodo():
    """Lazy import Zenodo strategy module."""
    _emit_deprecation_warning()
    from collector_core.acquire.strategies import zenodo
    return zenodo


def _lazy_import_dataverse():
    """Lazy import Dataverse strategy module."""
    _emit_deprecation_warning()
    from collector_core.acquire.strategies import dataverse
    return dataverse


def _lazy_import_figshare():
    """Lazy import Figshare strategy module."""
    _emit_deprecation_warning()
    from collector_core.acquire.strategies import figshare
    return figshare


def _lazy_import_github():
    """Lazy import GitHub release strategy module."""
    _emit_deprecation_warning()
    from collector_core.acquire.strategies import github_release
    return github_release


def _lazy_import_hf():
    """Lazy import HuggingFace strategy module."""
    _emit_deprecation_warning()
    from collector_core.acquire.strategies import hf
    return hf


def _lazy_import_s3():
    """Lazy import S3 strategy module."""
    _emit_deprecation_warning()
    from collector_core.acquire.strategies import s3
    return s3


def _lazy_import_torrent():
    """Lazy import torrent strategy module."""
    _emit_deprecation_warning()
    from collector_core.acquire.strategies import torrent
    return torrent


# ============================================================================
# Wrapper functions for backward compatibility
# ============================================================================


def validate_download_url(
    url: str,
    allow_non_global_hosts: bool,
    internal_mirror_allowlist: InternalMirrorAllowlist | None = None,
) -> tuple[bool, str | None]:
    """DEPRECATED: Use collector_core.acquire.strategies.http.validate_download_url."""
    http = _lazy_import_http()
    return http.validate_download_url(url, allow_non_global_hosts, internal_mirror_allowlist)


def normalize_download(download: dict[str, Any]) -> dict[str, Any]:
    """DEPRECATED: Use collector_core.acquire.strategies.http.normalize_download."""
    http = _lazy_import_http()
    return http.normalize_download(download)


def sha256_file(path: Path) -> str:
    """DEPRECATED: Use collector_core.utils.hash.sha256_file."""
    _emit_deprecation_warning()
    from collector_core.utils.hash import sha256_file as _sha256_file
    return _sha256_file(path)


def md5_file(path: Path) -> str:
    """DEPRECATED: Use collector_core.utils.hash.md5_file."""
    _emit_deprecation_warning()
    from collector_core.utils.hash import md5_file as _md5_file
    return _md5_file(path)


def run_cmd(cmd: list[str], cwd: Path | None = None) -> str:
    """DEPRECATED: Use subprocess.run directly."""
    _emit_deprecation_warning()
    import subprocess
    p = subprocess.run(
        cmd,
        cwd=str(cwd) if cwd else None,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    return p.stdout.decode("utf-8", errors="ignore")


def _http_download_with_resume(
    ctx: AcquireContext,
    url: str,
    out_path: Path,
    expected_size: int | None = None,
    expected_sha256: str | None = None,
) -> dict[str, Any]:
    """DEPRECATED: Use collector_core.acquire.strategies.http._http_download_with_resume."""
    _emit_deprecation_warning()
    from collector_core.acquire.strategies.http import _http_download_with_resume as _impl
    return _impl(ctx, url, out_path, expected_size, expected_sha256)


# ============================================================================
# Strategy handler re-exports
# ============================================================================


def handle_http(ctx: AcquireContext, row: dict[str, Any], out_dir: Path) -> list[dict[str, Any]]:
    """DEPRECATED: Use collector_core.acquire.strategies.http.handle_http."""
    http = _lazy_import_http()
    return http.handle_http(ctx, row, out_dir)


def handle_http_multi(
    ctx: AcquireContext, row: dict[str, Any], out_dir: Path
) -> list[dict[str, Any]]:
    """DEPRECATED: Use collector_core.acquire.strategies.http.handle_http_multi."""
    http = _lazy_import_http()
    return http.handle_http_multi(ctx, row, out_dir)


def handle_http_single(
    ctx: AcquireContext, row: dict[str, Any], out_dir: Path
) -> list[dict[str, Any]]:
    """DEPRECATED: Use collector_core.acquire.strategies.http.handle_http_single."""
    http = _lazy_import_http()
    return http.handle_http_single(ctx, row, out_dir)


def handle_ftp(ctx: AcquireContext, row: dict[str, Any], out_dir: Path) -> list[dict[str, Any]]:
    """DEPRECATED: Use collector_core.acquire.strategies.ftp.handle_ftp."""
    ftp = _lazy_import_ftp()
    return ftp.handle_ftp(ctx, row, out_dir)


def handle_git(ctx: AcquireContext, row: dict[str, Any], out_dir: Path) -> list[dict[str, Any]]:
    """DEPRECATED: Use collector_core.acquire.strategies.git.handle_git."""
    git = _lazy_import_git()
    return git.handle_git(ctx, row, out_dir)


def handle_zenodo(
    ctx: AcquireContext, row: dict[str, Any], out_dir: Path
) -> list[dict[str, Any]]:
    """DEPRECATED: Use collector_core.acquire.strategies.zenodo.handle_zenodo."""
    zenodo = _lazy_import_zenodo()
    return zenodo.handle_zenodo(ctx, row, out_dir)


def handle_dataverse(
    ctx: AcquireContext, row: dict[str, Any], out_dir: Path
) -> list[dict[str, Any]]:
    """DEPRECATED: Use collector_core.acquire.strategies.dataverse.handle_dataverse."""
    dataverse = _lazy_import_dataverse()
    return dataverse.handle_dataverse(ctx, row, out_dir)


def handle_figshare(
    ctx: AcquireContext, row: dict[str, Any], out_dir: Path
) -> list[dict[str, Any]]:
    """DEPRECATED: Use collector_core.acquire.strategies.figshare.handle_figshare_article."""
    figshare = _lazy_import_figshare()
    return figshare.handle_figshare_article(ctx, row, out_dir)


def handle_figshare_article(
    ctx: AcquireContext, row: dict[str, Any], out_dir: Path
) -> list[dict[str, Any]]:
    """DEPRECATED: Use collector_core.acquire.strategies.figshare.handle_figshare_article."""
    figshare = _lazy_import_figshare()
    return figshare.handle_figshare_article(ctx, row, out_dir)


def handle_figshare_files(
    ctx: AcquireContext, row: dict[str, Any], out_dir: Path
) -> list[dict[str, Any]]:
    """DEPRECATED: Use collector_core.acquire.strategies.figshare.handle_figshare_files."""
    figshare = _lazy_import_figshare()
    return figshare.handle_figshare_files(ctx, row, out_dir)


def make_github_release_handler(user_agent: str) -> StrategyHandler:
    """DEPRECATED: Use collector_core.acquire.strategies.github_release.make_handler."""
    github = _lazy_import_github()
    return github.make_handler(user_agent)


def handle_hf_datasets(
    ctx: AcquireContext, row: dict[str, Any], out_dir: Path
) -> list[dict[str, Any]]:
    """DEPRECATED: Use collector_core.acquire.strategies.hf.handle_hf_datasets."""
    hf = _lazy_import_hf()
    return hf.handle_hf_datasets(ctx, row, out_dir)


def handle_s3_sync(
    ctx: AcquireContext, row: dict[str, Any], out_dir: Path
) -> list[dict[str, Any]]:
    """DEPRECATED: Use collector_core.acquire.strategies.s3.handle_s3_sync."""
    s3 = _lazy_import_s3()
    return s3.handle_s3_sync(ctx, row, out_dir)


def handle_aws_requester_pays(
    ctx: AcquireContext, row: dict[str, Any], out_dir: Path
) -> list[dict[str, Any]]:
    """DEPRECATED: Use collector_core.acquire.strategies.s3.handle_aws_requester_pays."""
    s3 = _lazy_import_s3()
    return s3.handle_aws_requester_pays(ctx, row, out_dir)


def handle_torrent(
    ctx: AcquireContext, row: dict[str, Any], out_dir: Path
) -> list[dict[str, Any]]:
    """DEPRECATED: Use collector_core.acquire.strategies.torrent.handle_torrent."""
    torrent = _lazy_import_torrent()
    return torrent.handle_torrent(ctx, row, out_dir)


# ============================================================================
# DEFAULT_STRATEGY_HANDLERS - computed lazily
# ============================================================================


def _get_default_strategy_handlers() -> dict[str, StrategyHandler]:
    """Get the default strategy handlers dict. Computed lazily."""
    _emit_deprecation_warning()
    from collector_core.acquire.strategies.registry import build_default_handlers
    return build_default_handlers()


# Use a property-like access pattern for DEFAULT_STRATEGY_HANDLERS
class _LazyDict(dict[str, StrategyHandler]):
    """Dictionary that populates itself on first access."""

    _populated: bool = False

    def __getitem__(self, key: str) -> StrategyHandler:
        if not self._populated:
            self.update(_get_default_strategy_handlers())
            self._populated = True
        return super().__getitem__(key)

    def get(self, key: str, default: Any = None) -> Any:
        if not self._populated:
            self.update(_get_default_strategy_handlers())
            self._populated = True
        return super().get(key, default)

    def items(self) -> ItemsView[str, StrategyHandler]:
        if not self._populated:
            self.update(_get_default_strategy_handlers())
            self._populated = True
        return super().items()

    def keys(self) -> KeysView[str]:
        if not self._populated:
            self.update(_get_default_strategy_handlers())
            self._populated = True
        return super().keys()

    def values(self) -> ValuesView[StrategyHandler]:
        if not self._populated:
            self.update(_get_default_strategy_handlers())
            self._populated = True
        return super().values()


DEFAULT_STRATEGY_HANDLERS: dict[str, StrategyHandler] = _LazyDict()


# ============================================================================
# License pool handling
# ============================================================================

LICENSE_POOL_MAP = {
    "permissive": "permissive",
    "public_domain": "permissive",
    "record_level": "permissive",
    "copyleft": "copyleft",
    "unknown": "quarantine",
    "quarantine": "quarantine",
    "deny": "quarantine",
}


def resolve_license_pool(row: dict[str, Any]) -> str:
    """Resolve the license pool for a target row."""
    _emit_deprecation_warning()
    lp = str(row.get("license_profile") or row.get("license_pool") or "quarantine").lower()
    return LICENSE_POOL_MAP.get(lp, "quarantine")


# ============================================================================
# High-level functions (delegating to new modules)
# ============================================================================


def resolve_output_dir(ctx: AcquireContext, bucket: str, pool: str, target_id: str) -> Path:
    """DEPRECATED: Use collector_core.acquire.worker.resolve_output_dir."""
    _emit_deprecation_warning()
    from collector_core.acquire import worker
    return worker.resolve_output_dir(ctx, bucket, pool, target_id)


def write_done_marker(
    ctx: AcquireContext,
    target_id: str,
    bucket: str,
    status: str,
    extra: dict[str, Any] | None = None,
) -> None:
    """DEPRECATED: Use collector_core.acquire.worker.write_done_marker."""
    _emit_deprecation_warning()
    from collector_core.acquire import worker
    return worker.write_done_marker(ctx, target_id, bucket, status, extra)


def run_target(
    ctx: AcquireContext,
    bucket: str,
    row: dict[str, Any],
    strategy_handlers: dict[str, StrategyHandler],
    postprocess: PostProcessor | None = None,
) -> dict[str, Any]:
    """DEPRECATED: Use collector_core.acquire.worker.run_target."""
    _emit_deprecation_warning()
    from collector_core.acquire import worker
    return worker.run_target(ctx, bucket, row, strategy_handlers, postprocess)


def load_config(targets_path: Path | None) -> dict[str, Any]:
    """DEPRECATED: Use collector_core.acquire.worker.load_config."""
    _emit_deprecation_warning()
    from collector_core.acquire import worker
    return worker.load_config(targets_path)


def load_roots(
    cfg: dict[str, Any], overrides: Any, defaults: RootsDefaults
) -> Roots:
    """DEPRECATED: Use collector_core.acquire.worker.load_roots."""
    _emit_deprecation_warning()
    from collector_core.acquire import worker
    return worker.load_roots(cfg, overrides, defaults)


def run_acquire_worker(
    *,
    defaults: RootsDefaults,
    targets_yaml_label: str,
    strategy_handlers: dict[str, StrategyHandler],
    postprocess: PostProcessor | None = None,
) -> None:
    """DEPRECATED: Use collector_core.acquire.worker.run_acquire_worker."""
    _emit_deprecation_warning()
    from collector_core.acquire import worker
    return worker.run_acquire_worker(
        defaults=defaults,
        targets_yaml_label=targets_yaml_label,
        strategy_handlers=strategy_handlers,
        postprocess=postprocess,
    )


# ============================================================================
# safe_name alias
# ============================================================================

from collector_core.utils.paths import safe_filename

# Alias for backward compatibility
safe_name = safe_filename


# ============================================================================
# __all__ for explicit exports
# ============================================================================

__all__ = [
    # Context types (canonical location is acquire.context)
    "AcquireContext",
    "InternalMirrorAllowlist",
    "Limits",
    "PostProcessor",
    "RetryConfig",
    "Roots",
    "RootsDefaults",
    "RunMode",
    "StrategyHandler",
    # Limit utilities
    "build_run_budget",
    "build_target_limit_enforcer",
    "cleanup_path",
    "resolve_result_bytes",
    # Strategy handlers (deprecated - use acquire.strategies.*)
    "handle_http",
    "handle_http_multi",
    "handle_http_single",
    "handle_ftp",
    "handle_git",
    "handle_zenodo",
    "handle_dataverse",
    "handle_figshare",
    "handle_figshare_article",
    "handle_figshare_files",
    "make_github_release_handler",
    "handle_hf_datasets",
    "handle_s3_sync",
    "handle_aws_requester_pays",
    "handle_torrent",
    # Default handlers
    "DEFAULT_STRATEGY_HANDLERS",
    # Utilities
    "validate_download_url",
    "normalize_download",
    "sha256_file",
    "md5_file",
    "run_cmd",
    "safe_name",
    # License pool
    "LICENSE_POOL_MAP",
    "resolve_license_pool",
    # High-level functions
    "resolve_output_dir",
    "write_done_marker",
    "run_target",
    "load_config",
    "load_roots",
    "run_acquire_worker",
]
