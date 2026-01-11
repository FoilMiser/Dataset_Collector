from __future__ import annotations

_LOADED = False


def load_builtin_checks() -> None:
    global _LOADED
    if _LOADED:
        return
    from collector_core.checks import dual_use_scan, pii_scan, secret_scan  # noqa: F401

    _LOADED = True
