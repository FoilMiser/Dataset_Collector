#!/usr/bin/env python3
"""
catalog_builder.py (v2.0)

Thin wrapper that delegates to the spec-driven generic catalog builder.
"""
from __future__ import annotations
from collector_core.generic_workers import main_catalog  # noqa: E402

DOMAIN = "physics"


def main() -> None:
    main_catalog(DOMAIN)


if __name__ == "__main__":
    main()
