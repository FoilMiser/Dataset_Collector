#!/usr/bin/env python3
"""Normalize updated_utc fields to YYYY-MM-DD for YAML config files."""

from __future__ import annotations

import argparse
import re
from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path

UPDATED_UTC_RE = re.compile(r"^(?P<indent>\s*)updated_utc:\s*(?P<value>.+?)\s*$")


def _iter_yaml_files(paths: Iterable[Path]) -> Iterable[Path]:
    for path in paths:
        if path.is_dir():
            yield from sorted(path.glob("**/*.yaml"))
        elif path.suffix.lower() in {".yaml", ".yml"}:
            yield path


def _normalize_lines(lines: list[str], new_value: str) -> list[str]:
    updated = []
    for line in lines:
        match = UPDATED_UTC_RE.match(line)
        if match:
            indent = match.group("indent")
            updated.append(f'{indent}updated_utc: "{new_value}"\n')
        else:
            updated.append(line)
    return updated


def touch_files(paths: Iterable[Path], date_value: str) -> list[Path]:
    changed: list[Path] = []
    for path in _iter_yaml_files(paths):
        text = path.read_text(encoding="utf-8")
        if "updated_utc" not in text:
            continue
        lines = text.splitlines(keepends=True)
        updated_lines = _normalize_lines(lines, date_value)
        if updated_lines != lines:
            path.write_text("".join(updated_lines), encoding="utf-8")
            changed.append(path)
    return changed


def main() -> int:
    ap = argparse.ArgumentParser(description="Normalize updated_utc fields in YAML files.")
    ap.add_argument(
        "paths",
        nargs="*",
        default=["."],
        help="Files or directories to scan (defaults to repo root).",
    )
    ap.add_argument(
        "--date",
        default=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        help="Date to apply in YYYY-MM-DD format (defaults to today UTC).",
    )
    args = ap.parse_args()

    date_value = args.date
    try:
        datetime.strptime(date_value, "%Y-%m-%d")
    except ValueError as exc:
        raise SystemExit(f"Invalid --date value: {date_value}. Expected YYYY-MM-DD.") from exc

    paths = [Path(p).expanduser() for p in args.paths]
    changed = touch_files(paths, date_value)
    if changed:
        print("Updated updated_utc in:")
        for path in changed:
            print(f"  - {path}")
    else:
        print("No updated_utc entries modified.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
