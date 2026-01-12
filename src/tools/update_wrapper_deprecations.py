#!/usr/bin/env python3
"""
Update deprecation notices in pipeline wrapper files.

Issue 2.4 (v3.0): Updates wrapper files with proper deprecation notices
and v4.0 removal target.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path


WRAPPER_FILES = [
    "acquire_worker.py",
    "merge_worker.py",
    "yellow_screen_worker.py",
    "yellow_scrubber.py",
    "catalog_builder.py",
    "review_queue.py",
    "pipeline_driver.py",
    "pmc_worker.py",
]


def update_deprecation_notice(content: str, filename: str, domain: str) -> str:
    """Update deprecation notice to use v4.0 removal target."""
    # Update docstring removal target
    content = re.sub(
        r"Removal target: v3\.0\.",
        "Removal target: v4.0.",
        content
    )

    # Update deprecation message
    content = re.sub(
        r'Removal target: v3\.0\."',
        'Removal target: v4.0."',
        content
    )

    return content


def main() -> int:
    repo_root = Path(__file__).resolve().parents[2]

    updated = 0
    for pipeline_dir in sorted(repo_root.glob("*_pipeline_v2")):
        domain = pipeline_dir.name.removesuffix("_pipeline_v2")

        for filename in WRAPPER_FILES:
            filepath = pipeline_dir / filename
            if not filepath.exists():
                continue

            content = filepath.read_text()
            new_content = update_deprecation_notice(content, filename, domain)

            if new_content != content:
                filepath.write_text(new_content)
                print(f"Updated: {filepath.relative_to(repo_root)}")
                updated += 1

    print(f"\nUpdated {updated} files")
    return 0


if __name__ == "__main__":
    sys.exit(main())
