#!/usr/bin/env python3
"""
sync_pipeline_wrappers.py

One-command regeneration of all thin wrappers for pipeline directories.
Can also run in lint mode (--check) for CI to verify wrappers are up-to-date.

Usage:
    python tools/sync_pipeline_wrappers.py          # Regenerate all wrappers
    python tools/sync_pipeline_wrappers.py --check  # Check without writing (CI mode)
    python tools/sync_pipeline_wrappers.py --dry-run # Show what would be written
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import NamedTuple

# Add parent to path for collector_core imports
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from collector_core.pipeline_spec import get_pipeline_spec, list_pipelines  # noqa: E402


class WrapperTemplate(NamedTuple):
    filename: str
    template: str
    skip_if_large: bool = False
    max_lines_for_overwrite: int = 30


# Template for pipeline_driver.py
PIPELINE_DRIVER_TEMPLATE = '''#!/usr/bin/env python3
"""
pipeline_driver.py (v2.0)

Thin wrapper that delegates to the spec-driven pipeline factory.
"""
from __future__ import annotations

import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from collector_core.pipeline_factory import get_pipeline_driver  # noqa: E402

DOMAIN = "{domain}"

if __name__ == "__main__":
    get_pipeline_driver(DOMAIN).main()
'''

# Template for acquire_worker.py
ACQUIRE_WORKER_TEMPLATE = '''#!/usr/bin/env python3
"""
acquire_worker.py (v2.0)

Thin wrapper that delegates to the spec-driven generic acquire worker.
"""
from __future__ import annotations

import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from collector_core.generic_workers import main_acquire  # noqa: E402

DOMAIN = "{domain}"

if __name__ == "__main__":
    main_acquire(DOMAIN)
'''

# Template for yellow_scrubber.py
YELLOW_SCRUBBER_TEMPLATE = '''#!/usr/bin/env python3
"""
yellow_scrubber.py (v2.0)

Thin wrapper that delegates to the spec-driven yellow review helper.
"""
from __future__ import annotations

import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from collector_core.pipeline_spec import get_pipeline_spec  # noqa: E402
from collector_core.yellow_review_helpers import make_main  # noqa: E402

DOMAIN = "{domain}"

if __name__ == "__main__":
    spec = get_pipeline_spec(DOMAIN)
    assert spec is not None, f"Unknown domain: {{DOMAIN}}"
    make_main(domain_name=spec.name, domain_prefix=spec.prefix, targets_yaml_name=spec.targets_yaml)
'''

# Template for yellow_screen_worker.py
YELLOW_SCREEN_WORKER_TEMPLATE = '''#!/usr/bin/env python3
"""
yellow_screen_worker.py (v2.0)

Thin wrapper that delegates to the spec-driven yellow screen dispatch.
"""
from __future__ import annotations

import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from collector_core.yellow_screen_dispatch import main_yellow_screen  # noqa: E402

DOMAIN = "{domain}"

if __name__ == "__main__":
    main_yellow_screen(DOMAIN)
'''

TEMPLATES = [
    WrapperTemplate(
        "pipeline_driver.py",
        PIPELINE_DRIVER_TEMPLATE,
        skip_if_large=True,
        max_lines_for_overwrite=50,
    ),
    WrapperTemplate(
        "acquire_worker.py", ACQUIRE_WORKER_TEMPLATE, skip_if_large=True, max_lines_for_overwrite=30
    ),
    WrapperTemplate(
        "yellow_scrubber.py",
        YELLOW_SCRUBBER_TEMPLATE,
        skip_if_large=True,
        max_lines_for_overwrite=30,
    ),
    WrapperTemplate("yellow_screen_worker.py", YELLOW_SCREEN_WORKER_TEMPLATE, skip_if_large=False),
]


def get_pipeline_dir(domain: str) -> Path:
    """Get the pipeline directory for a domain."""
    spec = get_pipeline_spec(domain)
    if spec is None:
        raise ValueError(f"Unknown domain: {domain}")
    # Use domain name with _pipeline_v2 suffix
    return REPO_ROOT / f"{domain}_pipeline_v2"


def count_lines(path: Path) -> int:
    """Count lines in a file."""
    if not path.exists():
        return 0
    return len(path.read_text().splitlines())


def generate_wrapper(domain: str, template: WrapperTemplate) -> str:
    """Generate wrapper content from template."""
    return template.template.format(domain=domain)


def should_skip_file(path: Path, template: WrapperTemplate) -> bool:
    """Check if file should be skipped (large custom file)."""
    if not template.skip_if_large:
        return False
    if not path.exists():
        return False
    lines = count_lines(path)
    return lines > template.max_lines_for_overwrite


def sync_domain(domain: str, check_only: bool = False, dry_run: bool = False) -> list[str]:
    """Sync wrappers for a single domain. Returns list of issues/changes."""
    issues = []
    pipeline_dir = get_pipeline_dir(domain)

    if not pipeline_dir.exists():
        issues.append(f"SKIP: {pipeline_dir} does not exist")
        return issues

    for template in TEMPLATES:
        file_path = pipeline_dir / template.filename
        expected_content = generate_wrapper(domain, template)

        if should_skip_file(file_path, template):
            lines = count_lines(file_path)
            issues.append(f"SKIP: {file_path.name} ({lines} lines) - custom file")
            continue

        if file_path.exists():
            current_content = file_path.read_text()
            if current_content == expected_content:
                continue  # Already up to date
            else:
                if check_only:
                    issues.append(f"MISMATCH: {file_path}")
                elif dry_run:
                    issues.append(f"WOULD UPDATE: {file_path}")
                else:
                    file_path.write_text(expected_content)
                    issues.append(f"UPDATED: {file_path}")
        else:
            if check_only:
                issues.append(f"MISSING: {file_path}")
            elif dry_run:
                issues.append(f"WOULD CREATE: {file_path}")
            else:
                file_path.write_text(expected_content)
                issues.append(f"CREATED: {file_path}")

    return issues


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Sync pipeline wrapper files to canonical templates"
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Check mode: exit non-zero if any wrapper differs from expected",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without making changes",
    )
    parser.add_argument(
        "--domain",
        type=str,
        default=None,
        help="Only sync a specific domain (default: all domains)",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Verbose output",
    )
    args = parser.parse_args()

    domains = [args.domain] if args.domain else list_pipelines()
    all_issues: list[str] = []

    for domain in domains:
        if args.verbose:
            print(f"Processing {domain}...")
        issues = sync_domain(domain, check_only=args.check, dry_run=args.dry_run)
        all_issues.extend(issues)

    # Print results
    for issue in all_issues:
        print(issue)

    if not all_issues:
        print("All wrappers are up to date.")
        return 0

    # In check mode, any mismatch or missing file is an error
    if args.check:
        has_problems = any(
            issue.startswith("MISMATCH:") or issue.startswith("MISSING:") for issue in all_issues
        )
        if has_problems:
            print(
                "\nWrapper sync check failed. Run 'python tools/sync_pipeline_wrappers.py' to fix."
            )
            return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
