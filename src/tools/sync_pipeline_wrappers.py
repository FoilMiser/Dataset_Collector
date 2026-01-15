#!/usr/bin/env python3
"""
sync_pipeline_wrappers.py

One-command regeneration of all thin wrappers for pipeline directories.
Defaults to check mode unless --write is provided.

Usage:
    python -m tools.sync_pipeline_wrappers --write   # Regenerate all wrappers
    python -m tools.sync_pipeline_wrappers --check   # Check without writing (CI mode)
    python -m tools.sync_pipeline_wrappers --dry-run # Show what would be written
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import NamedTuple

import collector_core.pipeline_specs_registry  # noqa: E402,F401
from collector_core.pipeline_spec import get_pipeline_spec, list_pipelines  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[2]


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
from pathlib import Path
from collector_core.pipeline_factory import get_pipeline_driver  # noqa: E402

DOMAIN = "{domain}"

if __name__ == "__main__":
    get_pipeline_driver(DOMAIN).main()
'''

# Template for acquire_worker.py
ACQUIRE_WORKER_TEMPLATE = '''#!/usr/bin/env python3
"""
acquire_worker.py (v2.0)

Deprecated compatibility shim for `dc run --pipeline {domain} --stage acquire`.
Removal target: v3.0.
"""
from __future__ import annotations

import warnings
from pathlib import Path

from collector_core.acquire_strategies import (  # noqa: E402
    AcquireContext,
    Limits,
    RetryConfig,
    Roots,
    RunMode,
    _http_download_with_resume,
)
from collector_core.generic_workers import main_acquire  # noqa: E402

DOMAIN = "{domain}"
DEPRECATION_MESSAGE = (
    "acquire_worker.py is deprecated; use `dc run --pipeline {domain} --stage acquire` instead. "
    "Removal target: v3.0."
)

__all__ = [
    "AcquireContext",
    "Limits",
    "RetryConfig",
    "Roots",
    "RunMode",
    "_http_download_with_resume",
    "main",
]


def main() -> None:
    warnings.warn(DEPRECATION_MESSAGE, DeprecationWarning, stacklevel=2)
    main_acquire(DOMAIN, repo_root=Path(__file__).resolve().parents[1])


if __name__ == "__main__":
    main()
'''

# Template for merge_worker.py
MERGE_WORKER_TEMPLATE = '''#!/usr/bin/env python3
"""
merge_worker.py (v2.0)

Deprecated compatibility shim for `dc run --pipeline {domain} --stage merge`.
Removal target: v3.0.
"""
from __future__ import annotations

import warnings

from collector_core.generic_workers import main_merge  # noqa: E402

DOMAIN = "{domain}"
DEPRECATION_MESSAGE = (
    "merge_worker.py is deprecated; use `dc run --pipeline {domain} --stage merge` instead. "
    "Removal target: v3.0."
)


def main() -> None:
    warnings.warn(DEPRECATION_MESSAGE, DeprecationWarning, stacklevel=2)
    main_merge(DOMAIN)


if __name__ == "__main__":
    main()
'''

# Template for catalog_builder.py
CATALOG_BUILDER_TEMPLATE = '''#!/usr/bin/env python3
"""
catalog_builder.py (v2.0)

Thin wrapper that delegates to the spec-driven generic catalog builder.
"""
from __future__ import annotations
from pathlib import Path
from collector_core.generic_workers import main_catalog  # noqa: E402

DOMAIN = "{domain}"


def main() -> None:
    main_catalog(DOMAIN)


if __name__ == "__main__":
    main()
'''

# Template for review_queue.py
REVIEW_QUEUE_TEMPLATE = '''#!/usr/bin/env python3
"""
review_queue.py (v2.0)

Thin wrapper that delegates to the spec-driven review queue helper.
"""
from __future__ import annotations
from pathlib import Path
from collector_core.generic_workers import main_review_queue  # noqa: E402

DOMAIN = "{domain}"


def main() -> None:
    main_review_queue(DOMAIN)


if __name__ == "__main__":
    main()
'''

# Template for legacy/run_pipeline.sh
RUN_PIPELINE_TEMPLATE = """#!/usr/bin/env bash
#
# run_pipeline.sh (v2.0)
#
# Deprecated compatibility shim for the {domain} pipeline.
# Use: dc run --pipeline {domain} --stage <stage>
# Removal target: v3.0.
#
set -euo pipefail

YELLOW='\\033[0;33m'
RED='\\033[0;31m'
NC='\\033[0m'

TARGETS=""
EXECUTE=""
STAGE=""
EXTRA_ARGS=()

usage() {{
  cat << 'EOM'
Deprecated pipeline wrapper (v2)

Required:
  --targets FILE          Path to targets YAML
  --stage STAGE           Stage to run: classify, acquire, yellow_screen, merge, catalog, review

Options:
  --execute               Perform actions (default is dry-run/plan only)
  --                      Pass remaining args directly to the stage command
  -h, --help              Show this help

Notes:
  - This shim no longer resolves queue paths automatically.
  - Provide stage arguments after -- (for example: --queue, --bucket, --targets-yaml).
EOM
}}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --targets) TARGETS="$2"; shift 2 ;;
    --stage) STAGE="$2"; shift 2 ;;
    --execute) EXECUTE="--execute"; shift ;;
    --) shift; EXTRA_ARGS+=("$@"); break ;;
    -h|--help) usage; exit 0 ;;
    *) EXTRA_ARGS+=("$1"); shift ;;
  esac
done

if [[ -z "$TARGETS" || -z "$STAGE" ]]; then
  echo -e "${{RED}}--targets and --stage are required${{NC}}"
  usage
  exit 1
fi

if [[ ! -f "$TARGETS" ]]; then
  echo -e "${{RED}}targets file not found: $TARGETS${{NC}}"
  exit 1
fi

echo -e "${{YELLOW}}[deprecated] run_pipeline.sh is deprecated; use 'dc run --pipeline {domain} --stage <stage>' instead. Removal target: v3.0.${{NC}}" >&2

SCRIPT_DIR="$(cd "$(dirname "${{BASH_SOURCE[0]}}")" && pwd)"
REPO_ROOT="$(cd "${{SCRIPT_DIR}}/.." && pwd)"
export PYTHONPATH="${{REPO_ROOT}}:${{PYTHONPATH:-}}"

case "$STAGE" in
  classify)
    NO_FETCH=""
    if [[ -z "$EXECUTE" ]]; then
      NO_FETCH="--no-fetch"
    fi
    python -m collector_core.dc_cli pipeline {domain} -- --targets "$TARGETS" $NO_FETCH "${{EXTRA_ARGS[@]}}"
    ;;
  acquire)
    python -m collector_core.dc_cli run --pipeline {domain} --stage acquire -- --targets-yaml "$TARGETS" $EXECUTE "${{EXTRA_ARGS[@]}}"
    ;;
  yellow_screen)
    python -m collector_core.dc_cli run --pipeline {domain} --stage yellow_screen -- --targets "$TARGETS" $EXECUTE "${{EXTRA_ARGS[@]}}"
    ;;
  merge)
    python -m collector_core.dc_cli run --pipeline {domain} --stage merge -- --targets "$TARGETS" $EXECUTE "${{EXTRA_ARGS[@]}}"
    ;;
  catalog)
    python -m collector_core.dc_cli catalog-builder --pipeline {domain} -- --targets "$TARGETS" "${{EXTRA_ARGS[@]}}"
    ;;
  review)
    python -m collector_core.dc_cli review-queue --pipeline {domain} -- --targets "$TARGETS" "${{EXTRA_ARGS[@]}}"
    ;;
  *)
    echo -e "${{RED}}Unknown stage: $STAGE${{NC}}"
    usage
    exit 1
    ;;
esac
"""

# Template for yellow_scrubber.py
YELLOW_SCRUBBER_TEMPLATE = '''#!/usr/bin/env python3
"""
yellow_scrubber.py (v2.0)

Thin wrapper that delegates to the spec-driven yellow review helper.
"""
from __future__ import annotations
from pathlib import Path
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

Deprecated compatibility shim for `dc run --pipeline {domain} --stage yellow_screen`.
Removal target: v3.0.
"""
from __future__ import annotations

import warnings

from collector_core.yellow_screen_dispatch import main_yellow_screen  # noqa: E402

DOMAIN = "{domain}"
DEPRECATION_MESSAGE = (
    "yellow_screen_worker.py is deprecated; use `dc run --pipeline {domain} --stage yellow_screen` instead. "
    "Removal target: v3.0."
)

if __name__ == "__main__":
    warnings.warn(DEPRECATION_MESSAGE, DeprecationWarning, stacklevel=2)
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
        "acquire_worker.py", ACQUIRE_WORKER_TEMPLATE, skip_if_large=True, max_lines_for_overwrite=200
    ),
    WrapperTemplate("merge_worker.py", MERGE_WORKER_TEMPLATE, skip_if_large=False),
    WrapperTemplate("catalog_builder.py", CATALOG_BUILDER_TEMPLATE, skip_if_large=False),
    WrapperTemplate("review_queue.py", REVIEW_QUEUE_TEMPLATE, skip_if_large=False),
    WrapperTemplate(
        "yellow_scrubber.py",
        YELLOW_SCRUBBER_TEMPLATE,
        skip_if_large=True,
        max_lines_for_overwrite=30,
    ),
    WrapperTemplate("yellow_screen_worker.py", YELLOW_SCREEN_WORKER_TEMPLATE, skip_if_large=False),
    WrapperTemplate("legacy/run_pipeline.sh", RUN_PIPELINE_TEMPLATE, skip_if_large=False),
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
        file_path.parent.mkdir(parents=True, exist_ok=True)
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
                    tmp_path = file_path.with_suffix(file_path.suffix + ".tmp")
                    tmp_path.write_text(expected_content)
                    tmp_path.replace(file_path)
                    issues.append(f"UPDATED: {file_path}")
        else:
            if check_only:
                issues.append(f"MISSING: {file_path}")
            elif dry_run:
                issues.append(f"WOULD CREATE: {file_path}")
            else:
                tmp_path = file_path.with_suffix(file_path.suffix + ".tmp")
                tmp_path.write_text(expected_content)
                tmp_path.replace(file_path)
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
        "--write",
        action="store_true",
        help="Write updated wrappers (default is check-only).",
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

    if not args.write and not args.check and not args.dry_run:
        args.check = True

    domains = [args.domain] if args.domain else list_pipelines()
    all_issues: list[str] = []

    for domain in domains:
        if args.verbose:
            print(f"Processing {domain}...")
        issues = sync_domain(domain, check_only=args.check and not args.write, dry_run=args.dry_run)
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
                "\nWrapper sync check failed. Run 'python -m tools.sync_pipeline_wrappers' to fix."
            )
            return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
