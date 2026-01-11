#!/usr/bin/env python3
"""
sync_pipeline_wrappers.py

One-command regeneration of all thin wrappers for pipeline directories.
Can also run in lint mode (--check) for CI to verify wrappers are up-to-date.

Usage:
    python -m tools.sync_pipeline_wrappers          # Regenerate all wrappers
    python -m tools.sync_pipeline_wrappers --check  # Check without writing (CI mode)
    python -m tools.sync_pipeline_wrappers --dry-run # Show what would be written
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import NamedTuple

import collector_core.pipeline_specs_registry  # noqa: E402,F401
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

Thin wrapper that delegates to the spec-driven generic acquire worker.
"""
from __future__ import annotations
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
    main_acquire(DOMAIN, repo_root=Path(__file__).resolve().parents[1])


if __name__ == "__main__":
    main()
'''

# Template for merge_worker.py
MERGE_WORKER_TEMPLATE = '''#!/usr/bin/env python3
"""
merge_worker.py (v2.0)

Thin wrapper that delegates to the spec-driven generic merge worker.
"""
from __future__ import annotations
from pathlib import Path
from collector_core import merge as core_merge  # noqa: E402
from collector_core.generic_workers import main_merge  # noqa: E402
from collector_core.pipeline_spec import get_pipeline_spec  # noqa: E402

DOMAIN = "{domain}"
SPEC = get_pipeline_spec(DOMAIN)
if SPEC is None:
    raise SystemExit(f"Unknown pipeline domain: {{DOMAIN}}")

PIPELINE_ID = SPEC.pipeline_id
DEFAULT_ROOTS = core_merge.default_merge_roots(SPEC.prefix)

read_jsonl = core_merge.read_jsonl
write_json = core_merge.write_json


def resolve_roots(cfg: dict) -> core_merge.Roots:
    return core_merge.resolve_roots(cfg, DEFAULT_ROOTS)


def merge_records(cfg: dict, roots: core_merge.Roots, execute: bool) -> dict:
    return core_merge.merge_records(cfg, roots, execute, pipeline_id=PIPELINE_ID)


def main() -> None:
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

# Template for run_pipeline.sh
RUN_PIPELINE_TEMPLATE = """#!/usr/bin/env bash
#
# run_pipeline.sh (v2.0)
#
# Wrapper script for the {domain} pipeline using the unified dc CLI.
#
set -euo pipefail

RED='\\033[0;31m'
BLUE='\\033[0;34m'
NC='\\033[0m'

TARGETS=""
EXECUTE=""
STAGE="all"
LIMIT_TARGETS=""
LIMIT_FILES=""
WORKERS="4"

usage() {{
  cat << 'EOM'
Pipeline wrapper (v2)

Required:
  --targets FILE          Path to targets YAML

Options:
  --execute               Perform actions (default is dry-run/plan only)
  --stage STAGE           Stage to run: all, classify, acquire_green, acquire_yellow, \\
                          screen_yellow, merge, catalog, review
  --limit-targets N       Limit number of queue rows processed
  --limit-files N         Limit files per target during acquisition
  --workers N             Parallel workers for acquisition (default: 4)
  -h, --help              Show this help
EOM
}}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --targets) TARGETS="$2"; shift 2 ;;
    --stage) STAGE="$2"; shift 2 ;;
    --execute) EXECUTE="--execute"; shift ;;
    --limit-targets) LIMIT_TARGETS="$2"; shift 2 ;;
    --limit-files) LIMIT_FILES="$2"; shift 2 ;;
    --workers) WORKERS="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo -e "${{RED}}Unknown option: $1${{NC}}"; usage; exit 1 ;;
  esac
done

if [[ -z "$TARGETS" ]]; then
  echo -e "${{RED}}--targets is required${{NC}}"
  usage
  exit 1
fi

if [[ ! -f "$TARGETS" ]]; then
  echo -e "${{RED}}targets file not found: $TARGETS${{NC}}"
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${{BASH_SOURCE[0]}}")" && pwd)"
REPO_ROOT="$(cd "${{SCRIPT_DIR}}/.." && pwd)"
export PYTHONPATH="${{REPO_ROOT}}:${{PYTHONPATH:-}}"

QUEUES_ROOT=$(python - << PY
from pathlib import Path
from collector_core.config_validator import read_yaml
from collector_core.pipeline_spec import get_pipeline_spec
cfg = read_yaml(Path("${{TARGETS}}"), schema_name="targets") or {{}}
spec = get_pipeline_spec("{domain}")
prefix = spec.prefix if spec else "{domain}"
print(cfg.get("globals", {{}}).get("queues_root", f"/data/{{prefix}}/_queues"))
PY
)
CATALOGS_ROOT=$(python - << PY
from pathlib import Path
from collector_core.config_validator import read_yaml
from collector_core.pipeline_spec import get_pipeline_spec
cfg = read_yaml(Path("${{TARGETS}}"), schema_name="targets") or {{}}
spec = get_pipeline_spec("{domain}")
prefix = spec.prefix if spec else "{domain}"
print(cfg.get("globals", {{}}).get("catalogs_root", f"/data/{{prefix}}/_catalogs"))
PY
)
LIMIT_TARGETS_ARG=""; [[ -n "$LIMIT_TARGETS" ]] && LIMIT_TARGETS_ARG="--limit-targets $LIMIT_TARGETS"
LIMIT_FILES_ARG=""; [[ -n "$LIMIT_FILES" ]] && LIMIT_FILES_ARG="--limit-files $LIMIT_FILES"

run_classify() {{
  echo -e "${{BLUE}}== Stage: classify ==${{NC}}"
  local no_fetch=""
  if [[ -z "$EXECUTE" ]]; then
    no_fetch="--no-fetch"
  fi
  python -m collector_core.dc_cli pipeline {domain} -- --targets "$TARGETS" $no_fetch
}}

run_review() {{
  local queue_file="$QUEUES_ROOT/yellow_pipeline.jsonl"
  echo -e "${{BLUE}}== Stage: review ==${{NC}}"
  python -m collector_core.generic_workers --domain {domain} review-queue -- --queue "$queue_file" --targets "$TARGETS" --limit 50 || true
}}

run_acquire() {{
  local bucket="$1"
  local queue_file="$QUEUES_ROOT/${{bucket}}_download.jsonl"
  if [[ "$bucket" == "yellow" ]]; then
    queue_file="$QUEUES_ROOT/yellow_pipeline.jsonl"
  fi
  if [[ ! -f "$queue_file" ]]; then
    echo -e "${{RED}}Queue not found: $queue_file${{NC}}"
    exit 1
  fi
  echo -e "${{BLUE}}== Stage: acquire_${{bucket}} ==${{NC}}"
  python -m collector_core.dc_cli run --pipeline {domain} --stage acquire -- \\
    --queue "$queue_file" \\
    --targets-yaml "$TARGETS" \\
    --bucket "$bucket" \\
    --workers "$WORKERS" \\
    $EXECUTE \\
    $LIMIT_TARGETS_ARG \\
    $LIMIT_FILES_ARG
}}

run_screen_yellow() {{
  local queue_file="$QUEUES_ROOT/yellow_pipeline.jsonl"
  if [[ ! -f "$queue_file" ]]; then
    echo -e "${{RED}}Queue not found: $queue_file${{NC}}"
    exit 1
  fi
  echo -e "${{BLUE}}== Stage: screen_yellow ==${{NC}}"
  python -m collector_core.dc_cli run --pipeline {domain} --stage yellow_screen -- \\
    --targets "$TARGETS" \\
    --queue "$queue_file" \\
    $EXECUTE
}}

run_merge() {{
  echo -e "${{BLUE}}== Stage: merge ==${{NC}}"
  python -m collector_core.dc_cli run --pipeline {domain} --stage merge -- --targets "$TARGETS" $EXECUTE
}}

run_catalog() {{
  echo -e "${{BLUE}}== Stage: catalog ==${{NC}}"
  python -m collector_core.generic_workers --domain {domain} catalog -- --targets "$TARGETS" --output "${{CATALOGS_ROOT}}/catalog.json"
}}

case "$STAGE" in
  all)
    run_classify
    run_acquire green
    run_acquire yellow
    run_screen_yellow
    run_merge
    run_catalog
    ;;
  classify) run_classify ;;
  acquire_green) run_acquire green ;;
  acquire_yellow) run_acquire yellow ;;
  screen_yellow) run_screen_yellow ;;
  merge) run_merge ;;
  catalog) run_catalog ;;
  review) run_review ;;
  *) echo -e "${{RED}}Unknown stage: $STAGE${{NC}}"; usage; exit 1 ;;
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

Thin wrapper that delegates to the spec-driven yellow screen dispatch.
"""
from __future__ import annotations
from pathlib import Path
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
    WrapperTemplate("run_pipeline.sh", RUN_PIPELINE_TEMPLATE, skip_if_large=False),
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
                "\nWrapper sync check failed. Run 'python -m tools.sync_pipeline_wrappers' to fix."
            )
            return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
