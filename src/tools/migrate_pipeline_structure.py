#!/usr/bin/env python3
"""
Migration script for v3.0 pipeline structure consolidation.

Issue 2.3: Migrate domain logic from *_pipeline_v2/ directories into
collector_core/domains/<domain>/ and docs/pipelines/<domain>.md.

Usage:
    python src/tools/migrate_pipeline_structure.py --check    # Dry-run, show what would change
    python src/tools/migrate_pipeline_structure.py --execute  # Perform migration
    python src/tools/migrate_pipeline_structure.py --report   # Generate JSON report

This script is idempotent - running it multiple times produces the same result.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path


@dataclass
class MigrationAction:
    """A single migration action."""

    action: str  # "move", "copy", "create_dir", "update_deprecation", "skip"
    source: str | None
    destination: str | None
    reason: str


@dataclass
class MigrationReport:
    """Report of migration actions."""

    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    executed: bool = False
    actions: list[MigrationAction] = field(default_factory=list)
    moved_files: list[str] = field(default_factory=list)
    skipped_files: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "timestamp": self.timestamp,
            "executed": self.executed,
            "summary": {
                "total_actions": len(self.actions),
                "moved": len(self.moved_files),
                "skipped": len(self.skipped_files),
                "errors": len(self.errors),
            },
            "actions": [
                {
                    "action": a.action,
                    "source": a.source,
                    "destination": a.destination,
                    "reason": a.reason,
                }
                for a in self.actions
            ],
            "moved_files": self.moved_files,
            "skipped_files": self.skipped_files,
            "errors": self.errors,
        }


# Files that are pure boilerplate wrappers (no real logic)
WRAPPER_FILES = {
    "acquire_worker.py",
    "merge_worker.py",
    "yellow_screen_worker.py",
    "yellow_scrubber.py",
    "catalog_builder.py",
    "review_queue.py",
    "pipeline_driver.py",
    "pmc_worker.py",
}

# Files to always preserve/migrate
PRESERVE_FILES = {
    "README.md",
    "requirements.txt",
    "DOMAIN_PIPELINE_ADAPTATION.md",
}


def discover_pipelines(repo_root: Path) -> list[str]:
    """Discover all pipeline domains."""
    domains = []
    for path in sorted(repo_root.glob("*_pipeline_v2")):
        if path.is_dir():
            domain = path.name.removesuffix("_pipeline_v2")
            domains.append(domain)
    return domains


def analyze_pipeline_files(pipeline_dir: Path) -> tuple[list[Path], list[Path], list[Path]]:
    """Analyze files in a pipeline directory.

    Returns:
        (wrapper_files, real_logic_files, preserve_files)
    """
    wrappers = []
    real_logic = []
    preserve = []

    for path in pipeline_dir.iterdir():
        if path.is_dir():
            continue  # Skip directories like legacy/
        name = path.name
        if name in WRAPPER_FILES:
            wrappers.append(path)
        elif name in PRESERVE_FILES:
            preserve.append(path)
        elif name.endswith(".py"):
            # Check if it's a real logic file or another wrapper
            content = path.read_text(errors="replace")
            if "DEPRECATED" in content and "from collector_core" in content:
                wrappers.append(path)
            else:
                real_logic.append(path)
        else:
            preserve.append(path)

    return wrappers, real_logic, preserve


def plan_migration(repo_root: Path) -> MigrationReport:
    """Plan the migration without executing it."""
    report = MigrationReport()

    # Ensure target directories exist
    domains_dir = repo_root / "src" / "collector_core" / "domains"
    docs_pipelines_dir = repo_root / "docs" / "pipelines"

    if not domains_dir.exists():
        report.actions.append(
            MigrationAction(
                action="create_dir",
                source=None,
                destination=str(domains_dir),
                reason="Create collector_core/domains/ for domain-specific logic",
            )
        )

    if not docs_pipelines_dir.exists():
        report.actions.append(
            MigrationAction(
                action="create_dir",
                source=None,
                destination=str(docs_pipelines_dir),
                reason="Create docs/pipelines/ for domain documentation",
            )
        )

    # Analyze each pipeline
    for domain in discover_pipelines(repo_root):
        pipeline_dir = repo_root / f"{domain}_pipeline_v2"
        wrappers, real_logic, preserve = analyze_pipeline_files(pipeline_dir)

        # Plan wrapper file deprecation updates
        for path in wrappers:
            report.actions.append(
                MigrationAction(
                    action="update_deprecation",
                    source=str(path),
                    destination=None,
                    reason="Mark as deprecated wrapper (removal in v4.0)",
                )
            )
            report.skipped_files.append(str(path.relative_to(repo_root)))

        # Plan real logic file migration
        domain_target_dir = domains_dir / domain
        for path in real_logic:
            dest = domain_target_dir / path.name
            report.actions.append(
                MigrationAction(
                    action="move",
                    source=str(path),
                    destination=str(dest),
                    reason=f"Move real domain logic to collector_core/domains/{domain}/",
                )
            )
            report.moved_files.append(str(path.relative_to(repo_root)))

        # Plan README migration
        readme_path = pipeline_dir / "README.md"
        if readme_path.exists():
            dest = docs_pipelines_dir / f"{domain}.md"
            report.actions.append(
                MigrationAction(
                    action="copy",
                    source=str(readme_path),
                    destination=str(dest),
                    reason=f"Copy README to docs/pipelines/{domain}.md",
                )
            )

    return report


def execute_migration(repo_root: Path, report: MigrationReport) -> MigrationReport:
    """Execute the planned migration."""
    report.executed = True

    for action in report.actions:
        try:
            if action.action == "create_dir":
                path = Path(action.destination)
                path.mkdir(parents=True, exist_ok=True)
                # Create __init__.py for Python packages
                if "collector_core" in str(path):
                    init_file = path / "__init__.py"
                    if not init_file.exists():
                        tmp_path = init_file.with_suffix(init_file.suffix + ".tmp")
                        tmp_path.write_text(
                            f'"""Domain-specific logic for {path.name}."""\n'
                        )
                        tmp_path.replace(init_file)

            elif action.action == "move":
                src = Path(action.source)
                dest = Path(action.destination)
                dest.parent.mkdir(parents=True, exist_ok=True)
                if src.exists():
                    shutil.move(str(src), str(dest))
                    # Create __init__.py if needed
                    init_file = dest.parent / "__init__.py"
                    if not init_file.exists():
                        tmp_path = init_file.with_suffix(init_file.suffix + ".tmp")
                        tmp_path.write_text(
                            f'"""Domain-specific logic for {dest.parent.name}."""\n'
                        )
                        tmp_path.replace(init_file)

            elif action.action == "copy":
                src = Path(action.source)
                dest = Path(action.destination)
                dest.parent.mkdir(parents=True, exist_ok=True)
                if src.exists():
                    shutil.copy2(str(src), str(dest))

            elif action.action == "update_deprecation":
                # This is handled separately - we just mark the intent
                pass

        except Exception as e:
            report.errors.append(f"{action.action} {action.source} -> {action.destination}: {e}")

    return report


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Migrate pipeline structure for v3.0 consolidation"
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Dry-run: show what would change without executing",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Execute the migration",
    )
    parser.add_argument(
        "--report",
        type=str,
        help="Output migration report to JSON file",
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=Path.cwd(),
        help="Repository root directory",
    )

    args = parser.parse_args()

    if not args.check and not args.execute and not args.report:
        parser.print_help()
        return 1

    repo_root = args.repo_root.resolve()

    # Validate repo root
    if not (repo_root / "src" / "collector_core").is_dir():
        print(f"Error: {repo_root} does not appear to be the Dataset_Collector repo root")
        return 1

    # Plan the migration
    report = plan_migration(repo_root)

    if args.check:
        print("Migration Plan (dry-run)")
        print("=" * 60)
        print(f"Total actions: {len(report.actions)}")
        print(f"Files to move: {len(report.moved_files)}")
        print(f"Wrapper files (deprecation only): {len(report.skipped_files)}")
        print()
        for action in report.actions:
            if action.action == "create_dir":
                print(f"  CREATE DIR: {action.destination}")
            elif action.action == "move":
                print(f"  MOVE: {action.source}")
                print(f"    -> {action.destination}")
            elif action.action == "copy":
                print(f"  COPY: {action.source}")
                print(f"    -> {action.destination}")
            elif action.action == "update_deprecation":
                print(f"  DEPRECATE: {action.source}")
        print()
        print("Run with --execute to perform the migration")

    if args.execute:
        print("Executing migration...")
        report = execute_migration(repo_root, report)
        if report.errors:
            print(f"Migration completed with {len(report.errors)} errors:")
            for error in report.errors:
                print(f"  ERROR: {error}")
            return 1
        print("Migration completed successfully.")
        print(f"  Moved: {len(report.moved_files)} files")
        print(f"  Skipped (wrappers): {len(report.skipped_files)} files")

    if args.report:
        report_path = Path(args.report)
        report_path.write_text(json.dumps(report.to_dict(), indent=2))
        print(f"Report written to: {report_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
