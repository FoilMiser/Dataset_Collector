"""Output contract validation CLI tool.

This tool validates output artifacts against the output contract schema.
It can be run in CI to catch contract violations early.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from collector_core.output_contract import (  # noqa: F401
    REQUIRED_FIELDS,
    normalize_output_record,
    sha256_text,
    utc_now,
    validate_output_contract,
)
from collector_core.utils.io import read_jsonl


def validate_output_files(root: Path, verbose: bool = False) -> tuple[int, int, list[str]]:
    """Validate all output JSONL files against the output contract.

    Args:
        root: Root directory to search for output files
        verbose: If True, print detailed validation messages

    Returns:
        Tuple of (files_checked, records_checked, errors)
    """
    errors: list[str] = []
    files_checked = 0
    records_checked = 0

    # Look for screened output files (yellow screen output)
    output_patterns = [
        "screened/**/*.jsonl",
        "screened/**/*.jsonl.gz",
        "screened/**/*.jsonl.zst",
        "final/**/*.jsonl",
        "final/**/*.jsonl.gz",
        "final/**/*.jsonl.zst",
    ]

    for pattern in output_patterns:
        for file_path in root.glob(pattern):
            if not file_path.is_file():
                continue

            files_checked += 1
            if verbose:
                print(f"Checking: {file_path}")

            try:
                records = list(read_jsonl(file_path))
                for i, record in enumerate(records):
                    records_checked += 1
                    context = f"{file_path}:record[{i}]"
                    try:
                        validate_output_contract(record, context)
                    except ValueError as e:
                        errors.append(str(e))
            except Exception as e:
                errors.append(f"Failed to read {file_path}: {e}")

    return files_checked, records_checked, errors


def main(argv: list[str] | None = None) -> int:
    """Main entry point for output contract validation CLI.

    Args:
        argv: Command line arguments (defaults to sys.argv[1:])

    Returns:
        Exit code (0 for success, 1 for validation failures)
    """
    parser = argparse.ArgumentParser(
        description="Validate output artifacts against the output contract."
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=Path("."),
        help="Root directory to search for output files (default: .)",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Print detailed validation messages",
    )
    parser.add_argument(
        "--require-files",
        action="store_true",
        help="Fail if no output files are found to validate",
    )

    args = parser.parse_args(argv)

    root = args.root.expanduser().resolve()
    if not root.is_dir():
        print(f"Error: {root} is not a directory", file=sys.stderr)
        return 1

    files_checked, records_checked, errors = validate_output_files(root, args.verbose)

    print(f"Files checked: {files_checked}")
    print(f"Records checked: {records_checked}")

    if errors:
        print(f"\nValidation errors ({len(errors)}):", file=sys.stderr)
        for error in errors[:10]:  # Show first 10 errors
            print(f"  - {error}", file=sys.stderr)
        if len(errors) > 10:
            print(f"  ... and {len(errors) - 10} more errors", file=sys.stderr)
        return 1

    if args.require_files and files_checked == 0:
        print("Error: No output files found to validate", file=sys.stderr)
        return 1

    print("Output contract validation passed!")
    return 0


__all__ = [
    "REQUIRED_FIELDS",
    "normalize_output_record",
    "sha256_text",
    "utc_now",
    "validate_output_contract",
    "validate_output_files",
    "main",
]
