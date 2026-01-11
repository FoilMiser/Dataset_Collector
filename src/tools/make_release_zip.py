from __future__ import annotations

import argparse
import shutil
import sys
import tempfile
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

from tools import clean_repo_tree

sys.dont_write_bytecode = True

DEFAULT_IGNORE_PATTERNS = (
    ".git",
    ".pytest_cache",
    "__pycache__",
    "*.pyc",
    ".mypy_cache",
    ".ruff_cache",
    ".tox",
    ".nox",
    ".cache",
    "dist",
    "build",
    "*.egg-info",
    ".eggs",
    ".venv",
    "venv",
    ".coverage",
    "coverage.xml",
    "htmlcov",
    ".ipynb_checkpoints",
)


def _copy_repo(repo_root: Path, dest_root: Path) -> Path:
    target = dest_root / repo_root.name
    ignore = shutil.ignore_patterns(*DEFAULT_IGNORE_PATTERNS)
    shutil.copytree(repo_root, target, ignore=ignore, dirs_exist_ok=False)
    return target


def _run_clean_repo_tree(repo_root: Path) -> None:
    clean_repo_tree._remove_runtime_artifacts(repo_root)
    candidates = clean_repo_tree._dedupe(clean_repo_tree._iter_candidates(repo_root))
    clean_repo_tree._remove_paths(candidates)


def _write_zip(source_dir: Path, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with ZipFile(output_path, "w", compression=ZIP_DEFLATED) as zip_file:
        for path in source_dir.rglob("*"):
            if path.is_dir():
                continue
            arcname = path.relative_to(source_dir.parent)
            zip_file.write(path, arcname)


def main() -> int:
    parser = argparse.ArgumentParser(description="Create a clean release zip for the repo.")
    parser.add_argument("--repo-root", default=".", help="Repository root (default: .)")
    parser.add_argument(
        "--output",
        default="release.zip",
        help="Path to write the zip archive (default: release.zip)",
    )
    args = parser.parse_args()

    repo_root = Path(args.repo_root).expanduser().resolve()
    output_path = Path(args.output).expanduser().resolve()

    if not repo_root.exists():
        print(f"Repository root not found: {repo_root}", file=sys.stderr)
        return 1

    with tempfile.TemporaryDirectory(prefix="release_zip_") as temp_dir:
        temp_root = Path(temp_dir)
        temp_repo = _copy_repo(repo_root, temp_root)
        _run_clean_repo_tree(temp_repo)
        _write_zip(temp_repo, output_path)

    print(f"Wrote release archive to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
