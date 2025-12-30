from __future__ import annotations

import argparse
import shutil
from collections.abc import Iterable
from pathlib import Path

OUTPUT_DIR_NAMES = {
    "raw",
    "screened_yellow",
    "combined",
    "_logs",
    "_ledger",
    "_queues",
    "_manifests",
    "_catalogs",
    "_pitches",
}


def _iter_candidates(repo_root: Path) -> Iterable[Path]:
    for path in repo_root.rglob("__pycache__"):
        if path.is_dir():
            yield path
    for path in repo_root.rglob("*.pyc"):
        if path.is_file():
            yield path
    for path in repo_root.rglob("*"):
        if path.is_dir() and path.name in OUTPUT_DIR_NAMES:
            yield path


def _dedupe(paths: Iterable[Path]) -> list[Path]:
    seen = set()
    ordered: list[Path] = []
    for path in paths:
        resolved = path.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        ordered.append(path)
    return ordered


def _print_plan(paths: list[Path], repo_root: Path) -> None:
    if not paths:
        print("No cleanup targets found.")
        return
    print("Cleanup targets:")
    for path in paths:
        rel = path.resolve().relative_to(repo_root)
        print(f"  - {rel}")


def _remove_paths(paths: list[Path]) -> None:
    for path in paths:
        if path.is_dir():
            shutil.rmtree(path, ignore_errors=True)
        elif path.exists():
            path.unlink()


def main() -> int:
    ap = argparse.ArgumentParser(description="Clean local artifacts from the repo tree.")
    ap.add_argument("--repo-root", default=".", help="Repository root (default: .)")
    ap.add_argument("--yes", action="store_true", help="Delete files without prompting")
    args = ap.parse_args()

    repo_root = Path(args.repo_root).expanduser().resolve()
    candidates = _dedupe(_iter_candidates(repo_root))
    _print_plan(candidates, repo_root)

    if not candidates:
        return 0
    if not args.yes:
        print("\nRe-run with --yes to delete these paths.")
        return 1

    _remove_paths(candidates)
    print("\nCleanup complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
