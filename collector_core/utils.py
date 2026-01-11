"""
collector_core/utils.py

Shared utility functions for the Dataset Collector.
Consolidates common operations that were previously duplicated across modules.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import logging
import re
import tarfile
import time
import unicodedata
import zipfile
from collections.abc import Iterable, Iterator
from pathlib import Path, PurePosixPath
from typing import Any

logger = logging.getLogger(__name__)


def utc_now() -> str:
    """Return current UTC time in ISO 8601 format."""
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def ensure_dir(path: Path) -> None:
    """Create directory and parents if they don't exist."""
    path.mkdir(parents=True, exist_ok=True)


def sha256_bytes(data: bytes) -> str:
    """Compute SHA-256 hash of bytes."""
    return hashlib.sha256(data).hexdigest()


def sha256_text(text: str) -> str:
    """Compute SHA-256 hash of normalized text."""
    normalized = re.sub(r"\s+", " ", (text or "").strip())
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def sha256_file(path: Path) -> str | None:
    """Compute SHA-256 hash of a file. Returns None on error."""
    try:
        h = hashlib.sha256()
        with path.open("rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                h.update(chunk)
        return h.hexdigest()
    except Exception:
        logger.warning("Failed to compute SHA-256 hash for %s", path, exc_info=True)
        return None


def normalize_whitespace(text: str) -> str:
    """Collapse all whitespace to single spaces and strip."""
    return re.sub(r"\s+", " ", (text or "")).strip()


def lower(text: str) -> str:
    """Lowercase string, handling None."""
    return (text or "").lower()


def read_json(path: Path) -> dict[str, Any]:
    """Read JSON file and return as dict."""
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, obj: dict[str, Any], *, indent: int = 2) -> None:
    """Write dict to JSON file atomically."""
    ensure_dir(path.parent)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(obj, indent=indent, ensure_ascii=False) + "\n", encoding="utf-8")
    tmp_path.replace(path)


def read_jsonl(path: Path) -> Iterator[dict[str, Any]]:
    """Read JSONL file (supports .gz) and yield records."""
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    continue


def read_jsonl_list(path: Path) -> list[dict[str, Any]]:
    """Read JSONL file and return as list."""
    return list(read_jsonl(path))


def write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    """Write records to JSONL file (supports .gz)."""
    ensure_dir(path.parent)
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "wt", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def append_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    """Append records to JSONL file (supports .gz)."""
    ensure_dir(path.parent)
    if path.suffix == ".gz":
        with gzip.open(path, "ab") as f:
            for row in rows:
                f.write((json.dumps(row, ensure_ascii=False) + "\n").encode("utf-8"))
    else:
        with open(path, "a", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")


def safe_filename(
    s: str,
    max_length: int = 200,
    allow_unicode: bool = False,
) -> str:
    """
    Convert string to safe filename.

    - Removes or replaces dangerous characters
    - Prevents directory traversal
    - Handles Unicode normalization
    - Prevents reserved names on Windows
    """
    if not s:
        return "file"

    # Normalize Unicode
    if allow_unicode:
        s = unicodedata.normalize("NFKC", s)
    else:
        s = unicodedata.normalize("NFKD", s)
        s = s.encode("ascii", "ignore").decode("ascii")

    # Remove null bytes
    s = s.replace("\x00", "")

    # Replace directory separators and other dangerous chars
    dangerous = set('/<>:"\|?*\x00')
    s = "".join(c if c not in dangerous else "_" for c in s)

    # Remove leading/trailing dots and spaces
    s = s.strip(". ")

    # Replace runs of underscores/spaces
    s = re.sub(r"[_\s]+", "_", s)

    # Windows reserved names
    reserved = {
        "CON",
        "PRN",
        "AUX",
        "NUL",
        "COM1",
        "COM2",
        "COM3",
        "COM4",
        "COM5",
        "COM6",
        "COM7",
        "COM8",
        "COM9",
        "LPT1",
        "LPT2",
        "LPT3",
        "LPT4",
        "LPT5",
        "LPT6",
        "LPT7",
        "LPT8",
        "LPT9",
    }
    name_upper = s.upper().split(".")[0]
    if name_upper in reserved:
        s = f"_{s}"

    # Truncate
    if len(s) > max_length:
        # Preserve extension if present
        if "." in s:
            name, ext = s.rsplit(".", 1)
            ext = ext[:10]  # Limit extension length
            name = name[: max_length - len(ext) - 1]
            s = f"{name}.{ext}"
        else:
            s = s[:max_length]

    return s or "file"


DEFAULT_MAX_ARCHIVE_FILES = 10000
DEFAULT_MAX_ARCHIVE_BYTES = 512 * 1024 * 1024


def _normalize_archive_name(name: str) -> str:
    return (name or "").replace("\\", "/")


def _is_unsafe_archive_path(name: str) -> bool:
    normalized = _normalize_archive_name(name)
    if not normalized:
        return False
    if re.match(r"^[A-Za-z]:", normalized):
        return True
    path = PurePosixPath(normalized)
    return path.is_absolute() or ".." in path.parts


def _zipinfo_is_symlink(info: zipfile.ZipInfo) -> bool:
    mode = info.external_attr >> 16
    return (mode & 0o170000) == 0o120000


def validate_zip_archive(
    zf: zipfile.ZipFile,
    *,
    max_files: int = DEFAULT_MAX_ARCHIVE_FILES,
    max_total_size: int = DEFAULT_MAX_ARCHIVE_BYTES,
) -> None:
    total_size = 0
    file_count = 0
    for info in zf.infolist():
        name = info.filename
        if _is_unsafe_archive_path(name):
            raise ValueError(f"unsafe path in archive: {name}")
        if _zipinfo_is_symlink(info):
            raise ValueError(f"symlink entry in archive: {name}")
        if info.is_dir():
            continue
        file_count += 1
        if file_count > max_files:
            raise ValueError("archive file count exceeds limit")
        total_size += info.file_size
        if total_size > max_total_size:
            raise ValueError("archive total size exceeds limit")


def validate_tar_archive(
    tf: tarfile.TarFile,
    *,
    max_files: int = DEFAULT_MAX_ARCHIVE_FILES,
    max_total_size: int = DEFAULT_MAX_ARCHIVE_BYTES,
) -> None:
    total_size = 0
    file_count = 0
    for member in tf.getmembers():
        name = member.name
        if _is_unsafe_archive_path(name):
            raise ValueError(f"unsafe path in archive: {name}")
        if member.issym() or member.islnk():
            raise ValueError(f"symlink entry in archive: {name}")
        if not member.isfile():
            continue
        file_count += 1
        if file_count > max_files:
            raise ValueError("archive file count exceeds limit")
        total_size += member.size
        if total_size > max_total_size:
            raise ValueError("archive total size exceeds limit")


def contains_any(haystack: str, needles: list[str]) -> list[str]:
    """Return list of needles found in haystack (case-insensitive)."""
    h = lower(haystack)
    return [n for n in needles if n and lower(n) in h]


def coerce_int(val: Any, default: int | None = None) -> int | None:
    """Safely convert value to int, returning default on failure."""
    try:
        return int(val)
    except Exception:
        logger.debug("Failed to coerce value %r to int, using default %r", val, default)
        return default
