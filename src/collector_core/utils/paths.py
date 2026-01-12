from __future__ import annotations

import re
import tarfile
import unicodedata
import zipfile
from pathlib import Path, PurePosixPath

DEFAULT_MAX_ARCHIVE_FILES = 10000
DEFAULT_MAX_ARCHIVE_BYTES = 512 * 1024 * 1024


def ensure_dir(path: Path) -> None:
    """Create directory and parents if they don't exist."""
    path.mkdir(parents=True, exist_ok=True)


def ensure_under_root(path: Path, root: Path) -> Path:
    """Ensure a path lives under the provided root."""
    root = root.resolve()
    resolved = path.resolve()
    if resolved == root or root in resolved.parents:
        return resolved
    raise ValueError(f"Path {resolved} is outside of root {root}")


def safe_join(root: Path, *parts: str) -> Path:
    """Join paths and ensure the result remains under root."""
    return ensure_under_root(root.joinpath(*parts), root)


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
    dangerous = set('/<>:"\\|?*\x00')
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
