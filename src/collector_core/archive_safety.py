"""Archive extraction safety utilities.

This module provides safe archive extraction with protections against:
- Path traversal attacks (../, absolute paths)
- Symlink attacks
- Decompression bombs (max file count, max extracted size)
- Malicious filenames
"""

from __future__ import annotations

import logging
import os
import stat
import tarfile
import zipfile
from pathlib import Path
from typing import TYPE_CHECKING

from collector_core.stability import stable_api

if TYPE_CHECKING:
    from typing import BinaryIO

logger = logging.getLogger(__name__)

# Default limits for safe extraction
DEFAULT_MAX_FILES = 10_000
DEFAULT_MAX_EXTRACTED_BYTES = 10 * 1024 * 1024 * 1024  # 10 GB
DEFAULT_MAX_COMPRESSION_RATIO = 100  # Max ratio of extracted/compressed size


class ArchiveExtractionError(Exception):
    """Raised when archive extraction fails due to safety checks."""

    pass


class PathTraversalError(ArchiveExtractionError):
    """Raised when a path traversal attack is detected."""

    pass


class SymlinkError(ArchiveExtractionError):
    """Raised when a suspicious symlink is detected."""

    pass


class DecompressionBombError(ArchiveExtractionError):
    """Raised when a decompression bomb is detected."""

    pass


class TooManyFilesError(ArchiveExtractionError):
    """Raised when archive contains too many files."""

    pass


class ExtractedSizeLimitError(ArchiveExtractionError):
    """Raised when extracted size exceeds limit."""

    pass


@stable_api
def is_path_safe(member_path: str, dest_dir: Path) -> tuple[bool, str | None]:
    """Check if a member path is safe to extract.

    Args:
        member_path: The path from the archive member
        dest_dir: The destination directory for extraction

    Returns:
        Tuple of (is_safe, error_reason)
    """
    # Normalize the path
    normalized = os.path.normpath(member_path)

    # Check for absolute paths
    if os.path.isabs(normalized):
        return False, f"absolute_path:{member_path}"

    # Check for path traversal
    if normalized.startswith("..") or "/../" in normalized or normalized.endswith("/.."):
        return False, f"path_traversal:{member_path}"

    # Resolve the final path and ensure it's within dest_dir
    try:
        final_path = (dest_dir / normalized).resolve()
        dest_resolved = dest_dir.resolve()

        # Check that the final path is within the destination
        try:
            final_path.relative_to(dest_resolved)
        except ValueError:
            return False, f"escapes_dest:{member_path}"
    except (OSError, ValueError) as e:
        return False, f"path_resolution_error:{member_path}:{e}"

    return True, None


@stable_api
def safe_extract_zip(
    archive_path: Path,
    dest_dir: Path,
    *,
    max_files: int = DEFAULT_MAX_FILES,
    max_extracted_bytes: int = DEFAULT_MAX_EXTRACTED_BYTES,
    max_compression_ratio: int = DEFAULT_MAX_COMPRESSION_RATIO,
    allow_symlinks: bool = False,
) -> dict[str, object]:
    """Safely extract a ZIP archive with security checks.

    Args:
        archive_path: Path to the ZIP archive
        dest_dir: Destination directory for extraction
        max_files: Maximum number of files to extract
        max_extracted_bytes: Maximum total extracted size in bytes
        max_compression_ratio: Maximum compression ratio allowed
        allow_symlinks: Whether to allow symlinks (default False)

    Returns:
        Dict with extraction statistics

    Raises:
        ArchiveExtractionError: If any safety check fails
    """
    dest_dir = Path(dest_dir).resolve()
    dest_dir.mkdir(parents=True, exist_ok=True)

    stats: dict[str, object] = {
        "archive_path": str(archive_path),
        "dest_dir": str(dest_dir),
        "files_extracted": 0,
        "bytes_extracted": 0,
        "skipped_files": [],
        "errors": [],
    }

    compressed_size = archive_path.stat().st_size

    with zipfile.ZipFile(archive_path, "r") as zf:
        members = zf.infolist()

        # Check file count
        if len(members) > max_files:
            raise TooManyFilesError(
                f"Archive contains {len(members)} files, exceeds limit of {max_files}"
            )

        # Check total uncompressed size
        total_uncompressed = sum(m.file_size for m in members)
        if total_uncompressed > max_extracted_bytes:
            raise ExtractedSizeLimitError(
                f"Total uncompressed size {total_uncompressed} exceeds limit {max_extracted_bytes}"
            )

        # Check compression ratio
        if compressed_size > 0:
            ratio = total_uncompressed / compressed_size
            if ratio > max_compression_ratio:
                raise DecompressionBombError(
                    f"Compression ratio {ratio:.1f}x exceeds limit {max_compression_ratio}x"
                )

        extracted_bytes = 0
        extracted_files = 0

        for member in members:
            # Check path safety
            is_safe, reason = is_path_safe(member.filename, dest_dir)
            if not is_safe:
                raise PathTraversalError(f"Unsafe path in archive: {reason}")

            # Check for symlinks (ZIP doesn't normally have symlinks, but check anyway)
            # External file attributes can indicate symlinks on Unix
            if member.external_attr >> 16:
                mode = member.external_attr >> 16
                if stat.S_ISLNK(mode) and not allow_symlinks:
                    raise SymlinkError(f"Symlink not allowed: {member.filename}")

            # Extract the file
            target_path = dest_dir / member.filename

            if member.is_dir():
                target_path.mkdir(parents=True, exist_ok=True)
                continue

            # Track extracted size
            extracted_bytes += member.file_size
            if extracted_bytes > max_extracted_bytes:
                raise ExtractedSizeLimitError(
                    f"Extracted size {extracted_bytes} exceeds limit {max_extracted_bytes}"
                )

            # Ensure parent directory exists
            target_path.parent.mkdir(parents=True, exist_ok=True)

            # Extract with streaming to catch bombs during extraction
            with zf.open(member) as src, open(target_path, "wb") as dst:
                chunk_size = 1024 * 1024  # 1MB chunks
                written = 0
                while True:
                    chunk = src.read(chunk_size)
                    if not chunk:
                        break
                    written += len(chunk)
                    if written > member.file_size * 1.1:  # Allow 10% overhead
                        raise DecompressionBombError(
                            f"File {member.filename} expanded beyond declared size"
                        )
                    dst.write(chunk)

            extracted_files += 1

        stats["files_extracted"] = extracted_files
        stats["bytes_extracted"] = extracted_bytes

    logger.info(
        "ZIP extraction complete: archive=%s files=%d bytes=%d",
        archive_path,
        extracted_files,
        extracted_bytes,
    )
    return stats


@stable_api
def safe_extract_tar(
    archive_path: Path,
    dest_dir: Path,
    *,
    max_files: int = DEFAULT_MAX_FILES,
    max_extracted_bytes: int = DEFAULT_MAX_EXTRACTED_BYTES,
    max_compression_ratio: int = DEFAULT_MAX_COMPRESSION_RATIO,
    allow_symlinks: bool = False,
) -> dict[str, object]:
    """Safely extract a TAR archive with security checks.

    Supports: .tar, .tar.gz, .tgz, .tar.bz2, .tar.xz

    Args:
        archive_path: Path to the TAR archive
        dest_dir: Destination directory for extraction
        max_files: Maximum number of files to extract
        max_extracted_bytes: Maximum total extracted size in bytes
        max_compression_ratio: Maximum compression ratio allowed
        allow_symlinks: Whether to allow symlinks (default False)

    Returns:
        Dict with extraction statistics

    Raises:
        ArchiveExtractionError: If any safety check fails
    """
    dest_dir = Path(dest_dir).resolve()
    dest_dir.mkdir(parents=True, exist_ok=True)

    stats: dict[str, object] = {
        "archive_path": str(archive_path),
        "dest_dir": str(dest_dir),
        "files_extracted": 0,
        "bytes_extracted": 0,
        "skipped_files": [],
        "errors": [],
    }

    compressed_size = archive_path.stat().st_size

    # Determine mode based on extension
    suffix = archive_path.suffix.lower()
    name_lower = archive_path.name.lower()
    if suffix == ".gz" or name_lower.endswith(".tar.gz") or suffix == ".tgz":
        mode = "r:gz"
    elif suffix == ".bz2" or name_lower.endswith(".tar.bz2"):
        mode = "r:bz2"
    elif suffix == ".xz" or name_lower.endswith(".tar.xz"):
        mode = "r:xz"
    else:
        mode = "r"

    with tarfile.open(archive_path, mode) as tf:
        members = tf.getmembers()

        # Check file count
        if len(members) > max_files:
            raise TooManyFilesError(
                f"Archive contains {len(members)} files, exceeds limit of {max_files}"
            )

        # Calculate total size
        total_uncompressed = sum(m.size for m in members if m.isfile())
        if total_uncompressed > max_extracted_bytes:
            raise ExtractedSizeLimitError(
                f"Total uncompressed size {total_uncompressed} exceeds limit {max_extracted_bytes}"
            )

        # Check compression ratio
        if compressed_size > 0:
            ratio = total_uncompressed / compressed_size
            if ratio > max_compression_ratio:
                raise DecompressionBombError(
                    f"Compression ratio {ratio:.1f}x exceeds limit {max_compression_ratio}x"
                )

        extracted_bytes = 0
        extracted_files = 0

        for member in members:
            # Check path safety
            is_safe, reason = is_path_safe(member.name, dest_dir)
            if not is_safe:
                raise PathTraversalError(f"Unsafe path in archive: {reason}")

            # Check for symlinks and hardlinks
            if member.issym() or member.islnk():
                if not allow_symlinks:
                    raise SymlinkError(f"Symlink/hardlink not allowed: {member.name}")
                # Even if allowed, validate link target
                if member.linkname:
                    link_safe, link_reason = is_path_safe(member.linkname, dest_dir)
                    if not link_safe:
                        raise SymlinkError(f"Symlink target unsafe: {link_reason}")

            # Check for device files
            if member.isdev():
                raise ArchiveExtractionError(
                    f"Device file not allowed: {member.name}"
                )

            target_path = dest_dir / member.name

            if member.isdir():
                target_path.mkdir(parents=True, exist_ok=True)
                continue

            if member.isfile():
                # Track extracted size
                extracted_bytes += member.size
                if extracted_bytes > max_extracted_bytes:
                    raise ExtractedSizeLimitError(
                        f"Extracted size {extracted_bytes} exceeds limit {max_extracted_bytes}"
                    )

                # Ensure parent directory exists
                target_path.parent.mkdir(parents=True, exist_ok=True)

                # Extract with streaming
                src = tf.extractfile(member)
                if src is None:
                    continue

                with open(target_path, "wb") as dst:
                    chunk_size = 1024 * 1024  # 1MB chunks
                    written = 0
                    while True:
                        chunk = src.read(chunk_size)
                        if not chunk:
                            break
                        written += len(chunk)
                        if written > member.size * 1.1:  # Allow 10% overhead
                            raise DecompressionBombError(
                                f"File {member.name} expanded beyond declared size"
                            )
                        dst.write(chunk)

                extracted_files += 1

        stats["files_extracted"] = extracted_files
        stats["bytes_extracted"] = extracted_bytes

    logger.info(
        "TAR extraction complete: archive=%s files=%d bytes=%d",
        archive_path,
        extracted_files,
        extracted_bytes,
    )
    return stats


@stable_api
def safe_extract(
    archive_path: Path,
    dest_dir: Path,
    *,
    max_files: int = DEFAULT_MAX_FILES,
    max_extracted_bytes: int = DEFAULT_MAX_EXTRACTED_BYTES,
    max_compression_ratio: int = DEFAULT_MAX_COMPRESSION_RATIO,
    allow_symlinks: bool = False,
) -> dict[str, object]:
    """Safely extract an archive with automatic format detection.

    Supports: .zip, .tar, .tar.gz, .tgz, .tar.bz2, .tar.xz

    Args:
        archive_path: Path to the archive
        dest_dir: Destination directory for extraction
        max_files: Maximum number of files to extract
        max_extracted_bytes: Maximum total extracted size in bytes
        max_compression_ratio: Maximum compression ratio allowed
        allow_symlinks: Whether to allow symlinks (default False)

    Returns:
        Dict with extraction statistics

    Raises:
        ArchiveExtractionError: If any safety check fails
        ValueError: If archive format is not supported
    """
    archive_path = Path(archive_path)
    name_lower = archive_path.name.lower()

    if name_lower.endswith(".zip"):
        return safe_extract_zip(
            archive_path,
            dest_dir,
            max_files=max_files,
            max_extracted_bytes=max_extracted_bytes,
            max_compression_ratio=max_compression_ratio,
            allow_symlinks=allow_symlinks,
        )
    elif (
        name_lower.endswith(".tar")
        or name_lower.endswith(".tar.gz")
        or name_lower.endswith(".tgz")
        or name_lower.endswith(".tar.bz2")
        or name_lower.endswith(".tar.xz")
    ):
        return safe_extract_tar(
            archive_path,
            dest_dir,
            max_files=max_files,
            max_extracted_bytes=max_extracted_bytes,
            max_compression_ratio=max_compression_ratio,
            allow_symlinks=allow_symlinks,
        )
    else:
        raise ValueError(f"Unsupported archive format: {archive_path.suffix}")
