"""Tests for archive extraction safety."""

from __future__ import annotations

import io
import os
import tarfile
import zipfile
from pathlib import Path

import pytest

from collector_core.archive_safety import (
    ArchiveExtractionError,
    DecompressionBombError,
    ExtractedSizeLimitError,
    PathTraversalError,
    SymlinkError,
    TooManyFilesError,
    is_path_safe,
    safe_extract,
    safe_extract_tar,
    safe_extract_zip,
)


class TestIsPathSafe:
    """Tests for the is_path_safe function."""

    def test_safe_path(self, tmp_path: Path) -> None:
        is_safe, reason = is_path_safe("data/file.txt", tmp_path)
        assert is_safe is True
        assert reason is None

    def test_absolute_path_blocked(self, tmp_path: Path) -> None:
        is_safe, reason = is_path_safe("/etc/passwd", tmp_path)
        assert is_safe is False
        assert reason is not None
        assert "absolute_path" in reason

    def test_path_traversal_blocked(self, tmp_path: Path) -> None:
        is_safe, reason = is_path_safe("../../../etc/passwd", tmp_path)
        assert is_safe is False
        assert reason is not None
        assert "path_traversal" in reason

    def test_path_traversal_middle_blocked(self, tmp_path: Path) -> None:
        is_safe, reason = is_path_safe("data/../../../etc/passwd", tmp_path)
        assert is_safe is False
        assert reason is not None

    def test_escape_via_dots(self, tmp_path: Path) -> None:
        is_safe, reason = is_path_safe("foo/bar/../../../baz", tmp_path)
        assert is_safe is False


class TestSafeExtractZip:
    """Tests for safe ZIP extraction."""

    def test_extract_normal_zip(self, tmp_path: Path) -> None:
        """Test extracting a normal ZIP file."""
        archive_path = tmp_path / "test.zip"
        dest_dir = tmp_path / "extracted"

        # Create a simple zip file
        with zipfile.ZipFile(archive_path, "w") as zf:
            zf.writestr("file1.txt", "Hello, World!")
            zf.writestr("subdir/file2.txt", "Nested file content")

        stats = safe_extract_zip(archive_path, dest_dir)

        assert stats["files_extracted"] == 2
        assert (dest_dir / "file1.txt").read_text() == "Hello, World!"
        assert (dest_dir / "subdir" / "file2.txt").read_text() == "Nested file content"

    def test_block_path_traversal_zip(self, tmp_path: Path) -> None:
        """Test that path traversal is blocked in ZIP files."""
        archive_path = tmp_path / "evil.zip"
        dest_dir = tmp_path / "extracted"

        # Create a malicious zip with path traversal
        with zipfile.ZipFile(archive_path, "w") as zf:
            zf.writestr("../../../etc/passwd", "malicious content")

        with pytest.raises(PathTraversalError):
            safe_extract_zip(archive_path, dest_dir)

    def test_block_absolute_path_zip(self, tmp_path: Path) -> None:
        """Test that absolute paths are blocked in ZIP files."""
        archive_path = tmp_path / "evil.zip"
        dest_dir = tmp_path / "extracted"

        # Create a malicious zip with absolute path
        with zipfile.ZipFile(archive_path, "w") as zf:
            zf.writestr("/etc/passwd", "malicious content")

        with pytest.raises(PathTraversalError):
            safe_extract_zip(archive_path, dest_dir)

    def test_block_too_many_files(self, tmp_path: Path) -> None:
        """Test that archives with too many files are blocked."""
        archive_path = tmp_path / "many.zip"
        dest_dir = tmp_path / "extracted"

        with zipfile.ZipFile(archive_path, "w") as zf:
            for i in range(100):
                zf.writestr(f"file{i}.txt", f"content{i}")

        with pytest.raises(TooManyFilesError):
            safe_extract_zip(archive_path, dest_dir, max_files=50)

    def test_block_extracted_size_limit(self, tmp_path: Path) -> None:
        """Test that archives exceeding size limit are blocked."""
        archive_path = tmp_path / "big.zip"
        dest_dir = tmp_path / "extracted"

        # Create a zip with large content
        with zipfile.ZipFile(archive_path, "w") as zf:
            zf.writestr("big.txt", "x" * 1000)

        with pytest.raises(ExtractedSizeLimitError):
            safe_extract_zip(archive_path, dest_dir, max_extracted_bytes=100)

    def test_block_decompression_bomb(self, tmp_path: Path) -> None:
        """Test that decompression bombs are detected."""
        archive_path = tmp_path / "bomb.zip"
        dest_dir = tmp_path / "extracted"

        # Create a zip with high compression ratio (lots of zeros)
        content = b"\x00" * (1024 * 1024)  # 1MB of zeros
        with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("zeros.bin", content)

        # The compression ratio of zeros should be very high
        archive_size = archive_path.stat().st_size

        # Only test if compression actually achieved high ratio
        if 1024 * 1024 / archive_size > 50:
            with pytest.raises(DecompressionBombError):
                safe_extract_zip(archive_path, dest_dir, max_compression_ratio=10)


class TestSafeExtractTar:
    """Tests for safe TAR extraction."""

    def test_extract_normal_tar(self, tmp_path: Path) -> None:
        """Test extracting a normal TAR file."""
        archive_path = tmp_path / "test.tar"
        dest_dir = tmp_path / "extracted"

        # Create a simple tar file
        with tarfile.open(archive_path, "w") as tf:
            # Add a file
            content = b"Hello, World!"
            info = tarfile.TarInfo(name="file1.txt")
            info.size = len(content)
            tf.addfile(info, io.BytesIO(content))

            # Add a nested file
            nested_content = b"Nested file content"
            info2 = tarfile.TarInfo(name="subdir/file2.txt")
            info2.size = len(nested_content)
            tf.addfile(info2, io.BytesIO(nested_content))

        stats = safe_extract_tar(archive_path, dest_dir)

        assert stats["files_extracted"] == 2
        assert (dest_dir / "file1.txt").read_bytes() == b"Hello, World!"
        assert (dest_dir / "subdir" / "file2.txt").read_bytes() == b"Nested file content"

    def test_extract_tar_gz(self, tmp_path: Path) -> None:
        """Test extracting a .tar.gz file."""
        archive_path = tmp_path / "test.tar.gz"
        dest_dir = tmp_path / "extracted"

        with tarfile.open(archive_path, "w:gz") as tf:
            content = b"Compressed content"
            info = tarfile.TarInfo(name="file.txt")
            info.size = len(content)
            tf.addfile(info, io.BytesIO(content))

        stats = safe_extract_tar(archive_path, dest_dir)

        assert stats["files_extracted"] == 1
        assert (dest_dir / "file.txt").read_bytes() == b"Compressed content"

    def test_block_path_traversal_tar(self, tmp_path: Path) -> None:
        """Test that path traversal is blocked in TAR files."""
        archive_path = tmp_path / "evil.tar"
        dest_dir = tmp_path / "extracted"

        with tarfile.open(archive_path, "w") as tf:
            content = b"malicious"
            info = tarfile.TarInfo(name="../../../etc/passwd")
            info.size = len(content)
            tf.addfile(info, io.BytesIO(content))

        with pytest.raises(PathTraversalError):
            safe_extract_tar(archive_path, dest_dir)

    def test_block_symlink_tar(self, tmp_path: Path) -> None:
        """Test that symlinks are blocked by default in TAR files."""
        archive_path = tmp_path / "symlink.tar"
        dest_dir = tmp_path / "extracted"

        with tarfile.open(archive_path, "w") as tf:
            info = tarfile.TarInfo(name="evil_link")
            info.type = tarfile.SYMTYPE
            info.linkname = "/etc/passwd"
            tf.addfile(info)

        with pytest.raises(SymlinkError):
            safe_extract_tar(archive_path, dest_dir)

    def test_allow_symlink_when_enabled(self, tmp_path: Path) -> None:
        """Test that symlinks can be allowed when explicitly enabled."""
        archive_path = tmp_path / "symlink.tar"
        dest_dir = tmp_path / "extracted"

        # Create a file first
        (tmp_path / "target.txt").write_text("target content")

        with tarfile.open(archive_path, "w") as tf:
            # Add a regular file
            content = b"regular"
            info = tarfile.TarInfo(name="regular.txt")
            info.size = len(content)
            tf.addfile(info, io.BytesIO(content))

            # Add a safe symlink (within dest)
            link_info = tarfile.TarInfo(name="link.txt")
            link_info.type = tarfile.SYMTYPE
            link_info.linkname = "regular.txt"
            tf.addfile(link_info)

        # Should work with allow_symlinks=True
        stats = safe_extract_tar(archive_path, dest_dir, allow_symlinks=True)
        assert stats["files_extracted"] == 1  # Only regular file counts

    def test_block_device_file_tar(self, tmp_path: Path) -> None:
        """Test that device files are blocked in TAR files."""
        archive_path = tmp_path / "device.tar"
        dest_dir = tmp_path / "extracted"

        with tarfile.open(archive_path, "w") as tf:
            info = tarfile.TarInfo(name="device")
            info.type = tarfile.CHRTYPE
            info.devmajor = 1
            info.devminor = 3
            tf.addfile(info)

        with pytest.raises(ArchiveExtractionError, match="Device file"):
            safe_extract_tar(archive_path, dest_dir)

    def test_block_too_many_files_tar(self, tmp_path: Path) -> None:
        """Test that archives with too many files are blocked."""
        archive_path = tmp_path / "many.tar"
        dest_dir = tmp_path / "extracted"

        with tarfile.open(archive_path, "w") as tf:
            for i in range(100):
                content = f"content{i}".encode()
                info = tarfile.TarInfo(name=f"file{i}.txt")
                info.size = len(content)
                tf.addfile(info, io.BytesIO(content))

        with pytest.raises(TooManyFilesError):
            safe_extract_tar(archive_path, dest_dir, max_files=50)


class TestSafeExtract:
    """Tests for the auto-detecting safe_extract function."""

    def test_auto_detect_zip(self, tmp_path: Path) -> None:
        """Test auto-detection of ZIP format."""
        archive_path = tmp_path / "test.zip"
        dest_dir = tmp_path / "extracted"

        with zipfile.ZipFile(archive_path, "w") as zf:
            zf.writestr("file.txt", "content")

        stats = safe_extract(archive_path, dest_dir)
        assert stats["files_extracted"] == 1

    def test_auto_detect_tar(self, tmp_path: Path) -> None:
        """Test auto-detection of TAR format."""
        archive_path = tmp_path / "test.tar"
        dest_dir = tmp_path / "extracted"

        with tarfile.open(archive_path, "w") as tf:
            content = b"content"
            info = tarfile.TarInfo(name="file.txt")
            info.size = len(content)
            tf.addfile(info, io.BytesIO(content))

        stats = safe_extract(archive_path, dest_dir)
        assert stats["files_extracted"] == 1

    def test_auto_detect_tar_gz(self, tmp_path: Path) -> None:
        """Test auto-detection of .tar.gz format."""
        archive_path = tmp_path / "test.tar.gz"
        dest_dir = tmp_path / "extracted"

        with tarfile.open(archive_path, "w:gz") as tf:
            content = b"content"
            info = tarfile.TarInfo(name="file.txt")
            info.size = len(content)
            tf.addfile(info, io.BytesIO(content))

        stats = safe_extract(archive_path, dest_dir)
        assert stats["files_extracted"] == 1

    def test_unsupported_format(self, tmp_path: Path) -> None:
        """Test that unsupported formats raise ValueError."""
        archive_path = tmp_path / "test.rar"
        archive_path.write_bytes(b"not a real rar")
        dest_dir = tmp_path / "extracted"

        with pytest.raises(ValueError, match="Unsupported"):
            safe_extract(archive_path, dest_dir)


class TestSecurityRegressions:
    """Security regression tests for archive extraction."""

    def test_zipslip_attack(self, tmp_path: Path) -> None:
        """Test protection against ZipSlip vulnerability."""
        archive_path = tmp_path / "zipslip.zip"
        dest_dir = tmp_path / "extracted"

        # Create a malicious ZIP that tries to write outside dest_dir
        with zipfile.ZipFile(archive_path, "w") as zf:
            # Various path traversal attempts
            zf.writestr("../../etc/passwd", "malicious")

        with pytest.raises(PathTraversalError):
            safe_extract_zip(archive_path, dest_dir)

    def test_tarslip_attack(self, tmp_path: Path) -> None:
        """Test protection against TarSlip vulnerability."""
        archive_path = tmp_path / "tarslip.tar"
        dest_dir = tmp_path / "extracted"

        with tarfile.open(archive_path, "w") as tf:
            content = b"malicious"
            info = tarfile.TarInfo(name="../../etc/passwd")
            info.size = len(content)
            tf.addfile(info, io.BytesIO(content))

        with pytest.raises(PathTraversalError):
            safe_extract_tar(archive_path, dest_dir)

    def test_symlink_escape_attack(self, tmp_path: Path) -> None:
        """Test protection against symlink escape attacks."""
        archive_path = tmp_path / "symlink_escape.tar"
        dest_dir = tmp_path / "extracted"

        with tarfile.open(archive_path, "w") as tf:
            # Create a symlink pointing outside the extraction directory
            info = tarfile.TarInfo(name="escape")
            info.type = tarfile.SYMTYPE
            info.linkname = "../../../etc"
            tf.addfile(info)

        with pytest.raises(SymlinkError):
            safe_extract_tar(archive_path, dest_dir)

    def test_hardlink_escape_attack(self, tmp_path: Path) -> None:
        """Test protection against hardlink attacks."""
        archive_path = tmp_path / "hardlink.tar"
        dest_dir = tmp_path / "extracted"

        with tarfile.open(archive_path, "w") as tf:
            info = tarfile.TarInfo(name="hardlink")
            info.type = tarfile.LNKTYPE
            info.linkname = "/etc/passwd"
            tf.addfile(info)

        with pytest.raises(SymlinkError):
            safe_extract_tar(archive_path, dest_dir)
