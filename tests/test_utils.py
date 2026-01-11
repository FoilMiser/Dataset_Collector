"""Tests for collector_core.utils module."""

from __future__ import annotations

import io
import tarfile
import zipfile
from pathlib import Path

from collector_core.utils import (
    append_jsonl,
    coerce_int,
    contains_any,
    ensure_dir,
    lower,
    normalize_whitespace,
    read_json,
    read_jsonl_list,
    safe_filename,
    sha256_bytes,
    sha256_file,
    sha256_text,
    utc_now,
    validate_tar_archive,
    validate_zip_archive,
    write_json,
    write_jsonl,
)


class TestUtcNow:
    def test_format(self):
        result = utc_now()
        assert result.endswith("Z")
        assert "T" in result
        assert len(result) == 20  # YYYY-MM-DDTHH:MM:SSZ


class TestEnsureDir:
    def test_creates_nested_dirs(self, tmp_path: Path):
        target = tmp_path / "a" / "b" / "c"
        ensure_dir(target)
        assert target.exists()
        assert target.is_dir()

    def test_idempotent(self, tmp_path: Path):
        target = tmp_path / "exists"
        target.mkdir()
        ensure_dir(target)  # Should not raise
        assert target.exists()


class TestSha256:
    def test_sha256_bytes(self):
        result = sha256_bytes(b"hello")
        assert len(result) == 64
        assert result == "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"

    def test_sha256_text_normalizes_whitespace(self):
        result1 = sha256_text("hello  world")
        result2 = sha256_text("hello world")
        result3 = sha256_text("hello\n\tworld")
        assert result1 == result2 == result3

    def test_sha256_file(self, tmp_path: Path):
        file = tmp_path / "test.txt"
        file.write_bytes(b"hello")
        result = sha256_file(file)
        assert result == "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"

    def test_sha256_file_missing(self, tmp_path: Path):
        result = sha256_file(tmp_path / "nonexistent.txt")
        assert result is None


class TestNormalizeWhitespace:
    def test_collapses_spaces(self):
        assert normalize_whitespace("a  b   c") == "a b c"

    def test_handles_newlines_tabs(self):
        assert normalize_whitespace("a\n\tb") == "a b"

    def test_strips(self):
        assert normalize_whitespace("  hello  ") == "hello"

    def test_handles_none(self):
        assert normalize_whitespace(None) == ""


class TestLower:
    def test_lowercases(self):
        assert lower("HELLO") == "hello"

    def test_handles_none(self):
        assert lower(None) == ""


class TestJsonIO:
    def test_write_read_json(self, tmp_path: Path):
        file = tmp_path / "test.json"
        data = {"key": "value", "number": 42}
        write_json(file, data)
        result = read_json(file)
        assert result == data

    def test_write_json_atomic(self, tmp_path: Path):
        file = tmp_path / "test.json"
        write_json(file, {"a": 1})
        # No .tmp file should remain
        assert not (tmp_path / "test.json.tmp").exists()


class TestJsonlIO:
    def test_write_read_jsonl(self, tmp_path: Path):
        file = tmp_path / "test.jsonl"
        rows = [{"a": 1}, {"b": 2}]
        write_jsonl(file, rows)
        result = read_jsonl_list(file)
        assert result == rows

    def test_write_read_jsonl_gzip(self, tmp_path: Path):
        file = tmp_path / "test.jsonl.gz"
        rows = [{"a": 1}, {"b": 2}]
        write_jsonl(file, rows)
        result = read_jsonl_list(file)
        assert result == rows

    def test_append_jsonl(self, tmp_path: Path):
        file = tmp_path / "test.jsonl"
        write_jsonl(file, [{"a": 1}])
        append_jsonl(file, [{"b": 2}])
        result = read_jsonl_list(file)
        assert result == [{"a": 1}, {"b": 2}]

    def test_read_jsonl_skips_invalid(self, tmp_path: Path):
        file = tmp_path / "test.jsonl"
        file.write_text('{"valid": true}\ninvalid json\n{"also": "valid"}\n')
        result = read_jsonl_list(file)
        assert len(result) == 2


class TestSafeFilename:
    def test_replaces_special_chars(self):
        # Only dangerous filesystem chars are replaced (/<>:"|?*\x00)
        # !@# are valid filename chars on most systems
        assert safe_filename("hello world") == "hello_world"
        assert safe_filename("file<name>") == "file_name_"
        assert safe_filename("path/to") == "path_to"

    def test_truncates(self):
        result = safe_filename("a" * 300, max_length=10)
        assert len(result) == 10

    def test_handles_empty(self):
        assert safe_filename("") == "file"
        assert safe_filename(None) == "file"

    def test_removes_directory_separators(self):
        result = safe_filename("path/to/file.txt")
        assert "/" not in result
        assert result == "path_to_file.txt"

    def test_handles_windows_reserved_names(self):
        result = safe_filename("CON")
        assert result == "_CON"
        result = safe_filename("NUL.txt")
        assert result == "_NUL.txt"


class TestContainsAny:
    def test_finds_matches(self):
        result = contains_any("Hello World", ["hello", "foo"])
        assert result == ["hello"]

    def test_case_insensitive(self):
        result = contains_any("HELLO", ["hello"])
        assert result == ["hello"]

    def test_no_matches(self):
        result = contains_any("hello", ["foo", "bar"])
        assert result == []


class TestCoerceInt:
    def test_valid_int(self):
        assert coerce_int("42") == 42
        assert coerce_int(42) == 42

    def test_invalid_returns_default(self):
        assert coerce_int("not a number", default=0) == 0
        assert coerce_int(None, default=-1) == -1

    def test_invalid_no_default(self):
        assert coerce_int("invalid") is None


class TestArchiveValidation:
    def test_zip_rejects_traversal(self, tmp_path: Path):
        archive = tmp_path / "bad.zip"
        with zipfile.ZipFile(archive, "w") as zf:
            zf.writestr("../evil.txt", "nope")
        with zipfile.ZipFile(archive) as zf:
            try:
                validate_zip_archive(zf, max_files=10, max_total_size=1024)
            except ValueError as exc:
                assert "unsafe path" in str(exc)
            else:
                raise AssertionError("Expected unsafe path rejection")

    def test_zip_rejects_absolute(self, tmp_path: Path):
        archive = tmp_path / "abs.zip"
        with zipfile.ZipFile(archive, "w") as zf:
            zf.writestr("/abs.txt", "nope")
        with zipfile.ZipFile(archive) as zf:
            try:
                validate_zip_archive(zf, max_files=10, max_total_size=1024)
            except ValueError as exc:
                assert "unsafe path" in str(exc)
            else:
                raise AssertionError("Expected unsafe path rejection")

    def test_zip_rejects_symlink(self, tmp_path: Path):
        archive = tmp_path / "symlink.zip"
        info = zipfile.ZipInfo("link")
        info.create_system = 3
        info.external_attr = 0o120777 << 16
        with zipfile.ZipFile(archive, "w") as zf:
            zf.writestr(info, "target")
        with zipfile.ZipFile(archive) as zf:
            try:
                validate_zip_archive(zf, max_files=10, max_total_size=1024)
            except ValueError as exc:
                assert "symlink" in str(exc)
            else:
                raise AssertionError("Expected symlink rejection")

    def test_zip_rejects_file_count(self, tmp_path: Path):
        archive = tmp_path / "count.zip"
        with zipfile.ZipFile(archive, "w") as zf:
            zf.writestr("a.txt", "a")
            zf.writestr("b.txt", "b")
            zf.writestr("c.txt", "c")
        with zipfile.ZipFile(archive) as zf:
            try:
                validate_zip_archive(zf, max_files=2, max_total_size=1024)
            except ValueError as exc:
                assert "file count" in str(exc)
            else:
                raise AssertionError("Expected file count rejection")

    def test_zip_rejects_total_size(self, tmp_path: Path):
        archive = tmp_path / "size.zip"
        with zipfile.ZipFile(archive, "w") as zf:
            zf.writestr("a.txt", "a" * 10)
            zf.writestr("b.txt", "b" * 10)
        with zipfile.ZipFile(archive) as zf:
            try:
                validate_zip_archive(zf, max_files=10, max_total_size=15)
            except ValueError as exc:
                assert "total size" in str(exc)
            else:
                raise AssertionError("Expected size rejection")

    def test_tar_rejects_traversal(self, tmp_path: Path):
        archive = tmp_path / "bad.tar"
        with tarfile.open(archive, "w") as tf:
            info = tarfile.TarInfo(name="../evil.txt")
            data = io.BytesIO(b"nope")
            info.size = len(data.getvalue())
            tf.addfile(info, data)
        with tarfile.open(archive, "r") as tf:
            try:
                validate_tar_archive(tf, max_files=10, max_total_size=1024)
            except ValueError as exc:
                assert "unsafe path" in str(exc)
            else:
                raise AssertionError("Expected unsafe path rejection")

    def test_tar_rejects_symlink(self, tmp_path: Path):
        archive = tmp_path / "symlink.tar"
        with tarfile.open(archive, "w") as tf:
            info = tarfile.TarInfo(name="link")
            info.type = tarfile.SYMTYPE
            info.linkname = "target"
            tf.addfile(info)
        with tarfile.open(archive, "r") as tf:
            try:
                validate_tar_archive(tf, max_files=10, max_total_size=1024)
            except ValueError as exc:
                assert "symlink" in str(exc)
            else:
                raise AssertionError("Expected symlink rejection")
