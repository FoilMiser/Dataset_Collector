from __future__ import annotations

import gzip
import io
import json
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any

import zstandard as zstd

from collector_core.config_validator import read_yaml as read_yaml_config
from collector_core.utils.paths import ensure_dir


def read_yaml(path: Path, *, schema_name: str | None = None) -> dict[str, Any]:
    """Read and validate YAML config file."""
    return read_yaml_config(path, schema_name=schema_name) or {}


def read_json(path: Path) -> dict[str, Any]:
    """Read JSON file and return as dict."""
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, obj: dict[str, Any], *, indent: int = 2) -> None:
    """Write dict to JSON file atomically."""
    ensure_dir(path.parent)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(obj, indent=indent, ensure_ascii=False) + "\n", encoding="utf-8")
    tmp_path.replace(path)


def _open_text(path: Path, mode: str) -> io.TextIOBase:
    if path.suffix == ".gz":
        return gzip.open(path, mode, encoding="utf-8", errors="ignore")
    if path.suffix == ".zst":
        if "r" in mode:
            stream = zstd.ZstdDecompressor().stream_reader(path.open("rb"))
            return io.TextIOWrapper(stream, encoding="utf-8", errors="ignore")
        stream = zstd.ZstdCompressor().stream_writer(path.open("wb"))
        return io.TextIOWrapper(stream, encoding="utf-8")
    return open(path, mode, encoding="utf-8", errors="ignore")


def read_jsonl(path: Path) -> Iterator[dict[str, Any]]:
    """Read JSONL file (supports .gz/.zst) and yield records."""
    with _open_text(path, "rt") as f:
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
    """Write records to JSONL file (supports .gz/.zst)."""
    ensure_dir(path.parent)
    with _open_text(path, "wt") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def append_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    """Append records to JSONL file (supports .gz/.zst)."""
    ensure_dir(path.parent)
    if path.suffix == ".gz":
        with gzip.open(path, "ab") as f:
            for row in rows:
                f.write((json.dumps(row, ensure_ascii=False) + "\n").encode("utf-8"))
        return
    if path.suffix == ".zst":
        with path.open("ab") as raw:
            cctx = zstd.ZstdCompressor()
            with cctx.stream_writer(raw) as compressor:
                for row in rows:
                    compressor.write((json.dumps(row, ensure_ascii=False) + "\n").encode("utf-8"))
        return
    with open(path, "a", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def write_jsonl_gz(path: Path, rows: Iterable[dict[str, Any]]) -> tuple[int, int]:
    """Write rows to gzipped JSONL file, return (count, bytes)."""
    ensure_dir(path.parent)
    count = 0
    with gzip.open(path, "wt", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
            count += 1
    return count, path.stat().st_size
