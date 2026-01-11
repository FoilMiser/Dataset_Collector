from __future__ import annotations

from dataclasses import dataclass
from typing import Any


class YAMLError(ValueError):
    """Raised when the lightweight YAML parser encounters unsupported syntax."""


@dataclass
class _Line:
    indent: int
    content: str


def safe_load(text: str) -> Any:
    lines = _prepare_lines(text)
    if not lines:
        return None
    value, index = _parse_block(lines, 0, 0)
    if index != len(lines):
        raise YAMLError("Unexpected trailing content in YAML.")
    return value


def _prepare_lines(text: str) -> list[_Line]:
    prepared: list[_Line] = []
    for raw in text.splitlines():
        stripped = _strip_comment(raw)
        if not stripped.strip():
            continue
        indent = len(stripped) - len(stripped.lstrip(" "))
        prepared.append(_Line(indent=indent, content=stripped.lstrip(" ")))
    return prepared


def _strip_comment(line: str) -> str:
    in_single = False
    in_double = False
    for idx, ch in enumerate(line):
        if ch == "'" and not in_double:
            in_single = not in_single
        elif ch == '"' and not in_single:
            in_double = not in_double
        elif ch == "#" and not in_single and not in_double:
            return line[:idx]
    return line


def _parse_block(lines: list[_Line], start: int, indent: int) -> tuple[Any, int]:
    if start >= len(lines):
        return {}, start
    line = lines[start]
    if line.indent < indent:
        return {}, start
    if line.content.startswith("- "):
        return _parse_list(lines, start, indent)
    return _parse_mapping(lines, start, indent)


def _parse_list(lines: list[_Line], start: int, indent: int) -> tuple[list[Any], int]:
    items: list[Any] = []
    index = start
    while index < len(lines):
        line = lines[index]
        if line.indent != indent or not line.content.startswith("- "):
            break
        entry = line.content[2:].strip()
        if not entry:
            value, index = _parse_block(lines, index + 1, _next_indent(lines, index))
            items.append(value)
            continue
        if _looks_like_inline_mapping(entry):
            key, remainder = _split_key_value(entry)
            item: dict[str, Any] = {key: _parse_scalar(remainder)}
            index += 1
            if index < len(lines) and lines[index].indent > indent:
                extra, index = _parse_mapping(lines, index, lines[index].indent)
                item.update(extra)
            items.append(item)
            continue
        value = _parse_scalar(entry)
        index += 1
        value = _maybe_append_continuation(lines, index, indent, value)
        index = _advance_continuations(lines, index, indent, value)
        items.append(value)
    return items, index


def _parse_mapping(lines: list[_Line], start: int, indent: int) -> tuple[dict[str, Any], int]:
    mapping: dict[str, Any] = {}
    index = start
    while index < len(lines):
        line = lines[index]
        if line.indent != indent or line.content.startswith("- "):
            break
        if ":" not in line.content:
            raise YAMLError(f"Expected key-value pair, got: {line.content}")
        key, remainder = _split_key_value(line.content)
        if remainder == "":
            next_indent = _block_indent_for_key(lines, index, indent)
            value, index = _parse_block(lines, index + 1, next_indent)
        else:
            value = _parse_scalar(remainder)
            index += 1
            value = _maybe_append_continuation(lines, index, indent, value)
            index = _advance_continuations(lines, index, indent, value)
        mapping[key] = value
    return mapping, index


def _split_key_value(content: str) -> tuple[str, str]:
    key, remainder = content.split(":", 1)
    return key.strip(), remainder.strip()


def _parse_scalar(token: str) -> Any:
    if token in {"", "null", "Null", "NULL", "~"}:
        return None
    if token in {"true", "True", "TRUE"}:
        return True
    if token in {"false", "False", "FALSE"}:
        return False
    if token.startswith("'") and token.endswith("'") and len(token) >= 2:
        return token[1:-1].replace("''", "'")
    if token.startswith('"') and token.endswith('"') and len(token) >= 2:
        return token[1:-1].replace('\\"', '"')
    try:
        if token.startswith("0") and token != "0" and not token.startswith("0."):
            return token
        if "." in token or "e" in token.lower():
            return float(token)
        return int(token)
    except ValueError:
        return token


def _looks_like_inline_mapping(entry: str) -> bool:
    return ":" in entry and not entry.lstrip().startswith("#")


def _next_indent(lines: list[_Line], index: int) -> int:
    for idx in range(index + 1, len(lines)):
        if lines[idx].indent > lines[index].indent:
            return lines[idx].indent
        if lines[idx].indent == lines[index].indent:
            break
    return lines[index].indent + 2


def _block_indent_for_key(lines: list[_Line], index: int, indent: int) -> int:
    if index + 1 >= len(lines):
        return indent + 2
    next_line = lines[index + 1]
    if next_line.indent == indent and next_line.content.startswith("- "):
        return indent
    if next_line.indent > indent:
        return next_line.indent
    return indent + 2


def _maybe_append_continuation(
    lines: list[_Line],
    index: int,
    indent: int,
    value: Any,
) -> Any:
    if not isinstance(value, str):
        return value
    parts = [value]
    while index < len(lines):
        line = lines[index]
        if line.indent <= indent:
            break
        if line.content.startswith("- "):
            break
        if ":" in line.content:
            break
        parts.append(line.content.strip())
        index += 1
    return " ".join(parts)


def _advance_continuations(lines: list[_Line], index: int, indent: int, value: Any) -> int:
    if not isinstance(value, str):
        return index
    while index < len(lines):
        line = lines[index]
        if line.indent <= indent:
            break
        if line.content.startswith("- ") or ":" in line.content:
            break
        index += 1
    return index
