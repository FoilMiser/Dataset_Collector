from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

REDACTED = "<REDACTED>"

_SENSITIVE_KEY_NORMALIZED = {
    "authorization",
    "apikey",
    "xapikey",
    "accesstoken",
    "token",
}

_KEY_VALUE_RE = re.compile(
    r"(?i)(authorization|x-api-key|api[-_]?key|access[-_]?token|token)(\s*[:=]\s*)"
    r"(\"[^\"]*\"|'[^']*'|Bearer\s+[^,\s]+|[^,\s]+)"
)
_BEARER_RE = re.compile(r"(?i)Bearer\s+[^\s,\"']+")
_TOKEN_PATTERNS = [
    re.compile(r"gh[pousr]_[A-Za-z0-9_]{30,}"),
    re.compile(r"AKIA[0-9A-Z]{16}"),
    re.compile(r"eyJ[a-zA-Z0-9_-]{10,}\.[a-zA-Z0-9._-]{10,}\.[a-zA-Z0-9._-]{10,}"),
]


def _normalize_key(key: str) -> str:
    return re.sub(r"[^a-z0-9]", "", key.lower())


def is_sensitive_key(key: str) -> bool:
    return _normalize_key(key) in _SENSITIVE_KEY_NORMALIZED


class SecretStr:
    """
    String-like wrapper for sensitive values that should be redacted in logs.

    This class provides a way to handle sensitive data (API keys, tokens, passwords,
    etc.) safely within the application. When logged or converted to a string, the
    actual value is replaced with ``<REDACTED>`` to prevent accidental exposure of
    secrets in logs, error messages, or debugging output.

    **When to use:**
        - Wrap API keys, access tokens, passwords, or other sensitive credentials
        - Store values that should never appear in logs or stack traces
        - Pass sensitive data through the application safely

    **Usage:**
        Create a SecretStr by passing any value to the constructor. Use the ``reveal()``
        method to access the actual value when needed for API calls or authentication::

            secret = SecretStr("my-api-key-12345")
            print(secret)           # Output: <REDACTED>
            print(str(secret))      # Output: <REDACTED>
            print(secret.reveal())  # Output: my-api-key-12345

    **Behavior:**
        - The constructor stores a string representation of the input value
        - ``__str__()`` and ``__repr__()`` return ``<REDACTED>`` instead of the actual value
        - ``reveal()`` returns the actual value as a string - use this when you need
          the real value for authentication or API calls
        - If ``value`` is ``None``, it is normalized to the empty string ``""``:
            - ``SecretStr(None).reveal()`` returns ``""``
            - ``SecretStr("")`` also results in ``reveal()`` returning ``""``
        - As a result, ``reveal()`` cannot distinguish between an explicitly empty
          secret and a ``None`` value; both appear as the empty string

    **Note:** The ``reveal()`` method should only be called when the actual value is
    needed for authentication, API calls, or similar operations. Avoid using ``reveal()``
    in contexts where the value might be logged or displayed.
    """
    def __init__(self, value: Any) -> None:
        self._value = "" if value is None else str(value)

    def reveal(self) -> str:
        return self._value

    def __repr__(self) -> str:  # pragma: no cover - trivial
        return REDACTED

    def __str__(self) -> str:
        return REDACTED

    def __eq__(self, other: object) -> bool:
        if isinstance(other, SecretStr):
            return self._value == other._value
        return False


def redact_string(text: str) -> str:
    redacted = text

    def replace_match(match: re.Match[str]) -> str:
        value = match.group(3)
        if value.startswith(("'", '"')) and value.endswith(value[0]):
            quote = value[0]
            return f"{match.group(1)}{match.group(2)}{quote}{REDACTED}{quote}"
        return f"{match.group(1)}{match.group(2)}{REDACTED}"

    redacted = _KEY_VALUE_RE.sub(replace_match, redacted)
    redacted = _BEARER_RE.sub(f"Bearer {REDACTED}", redacted)
    for pattern in _TOKEN_PATTERNS:
        redacted = pattern.sub(REDACTED, redacted)
    return redacted


def redact_structure(value: Any) -> Any:
    if isinstance(value, SecretStr):
        return value
    if isinstance(value, str):
        return redact_string(value)
    if isinstance(value, Mapping):
        return {
            key: SecretStr(val) if is_sensitive_key(str(key)) else redact_structure(val)
            for key, val in value.items()
        }
    if isinstance(value, tuple):
        return tuple(redact_structure(item) for item in value)
    if isinstance(value, list):
        return [redact_structure(item) for item in value]
    if isinstance(value, set):
        return {redact_structure(item) for item in value}
    return value


def redact_headers(headers: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: SecretStr(value) if is_sensitive_key(str(key)) else redact_structure(value)
        for key, value in headers.items()
    }
