from __future__ import annotations

import json
import logging

from collector_core.logging_config import JsonFormatter, TextFormatter
from collector_core.secrets import REDACTED, SecretStr, redact_headers


def test_text_formatter_redacts_sensitive_headers() -> None:
    formatter = TextFormatter()
    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="Headers: %s",
        args=(
            {
                "Authorization": "Bearer secret-token",
                "X-API-Key": "api-key-123",
                "User-Agent": "collector",
            },
        ),
        exc_info=None,
        func=None,
    )
    output = formatter.format(record)
    assert "secret-token" not in output
    assert "api-key-123" not in output
    assert REDACTED in output


def test_json_formatter_redacts_inline_tokens() -> None:
    formatter = JsonFormatter()
    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="Authorization: Bearer abc123 token=xyz456",
        args=(),
        exc_info=None,
        func=None,
    )
    payload = json.loads(formatter.format(record))
    message = payload["message"]
    assert "abc123" not in message
    assert "xyz456" not in message
    assert REDACTED in message


def test_secret_str_redacts_repr_and_str() -> None:
    secret = SecretStr("super-secret")
    assert str(secret) == REDACTED
    assert repr(secret) == REDACTED
    assert secret.reveal() == "super-secret"


def test_redact_headers_wraps_sensitive_values() -> None:
    redacted = redact_headers({"Authorization": "Bearer token", "User-Agent": "demo"})
    assert isinstance(redacted["Authorization"], SecretStr)
    assert redacted["User-Agent"] == "demo"
