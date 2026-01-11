from __future__ import annotations

import json
import logging

from collector_core.logging_config import (
    JsonFormatter,
    TextFormatter,
    clear_log_context,
    set_log_context,
)
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
    )
    payload = json.loads(formatter.format(record))
    message = payload["message"]
    assert "abc123" not in message
    assert "xyz456" not in message
    assert REDACTED in message


def test_json_formatter_redacts_known_secrets() -> None:
    formatter = JsonFormatter()
    secret_block = (
        "-----BEGIN PRIVATE KEY-----\n"
        "MIIBVgIBADANBgkqhkiG9w0BAQEFAASCAT8wggE7AgEAAkEAsnEXAMPLE\n"
        "-----END PRIVATE KEY-----"
    )
    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg=(
            "password=hunter2 refresh_token=r1-abcdef "
            "slack=xoxb-1234567890-abcdefg "
            "google=AIzaSyDUMMYKEY1234567890abcdefgHIJKL "
            f"key={secret_block}"
        ),
        args=(),
        exc_info=None,
    )
    payload = json.loads(formatter.format(record))
    message = payload["message"]
    assert "hunter2" not in message
    assert "r1-abcdef" not in message
    assert "xoxb-1234567890-abcdefg" not in message
    assert "AIzaSyDUMMYKEY1234567890abcdefgHIJKL" not in message
    assert "BEGIN PRIVATE KEY" not in message
    assert REDACTED in message


def test_text_formatter_redacts_sensitive_keys_in_structures() -> None:
    formatter = TextFormatter()
    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="Payload: %s",
        args=({"password": "topsecret", "user": "demo"},),
        exc_info=None,
    )
    output = formatter.format(record)
    assert "topsecret" not in output
    assert "demo" in output
    assert REDACTED in output


def test_json_formatter_includes_log_context_fields() -> None:
    formatter = JsonFormatter()
    set_log_context(
        run_id="run-123",
        domain="chemistry",
        target_id="target-7",
        strategy="http",
        bytes=2048,
        duration_ms=12.5,
        error_types=["timeout"],
    )
    try:
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname=__file__,
            lineno=1,
            msg="test message",
            args=(),
            exc_info=None,
        )
        payload = json.loads(formatter.format(record))
        assert payload["run_id"] == "run-123"
        assert payload["domain"] == "chemistry"
        assert payload["target_id"] == "target-7"
        assert payload["strategy"] == "http"
        assert payload["bytes"] == 2048
        assert payload["duration_ms"] == 12.5
        assert payload["error_types"] == ["timeout"]
        assert payload["context"]["run_id"] == "run-123"
    finally:
        clear_log_context()


def test_secret_str_redacts_repr_and_str() -> None:
    secret = SecretStr("super-secret")
    assert str(secret) == REDACTED
    assert repr(secret) == REDACTED
    assert secret.reveal() == "super-secret"


def test_redact_headers_wraps_sensitive_values() -> None:
    redacted = redact_headers({"Authorization": "Bearer token", "User-Agent": "demo"})
    assert isinstance(redacted["Authorization"], SecretStr)
    assert redacted["User-Agent"] == "demo"
