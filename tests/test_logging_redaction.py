from __future__ import annotations

import json
import logging
import sys

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


def test_secret_str_redacts_repr_and_str() -> None:
    secret = SecretStr("super-secret")
    assert str(secret) == REDACTED
    assert repr(secret) == REDACTED
    assert secret.reveal() == "super-secret"


def test_redact_headers_wraps_sensitive_values() -> None:
    redacted = redact_headers({"Authorization": "Bearer token", "User-Agent": "demo"})
    assert isinstance(redacted["Authorization"], SecretStr)
    assert redacted["User-Agent"] == "demo"


def test_json_formatter_redacts_exception_info() -> None:
    """Test that JsonFormatter redacts sensitive values in exception tracebacks."""
    formatter = JsonFormatter()

    # Create an exception with a sensitive value in it
    try:
        api_key = "ghp_FakeTestToken1234567890abcdefghijklmnopqrstuvwxyz"
        raise ValueError(f"API call failed with key: {api_key}")
    except ValueError:
        exc_info = sys.exc_info()
    
    record = logging.LogRecord(
        name="test",
        level=logging.ERROR,
        pathname=__file__,
        lineno=1,
        msg="An error occurred",
        args=(),
        exc_info=exc_info,
    )
    
    output = formatter.format(record)
    payload = json.loads(output)
    
    # Verify the exception info is included
    assert "exc_info" in payload
    
    # Verify the sensitive API key is redacted in the exception traceback
    assert "ghp_FakeTestToken1234567890abcdefghijklmnopqrstuvwxyz" not in payload["exc_info"]
    assert REDACTED in payload["exc_info"]
    assert "ValueError" in payload["exc_info"]


def test_text_formatter_redacts_exception_info() -> None:
    """Test that TextFormatter redacts sensitive values in exception tracebacks."""
    formatter = TextFormatter()

    # Create an exception with a sensitive value in it
    try:
        token = "Bearer eyJfYWtlIjoiVGVzdCIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJGYWtlVGVzdFRva2VuIn0.FakeSigForTestingPurposesOnly"
        raise RuntimeError(f"Authentication failed: {token}")
    except RuntimeError:
        exc_info = sys.exc_info()
    
    record = logging.LogRecord(
        name="test",
        level=logging.ERROR,
        pathname=__file__,
        lineno=1,
        msg="An error occurred",
        args=(),
        exc_info=exc_info,
    )
    
    output = formatter.format(record)
    
    # Verify the JWT token is redacted in the output
    assert "eyJfYWtlIjoiVGVzdCIsInR5cCI6IkpXVCJ9" not in output
    assert REDACTED in output
    assert "RuntimeError" in output
