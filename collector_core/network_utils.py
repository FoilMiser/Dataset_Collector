from __future__ import annotations

import time
from collections.abc import Callable
from typing import TypeVar

import requests

T = TypeVar("T")


def _is_retryable_http_exception(
    exc: Exception,
    retry_on_429: bool = True,
    retry_on_403: bool = False,
) -> bool:
    """Check if an HTTP exception is retryable.

    Args:
        exc: The exception to check
        retry_on_429: Whether to retry on HTTP 429 Too Many Requests
        retry_on_403: Whether to retry on HTTP 403 Forbidden (GitHub uses for rate limits)

    Returns:
        True if the exception is retryable
    """
    if isinstance(exc, requests.exceptions.HTTPError):
        status_code = exc.response.status_code if exc.response is not None else None
        if status_code is None:
            return False
        # Server errors (5xx) are always retryable
        if status_code >= 500:
            return True
        # Rate limit errors (429) are retryable if configured
        if status_code == 429 and retry_on_429:
            return True
        # Forbidden (403) - GitHub uses this for rate limits
        if status_code == 403 and retry_on_403:
            return True
        return False
    return isinstance(
        exc,
        (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            requests.exceptions.ChunkedEncodingError,
            requests.exceptions.ContentDecodingError,
            requests.exceptions.TooManyRedirects,
        ),
    )


def _with_retries(
    fn: Callable[[], T],
    *,
    max_attempts: int = 3,
    backoff_base: float = 2.0,
    backoff_max: float = 60.0,
    on_retry: Callable[[int, Exception], None] | None = None,
    retry_on_429: bool = True,
    retry_on_403: bool = False,
) -> T:
    """Execute a function with retry logic.

    Args:
        fn: The function to execute
        max_attempts: Maximum number of attempts
        backoff_base: Base for exponential backoff (seconds)
        backoff_max: Maximum backoff time (seconds)
        on_retry: Optional callback called on each retry with (attempt_num, exception)
        retry_on_429: Whether to retry on HTTP 429 Too Many Requests
        retry_on_403: Whether to retry on HTTP 403 Forbidden (GitHub rate limits)

    Returns:
        The result of fn()

    Raises:
        Exception: The last exception if all retries fail
    """
    attempts = max(1, max_attempts)
    for attempt in range(attempts):
        try:
            return fn()
        except Exception as exc:
            is_retryable = _is_retryable_http_exception(
                exc, retry_on_429=retry_on_429, retry_on_403=retry_on_403
            )
            if not is_retryable or attempt >= attempts - 1:
                raise
            if on_retry:
                on_retry(attempt + 1, exc)
            sleep_time = min(backoff_base**attempt, backoff_max)
            time.sleep(sleep_time)
    raise RuntimeError("unreachable")
