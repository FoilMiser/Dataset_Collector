from __future__ import annotations

import time
from collections.abc import Callable
from typing import TypeVar

import requests

T = TypeVar("T")


def _is_retryable_http_exception(exc: Exception) -> bool:
    if isinstance(exc, requests.exceptions.HTTPError):
        status_code = exc.response.status_code if exc.response is not None else None
        return status_code is not None and status_code >= 500
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
) -> T:
    attempts = max(1, max_attempts)
    for attempt in range(attempts):
        try:
            return fn()
        except Exception as exc:
            if not _is_retryable_http_exception(exc) or attempt >= attempts - 1:
                raise
            if on_retry:
                on_retry(attempt + 1, exc)
            sleep_time = min(backoff_base**attempt, backoff_max)
            time.sleep(sleep_time)
    raise RuntimeError("unreachable")
