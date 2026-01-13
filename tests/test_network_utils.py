"""Tests for collector_core.network_utils module."""

from __future__ import annotations

import time
from unittest.mock import Mock, patch

import pytest
import requests

from collector_core.network_utils import _is_retryable_http_exception, _with_retries


class TestIsRetryableHttpException:
    """Test _is_retryable_http_exception function."""

    def test_5xx_server_error_is_retryable(self) -> None:
        """5xx server errors should always be retryable."""
        for status_code in [500, 502, 503, 504]:
            response = Mock()
            response.status_code = status_code
            exc = requests.exceptions.HTTPError(response=response)
            assert _is_retryable_http_exception(exc) is True

    def test_429_rate_limit_retryable_by_default(self) -> None:
        """429 Too Many Requests should be retryable by default."""
        response = Mock()
        response.status_code = 429
        exc = requests.exceptions.HTTPError(response=response)
        assert _is_retryable_http_exception(exc, retry_on_429=True) is True

    def test_429_not_retryable_when_disabled(self) -> None:
        """429 should not be retryable when retry_on_429=False."""
        response = Mock()
        response.status_code = 429
        exc = requests.exceptions.HTTPError(response=response)
        assert _is_retryable_http_exception(exc, retry_on_429=False) is False

    def test_403_forbidden_not_retryable_by_default(self) -> None:
        """403 Forbidden should not be retryable by default."""
        response = Mock()
        response.status_code = 403
        exc = requests.exceptions.HTTPError(response=response)
        assert _is_retryable_http_exception(exc, retry_on_403=False) is False

    def test_403_retryable_when_enabled(self) -> None:
        """403 should be retryable when retry_on_403=True (GitHub rate limits)."""
        response = Mock()
        response.status_code = 403
        exc = requests.exceptions.HTTPError(response=response)
        assert _is_retryable_http_exception(exc, retry_on_403=True) is True

    def test_4xx_client_errors_not_retryable(self) -> None:
        """4xx client errors (except 429/403) should not be retryable."""
        for status_code in [400, 401, 404, 405, 408, 422]:
            response = Mock()
            response.status_code = status_code
            exc = requests.exceptions.HTTPError(response=response)
            assert _is_retryable_http_exception(exc) is False, f"Status {status_code} should not be retryable"

    def test_http_error_with_none_response(self) -> None:
        """HTTPError with None response should not be retryable."""
        exc = requests.exceptions.HTTPError(response=None)
        assert _is_retryable_http_exception(exc) is False

    def test_connection_error_is_retryable(self) -> None:
        """ConnectionError should be retryable."""
        exc = requests.exceptions.ConnectionError()
        assert _is_retryable_http_exception(exc) is True

    def test_timeout_is_retryable(self) -> None:
        """Timeout should be retryable."""
        exc = requests.exceptions.Timeout()
        assert _is_retryable_http_exception(exc) is True

    def test_chunked_encoding_error_is_retryable(self) -> None:
        """ChunkedEncodingError should be retryable."""
        exc = requests.exceptions.ChunkedEncodingError()
        assert _is_retryable_http_exception(exc) is True

    def test_content_decoding_error_is_retryable(self) -> None:
        """ContentDecodingError should be retryable."""
        exc = requests.exceptions.ContentDecodingError()
        assert _is_retryable_http_exception(exc) is True

    def test_too_many_redirects_is_retryable(self) -> None:
        """TooManyRedirects should be retryable."""
        exc = requests.exceptions.TooManyRedirects()
        assert _is_retryable_http_exception(exc) is True

    def test_generic_exception_not_retryable(self) -> None:
        """Generic exceptions should not be retryable."""
        exc = ValueError("some error")
        assert _is_retryable_http_exception(exc) is False

    def test_request_exception_not_retryable(self) -> None:
        """Base RequestException should not be retryable."""
        exc = requests.exceptions.RequestException()
        assert _is_retryable_http_exception(exc) is False


class TestWithRetries:
    """Test _with_retries function."""

    def test_success_on_first_attempt(self) -> None:
        """Function succeeds on first attempt."""
        fn = Mock(return_value="success")
        result = _with_retries(fn, max_attempts=3)
        assert result == "success"
        assert fn.call_count == 1

    def test_success_on_retry_after_retryable_error(self) -> None:
        """Function succeeds after retryable error."""
        response = Mock()
        response.status_code = 503
        retryable_exc = requests.exceptions.HTTPError(response=response)

        fn = Mock(side_effect=[retryable_exc, "success"])
        with patch("collector_core.network_utils.time.sleep"):
            result = _with_retries(fn, max_attempts=3, backoff_base=0.01)
        assert result == "success"
        assert fn.call_count == 2

    def test_max_attempts_exceeded_raises(self) -> None:
        """All retries exhausted raises the last exception."""
        exc = requests.exceptions.ConnectionError("network issue")
        fn = Mock(side_effect=exc)
        with patch("collector_core.network_utils.time.sleep"):
            with pytest.raises(requests.exceptions.ConnectionError):
                _with_retries(fn, max_attempts=3, backoff_base=0.01)
        assert fn.call_count == 3

    def test_non_retryable_error_raises_immediately(self) -> None:
        """Non-retryable errors raise immediately without retry."""
        response = Mock()
        response.status_code = 404
        exc = requests.exceptions.HTTPError(response=response)
        fn = Mock(side_effect=exc)
        with pytest.raises(requests.exceptions.HTTPError):
            _with_retries(fn, max_attempts=3)
        assert fn.call_count == 1

    def test_on_retry_callback_called(self) -> None:
        """on_retry callback is called on each retry."""
        response = Mock()
        response.status_code = 500
        exc = requests.exceptions.HTTPError(response=response)
        fn = Mock(side_effect=[exc, exc, "success"])
        on_retry = Mock()

        with patch("collector_core.network_utils.time.sleep"):
            result = _with_retries(fn, max_attempts=3, on_retry=on_retry, backoff_base=0.01)

        assert result == "success"
        assert on_retry.call_count == 2
        # First retry is attempt 1, second retry is attempt 2
        on_retry.assert_any_call(1, exc)
        on_retry.assert_any_call(2, exc)

    def test_exponential_backoff(self) -> None:
        """Backoff increases exponentially."""
        exc = requests.exceptions.Timeout()
        fn = Mock(side_effect=[exc, exc, "success"])
        sleep_times = []

        def mock_sleep(t: float) -> None:
            sleep_times.append(t)

        with patch("collector_core.network_utils.time.sleep", side_effect=mock_sleep):
            _with_retries(fn, max_attempts=3, backoff_base=2.0, backoff_max=60.0)

        # First backoff: 2^0 = 1, Second backoff: 2^1 = 2
        assert sleep_times == [1.0, 2.0]

    def test_backoff_max_cap(self) -> None:
        """Backoff is capped at backoff_max."""
        exc = requests.exceptions.Timeout()
        fn = Mock(side_effect=[exc, exc, exc, exc, "success"])
        sleep_times = []

        def mock_sleep(t: float) -> None:
            sleep_times.append(t)

        with patch("collector_core.network_utils.time.sleep", side_effect=mock_sleep):
            _with_retries(fn, max_attempts=5, backoff_base=2.0, backoff_max=3.0)

        # Backoffs: 2^0=1, 2^1=2, 2^2=4->capped to 3, 2^3=8->capped to 3
        assert sleep_times == [1.0, 2.0, 3.0, 3.0]

    def test_max_attempts_minimum_one(self) -> None:
        """max_attempts of 0 or negative becomes 1."""
        fn = Mock(return_value="success")
        result = _with_retries(fn, max_attempts=0)
        assert result == "success"
        assert fn.call_count == 1

    def test_retry_respects_429_flag(self) -> None:
        """429 retry behavior respects retry_on_429 flag."""
        response = Mock()
        response.status_code = 429
        exc = requests.exceptions.HTTPError(response=response)
        fn = Mock(side_effect=exc)

        # With retry_on_429=False, should not retry
        with pytest.raises(requests.exceptions.HTTPError):
            _with_retries(fn, max_attempts=3, retry_on_429=False)
        assert fn.call_count == 1

    def test_retry_respects_403_flag(self) -> None:
        """403 retry behavior respects retry_on_403 flag."""
        response = Mock()
        response.status_code = 403
        exc = requests.exceptions.HTTPError(response=response)
        fn = Mock(side_effect=[exc, "success"])

        # With retry_on_403=True, should retry
        with patch("collector_core.network_utils.time.sleep"):
            result = _with_retries(fn, max_attempts=3, retry_on_403=True, backoff_base=0.01)
        assert result == "success"
        assert fn.call_count == 2
