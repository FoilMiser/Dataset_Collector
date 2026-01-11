"""
Tests for collector_core.rate_limit module.

All tests use deterministic clocks (no real sleeping).
"""

from __future__ import annotations

from collector_core.rate_limit import (  # noqa: E402
    DEFAULT_RATE_LIMITS,
    RateLimiter,
    RateLimiterConfig,
    get_rate_limiter,
    get_resolver_rate_limiter,
    get_service_rate_limiter,
    reset_rate_limiters,
)


class DeterministicClock:
    """A clock that advances only when explicitly told to."""

    def __init__(self, start: float = 0.0) -> None:
        self.time = start
        self.sleep_calls: list[float] = []

    def __call__(self) -> float:
        return self.time

    def advance(self, seconds: float) -> None:
        self.time += seconds

    def sleep(self, seconds: float) -> None:
        """Fake sleep that records calls and advances time."""
        self.sleep_calls.append(seconds)
        self.advance(seconds)


class TestRateLimiter:
    """Tests for RateLimiter class."""

    def test_initial_tokens_at_capacity(self) -> None:
        """Limiter starts with full capacity."""
        clock = DeterministicClock()
        limiter = RateLimiter(capacity=10, refill_rate=1.0, clock=clock, sleep=clock.sleep)
        assert limiter.available_tokens() == 10.0

    def test_try_acquire_succeeds_with_tokens(self) -> None:
        """try_acquire returns True when tokens available."""
        clock = DeterministicClock()
        limiter = RateLimiter(capacity=5, refill_rate=1.0, clock=clock, sleep=clock.sleep)

        assert limiter.try_acquire() is True
        assert limiter.available_tokens() == 4.0

    def test_try_acquire_fails_when_empty(self) -> None:
        """try_acquire returns False when no tokens available."""
        clock = DeterministicClock()
        limiter = RateLimiter(capacity=2, refill_rate=1.0, clock=clock, sleep=clock.sleep)

        assert limiter.try_acquire() is True
        assert limiter.try_acquire() is True
        assert limiter.try_acquire() is False  # Empty

    def test_tokens_refill_over_time(self) -> None:
        """Tokens are refilled based on elapsed time."""
        clock = DeterministicClock()
        limiter = RateLimiter(capacity=10, refill_rate=2.0, clock=clock, sleep=clock.sleep)

        # Drain all tokens
        for _ in range(10):
            limiter.try_acquire()
        assert limiter.available_tokens() == 0.0

        # Advance 3 seconds at 2 tokens/sec = 6 tokens
        clock.advance(3.0)
        assert limiter.available_tokens() == 6.0

    def test_tokens_capped_at_capacity(self) -> None:
        """Tokens don't exceed capacity."""
        clock = DeterministicClock()
        limiter = RateLimiter(capacity=10, refill_rate=100.0, clock=clock, sleep=clock.sleep)

        clock.advance(1000.0)  # Would add 100,000 tokens
        assert limiter.available_tokens() == 10.0  # Capped at capacity

    def test_acquire_waits_when_empty(self) -> None:
        """acquire blocks and returns wait time."""
        clock = DeterministicClock()
        limiter = RateLimiter(capacity=1, refill_rate=1.0, clock=clock, sleep=clock.sleep)

        limiter.try_acquire()  # Empty the bucket
        waited = limiter.acquire()  # Should wait for refill

        assert waited > 0
        assert len(clock.sleep_calls) > 0

    def test_acquire_multiple_tokens(self) -> None:
        """Can acquire multiple tokens at once."""
        clock = DeterministicClock()
        limiter = RateLimiter(capacity=10, refill_rate=1.0, clock=clock, sleep=clock.sleep)

        assert limiter.try_acquire(5) is True
        assert limiter.available_tokens() == 5.0

        assert limiter.try_acquire(6) is False  # Not enough
        assert limiter.try_acquire(5) is True  # Exact amount
        assert limiter.available_tokens() == 0.0

    def test_reset_restores_capacity(self) -> None:
        """reset() fills bucket to capacity."""
        clock = DeterministicClock()
        limiter = RateLimiter(capacity=10, refill_rate=1.0, clock=clock, sleep=clock.sleep)

        for _ in range(10):
            limiter.try_acquire()
        assert limiter.available_tokens() == 0.0

        limiter.reset()
        assert limiter.available_tokens() == 10.0


class TestRateLimiterConfig:
    """Tests for RateLimiterConfig class."""

    def test_from_dict_with_all_fields(self) -> None:
        """Config loads all fields from dict."""
        config = RateLimiterConfig.from_dict(
            {"capacity": 100, "refill_rate": 5.0, "initial_tokens": 50}
        )
        assert config.capacity == 100.0
        assert config.refill_rate == 5.0
        assert config.initial_tokens == 50.0

    def test_from_dict_with_defaults(self) -> None:
        """Config uses defaults for missing fields."""
        config = RateLimiterConfig.from_dict({})
        assert config.capacity == 60.0
        assert config.refill_rate == 1.0
        assert config.initial_tokens is None

    def test_from_dict_with_none(self) -> None:
        """Config handles None input."""
        config = RateLimiterConfig.from_dict(None)
        assert config.capacity == 60.0

    def test_from_config_with_initial_tokens(self) -> None:
        """Limiter respects initial_tokens from config."""
        config = RateLimiterConfig(capacity=100, refill_rate=1.0, initial_tokens=25)
        clock = DeterministicClock()
        limiter = RateLimiter.from_config(config, clock=clock)

        assert limiter.available_tokens() == 25.0

    def test_initial_tokens_capped_at_capacity(self) -> None:
        """initial_tokens can't exceed capacity."""
        config = RateLimiterConfig(capacity=10, refill_rate=1.0, initial_tokens=100)
        limiter = RateLimiter.from_config(config)

        assert limiter.available_tokens() == 10.0

    def test_from_dict_requests_per_minute(self) -> None:
        """Config converts requests_per_minute to refill_rate."""
        config = RateLimiterConfig.from_dict({"requests_per_minute": 60})
        assert config.refill_rate == 1.0  # 60/60 = 1 per second

        config = RateLimiterConfig.from_dict({"requests_per_minute": 120})
        assert config.refill_rate == 2.0  # 120/60 = 2 per second

    def test_from_dict_requests_per_hour(self) -> None:
        """Config converts requests_per_hour to refill_rate."""
        config = RateLimiterConfig.from_dict({"requests_per_hour": 3600})
        assert config.refill_rate == 1.0  # 3600/3600 = 1 per second

        config = RateLimiterConfig.from_dict({"requests_per_hour": 60})
        assert abs(config.refill_rate - (60 / 3600)) < 0.0001  # ~0.0167 per second

    def test_from_dict_requests_per_second(self) -> None:
        """Config uses requests_per_second directly."""
        config = RateLimiterConfig.from_dict({"requests_per_second": 5.0})
        assert config.refill_rate == 5.0

    def test_from_dict_burst(self) -> None:
        """Config uses burst as capacity."""
        config = RateLimiterConfig.from_dict({"burst": 100})
        assert config.capacity == 100.0

    def test_from_dict_retry_options(self) -> None:
        """Config loads retry options."""
        config = RateLimiterConfig.from_dict(
            {"retry_on_429": False, "retry_on_403": True, "exponential_backoff": False}
        )
        assert config.retry_on_429 is False
        assert config.retry_on_403 is True
        assert config.exponential_backoff is False

    def test_from_dict_yaml_friendly_combination(self) -> None:
        """Config handles typical YAML config from targets file."""
        # Example config from targets_*.yaml
        yaml_config = {"requests_per_minute": 30, "burst": 10, "retry_on_403": True}
        config = RateLimiterConfig.from_dict(yaml_config)
        assert config.capacity == 10.0  # burst
        assert config.refill_rate == 0.5  # 30/60
        assert config.retry_on_403 is True


class TestSharedLimiters:
    """Tests for shared rate limiter registry."""

    def setup_method(self) -> None:
        """Clear registry before each test."""
        reset_rate_limiters()

    def test_get_rate_limiter_creates_new(self) -> None:
        """get_rate_limiter creates a new limiter."""
        limiter = get_rate_limiter("test_service", capacity=50, refill_rate=2.0)
        assert limiter.capacity == 50.0
        assert limiter.refill_rate == 2.0

    def test_get_rate_limiter_returns_existing(self) -> None:
        """get_rate_limiter returns existing limiter for same name."""
        limiter1 = get_rate_limiter("test_service", capacity=50)
        limiter2 = get_rate_limiter("test_service", capacity=100)  # Different capacity

        assert limiter1 is limiter2
        assert limiter1.capacity == 50.0  # Original value kept

    def test_different_names_different_limiters(self) -> None:
        """Different names get different limiters."""
        limiter1 = get_rate_limiter("service_a", capacity=10)
        limiter2 = get_rate_limiter("service_b", capacity=20)

        assert limiter1 is not limiter2
        assert limiter1.capacity == 10.0
        assert limiter2.capacity == 20.0

    def test_reset_rate_limiters_clears_all(self) -> None:
        """reset_rate_limiters clears the registry."""
        limiter1 = get_rate_limiter("test", capacity=10)
        reset_rate_limiters()
        limiter2 = get_rate_limiter("test", capacity=20)

        assert limiter1 is not limiter2
        assert limiter2.capacity == 20.0


class TestServiceRateLimiters:
    """Tests for service-specific rate limiters."""

    def setup_method(self) -> None:
        reset_rate_limiters()

    def test_github_default_config(self) -> None:
        """GitHub limiter uses default config."""
        limiter = get_service_rate_limiter("github")

        cfg = DEFAULT_RATE_LIMITS["github"]
        assert limiter.capacity == cfg.capacity
        assert limiter.refill_rate == cfg.refill_rate

    def test_service_with_override(self) -> None:
        """Service config can be overridden."""
        limiter = get_service_rate_limiter("github", {"capacity": 1000, "refill_rate": 10.0})

        assert limiter.capacity == 1000.0
        assert limiter.refill_rate == 10.0

    def test_unknown_service_uses_defaults(self) -> None:
        """Unknown service gets default RateLimiterConfig."""
        limiter = get_service_rate_limiter("unknown_api")

        cfg = RateLimiterConfig()  # Default values
        assert limiter.capacity == cfg.capacity


class TestRateLimitNoRequestBursts:
    """Test that rate limits actually prevent request bursts."""

    def test_burst_prevention(self) -> None:
        """
        Verify configured rate limits throttle requests.

        This is the acceptance criterion from the checklist:
        "Configured rate limits actually throttle requests;
        tests validate no request bursts beyond the configured capacity."
        """
        clock = DeterministicClock()
        # 5 requests per second, burst of 3
        limiter = RateLimiter(capacity=3, refill_rate=5.0, clock=clock, sleep=clock.sleep)

        # Should immediately get 3 (burst capacity)
        acquired_count = 0
        for _ in range(10):
            if limiter.try_acquire():
                acquired_count += 1

        assert acquired_count == 3, "Should only allow burst capacity"

        # After 1 second, should be able to get 5 more (refill_rate)
        clock.advance(1.0)

        acquired_count = 0
        for _ in range(10):
            if limiter.try_acquire():
                acquired_count += 1

        assert acquired_count == 3, "Tokens capped at capacity"

        # After another 0.21 seconds, should get ~1 token (0.21 * 5 = 1.05)
        clock.advance(0.21)
        assert limiter.try_acquire() is True
        assert limiter.try_acquire() is False  # Only ~1 token was added


class TestGetResolverRateLimiter:
    """Tests for get_resolver_rate_limiter function."""

    def setup_method(self) -> None:
        reset_rate_limiters()

    def test_returns_none_limiter_when_no_config(self) -> None:
        """Returns None limiter when no rate_limit config."""
        limiter, config = get_resolver_rate_limiter(None, "github")
        assert limiter is None
        # But config should be the default for github
        assert config.capacity == DEFAULT_RATE_LIMITS["github"].capacity

    def test_returns_none_limiter_when_no_resolver_config(self) -> None:
        """Returns None limiter when resolver has no rate_limit."""
        cfg = {"resolvers": {"github": {"base_url": "https://api.github.com"}}}
        limiter, config = get_resolver_rate_limiter(cfg, "github")
        assert limiter is None

    def test_returns_limiter_when_rate_limit_configured(self) -> None:
        """Returns limiter when rate_limit is configured."""
        cfg = {
            "resolvers": {
                "github": {
                    "rate_limit": {"requests_per_minute": 60, "burst": 10, "retry_on_403": True}
                }
            }
        }
        limiter, config = get_resolver_rate_limiter(cfg, "github")
        assert limiter is not None
        assert limiter.capacity == 10.0  # burst
        assert limiter.refill_rate == 1.0  # 60/60
        assert config.retry_on_403 is True

    def test_resolver_rate_limiter_figshare(self) -> None:
        """Figshare rate limiting from config."""
        cfg = {
            "resolvers": {
                "figshare": {"rate_limit": {"requests_per_minute": 30, "burst": 5}}
            }
        }
        limiter, config = get_resolver_rate_limiter(cfg, "figshare")
        assert limiter is not None
        assert limiter.capacity == 5.0
        assert limiter.refill_rate == 0.5  # 30/60

    def test_unknown_resolver_returns_default_config(self) -> None:
        """Unknown resolver returns default config."""
        limiter, config = get_resolver_rate_limiter({}, "unknown_resolver")
        assert limiter is None
        assert config.capacity == RateLimiterConfig().capacity
