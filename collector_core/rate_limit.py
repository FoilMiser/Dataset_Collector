"""
collector_core/rate_limit.py

Rate limiting utilities using token bucket algorithm.

Features:
- Token bucket rate limiter with configurable capacity and refill rate
- Thread-safe for concurrent use
- Deterministic clock injection for testing
- Configurable from YAML resolver blocks

Usage:
------
    from collector_core.rate_limit import RateLimiter, get_rate_limiter

    # Create a limiter: 10 requests per second, burst of 20
    limiter = RateLimiter(capacity=20, refill_rate=10.0)

    # Acquire before each request (blocks if needed)
    limiter.acquire()
    response = requests.get(url)

    # Or use try_acquire for non-blocking check
    if limiter.try_acquire():
        response = requests.get(url)
    else:
        print("Rate limited, try again later")

    # Get shared limiter by name (for resolver-level rate limiting)
    github_limiter = get_rate_limiter("github", capacity=60, refill_rate=1.0)

Configuration in targets_*.yaml:
--------------------------------
    resolvers:
      github:
        rate_limit:
          capacity: 60          # Maximum burst size
          refill_rate: 1.0      # Tokens per second
          initial_tokens: 60    # Starting tokens (optional, defaults to capacity)
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

# Registry of shared rate limiters by name
_limiters: dict[str, RateLimiter] = {}
_limiters_lock = threading.Lock()


@dataclass
class RateLimiterConfig:
    """Configuration for a rate limiter."""

    capacity: float = 60.0  # Maximum tokens (burst size)
    refill_rate: float = 1.0  # Tokens added per second
    initial_tokens: float | None = None  # Starting tokens (defaults to capacity)

    @classmethod
    def from_dict(cls, d: dict[str, Any] | None) -> RateLimiterConfig:
        """Create config from dict (e.g., YAML config)."""
        if not d:
            return cls()
        return cls(
            capacity=float(d.get("capacity", 60.0)),
            refill_rate=float(d.get("refill_rate", 1.0)),
            initial_tokens=float(d["initial_tokens"]) if d.get("initial_tokens") else None,
        )


@dataclass
class RateLimiter:
    """
    Token bucket rate limiter.

    Thread-safe implementation that supports both blocking and non-blocking
    token acquisition.

    Attributes:
        capacity: Maximum number of tokens in the bucket
        refill_rate: Tokens added per second
        tokens: Current number of tokens
        last_refill: Timestamp of last token refill
        clock: Time function for testing (defaults to time.monotonic)
        sleep: Sleep function for testing (defaults to time.sleep)
    """

    capacity: float = 60.0
    refill_rate: float = 1.0
    tokens: float = field(init=False, default=0.0)
    last_refill: float = field(init=False, default=0.0)
    _lock: threading.Lock = field(init=False, default_factory=threading.Lock)
    clock: Callable[[], float] = field(default=time.monotonic, repr=False)
    sleep: Callable[[float], None] = field(default=time.sleep, repr=False)

    def __post_init__(self) -> None:
        self.tokens = self.capacity
        self.last_refill = self.clock()

    @classmethod
    def from_config(
        cls, config: RateLimiterConfig, *, clock: Callable[[], float] | None = None
    ) -> RateLimiter:
        """Create limiter from config."""
        limiter = cls(
            capacity=config.capacity,
            refill_rate=config.refill_rate,
            clock=clock or time.monotonic,
        )
        if config.initial_tokens is not None:
            limiter.tokens = min(config.initial_tokens, config.capacity)
        return limiter

    def _refill(self) -> None:
        """Refill tokens based on elapsed time (must hold lock)."""
        now = self.clock()
        elapsed = now - self.last_refill
        if elapsed > 0:
            added = elapsed * self.refill_rate
            self.tokens = min(self.capacity, self.tokens + added)
            self.last_refill = now

    def try_acquire(self, tokens: float = 1.0) -> bool:
        """
        Try to acquire tokens without blocking.

        Returns True if tokens were acquired, False if rate limited.
        """
        with self._lock:
            self._refill()
            if self.tokens >= tokens:
                self.tokens -= tokens
                return True
            return False

    def acquire(self, tokens: float = 1.0) -> float:
        """
        Acquire tokens, blocking if necessary.

        Returns the time waited in seconds.
        """
        waited = 0.0
        while True:
            with self._lock:
                self._refill()
                if self.tokens >= tokens:
                    self.tokens -= tokens
                    return waited
                # Calculate wait time
                needed = tokens - self.tokens
                wait_time = needed / self.refill_rate if self.refill_rate > 0 else 1.0
            # Release lock while sleeping
            self.sleep(min(wait_time, 1.0))  # Cap at 1 second for responsiveness
            waited += min(wait_time, 1.0)

    def available_tokens(self) -> float:
        """Get current available tokens (after refill)."""
        with self._lock:
            self._refill()
            return self.tokens

    def reset(self) -> None:
        """Reset limiter to full capacity."""
        with self._lock:
            self.tokens = self.capacity
            self.last_refill = self.clock()


def get_rate_limiter(
    name: str,
    capacity: float = 60.0,
    refill_rate: float = 1.0,
    *,
    config: RateLimiterConfig | dict[str, Any] | None = None,
) -> RateLimiter:
    """
    Get or create a shared rate limiter by name.

    If a limiter with this name exists, returns the existing one.
    Otherwise creates a new one with the given parameters.

    Args:
        name: Unique name for this limiter (e.g., "github", "zenodo")
        capacity: Maximum burst size
        refill_rate: Tokens per second
        config: Optional config dict or RateLimiterConfig

    Returns:
        The rate limiter for this name
    """
    with _limiters_lock:
        if name not in _limiters:
            if isinstance(config, dict):
                cfg = RateLimiterConfig.from_dict(config)
            elif isinstance(config, RateLimiterConfig):
                cfg = config
            else:
                cfg = RateLimiterConfig(capacity=capacity, refill_rate=refill_rate)
            _limiters[name] = RateLimiter.from_config(cfg)
        return _limiters[name]


def reset_rate_limiters() -> None:
    """Clear all shared rate limiters (for testing)."""
    with _limiters_lock:
        _limiters.clear()


# Default rate limit configurations for known services
DEFAULT_RATE_LIMITS: dict[str, RateLimiterConfig] = {
    # GitHub: 60 requests per hour for unauthenticated, 5000 for authenticated
    # We use conservative defaults for unauthenticated access
    "github": RateLimiterConfig(capacity=60, refill_rate=1.0),
    # Zenodo: Be conservative with API calls
    "zenodo": RateLimiterConfig(capacity=30, refill_rate=0.5),
    # Dataverse: Varies by instance, use moderate defaults
    "dataverse": RateLimiterConfig(capacity=30, refill_rate=0.5),
    # Figshare: Be conservative
    "figshare": RateLimiterConfig(capacity=30, refill_rate=0.5),
}


def get_service_rate_limiter(
    service: str, config_override: dict[str, Any] | None = None
) -> RateLimiter:
    """
    Get rate limiter for a known service with default configuration.

    Args:
        service: Service name (github, zenodo, dataverse, figshare)
        config_override: Optional config to override defaults

    Returns:
        Rate limiter configured for the service
    """
    default_cfg = DEFAULT_RATE_LIMITS.get(service, RateLimiterConfig())
    if config_override:
        cfg = RateLimiterConfig(
            capacity=config_override.get("capacity", default_cfg.capacity),
            refill_rate=config_override.get("refill_rate", default_cfg.refill_rate),
            initial_tokens=config_override.get("initial_tokens"),
        )
    else:
        cfg = default_cfg
    return get_rate_limiter(service, config=cfg)
