from __future__ import annotations

from typing import Any

from collector_core.__version__ import __version__ as VERSION
from collector_core.dependencies import _try_import, requires
from collector_core.exceptions import DependencyMissingError

requests = _try_import("requests")

DEFAULT_CONNECT_TIMEOUT = 15
DEFAULT_READ_TIMEOUT = 300
DEFAULT_TIMEOUT = (DEFAULT_CONNECT_TIMEOUT, DEFAULT_READ_TIMEOUT)
DEFAULT_RETRY_STATUS_CODES = {429, 500, 502, 503, 504}


def build_user_agent(name: str = "collector-core", version: str = VERSION) -> str:
    """Build a default User-Agent string."""
    return f"{name}/{version}"


def require_requests() -> Any:
    """Return requests module or raise DependencyMissingError."""
    missing = requires("requests", requests, install="pip install requests")
    if missing:
        raise DependencyMissingError(
            missing,
            dependency="requests",
            install="pip install requests",
        )
    return requests


def create_retry_session(
    *,
    total_retries: int = 3,
    backoff_factor: float = 0.5,
    status_forcelist: set[int] | None = None,
) -> Any:
    """Create a requests session with basic retry/backoff handling."""
    req = require_requests()
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    status_list = status_forcelist or DEFAULT_RETRY_STATUS_CODES
    retries = Retry(
        total=total_retries,
        backoff_factor=backoff_factor,
        status_forcelist=list(status_list),
        allowed_methods=("GET", "HEAD"),
        raise_on_status=False,
    )
    session = req.Session()
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def http_get_bytes(
    url: str,
    *,
    timeout_s: int = 120,
    user_agent: str | None = None,
) -> tuple[bytes, dict]:
    """Fetch content via HTTP and return bytes + metadata."""
    req = require_requests()
    headers = {"User-Agent": user_agent} if user_agent else None
    with req.get(url, stream=True, timeout=timeout_s, headers=headers) as response:
        response.raise_for_status()
        return response.content, {
            "status_code": response.status_code,
            "bytes": len(response.content),
        }
