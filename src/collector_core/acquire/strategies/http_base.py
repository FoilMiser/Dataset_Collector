"""Shared HTTP download utilities for sync and async strategies.

This module provides common functionality used by both synchronous (requests-based)
and asynchronous (aiohttp/httpx-based) HTTP download strategies.

Classes:
    DownloadResult: Dataclass representing the outcome of a download operation
    HttpDownloadBase: Mixin class with shared download utilities

Functions:
    validate_url: Validate URL safety (no private IPs, valid scheme, etc.)
    compute_file_hash: Compute SHA-256 hash of a file
    parse_content_disposition: Extract filename from Content-Disposition header
"""

from __future__ import annotations

import hashlib
import ipaddress
import re
import socket
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlparse


# Constants
CHUNK_SIZE = 1024 * 1024  # 1 MB chunks for hashing and streaming
DEFAULT_CONNECT_TIMEOUT = 15.0
DEFAULT_READ_TIMEOUT = 300.0
MAX_REDIRECTS = 10

# URL validation
ALLOWED_SCHEMES = {"http", "https"}
DANGEROUS_PORTS = {22, 23, 25, 110, 143, 993, 995}  # SSH, Telnet, SMTP, POP, IMAP


@dataclass
class DownloadResult:
    """Result of a download operation.

    Attributes:
        success: Whether the download completed successfully
        path: Path to the downloaded file (None if failed)
        bytes_downloaded: Total bytes downloaded
        sha256: SHA-256 hash of the downloaded file (None if failed)
        error: Error message if download failed
        resumed: Whether this was a resumed download
        status_code: HTTP status code from the response
        headers: Response headers (redacted for logging)
        content_type: Content-Type header value
    """

    success: bool
    path: Path | None
    bytes_downloaded: int
    sha256: str | None
    error: str | None = None
    resumed: bool = False
    status_code: int | None = None
    headers: dict[str, str] = field(default_factory=dict)
    content_type: str | None = None


@dataclass
class UrlValidationResult:
    """Result of URL validation.

    Attributes:
        valid: Whether the URL is safe to fetch
        error: Error message if invalid
        normalized_url: The normalized/cleaned URL
        hostname: Extracted hostname
        port: Extracted or default port
        is_https: Whether URL uses HTTPS
    """

    valid: bool
    error: str | None = None
    normalized_url: str | None = None
    hostname: str | None = None
    port: int | None = None
    is_https: bool = False


def validate_url(
    url: str,
    *,
    allow_private: bool = False,
    allow_localhost: bool = False,
    allowed_schemes: set[str] | None = None,
) -> UrlValidationResult:
    """Validate that a URL is safe to fetch.

    Performs security checks including:
    - Valid URL scheme (http/https by default)
    - No private/internal IP addresses (unless allow_private=True)
    - No localhost (unless allow_localhost=True)
    - No dangerous ports
    - Valid hostname format

    Args:
        url: The URL to validate
        allow_private: Allow private/internal IP addresses
        allow_localhost: Allow localhost/127.0.0.1
        allowed_schemes: Set of allowed URL schemes (default: http, https)

    Returns:
        UrlValidationResult with validation outcome
    """
    schemes = allowed_schemes or ALLOWED_SCHEMES

    if not url or not isinstance(url, str):
        return UrlValidationResult(valid=False, error="URL is empty or not a string")

    try:
        parsed = urlparse(url)
    except Exception as e:
        return UrlValidationResult(valid=False, error=f"Failed to parse URL: {e}")

    # Check scheme
    if parsed.scheme.lower() not in schemes:
        return UrlValidationResult(
            valid=False,
            error=f"Invalid scheme '{parsed.scheme}'. Allowed: {schemes}",
        )

    # Check hostname exists
    if not parsed.hostname:
        return UrlValidationResult(valid=False, error="URL has no hostname")

    hostname = parsed.hostname.lower()

    # Check for localhost
    if not allow_localhost:
        if (
            hostname in ("localhost", "127.0.0.1", "::1")
            or hostname.endswith(".localhost")
        ):
            return UrlValidationResult(valid=False, error="Localhost URLs not allowed")

    # Resolve hostname and check IP
    try:
        ip_addresses = socket.getaddrinfo(
            hostname, None, socket.AF_UNSPEC, socket.SOCK_STREAM
        )
        for family, _, _, _, sockaddr in ip_addresses:
            ip_str = sockaddr[0]
            try:
                ip = ipaddress.ip_address(ip_str)
                if not allow_private:
                    if ip.is_private:
                        return UrlValidationResult(
                            valid=False,
                            error=f"Private IP address not allowed: {ip_str}",
                        )
                    if ip.is_loopback:
                        return UrlValidationResult(
                            valid=False,
                            error=f"Loopback address not allowed: {ip_str}",
                        )
                    if ip.is_link_local:
                        return UrlValidationResult(
                            valid=False,
                            error=f"Link-local address not allowed: {ip_str}",
                        )
            except ValueError:
                continue
    except socket.gaierror:
        # DNS resolution failed - this might be okay for dry runs
        pass

    # Check port
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    if port in DANGEROUS_PORTS:
        return UrlValidationResult(
            valid=False, error=f"Dangerous port not allowed: {port}"
        )

    return UrlValidationResult(
        valid=True,
        normalized_url=url,
        hostname=hostname,
        port=port,
        is_https=parsed.scheme.lower() == "https",
    )


def compute_file_hash(path: Path, algorithm: str = "sha256") -> str:
    """Compute hash of a file.

    Args:
        path: Path to the file
        algorithm: Hash algorithm (default: sha256)

    Returns:
        Lowercase hex-encoded hash string

    Raises:
        FileNotFoundError: If file doesn't exist
        ValueError: If algorithm is not supported
    """
    if algorithm not in hashlib.algorithms_available:
        raise ValueError(f"Unsupported hash algorithm: {algorithm}")

    h = hashlib.new(algorithm)
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(CHUNK_SIZE), b""):
            h.update(chunk)
    return h.hexdigest()


def supports_resume(headers: dict[str, str]) -> bool:
    """Check if server supports range requests for resume.

    Args:
        headers: Response headers from HEAD or GET request

    Returns:
        True if server accepts range requests
    """
    accept_ranges = headers.get("Accept-Ranges", "").lower()
    return accept_ranges == "bytes"


def build_resume_headers(
    existing_size: int,
    base_headers: dict[str, str] | None = None,
) -> dict[str, str]:
    """Build headers for resuming a partial download.

    Args:
        existing_size: Bytes already downloaded
        base_headers: Optional base headers to include

    Returns:
        Headers dict with Range header set
    """
    headers = dict(base_headers or {})
    headers["Range"] = f"bytes={existing_size}-"
    return headers


def parse_content_disposition(header: str | None) -> str | None:
    """Extract filename from Content-Disposition header.

    Args:
        header: Content-Disposition header value

    Returns:
        Extracted filename or None
    """
    if not header:
        return None

    # Try filename*= (RFC 5987) first
    match = re.search(
        r"filename\*=(?:UTF-8''|utf-8'')([^;]+)", header, re.IGNORECASE
    )
    if match:
        from urllib.parse import unquote

        return unquote(match.group(1))

    # Try filename= (quoted)
    match = re.search(r'filename="([^"]+)"', header)
    if match:
        return match.group(1)

    # Try filename= (unquoted)
    match = re.search(r"filename=([^;\s]+)", header)
    if match:
        return match.group(1)

    return None


def redact_headers_for_logging(headers: dict[str, str]) -> dict[str, str]:
    """Redact sensitive headers for safe logging.

    Args:
        headers: Original headers dict

    Returns:
        Headers with sensitive values redacted
    """
    sensitive_keys = {
        "authorization",
        "x-api-key",
        "api-key",
        "cookie",
        "set-cookie",
        "x-auth-token",
        "x-access-token",
    }

    redacted = {}
    for key, value in headers.items():
        if key.lower() in sensitive_keys:
            redacted[key] = "[REDACTED]"
        else:
            redacted[key] = value
    return redacted


def estimate_download_size(headers: dict[str, str]) -> int | None:
    """Estimate download size from response headers.

    Args:
        headers: Response headers

    Returns:
        Estimated size in bytes or None if unknown
    """
    content_length = headers.get("Content-Length")
    if content_length:
        try:
            return int(content_length)
        except ValueError:
            pass
    return None


def get_filename_from_url(url: str) -> str | None:
    """Extract filename from URL path.

    Args:
        url: The URL to extract filename from

    Returns:
        Filename or None if not determinable
    """
    try:
        parsed = urlparse(url)
        path = parsed.path
        if path:
            # Get the last path component
            filename = path.rsplit("/", 1)[-1]
            if filename and "." in filename:
                return filename
    except Exception:
        pass
    return None


class HttpDownloadBase:
    """Base class with shared HTTP download utilities.

    This class is designed to be used as a mixin or base class for both
    synchronous and asynchronous HTTP download implementations.
    """

    # Class-level configuration (can be overridden in subclasses)
    chunk_size: int = CHUNK_SIZE
    connect_timeout: float = DEFAULT_CONNECT_TIMEOUT
    read_timeout: float = DEFAULT_READ_TIMEOUT
    max_redirects: int = MAX_REDIRECTS

    def validate_url(
        self,
        url: str,
        *,
        allow_private: bool = False,
    ) -> UrlValidationResult:
        """Validate URL for download.

        See module-level validate_url for details.
        """
        return validate_url(url, allow_private=allow_private)

    def compute_file_hash(self, path: Path) -> str:
        """Compute SHA-256 hash of downloaded file.

        See module-level compute_file_hash for details.
        """
        return compute_file_hash(path, algorithm="sha256")

    def supports_resume(self, headers: dict[str, str]) -> bool:
        """Check if server supports resume.

        See module-level supports_resume for details.
        """
        return supports_resume(headers)

    def build_resume_headers(
        self,
        existing_size: int,
        base_headers: dict[str, str] | None = None,
    ) -> dict[str, str]:
        """Build headers for resume.

        See module-level build_resume_headers for details.
        """
        return build_resume_headers(existing_size, base_headers)

    def parse_content_disposition(self, header: str | None) -> str | None:
        """Parse Content-Disposition header.

        See module-level parse_content_disposition for details.
        """
        return parse_content_disposition(header)

    def redact_headers(self, headers: dict[str, str]) -> dict[str, str]:
        """Redact sensitive headers for logging.

        See module-level redact_headers_for_logging for details.
        """
        return redact_headers_for_logging(headers)

    def create_result(
        self,
        *,
        success: bool,
        path: Path | None = None,
        bytes_downloaded: int = 0,
        sha256: str | None = None,
        error: str | None = None,
        resumed: bool = False,
        status_code: int | None = None,
        headers: dict[str, str] | None = None,
        content_type: str | None = None,
    ) -> DownloadResult:
        """Create a DownloadResult with the given parameters.

        Convenience method for creating result objects with proper defaults.
        """
        return DownloadResult(
            success=success,
            path=path,
            bytes_downloaded=bytes_downloaded,
            sha256=sha256,
            error=error,
            resumed=resumed,
            status_code=status_code,
            headers=self.redact_headers(headers or {}),
            content_type=content_type,
        )


__all__ = [
    "CHUNK_SIZE",
    "DEFAULT_CONNECT_TIMEOUT",
    "DEFAULT_READ_TIMEOUT",
    "MAX_REDIRECTS",
    "ALLOWED_SCHEMES",
    "DANGEROUS_PORTS",
    "DownloadResult",
    "UrlValidationResult",
    "validate_url",
    "compute_file_hash",
    "supports_resume",
    "build_resume_headers",
    "parse_content_disposition",
    "redact_headers_for_logging",
    "estimate_download_size",
    "get_filename_from_url",
    "HttpDownloadBase",
]
