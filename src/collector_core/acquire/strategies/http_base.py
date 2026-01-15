"""Shared HTTP download helpers and constants."""

from __future__ import annotations

import hashlib
import ipaddress
import socket
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from email.message import Message
from email.utils import decode_rfc2231
from pathlib import Path
from urllib.parse import unquote, urlparse

from collector_core.acquire.context import InternalMirrorAllowlist

CHUNK_SIZE = 1024 * 1024  # 1 MB chunks
DEFAULT_CONNECT_TIMEOUT = 15.0
DEFAULT_READ_TIMEOUT = 300.0
SUPPORTED_HTTP_SCHEMES = {"http", "https"}


@dataclass(frozen=True)
class UrlValidationResult:
    """Result of URL validation."""

    allowed: bool
    reason: str | None = None
    blocked_url: str | None = None


@dataclass(frozen=True)
class DownloadResult:
    """Structured representation of a download result."""

    status: str
    path: str | None = None
    resolved_url: str | None = None
    content_length: int | None = None
    sha256: str | None = None
    cached: bool | None = None
    error: str | None = None
    message: str | None = None
    url: str | None = None
    reason: str | None = None
    blocked_url: str | None = None
    expected_sha256: str | None = None

    def to_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {"status": self.status}
        for key, value in {
            "path": self.path,
            "resolved_url": self.resolved_url,
            "content_length": self.content_length,
            "sha256": self.sha256,
            "cached": self.cached,
            "error": self.error,
            "message": self.message,
            "url": self.url,
            "reason": self.reason,
            "blocked_url": self.blocked_url,
            "expected_sha256": self.expected_sha256,
        }.items():
            if value is not None:
                payload[key] = value
        return payload


def _non_global_ip_reason(ip: ipaddress.IPv4Address | ipaddress.IPv6Address) -> str:
    if ip.is_private:
        return "private"
    if ip.is_loopback:
        return "loopback"
    if ip.is_link_local:
        return "link_local"
    if ip.is_multicast:
        return "multicast"
    if ip.is_reserved:
        return "reserved"
    if ip.is_unspecified:
        return "unspecified"
    return "non_global"


def _resolve_host_ips(hostname: str) -> list[str]:
    ips: set[str] = set()
    try:
        addrinfo = socket.getaddrinfo(hostname, None)
    except socket.gaierror:
        return []
    for item in addrinfo:
        sockaddr = item[4]
        if sockaddr:
            ips.add(sockaddr[0])
    return sorted(ips)


def _get_header(headers: Mapping[str, str], name: str) -> str | None:
    if name in headers:
        return headers[name]
    name_lower = name.lower()
    for key, value in headers.items():
        if key.lower() == name_lower:
            return value
    return None


class HttpDownloadBase:
    """Shared helper methods for HTTP download implementations."""

    @staticmethod
    def sha256_file(path: Path, chunk_size: int = CHUNK_SIZE) -> str:
        h = hashlib.sha256()
        with path.open("rb") as f:
            for chunk in iter(lambda: f.read(chunk_size), b""):
                h.update(chunk)
        return h.hexdigest()

    @staticmethod
    def parse_content_length(
        headers: Mapping[str, str],
        status_code: int,
        existing: int,
    ) -> int | None:
        content_range = _get_header(headers, "Content-Range")
        if content_range and "/" in content_range:
            total = content_range.split("/", 1)[1]
            if total.isdigit():
                return int(total)
        content_length = _get_header(headers, "Content-Length")
        if content_length and content_length.isdigit():
            length = int(content_length)
            if status_code == 206 and existing:
                return existing + length
            return length
        return None

    @staticmethod
    def valid_content_range(header: str | None, start_offset: int) -> bool:
        if not header:
            return False
        if not header.startswith("bytes "):
            return False
        try:
            range_part = header.split(" ", 1)[1]
            span, _total = range_part.split("/", 1)
            start_str, _end_str = span.split("-", 1)
            return int(start_str) == start_offset
        except ValueError:
            return False

    @staticmethod
    def parse_content_disposition_filename(header: str | None) -> str | None:
        if not header:
            return None
        message = Message()
        message["content-disposition"] = header
        filename_star = message.get_param("filename*", header="content-disposition")
        if filename_star:
            if isinstance(filename_star, tuple):
                charset, _language, text = filename_star
                if charset:
                    try:
                        return text.encode("latin-1").decode(charset)
                    except (LookupError, UnicodeEncodeError, UnicodeDecodeError):
                        return text
                return text
            else:
                charset, _language, text = decode_rfc2231(filename_star)
            try:
                return unquote(text, encoding=charset or "utf-8")
            except LookupError:
                return unquote(text)
        filename = message.get_param("filename", header="content-disposition")
        if filename:
            if isinstance(filename, tuple):
                charset, _language, text = filename
                if charset:
                    try:
                        return text.encode("latin-1").decode(charset)
                    except (LookupError, UnicodeEncodeError, UnicodeDecodeError):
                        return text
                return text
            return filename
        return None

    @staticmethod
    def validate_download_url(
        url: str,
        allow_non_global_hosts: bool,
        internal_mirror_allowlist: InternalMirrorAllowlist | None = None,
    ) -> UrlValidationResult:
        parsed = urlparse(url)
        scheme = (parsed.scheme or "").lower()
        if scheme not in SUPPORTED_HTTP_SCHEMES:
            return UrlValidationResult(False, f"unsupported_scheme:{scheme or 'missing'}")
        if not parsed.hostname:
            return UrlValidationResult(False, "missing_hostname")
        if allow_non_global_hosts:
            return UrlValidationResult(True)
        allowlist = internal_mirror_allowlist or InternalMirrorAllowlist()
        hostname = parsed.hostname
        if allowlist.allows_host(hostname):
            return UrlValidationResult(True)
        try:
            ip_value = ipaddress.ip_address(hostname)
        except ValueError:
            ips = _resolve_host_ips(hostname)
            if not ips:
                return UrlValidationResult(False, "unresolvable_hostname")
            for ip_str in ips:
                ip_value = ipaddress.ip_address(ip_str)
                if ip_value.is_global:
                    continue
                if allowlist.allows_ip(ip_value):
                    continue
                return UrlValidationResult(
                    False,
                    f"blocked_ip:{ip_str}:{_non_global_ip_reason(ip_value)}",
                )
            return UrlValidationResult(True)
        if not ip_value.is_global and not allowlist.allows_ip(ip_value):
            return UrlValidationResult(
                False,
                f"blocked_ip:{ip_value}:{_non_global_ip_reason(ip_value)}",
            )
        return UrlValidationResult(True)

    @staticmethod
    def validate_redirect_urls(
        redirect_urls: Iterable[str],
        allow_non_global_hosts: bool,
        internal_mirror_allowlist: InternalMirrorAllowlist | None = None,
    ) -> UrlValidationResult:
        for redirect_url in redirect_urls:
            result = HttpDownloadBase.validate_download_url(
                redirect_url, allow_non_global_hosts, internal_mirror_allowlist
            )
            if not result.allowed:
                return UrlValidationResult(
                    False, result.reason, blocked_url=redirect_url
                )
        return UrlValidationResult(True)
