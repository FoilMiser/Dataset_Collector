# Dataset Collector: A-Grade Upgrade Plan

## Executive Summary

This document provides a comprehensive, executable plan to upgrade the Dataset Collector repository from its current B+ state to A-grade production quality. The plan is organized into five phases with concrete tasks, code examples, and acceptance criteria.

**Current State Assessment:**
- ~39,000 lines of Python across 200+ files
- 18 domain pipelines with significant boilerplate duplication
- GREEN/YELLOW/RED license classification system (well-designed)
- 358 tests but gaps in coverage
- Incomplete type annotations
- Several stub implementations needing completion

**Target State:**
- Clean architecture with no deprecated code
- Full feature implementation (near-duplicate detection, domain screeners, content checks)
- 90%+ test coverage with property-based testing
- Complete type annotations
- Production-ready with metrics, checkpointing, and documentation

---

## Phase 1: Technical Debt Elimination

### 1.1 Remove Deprecated Pipeline Wrappers

**Problem:** Each of the 18 pipelines contains 6-7 wrapper files that are pure boilerplate, totaling ~2,500 lines of duplicated code. These are deprecated compatibility shims that delegate to `collector_core`.

**Files to Delete:**

For each pipeline directory (`*_pipeline_v2/`), remove these files:
- `acquire_worker.py`
- `merge_worker.py`
- `yellow_screen_worker.py`
- `pipeline_driver.py`
- `catalog_builder.py`
- `review_queue.py`
- `pmc_worker.py` (where present)
- `legacy/` directory (entire directory)

**Pipelines to process:**
```
3d_modeling_pipeline_v2/
agri_circular_pipeline_v2/
biology_pipeline_v2/
chem_pipeline_v2/
code_pipeline_v2/
cyber_pipeline_v2/
earth_pipeline_v2/
econ_stats_decision_adaptation_pipeline_v2/
engineering_pipeline_v2/
fixture_pipeline_v2/
kg_nav_pipeline_v2/
logic_pipeline_v2/
materials_science_pipeline_v2/
math_pipeline_v2/
metrology_pipeline_v2/
nlp_pipeline_v2/
physics_pipeline_v2/
regcomp_pipeline_v2/
safety_incident_pipeline_v2/
```

**Files to KEEP** (these contain actual domain-specific logic):
- `code_pipeline_v2/code_worker.py`
- `code_pipeline_v2/acquire_plugin.py`
- `cyber_pipeline_v2/nvd_worker.py`
- `cyber_pipeline_v2/stix_worker.py`
- `cyber_pipeline_v2/advisory_worker.py`
- `3d_modeling_pipeline_v2/mesh_worker.py`
- `3d_modeling_pipeline_v2/acquire_plugin.py`
- `metrology_pipeline_v2/acquire_plugin.py`
- `chem_pipeline_v2/yellow_scrubber.py` (has domain-specific logic)
- `code_pipeline_v2/yellow_scrubber.py` (has domain-specific logic)
- `regcomp_pipeline_v2/yellow_scrubber.py` (has domain-specific logic)

**Each pipeline directory should contain only:**
```
*_pipeline_v2/
├── README.md
├── requirements.txt
└── [domain-specific workers if any]
```

**Implementation:**

```bash
# Script to execute the cleanup
#!/bin/bash

PIPELINES=(
    "3d_modeling_pipeline_v2"
    "agri_circular_pipeline_v2"
    "biology_pipeline_v2"
    "chem_pipeline_v2"
    "code_pipeline_v2"
    "cyber_pipeline_v2"
    "earth_pipeline_v2"
    "econ_stats_decision_adaptation_pipeline_v2"
    "engineering_pipeline_v2"
    "fixture_pipeline_v2"
    "kg_nav_pipeline_v2"
    "logic_pipeline_v2"
    "materials_science_pipeline_v2"
    "math_pipeline_v2"
    "metrology_pipeline_v2"
    "nlp_pipeline_v2"
    "physics_pipeline_v2"
    "regcomp_pipeline_v2"
    "safety_incident_pipeline_v2"
)

# Files that are always boilerplate (delete these)
BOILERPLATE_FILES=(
    "acquire_worker.py"
    "merge_worker.py"
    "yellow_screen_worker.py"
    "pipeline_driver.py"
    "catalog_builder.py"
    "review_queue.py"
    "pmc_worker.py"
)

for pipeline in "${PIPELINES[@]}"; do
    echo "Processing $pipeline..."
    
    # Remove legacy directory
    rm -rf "$pipeline/legacy"
    
    # Remove boilerplate files
    for file in "${BOILERPLATE_FILES[@]}"; do
        if [ -f "$pipeline/$file" ]; then
            rm "$pipeline/$file"
            echo "  Removed $pipeline/$file"
        fi
    done
done
```

**Add migration helper to collector_core:**

Create `src/collector_core/migration.py`:

```python
"""Migration helpers for deprecated imports."""

from __future__ import annotations

import warnings
from typing import NoReturn


def deprecated_import_error(module_name: str, replacement: str) -> NoReturn:
    """Raise ImportError with migration guidance.
    
    This function is called when deprecated modules are imported directly.
    
    Args:
        module_name: The deprecated module being imported
        replacement: The replacement command or import path
        
    Raises:
        ImportError: Always, with migration guidance
    """
    raise ImportError(
        f"The module '{module_name}' has been removed in v3.0.\n"
        f"Use instead: {replacement}\n"
        f"See docs/migration_guide.md for details."
    )


def emit_deprecation_warning(
    old_usage: str,
    new_usage: str,
    removal_version: str = "4.0",
) -> None:
    """Emit a deprecation warning with migration guidance.
    
    Args:
        old_usage: Description of the deprecated usage
        new_usage: Description of the replacement
        removal_version: Version when old usage will be removed
    """
    warnings.warn(
        f"{old_usage} is deprecated and will be removed in v{removal_version}. "
        f"Use {new_usage} instead.",
        DeprecationWarning,
        stacklevel=3,
    )
```

**Update documentation:**

Update `docs/migration_guide.md` to include:

```markdown
## Removed in v3.0

### Per-Pipeline Worker Scripts

The following files have been removed from all pipeline directories:
- `acquire_worker.py`
- `merge_worker.py`
- `yellow_screen_worker.py`
- `pipeline_driver.py`
- `catalog_builder.py`
- `review_queue.py`
- `pmc_worker.py`

**Migration:** Use the unified CLI instead:

```bash
# Old (removed):
python math_pipeline_v2/acquire_worker.py --queue /data/math/_queues/green.jsonl

# New:
dc run --pipeline math --stage acquire -- --queue /data/math/_queues/green.jsonl
```

### Legacy Shell Scripts

The `legacy/` directories have been removed from all pipelines. Use `dc run` or `dc pipeline` commands.
```

**Acceptance Criteria:**
- [ ] All boilerplate wrapper files deleted
- [ ] All `legacy/` directories deleted
- [ ] Domain-specific workers preserved
- [ ] `dc run` works for all pipelines
- [ ] CI passes
- [ ] Migration guide updated

---

### 1.2 Fix Source Tree Issues

**Problem:** Broken symlink at `src/schemas` pointing to non-existent path.

**Solution:**

1. Remove the broken symlink:
```bash
rm -f src/schemas
```

2. Copy schemas into package:
```bash
mkdir -p src/collector_core/schemas
cp schemas/*.json src/collector_core/schemas/
```

3. Update `pyproject.toml`:

```toml
[tool.setuptools.package-data]
collector_core = ["py.typed", "schemas/*.json"]
```

4. Update schema loading code in `src/collector_core/config_validator.py`:

```python
from importlib.resources import files
from pathlib import Path


def get_schema_path(schema_name: str) -> Path:
    """Get path to a JSON schema file.
    
    Args:
        schema_name: Name of the schema (without .json extension)
        
    Returns:
        Path to the schema file
        
    Raises:
        FileNotFoundError: If schema doesn't exist
    """
    # Try package resources first
    try:
        schema_files = files("collector_core.schemas")
        schema_path = schema_files.joinpath(f"{schema_name}.schema.json")
        if schema_path.is_file():
            return Path(str(schema_path))
    except (ImportError, TypeError):
        pass
    
    # Fall back to relative path from repo root
    repo_root = Path(__file__).resolve().parents[2]
    schema_path = repo_root / "schemas" / f"{schema_name}.schema.json"
    if schema_path.exists():
        return schema_path
    
    raise FileNotFoundError(f"Schema not found: {schema_name}")
```

**Acceptance Criteria:**
- [ ] No broken symlinks in source tree
- [ ] Schemas accessible via `importlib.resources`
- [ ] `pip install -e .` works cleanly
- [ ] Schema validation still works

---

### 1.3 Consolidate HTTP Strategies

**Problem:** `http.py` (23KB, 600+ lines) and `http_async.py` (42KB, 1276 lines) contain significant duplicated logic for URL validation, resume support, hash computation, and error handling.

**Solution:** Extract shared utilities into a base module.

**Create `src/collector_core/acquire/strategies/http_base.py`:**

```python
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

from collector_core.stability import stable_api


# Constants
CHUNK_SIZE = 1024 * 1024  # 1 MB chunks for hashing and streaming
DEFAULT_CONNECT_TIMEOUT = 15.0
DEFAULT_READ_TIMEOUT = 300.0
MAX_REDIRECTS = 10

# URL validation
ALLOWED_SCHEMES = {"http", "https"}
DANGEROUS_PORTS = {22, 23, 25, 110, 143, 993, 995}  # SSH, Telnet, SMTP, POP, IMAP


@stable_api
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


@stable_api
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


@stable_api
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
            error=f"Invalid scheme '{parsed.scheme}'. Allowed: {schemes}"
        )
    
    # Check hostname exists
    if not parsed.hostname:
        return UrlValidationResult(valid=False, error="URL has no hostname")
    
    hostname = parsed.hostname.lower()
    
    # Check for localhost
    if not allow_localhost:
        if hostname in ("localhost", "127.0.0.1", "::1") or hostname.endswith(".localhost"):
            return UrlValidationResult(valid=False, error="Localhost URLs not allowed")
    
    # Resolve hostname and check IP
    try:
        ip_addresses = socket.getaddrinfo(hostname, None, socket.AF_UNSPEC, socket.SOCK_STREAM)
        for family, _, _, _, sockaddr in ip_addresses:
            ip_str = sockaddr[0]
            try:
                ip = ipaddress.ip_address(ip_str)
                if not allow_private:
                    if ip.is_private:
                        return UrlValidationResult(
                            valid=False,
                            error=f"Private IP address not allowed: {ip_str}"
                        )
                    if ip.is_loopback:
                        return UrlValidationResult(
                            valid=False,
                            error=f"Loopback address not allowed: {ip_str}"
                        )
                    if ip.is_link_local:
                        return UrlValidationResult(
                            valid=False,
                            error=f"Link-local address not allowed: {ip_str}"
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
            valid=False,
            error=f"Dangerous port not allowed: {port}"
        )
    
    return UrlValidationResult(
        valid=True,
        normalized_url=url,
        hostname=hostname,
        port=port,
        is_https=parsed.scheme.lower() == "https",
    )


@stable_api
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


@stable_api
def supports_resume(headers: dict[str, str]) -> bool:
    """Check if server supports range requests for resume.
    
    Args:
        headers: Response headers from HEAD or GET request
        
    Returns:
        True if server accepts range requests
    """
    accept_ranges = headers.get("Accept-Ranges", "").lower()
    return accept_ranges == "bytes"


@stable_api
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


@stable_api
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
    match = re.search(r"filename\*=(?:UTF-8''|utf-8'')([^;]+)", header, re.IGNORECASE)
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


@stable_api
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


@stable_api
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
```

**Update `src/collector_core/acquire/strategies/http.py`:**

Refactor to use the base class. The file should import from http_base and extend HttpDownloadBase:

```python
"""Synchronous HTTP acquisition strategy.

This module provides synchronous HTTP download functionality using the requests library.
For async downloads, see http_async.py.
"""

from __future__ import annotations

import logging
import os
import time
from pathlib import Path
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from collector_core.acquire.context import AcquireContext
from collector_core.acquire.strategies.http_base import (
    DownloadResult,
    HttpDownloadBase,
    UrlValidationResult,
    validate_url,
)
from collector_core.stability import stable_api
from collector_core.utils.paths import ensure_dir, safe_filename

logger = logging.getLogger(__name__)


@stable_api
class SyncHttpStrategy(HttpDownloadBase):
    """Synchronous HTTP download strategy using requests.
    
    Features:
    - Resume support for interrupted downloads
    - Configurable retry with exponential backoff
    - URL validation (no private IPs by default)
    - Progress tracking
    - SHA-256 verification
    
    Example:
        strategy = SyncHttpStrategy(
            connect_timeout=30.0,
            read_timeout=600.0,
            max_retries=3,
        )
        result = strategy.download(
            url="https://example.com/data.zip",
            dest=Path("/data/downloads/data.zip"),
        )
        if result.success:
            print(f"Downloaded {result.bytes_downloaded} bytes")
            print(f"SHA-256: {result.sha256}")
    """
    
    def __init__(
        self,
        *,
        connect_timeout: float | None = None,
        read_timeout: float | None = None,
        max_retries: int = 3,
        backoff_factor: float = 1.0,
        user_agent: str | None = None,
    ):
        """Initialize the HTTP strategy.
        
        Args:
            connect_timeout: Connection timeout in seconds
            read_timeout: Read timeout in seconds
            max_retries: Maximum retry attempts
            backoff_factor: Exponential backoff factor
            user_agent: Custom User-Agent header
        """
        if connect_timeout is not None:
            self.connect_timeout = connect_timeout
        if read_timeout is not None:
            self.read_timeout = read_timeout
        
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.user_agent = user_agent or "DatasetCollector/3.0"
        
        self._session: requests.Session | None = None
    
    def _get_session(self) -> requests.Session:
        """Get or create a requests session with retry configuration."""
        if self._session is None:
            self._session = requests.Session()
            
            retry_strategy = Retry(
                total=self.max_retries,
                backoff_factor=self.backoff_factor,
                status_forcelist=[429, 500, 502, 503, 504],
                allowed_methods=["HEAD", "GET"],
            )
            adapter = HTTPAdapter(max_retries=retry_strategy)
            self._session.mount("http://", adapter)
            self._session.mount("https://", adapter)
            self._session.headers["User-Agent"] = self.user_agent
        
        return self._session
    
    def download(
        self,
        url: str,
        dest: Path,
        *,
        headers: dict[str, str] | None = None,
        allow_private: bool = False,
        allow_resume: bool = True,
        expected_sha256: str | None = None,
    ) -> DownloadResult:
        """Download a file from URL.
        
        Args:
            url: URL to download
            dest: Destination file path
            headers: Additional headers to send
            allow_private: Allow private/internal IP addresses
            allow_resume: Attempt to resume partial downloads
            expected_sha256: Expected SHA-256 hash for verification
            
        Returns:
            DownloadResult with outcome
        """
        # Validate URL
        validation = self.validate_url(url, allow_private=allow_private)
        if not validation.valid:
            return self.create_result(
                success=False,
                error=f"URL validation failed: {validation.error}",
            )
        
        # Prepare destination
        ensure_dir(dest.parent)
        
        # Check for existing partial download
        existing_size = 0
        resumed = False
        if allow_resume and dest.exists():
            existing_size = dest.stat().st_size
        
        # Build headers
        request_headers = dict(headers or {})
        if existing_size > 0:
            # Check if server supports resume
            try:
                head_response = self._get_session().head(
                    url,
                    timeout=(self.connect_timeout, self.read_timeout),
                    allow_redirects=True,
                )
                if self.supports_resume(dict(head_response.headers)):
                    request_headers = self.build_resume_headers(existing_size, request_headers)
                    resumed = True
                else:
                    existing_size = 0  # Can't resume, start fresh
            except requests.RequestException:
                existing_size = 0  # HEAD failed, start fresh
        
        # Download
        try:
            response = self._get_session().get(
                url,
                headers=request_headers,
                timeout=(self.connect_timeout, self.read_timeout),
                stream=True,
                allow_redirects=True,
            )
            response.raise_for_status()
            
            # Handle 206 Partial Content vs 200 OK
            if response.status_code == 206:
                mode = "ab"  # Append for resume
            else:
                mode = "wb"  # Overwrite for fresh download
                existing_size = 0
                resumed = False
            
            bytes_downloaded = existing_size
            content_type = response.headers.get("Content-Type")
            
            with dest.open(mode) as f:
                for chunk in response.iter_content(chunk_size=self.chunk_size):
                    if chunk:
                        f.write(chunk)
                        bytes_downloaded += len(chunk)
            
            # Compute hash
            file_hash = self.compute_file_hash(dest)
            
            # Verify hash if expected
            if expected_sha256 and file_hash != expected_sha256.lower():
                return self.create_result(
                    success=False,
                    path=dest,
                    bytes_downloaded=bytes_downloaded,
                    sha256=file_hash,
                    error=f"Hash mismatch: expected {expected_sha256}, got {file_hash}",
                    status_code=response.status_code,
                    headers=dict(response.headers),
                )
            
            return self.create_result(
                success=True,
                path=dest,
                bytes_downloaded=bytes_downloaded,
                sha256=file_hash,
                resumed=resumed,
                status_code=response.status_code,
                headers=dict(response.headers),
                content_type=content_type,
            )
            
        except requests.RequestException as e:
            return self.create_result(
                success=False,
                error=str(e),
                bytes_downloaded=existing_size,
            )
    
    def close(self) -> None:
        """Close the session."""
        if self._session is not None:
            self._session.close()
            self._session = None
    
    def __enter__(self) -> "SyncHttpStrategy":
        return self
    
    def __exit__(self, *args: Any) -> None:
        self.close()


# Convenience function for simple downloads
@stable_api
def http_download(
    url: str,
    dest: Path,
    *,
    timeout: float = 300.0,
    **kwargs: Any,
) -> DownloadResult:
    """Simple HTTP download function.
    
    For more control, use SyncHttpStrategy directly.
    
    Args:
        url: URL to download
        dest: Destination path
        timeout: Read timeout in seconds
        **kwargs: Additional arguments passed to SyncHttpStrategy.download
        
    Returns:
        DownloadResult
    """
    with SyncHttpStrategy(read_timeout=timeout) as strategy:
        return strategy.download(url, dest, **kwargs)
```

**Update `src/collector_core/acquire/strategies/http_async.py`:**

Similarly refactor to use HttpDownloadBase. The async strategy should:

1. Import from http_base
2. Extend HttpDownloadBase
3. Remove all duplicated validation/hashing logic
4. Keep only async-specific implementation

**Acceptance Criteria:**
- [ ] `http_base.py` contains all shared utilities
- [ ] `http.py` uses HttpDownloadBase
- [ ] `http_async.py` uses HttpDownloadBase  
- [ ] No duplicated URL validation logic
- [ ] No duplicated hash computation logic
- [ ] All existing tests pass
- [ ] Total line count reduced by ~800 lines

---

## Phase 2: Feature Implementation

### 2.1 Domain-Specific Yellow Screeners

**Problem:** The domain-specific yellow screeners in `src/collector_core/yellow/domains/` are stubs that just delegate to `standard_filter`. They provide no domain-specific logic.

**Solution:** Implement actual domain-specific screening logic.

#### 2.1.1 Chemistry Screener

**Update `src/collector_core/yellow/domains/chem.py`:**

```python
"""Chemistry-specific yellow screening with dual-use detection.

This module provides chemistry-specific filtering for the yellow screening stage.
It includes:
- Controlled substance synthesis detection
- CAS Registry Number extraction and validation
- Quality indicator scoring for legitimate research content
- Dual-use chemical content flagging
"""

from __future__ import annotations

import re
from typing import Any

from collector_core.yellow.base import (
    DomainContext,
    FilterDecision,
    standard_filter,
    standard_transform,
)

# CAS Registry Number pattern (format: XXXXXXX-XX-X)
CAS_PATTERN = re.compile(r"\b(\d{2,7})-(\d{2})-(\d)\b")

# Controlled substance synthesis patterns
# These patterns detect potential instructions for synthesizing dangerous chemicals
CONTROLLED_PATTERNS = [
    # Drug synthesis
    re.compile(
        r"\b(synthesis|synthesize|prepare|preparation|route)\b"
        r".{0,100}"
        r"\b(fentanyl|methamphetamine|mdma|lsd|heroin|cocaine)\b",
        re.IGNORECASE,
    ),
    # Chemical weapons / nerve agents
    re.compile(
        r"\b(nerve\s+agent|chemical\s+weapon|mustard\s+gas|sarin|tabun|soman|vx|novichok)\b",
        re.IGNORECASE,
    ),
    # Explosives precursors
    re.compile(
        r"\b(synthesis|prepare|make|manufacture)\b"
        r".{0,50}"
        r"\b(explosive|detonator|rdx|petn|tatp|hmtd)\b",
        re.IGNORECASE,
    ),
    # Toxic industrial chemicals misuse
    re.compile(
        r"\b(weaponize|weapon|attack|poison)\b"
        r".{0,50}"
        r"\b(chlorine|phosgene|hydrogen\s+cyanide|ricin)\b",
        re.IGNORECASE,
    ),
]

# Quality indicators that suggest legitimate research content
QUALITY_INDICATORS = [
    "peer-reviewed",
    "peer reviewed",
    "crystallographic",
    "spectroscopic",
    "computational chemistry",
    "density functional theory",
    "dft calculation",
    "molecular dynamics",
    "quantum chemistry",
    "ab initio",
    "nmr spectr",
    "mass spectr",
    "x-ray diffraction",
    "xrd",
    "ftir",
    "raman",
]

# Safety-related positive indicators
SAFETY_INDICATORS = [
    "safety data sheet",
    "sds",
    "msds",
    "hazard classification",
    "ghs",
    "exposure limit",
    "ppe requirement",
    "toxicological",
    "environmental impact",
]


def validate_cas_number(cas_string: str) -> bool:
    """Validate CAS Registry Number checksum.
    
    CAS numbers use a checksum digit calculated as:
    sum of (digit * position from right) mod 10
    
    Args:
        cas_string: CAS number in format XXXXXXX-XX-X
        
    Returns:
        True if valid CAS number
    """
    match = CAS_PATTERN.match(cas_string)
    if not match:
        return False
    
    # Remove hyphens and get digits
    digits = match.group(1) + match.group(2) + match.group(3)
    check_digit = int(digits[-1])
    
    # Calculate checksum
    total = 0
    for i, digit in enumerate(reversed(digits[:-1])):
        total += int(digit) * (i + 1)
    
    return (total % 10) == check_digit


def extract_cas_numbers(text: str) -> list[dict[str, Any]]:
    """Extract and validate CAS numbers from text.
    
    Args:
        text: Text to search for CAS numbers
        
    Returns:
        List of dicts with cas_number and is_valid keys
    """
    results = []
    seen = set()
    
    for match in CAS_PATTERN.finditer(text):
        cas = match.group(0)
        if cas not in seen:
            seen.add(cas)
            results.append({
                "cas_number": cas,
                "is_valid": validate_cas_number(cas),
            })
    
    return results


def compute_quality_score(text: str) -> tuple[int, list[str]]:
    """Compute quality score based on research indicators.
    
    Args:
        text: Text to analyze
        
    Returns:
        Tuple of (score, list of matched indicators)
    """
    text_lower = text.lower()
    matched = []
    
    for indicator in QUALITY_INDICATORS:
        if indicator in text_lower:
            matched.append(indicator)
    
    return len(matched), matched


def check_controlled_content(text: str) -> tuple[bool, str | None]:
    """Check for controlled substance synthesis content.
    
    Args:
        text: Text to check
        
    Returns:
        Tuple of (has_controlled_content, matched_pattern_description)
    """
    for pattern in CONTROLLED_PATTERNS:
        match = pattern.search(text)
        if match:
            return True, match.group(0)[:100]  # Truncate for logging
    
    return False, None


def filter_record(raw: dict[str, Any], ctx: DomainContext) -> FilterDecision:
    """Chemistry-specific filtering with dual-use screening.
    
    This function extends the standard filter with chemistry-specific checks:
    1. Controlled substance synthesis detection (reject)
    2. CAS number extraction and validation
    3. Quality indicator scoring
    
    Args:
        raw: Raw record to filter
        ctx: Domain context with configuration
        
    Returns:
        FilterDecision with chemistry-specific metadata
    """
    # Get text content
    text = (
        raw.get("text", "") or 
        raw.get("abstract", "") or 
        raw.get("content", "") or 
        raw.get("body", "") or
        ""
    )
    
    # Check for controlled substance content first (hard reject)
    has_controlled, controlled_match = check_controlled_content(text)
    if has_controlled:
        return FilterDecision(
            allow=False,
            reason="controlled_substance_content",
            text=text[:500] if text else None,
            extra={
                "rejection_type": "dual_use",
                "matched_content": controlled_match,
            },
        )
    
    # Extract CAS numbers
    cas_numbers = extract_cas_numbers(text)
    valid_cas_count = sum(1 for cas in cas_numbers if cas["is_valid"])
    
    # Compute quality score
    quality_score, quality_matches = compute_quality_score(text)
    
    # Run standard filter
    decision = standard_filter(raw, ctx)
    
    # Enhance decision with chemistry-specific metadata
    decision.extra = decision.extra or {}
    decision.extra.update({
        "cas_numbers_found": len(cas_numbers),
        "cas_numbers_valid": valid_cas_count,
        "cas_numbers": cas_numbers[:10],  # Limit to first 10
        "quality_score": quality_score,
        "quality_indicators": quality_matches,
    })
    
    # Boost allow probability for high-quality research content
    if quality_score >= 3 and decision.allow:
        decision.extra["quality_boost"] = True
    
    return decision


def transform_record(
    raw: dict[str, Any],
    ctx: DomainContext,
    decision: FilterDecision,
    *,
    license_profile: str,
) -> dict[str, Any] | None:
    """Transform chemistry record with domain-specific fields.
    
    Adds chemistry-specific fields to the output record:
    - cas_numbers: Extracted CAS Registry Numbers
    - quality_score: Research quality indicator score
    
    Args:
        raw: Raw input record
        ctx: Domain context
        decision: Filter decision
        license_profile: License profile for the record
        
    Returns:
        Transformed record or None if should be excluded
    """
    result = standard_transform(raw, ctx, decision, license_profile=license_profile)
    
    if result is None:
        return None
    
    # Add chemistry-specific fields
    extra = decision.extra or {}
    if extra.get("cas_numbers"):
        result["extracted_cas_numbers"] = [
            cas["cas_number"] for cas in extra["cas_numbers"] if cas["is_valid"]
        ]
    
    if extra.get("quality_score"):
        result["research_quality_score"] = extra["quality_score"]
    
    return result


__all__ = [
    "filter_record",
    "transform_record",
    "validate_cas_number",
    "extract_cas_numbers",
]
```

#### 2.1.2 Biology Screener

**Update `src/collector_core/yellow/domains/biology.py`:**

```python
"""Biology-specific yellow screening with biosecurity checks.

This module provides biology-specific filtering including:
- Biosecurity screening for dangerous pathogen content
- Gene/protein ID extraction and validation
- Taxonomy verification
- Sequence data quality assessment
"""

from __future__ import annotations

import re
from typing import Any

from collector_core.yellow.base import (
    DomainContext,
    FilterDecision,
    standard_filter,
    standard_transform,
)


# Select Agent and Toxin patterns (CDC/USDA regulated)
# Reference: https://www.selectagents.gov/sat/list.htm
BIOSECURITY_PATTERNS = [
    # Tier 1 Select Agents (highest concern)
    re.compile(
        r"\b(ebola|marburg|variola|smallpox|yersinia\s+pestis|"
        r"bacillus\s+anthracis|anthrax|botulinum|ricin|"
        r"francisella\s+tularensis|tularemia)\b",
        re.IGNORECASE,
    ),
    # Gain of function / enhanced transmissibility
    re.compile(
        r"\b(gain[\s-]+of[\s-]+function|enhanced\s+transmissibility|"
        r"increased\s+virulence|pandemic\s+potential)\b"
        r".{0,100}"
        r"\b(influenza|coronavirus|sars|mers)\b",
        re.IGNORECASE,
    ),
    # Synthesis instructions for dangerous pathogens
    re.compile(
        r"\b(synthesize|reconstruct|engineer|create)\b"
        r".{0,50}"
        r"\b(pathogen|virus|toxin|bioweapon)\b",
        re.IGNORECASE,
    ),
]

# Gene/Protein ID patterns
GENE_ID_PATTERNS = {
    "ncbi_gene": re.compile(r"\bGeneID[:\s]*(\d{4,10})\b", re.IGNORECASE),
    "ensembl": re.compile(r"\b(ENS[A-Z]{0,3}G\d{11})\b"),
    "uniprot": re.compile(r"\b([OPQ][0-9][A-Z0-9]{3}[0-9]|[A-NR-Z][0-9]([A-Z][A-Z0-9]{2}[0-9]){1,2})\b"),
    "refseq_mrna": re.compile(r"\b(NM_\d{6,9})\b"),
    "refseq_protein": re.compile(r"\b(NP_\d{6,9})\b"),
    "pdb": re.compile(r"\bPDB[:\s]*([0-9][A-Za-z0-9]{3})\b", re.IGNORECASE),
}

# Sequence patterns
DNA_SEQUENCE_PATTERN = re.compile(r"\b[ATCG]{50,}\b")
PROTEIN_SEQUENCE_PATTERN = re.compile(r"\b[ACDEFGHIKLMNPQRSTVWY]{30,}\b")

# Quality indicators
QUALITY_INDICATORS = [
    "peer-reviewed",
    "peer reviewed",
    "published in",
    "doi:",
    "pmid:",
    "pmc",
    "nature",
    "science",
    "cell",
    "experimental validation",
    "clinical trial",
    "ncbi",
    "genbank",
    "uniprot",
]


def check_biosecurity_content(text: str) -> tuple[bool, str | None]:
    """Check for biosecurity-sensitive content.
    
    Args:
        text: Text to check
        
    Returns:
        Tuple of (has_biosecurity_concern, description)
    """
    for pattern in BIOSECURITY_PATTERNS:
        match = pattern.search(text)
        if match:
            return True, match.group(0)[:100]
    return False, None


def extract_gene_ids(text: str) -> dict[str, list[str]]:
    """Extract gene and protein identifiers from text.
    
    Args:
        text: Text to search
        
    Returns:
        Dict mapping ID type to list of found IDs
    """
    results: dict[str, list[str]] = {}
    
    for id_type, pattern in GENE_ID_PATTERNS.items():
        matches = pattern.findall(text)
        if matches:
            # Handle tuple results from groups
            if matches and isinstance(matches[0], tuple):
                matches = [m[0] for m in matches]
            results[id_type] = list(set(matches))[:20]  # Dedupe and limit
    
    return results


def detect_sequence_content(text: str) -> dict[str, Any]:
    """Detect and characterize sequence content.
    
    Args:
        text: Text to analyze
        
    Returns:
        Dict with sequence statistics
    """
    dna_matches = DNA_SEQUENCE_PATTERN.findall(text)
    protein_matches = PROTEIN_SEQUENCE_PATTERN.findall(text)
    
    return {
        "has_dna_sequences": len(dna_matches) > 0,
        "dna_sequence_count": len(dna_matches),
        "total_dna_bases": sum(len(m) for m in dna_matches),
        "has_protein_sequences": len(protein_matches) > 0,
        "protein_sequence_count": len(protein_matches),
        "total_amino_acids": sum(len(m) for m in protein_matches),
    }


def compute_quality_score(text: str) -> tuple[int, list[str]]:
    """Compute quality score based on research indicators.
    
    Args:
        text: Text to analyze
        
    Returns:
        Tuple of (score, matched indicators)
    """
    text_lower = text.lower()
    matched = []
    
    for indicator in QUALITY_INDICATORS:
        if indicator in text_lower:
            matched.append(indicator)
    
    return len(matched), matched


def filter_record(raw: dict[str, Any], ctx: DomainContext) -> FilterDecision:
    """Biology-specific filtering with biosecurity screening.
    
    Args:
        raw: Raw record to filter
        ctx: Domain context
        
    Returns:
        FilterDecision with biology-specific metadata
    """
    text = (
        raw.get("text", "") or
        raw.get("abstract", "") or
        raw.get("content", "") or
        ""
    )
    
    # Check biosecurity concerns first
    has_biosecurity, biosecurity_match = check_biosecurity_content(text)
    if has_biosecurity:
        return FilterDecision(
            allow=False,
            reason="biosecurity_concern",
            text=text[:500] if text else None,
            extra={
                "rejection_type": "biosecurity",
                "matched_content": biosecurity_match,
            },
        )
    
    # Extract identifiers
    gene_ids = extract_gene_ids(text)
    sequence_info = detect_sequence_content(text)
    quality_score, quality_matches = compute_quality_score(text)
    
    # Run standard filter
    decision = standard_filter(raw, ctx)
    
    # Add biology-specific metadata
    decision.extra = decision.extra or {}
    decision.extra.update({
        "gene_ids": gene_ids,
        "sequence_info": sequence_info,
        "quality_score": quality_score,
        "quality_indicators": quality_matches,
        "total_identifiers_found": sum(len(ids) for ids in gene_ids.values()),
    })
    
    return decision


def transform_record(
    raw: dict[str, Any],
    ctx: DomainContext,
    decision: FilterDecision,
    *,
    license_profile: str,
) -> dict[str, Any] | None:
    """Transform biology record with domain-specific fields."""
    result = standard_transform(raw, ctx, decision, license_profile=license_profile)
    
    if result is None:
        return None
    
    extra = decision.extra or {}
    
    # Add extracted identifiers
    if extra.get("gene_ids"):
        result["extracted_gene_ids"] = extra["gene_ids"]
    
    if extra.get("sequence_info"):
        result["sequence_statistics"] = extra["sequence_info"]
    
    return result


__all__ = ["filter_record", "transform_record"]
```

#### 2.1.3 Code Screener

**Update `src/collector_core/yellow/domains/code.py`:**

```python
"""Code-specific yellow screening with license and security checks.

This module provides code-specific filtering including:
- License header extraction and validation
- Secret/credential detection
- Malware pattern detection
- Code quality assessment
"""

from __future__ import annotations

import re
from typing import Any

from collector_core.yellow.base import (
    DomainContext,
    FilterDecision,
    standard_filter,
    standard_transform,
)


# SPDX License Identifier pattern
SPDX_PATTERN = re.compile(
    r"SPDX-License-Identifier:\s*([A-Za-z0-9\-\.+]+(?:\s+(?:AND|OR|WITH)\s+[A-Za-z0-9\-\.+]+)*)",
    re.IGNORECASE,
)

# License header patterns
LICENSE_PATTERNS = {
    "MIT": re.compile(
        r"(?:MIT License|Permission is hereby granted,?\s+free of charge)",
        re.IGNORECASE,
    ),
    "Apache-2.0": re.compile(
        r"(?:Apache License.*Version 2\.0|Licensed under the Apache License)",
        re.IGNORECASE,
    ),
    "GPL-3.0": re.compile(
        r"(?:GNU General Public License.*(?:version\s+)?3|GPLv3)",
        re.IGNORECASE,
    ),
    "GPL-2.0": re.compile(
        r"(?:GNU General Public License.*(?:version\s+)?2|GPLv2)",
        re.IGNORECASE,
    ),
    "BSD-3-Clause": re.compile(
        r"(?:BSD 3-Clause|three conditions|Redistributions? of source code)",
        re.IGNORECASE,
    ),
    "BSD-2-Clause": re.compile(
        r"(?:BSD 2-Clause|Simplified BSD|two conditions)",
        re.IGNORECASE,
    ),
    "LGPL": re.compile(
        r"(?:GNU Lesser General Public License|LGPL)",
        re.IGNORECASE,
    ),
    "MPL-2.0": re.compile(
        r"(?:Mozilla Public License.*2\.0|MPL-2\.0)",
        re.IGNORECASE,
    ),
    "Unlicense": re.compile(
        r"(?:This is free and unencumbered software|Unlicense)",
        re.IGNORECASE,
    ),
    "CC0-1.0": re.compile(
        r"(?:CC0|Creative Commons Zero|Public Domain Dedication)",
        re.IGNORECASE,
    ),
}

# Secret/credential patterns
SECRET_PATTERNS = [
    # API keys
    (re.compile(r"\b(?:api[_-]?key|apikey)\s*[:=]\s*['\"]?([a-zA-Z0-9_\-]{20,})['\"]?", re.IGNORECASE), "api_key"),
    # AWS credentials
    (re.compile(r"\b(?:AKIA|ABIA|ACCA|ASIA)[A-Z0-9]{16}\b"), "aws_access_key"),
    (re.compile(r"\baws[_-]?secret[_-]?access[_-]?key\s*[:=]\s*['\"]?([a-zA-Z0-9/+=]{40})['\"]?", re.IGNORECASE), "aws_secret_key"),
    # Private keys
    (re.compile(r"-----BEGIN (?:RSA |EC |DSA |OPENSSH )?PRIVATE KEY-----"), "private_key"),
    # Passwords in config
    (re.compile(r"\b(?:password|passwd|pwd)\s*[:=]\s*['\"]([^'\"]{8,})['\"]", re.IGNORECASE), "password"),
    # Database connection strings
    (re.compile(r"(?:mysql|postgres|mongodb)://[^:]+:[^@]+@", re.IGNORECASE), "database_url"),
    # JWT tokens
    (re.compile(r"\beyJ[a-zA-Z0-9_-]*\.eyJ[a-zA-Z0-9_-]*\.[a-zA-Z0-9_-]*\b"), "jwt_token"),
    # GitHub tokens
    (re.compile(r"\b(?:ghp|gho|ghu|ghs|ghr)_[a-zA-Z0-9]{36}\b"), "github_token"),
]

# Malware/exploit patterns
MALWARE_PATTERNS = [
    # Shell injection
    re.compile(r"(?:;\s*(?:rm|wget|curl|nc|bash|sh)\s+-|`.*`|\$\(.*\))", re.IGNORECASE),
    # Eval with user input
    re.compile(r"\beval\s*\(\s*(?:request|input|argv|params)", re.IGNORECASE),
    # SQL injection vectors
    re.compile(r"(?:UNION\s+SELECT|OR\s+1\s*=\s*1|'\s*OR\s+')", re.IGNORECASE),
    # Known malware signatures
    re.compile(r"(?:mimikatz|metasploit|cobalt\s*strike|beacon)", re.IGNORECASE),
]

# Code quality indicators
QUALITY_INDICATORS = [
    ("has_tests", re.compile(r"(?:def test_|class Test|@pytest|unittest)", re.IGNORECASE)),
    ("has_docstrings", re.compile(r'"""[\s\S]*?"""|\'\'\'[\s\S]*?\'\'\'')),
    ("has_type_hints", re.compile(r"def\s+\w+\([^)]*:\s*\w+[^)]*\)\s*(?:->|:)")),
    ("has_logging", re.compile(r"(?:logging\.|logger\.|log\.)", re.IGNORECASE)),
    ("has_error_handling", re.compile(r"(?:try:|except:|raise\s+\w+)")),
]


def extract_license_info(text: str, max_lines: int = 100) -> dict[str, Any]:
    """Extract license information from code header.
    
    Args:
        text: Code content
        max_lines: Maximum lines to scan for license header
        
    Returns:
        Dict with license detection results
    """
    # Look at first N lines for license
    lines = text.split("\n")[:max_lines]
    header = "\n".join(lines)
    
    result: dict[str, Any] = {
        "has_license_header": False,
        "detected_spdx": None,
        "detected_license": None,
        "confidence": 0.0,
    }
    
    # Try SPDX identifier first (highest confidence)
    spdx_match = SPDX_PATTERN.search(header)
    if spdx_match:
        result["has_license_header"] = True
        result["detected_spdx"] = spdx_match.group(1)
        result["confidence"] = 1.0
        return result
    
    # Try pattern matching
    for license_id, pattern in LICENSE_PATTERNS.items():
        if pattern.search(header):
            result["has_license_header"] = True
            result["detected_license"] = license_id
            result["confidence"] = 0.8
            return result
    
    return result


def detect_secrets(text: str) -> list[dict[str, Any]]:
    """Detect potential secrets/credentials in code.
    
    Args:
        text: Code content
        
    Returns:
        List of detected secret locations (without actual values)
    """
    findings = []
    
    for pattern, secret_type in SECRET_PATTERNS:
        for match in pattern.finditer(text):
            # Find line number
            line_start = text.rfind("\n", 0, match.start()) + 1
            line_num = text.count("\n", 0, match.start()) + 1
            
            findings.append({
                "type": secret_type,
                "line": line_num,
                "column": match.start() - line_start,
                # Don't include actual secret value
            })
    
    return findings


def detect_malware_patterns(text: str) -> list[dict[str, Any]]:
    """Detect potential malware/exploit patterns.
    
    Args:
        text: Code content
        
    Returns:
        List of suspicious pattern matches
    """
    findings = []
    
    for pattern in MALWARE_PATTERNS:
        for match in pattern.finditer(text):
            line_num = text.count("\n", 0, match.start()) + 1
            findings.append({
                "pattern": pattern.pattern[:50],
                "line": line_num,
                "matched": match.group(0)[:50],
            })
    
    return findings


def assess_code_quality(text: str) -> dict[str, Any]:
    """Assess code quality indicators.
    
    Args:
        text: Code content
        
    Returns:
        Dict with quality assessments
    """
    result = {"quality_score": 0}
    
    for indicator_name, pattern in QUALITY_INDICATORS:
        has_indicator = bool(pattern.search(text))
        result[indicator_name] = has_indicator
        if has_indicator:
            result["quality_score"] += 1
    
    return result


def filter_record(raw: dict[str, Any], ctx: DomainContext) -> FilterDecision:
    """Code-specific filtering with license and security checks.
    
    Args:
        raw: Raw record to filter
        ctx: Domain context
        
    Returns:
        FilterDecision with code-specific metadata
    """
    text = (
        raw.get("content", "") or
        raw.get("code", "") or
        raw.get("text", "") or
        ""
    )
    
    # Detect malware patterns (hard reject)
    malware_findings = detect_malware_patterns(text)
    if malware_findings:
        return FilterDecision(
            allow=False,
            reason="malware_pattern_detected",
            extra={
                "rejection_type": "security",
                "malware_findings": malware_findings[:5],
            },
        )
    
    # Detect secrets (flag for review but don't auto-reject)
    secret_findings = detect_secrets(text)
    
    # Extract license info
    license_info = extract_license_info(text)
    
    # Assess code quality
    quality_info = assess_code_quality(text)
    
    # Run standard filter
    decision = standard_filter(raw, ctx)
    
    # Flag if secrets detected
    if secret_findings:
        decision.reason = decision.reason or ""
        if decision.reason:
            decision.reason += "; "
        decision.reason += "secrets_detected"
    
    # Add code-specific metadata
    decision.extra = decision.extra or {}
    decision.extra.update({
        "license_info": license_info,
        "secrets_found": len(secret_findings),
        "secret_types": list(set(s["type"] for s in secret_findings)),
        "quality_info": quality_info,
    })
    
    return decision


def transform_record(
    raw: dict[str, Any],
    ctx: DomainContext,
    decision: FilterDecision,
    *,
    license_profile: str,
) -> dict[str, Any] | None:
    """Transform code record with domain-specific fields."""
    result = standard_transform(raw, ctx, decision, license_profile=license_profile)
    
    if result is None:
        return None
    
    extra = decision.extra or {}
    
    # Add extracted license
    license_info = extra.get("license_info", {})
    if license_info.get("detected_spdx"):
        result["detected_spdx"] = license_info["detected_spdx"]
    elif license_info.get("detected_license"):
        result["detected_license"] = license_info["detected_license"]
    
    # Add quality metrics
    if extra.get("quality_info"):
        result["code_quality"] = extra["quality_info"]
    
    # Flag if secrets were found (content should be scrubbed separately)
    if extra.get("secrets_found", 0) > 0:
        result["_secrets_detected"] = True
    
    return result


__all__ = ["filter_record", "transform_record"]
```

#### 2.1.4 Implement Remaining Domain Screeners

Create similar implementations for:

**`src/collector_core/yellow/domains/nlp.py`:**
- Language detection
- Toxicity/hate speech patterns
- PII detection (names, emails, phone numbers)
- Quality assessment (vocabulary diversity, coherence indicators)

**`src/collector_core/yellow/domains/cyber.py`:**
- CVE ID validation
- Exploit code detection (differentiate from security research)
- Malware hash detection
- Attack pattern classification

**`src/collector_core/yellow/domains/safety.py`:**
- Incident type classification
- PII in incident reports
- Severity assessment
- Regulatory compliance indicators

**`src/collector_core/yellow/domains/econ.py`:**
- Financial data sensitivity
- PII in economic data
- Temporal data validation
- Statistical methodology indicators

**`src/collector_core/yellow/domains/kg_nav.py`:**
- Entity validation against known KGs
- Relationship extraction quality
- Geospatial data validation
- Ontology compliance checks

**Acceptance Criteria:**
- [ ] All domain screeners have actual implementation
- [ ] Each screener has domain-specific detection patterns
- [ ] Quality scoring implemented for each domain
- [ ] Tests added for each domain screener
- [ ] Documentation updated

---

### 2.2 Implement Near-Duplicate Detection

**Create `src/collector_core/checks/near_duplicate.py`:**

```python
"""Near-duplicate detection using MinHash LSH.

This module provides efficient near-duplicate detection for text documents
using MinHash signatures and Locality-Sensitive Hashing (LSH).

The implementation supports both the datasketch library (if installed) and
a pure Python fallback for environments without optional dependencies.

Example:
    detector = NearDuplicateDetector(threshold=0.8)
    
    # Add documents to index
    detector.add("doc1", "This is the first document about machine learning.")
    detector.add("doc2", "This is the second document about deep learning.")
    
    # Check for duplicates
    result = detector.query("This is the first document about machine learning!")
    if result.is_duplicate:
        print(f"Found duplicate: {result.matched_ids}")
"""

from __future__ import annotations

import hashlib
import struct
from dataclasses import dataclass, field
from typing import Any, Iterator

from collector_core.dependencies import _try_import
from collector_core.stability import stable_api

# Try to import datasketch for production use
datasketch = _try_import("datasketch")


@stable_api
@dataclass
class DuplicateResult:
    """Result of a near-duplicate query.
    
    Attributes:
        is_duplicate: Whether the query document is a near-duplicate
        similarity: Jaccard similarity with best match (0.0-1.0)
        matched_ids: List of document IDs that match above threshold
        query_time_ms: Time taken for query in milliseconds
    """
    is_duplicate: bool
    similarity: float
    matched_ids: list[str] = field(default_factory=list)
    query_time_ms: float = 0.0


@stable_api
@dataclass
class DetectorStats:
    """Statistics about the near-duplicate detector.
    
    Attributes:
        document_count: Number of documents indexed
        total_shingles: Total shingles across all documents
        avg_shingles_per_doc: Average shingles per document
        memory_estimate_mb: Estimated memory usage in MB
    """
    document_count: int
    total_shingles: int
    avg_shingles_per_doc: float
    memory_estimate_mb: float


class _PureMinHash:
    """Pure Python MinHash implementation.
    
    This is a fallback when datasketch is not installed.
    Uses the same algorithm but may be slower for large datasets.
    """
    
    # Large prime for hash functions
    _MERSENNE_PRIME = (1 << 61) - 1
    _MAX_HASH = (1 << 32) - 1
    
    def __init__(self, num_perm: int = 128, seed: int = 1):
        self.num_perm = num_perm
        self.seed = seed
        self.hashvalues = [self._MAX_HASH] * num_perm
        
        # Generate hash function parameters
        import random
        gen = random.Random(seed)
        self._a = [gen.randint(1, self._MERSENNE_PRIME - 1) for _ in range(num_perm)]
        self._b = [gen.randint(0, self._MERSENNE_PRIME - 1) for _ in range(num_perm)]
    
    def update(self, data: bytes) -> None:
        """Update MinHash with a new element."""
        # Hash the data
        h = int(hashlib.sha1(data).hexdigest()[:16], 16)
        
        # Update each hash function
        for i in range(self.num_perm):
            hv = ((self._a[i] * h + self._b[i]) % self._MERSENNE_PRIME) & self._MAX_HASH
            if hv < self.hashvalues[i]:
                self.hashvalues[i] = hv
    
    def jaccard(self, other: "_PureMinHash") -> float:
        """Estimate Jaccard similarity with another MinHash."""
        if self.num_perm != other.num_perm:
            raise ValueError("MinHash must have same num_perm")
        
        matches = sum(1 for a, b in zip(self.hashvalues, other.hashvalues) if a == b)
        return matches / self.num_perm


class _PureMinHashLSH:
    """Pure Python MinHash LSH implementation."""
    
    def __init__(self, threshold: float = 0.5, num_perm: int = 128):
        self.threshold = threshold
        self.num_perm = num_perm
        
        # Calculate optimal band/row configuration
        # We want P(candidate) ≈ threshold
        self.b, self.r = self._optimal_params(threshold, num_perm)
        
        # Hash tables for each band
        self.hashtables: list[dict[int, list[str]]] = [
            {} for _ in range(self.b)
        ]
    
    def _optimal_params(self, threshold: float, num_perm: int) -> tuple[int, int]:
        """Find optimal band/row configuration for threshold."""
        # b bands, r rows per band: P(candidate) ≈ 1 - (1 - s^r)^b
        # We want this ≈ threshold for s = threshold
        
        best_b, best_r = 1, num_perm
        best_error = float("inf")
        
        for b in range(1, num_perm + 1):
            if num_perm % b != 0:
                continue
            r = num_perm // b
            # Probability at threshold
            p = 1 - (1 - threshold**r)**b
            error = abs(p - threshold)
            if error < best_error:
                best_error = error
                best_b, best_r = b, r
        
        return best_b, best_r
    
    def _hash_band(self, hashvalues: list[int], band_idx: int) -> int:
        """Hash a band of the MinHash signature."""
        start = band_idx * self.r
        end = start + self.r
        band = tuple(hashvalues[start:end])
        return hash(band)
    
    def insert(self, key: str, minhash: _PureMinHash) -> None:
        """Insert a document into the LSH index."""
        for i in range(self.b):
            band_hash = self._hash_band(minhash.hashvalues, i)
            if band_hash not in self.hashtables[i]:
                self.hashtables[i][band_hash] = []
            self.hashtables[i][band_hash].append(key)
    
    def query(self, minhash: _PureMinHash) -> list[str]:
        """Query for candidate duplicates."""
        candidates: set[str] = set()
        
        for i in range(self.b):
            band_hash = self._hash_band(minhash.hashvalues, i)
            if band_hash in self.hashtables[i]:
                candidates.update(self.hashtables[i][band_hash])
        
        return list(candidates)


@stable_api
class NearDuplicateDetector:
    """Near-duplicate detector using MinHash LSH.
    
    This class provides efficient near-duplicate detection for text documents.
    It uses MinHash signatures to create compact document representations and
    LSH (Locality-Sensitive Hashing) for fast candidate retrieval.
    
    Args:
        num_perm: Number of permutations for MinHash (more = more accurate but slower)
        threshold: Jaccard similarity threshold for duplicate detection (0.0-1.0)
        shingle_size: Size of n-gram shingles (words)
        
    Example:
        detector = NearDuplicateDetector(threshold=0.8)
        detector.add("doc1", "Machine learning is a subset of artificial intelligence.")
        
        result = detector.query("Machine learning is part of artificial intelligence.")
        print(f"Is duplicate: {result.is_duplicate}")  # True
        print(f"Similarity: {result.similarity:.2f}")  # ~0.85
    """
    
    def __init__(
        self,
        num_perm: int = 128,
        threshold: float = 0.8,
        shingle_size: int = 3,
    ):
        if not 0.0 < threshold <= 1.0:
            raise ValueError("threshold must be between 0 and 1")
        if num_perm < 16:
            raise ValueError("num_perm must be at least 16")
        if shingle_size < 1:
            raise ValueError("shingle_size must be at least 1")
        
        self.num_perm = num_perm
        self.threshold = threshold
        self.shingle_size = shingle_size
        
        # Use datasketch if available, otherwise pure Python
        self._use_datasketch = datasketch is not None
        
        if self._use_datasketch:
            self._lsh = datasketch.MinHashLSH(threshold=threshold, num_perm=num_perm)
        else:
            self._lsh = _PureMinHashLSH(threshold=threshold, num_perm=num_perm)
        
        # Store MinHash signatures for similarity computation
        self._signatures: dict[str, Any] = {}
        
        # Statistics
        self._total_shingles = 0
    
    def _tokenize(self, text: str) -> Iterator[str]:
        """Generate word n-gram shingles from text.
        
        Args:
            text: Input text
            
        Yields:
            Shingle strings
        """
        # Normalize text
        text = text.lower()
        
        # Simple word tokenization
        words = text.split()
        
        # Generate shingles
        for i in range(len(words) - self.shingle_size + 1):
            yield " ".join(words[i:i + self.shingle_size])
    
    def _create_minhash(self, text: str) -> Any:
        """Create MinHash signature for text.
        
        Args:
            text: Input text
            
        Returns:
            MinHash object (datasketch or pure Python)
        """
        if self._use_datasketch:
            mh = datasketch.MinHash(num_perm=self.num_perm)
        else:
            mh = _PureMinHash(num_perm=self.num_perm)
        
        shingle_count = 0
        for shingle in self._tokenize(text):
            mh.update(shingle.encode("utf-8"))
            shingle_count += 1
        
        self._total_shingles += shingle_count
        return mh
    
    def add(self, doc_id: str, text: str) -> None:
        """Add a document to the index.
        
        Args:
            doc_id: Unique document identifier
            text: Document text content
        """
        if doc_id in self._signatures:
            raise ValueError(f"Document {doc_id} already in index")
        
        mh = self._create_minhash(text)
        self._lsh.insert(doc_id, mh)
        self._signatures[doc_id] = mh
    
    def query(self, text: str) -> DuplicateResult:
        """Check if text is a near-duplicate of indexed documents.
        
        Args:
            text: Query text
            
        Returns:
            DuplicateResult with match information
        """
        import time
        start = time.perf_counter()
        
        mh = self._create_minhash(text)
        candidates = self._lsh.query(mh)
        
        if not candidates:
            elapsed = (time.perf_counter() - start) * 1000
            return DuplicateResult(
                is_duplicate=False,
                similarity=0.0,
                matched_ids=[],
                query_time_ms=elapsed,
            )
        
        # Compute exact similarity with candidates
        matches = []
        best_similarity = 0.0
        
        for cand_id in candidates:
            cand_mh = self._signatures[cand_id]
            similarity = mh.jaccard(cand_mh)
            
            if similarity >= self.threshold:
                matches.append(cand_id)
                best_similarity = max(best_similarity, similarity)
        
        elapsed = (time.perf_counter() - start) * 1000
        
        return DuplicateResult(
            is_duplicate=len(matches) > 0,
            similarity=best_similarity,
            matched_ids=matches,
            query_time_ms=elapsed,
        )
    
    def contains(self, doc_id: str) -> bool:
        """Check if document ID is in the index."""
        return doc_id in self._signatures
    
    def remove(self, doc_id: str) -> bool:
        """Remove a document from the index.
        
        Note: LSH removal is not supported by datasketch, so this only
        removes from the signature store. The LSH index entry remains
        but will not match since we verify candidates.
        
        Args:
            doc_id: Document ID to remove
            
        Returns:
            True if document was found and removed
        """
        if doc_id in self._signatures:
            del self._signatures[doc_id]
            return True
        return False
    
    def get_stats(self) -> DetectorStats:
        """Get statistics about the detector."""
        doc_count = len(self._signatures)
        
        # Estimate memory usage
        # MinHash: num_perm * 4 bytes per signature
        # LSH: varies, estimate as 2x signature size
        bytes_per_sig = self.num_perm * 4
        memory_mb = (doc_count * bytes_per_sig * 3) / (1024 * 1024)
        
        return DetectorStats(
            document_count=doc_count,
            total_shingles=self._total_shingles,
            avg_shingles_per_doc=self._total_shingles / max(1, doc_count),
            memory_estimate_mb=memory_mb,
        )
    
    def clear(self) -> None:
        """Clear all documents from the index."""
        if self._use_datasketch:
            self._lsh = datasketch.MinHashLSH(
                threshold=self.threshold,
                num_perm=self.num_perm,
            )
        else:
            self._lsh = _PureMinHashLSH(
                threshold=self.threshold,
                num_perm=self.num_perm,
            )
        self._signatures.clear()
        self._total_shingles = 0


@stable_api
def create_detector(
    threshold: float = 0.8,
    num_perm: int = 128,
) -> NearDuplicateDetector:
    """Create a near-duplicate detector with sensible defaults.
    
    Args:
        threshold: Similarity threshold (0.8 is good for near-duplicates)
        num_perm: Number of permutations (128 balances speed/accuracy)
        
    Returns:
        Configured NearDuplicateDetector
    """
    return NearDuplicateDetector(
        threshold=threshold,
        num_perm=num_perm,
        shingle_size=3,
    )
```

**Update pyproject.toml to add optional dependency:**

```toml
[project.optional-dependencies]
# ... existing ...

dedup = [
    "datasketch>=1.6.0",
]
```

**Integrate with merge stage:**

Update `src/collector_core/merge/__init__.py` to support near-duplicate detection:

```python
# Add to existing merge module

from collector_core.checks.near_duplicate import (
    NearDuplicateDetector,
    create_detector,
)

def merge_with_dedup(
    sources: Iterable[Path],
    output_dir: Path,
    *,
    exact_dedup: bool = True,
    near_dedup: bool = False,
    near_dedup_threshold: float = 0.8,
    text_field: str = "text",
) -> MergeStats:
    """Merge sources with optional deduplication.
    
    Args:
        sources: Input source paths (JSONL files)
        output_dir: Output directory
        exact_dedup: Enable exact hash-based deduplication
        near_dedup: Enable near-duplicate detection
        near_dedup_threshold: Similarity threshold for near-duplicates
        text_field: Field containing text for near-duplicate detection
        
    Returns:
        MergeStats with counts
    """
    # ... implementation
```

**Acceptance Criteria:**
- [ ] Near-duplicate detection module implemented
- [ ] Pure Python fallback works without datasketch
- [ ] Integration with merge stage complete
- [ ] Performance acceptable (< 1ms per query for 100K docs)
- [ ] Tests achieve 90%+ coverage of module

---

### 2.3 Implement Content Checks

The `SUPPORTED_CONTENT_CHECKS` set in `pipeline_driver_base.py` lists many checks. Implement the critical ones:

**Create `src/collector_core/checks/implementations/` directory with:**

```
src/collector_core/checks/implementations/
├── __init__.py
├── language_detect.py
├── license_validate.py
├── schema_validate.py
├── toxicity_scan.py
└── distribution_statement.py
```

Each implementation should follow this pattern:

```python
"""Content check implementation template.

Each content check module should export:
- check_name: str - The name used in targets YAML
- check(record: dict, config: dict) -> CheckResult
- CheckResult dataclass with standardized fields
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from collector_core.stability import stable_api


@stable_api
@dataclass
class CheckResult:
    """Result of a content check.
    
    Attributes:
        passed: Whether the check passed
        action: Action to take (keep, filter, flag, reject)
        reason: Human-readable reason
        details: Additional check-specific details
        confidence: Confidence in the result (0.0-1.0)
    """
    passed: bool
    action: str  # keep | filter | flag | reject
    reason: str | None = None
    details: dict[str, Any] | None = None
    confidence: float = 1.0


# Check name used in targets YAML
check_name = "example_check"


def check(record: dict[str, Any], config: dict[str, Any]) -> CheckResult:
    """Run the content check on a record.
    
    Args:
        record: The record to check
        config: Check configuration from targets YAML
        
    Returns:
        CheckResult with outcome
    """
    # Implementation here
    return CheckResult(passed=True, action="keep")
```

**Acceptance Criteria:**
- [ ] At least 5 content checks implemented
- [ ] Check registry updated to load implementations
- [ ] Tests for each check
- [ ] Documentation for check configuration

---

## Phase 3: Type Safety & Testing

### 3.1 Full Type Coverage

**Update `pyproject.toml` mypy configuration:**

```toml
[tool.mypy]
python_version = "3.10"
warn_unused_configs = true
warn_redundant_casts = true
warn_unused_ignores = true
strict_equality = true
strict_concatenate = true
check_untyped_defs = true
disallow_untyped_defs = true
disallow_incomplete_defs = true
disallow_untyped_decorators = true
no_implicit_optional = true
warn_return_any = true

# Remove global ignore_missing_imports
# Add specific overrides only where needed

[[tool.mypy.overrides]]
module = [
    "datasets.*",
    "boto3.*",
    "botocore.*",
    "datasketch.*",
    "trafilatura.*",
]
ignore_missing_imports = true
```

**Install type stubs:**

```bash
pip install boto3-stubs types-beautifulsoup4 types-lxml types-requests types-PyYAML
```

**Priority files needing type annotations:**

1. `src/collector_core/utils/io.py`
2. `src/collector_core/utils/paths.py`
3. `src/collector_core/pipeline_driver_base.py`
4. `src/collector_core/acquire/strategies/http.py`
5. `src/collector_core/merge/__init__.py`
6. `src/collector_core/yellow/base.py`

**Acceptance Criteria:**
- [ ] `mypy src/collector_core --strict` passes with no errors
- [ ] All public APIs have complete type annotations
- [ ] Type stubs installed for external dependencies

---

### 3.2 Increase Test Coverage to 90%+

**Add tests for new features:**

```
tests/
├── test_near_duplicate.py           # Near-duplicate detection
├── test_http_base.py                # HTTP base utilities
├── test_domain_screeners/
│   ├── test_chem_screener.py
│   ├── test_bio_screener.py
│   ├── test_code_screener.py
│   └── ...
├── test_content_checks/
│   ├── test_language_detect.py
│   ├── test_license_validate.py
│   └── ...
└── test_cli_comprehensive.py        # Full CLI coverage
```

**Example test file structure:**

```python
"""Tests for near-duplicate detection."""

from __future__ import annotations

import pytest
from hypothesis import given, strategies as st

from collector_core.checks.near_duplicate import (
    NearDuplicateDetector,
    DuplicateResult,
    create_detector,
)


class TestNearDuplicateDetector:
    """Tests for NearDuplicateDetector class."""
    
    def test_init_valid_params(self) -> None:
        """Test initialization with valid parameters."""
        detector = NearDuplicateDetector(
            num_perm=64,
            threshold=0.7,
            shingle_size=2,
        )
        assert detector.num_perm == 64
        assert detector.threshold == 0.7
    
    def test_init_invalid_threshold(self) -> None:
        """Test initialization fails with invalid threshold."""
        with pytest.raises(ValueError, match="threshold"):
            NearDuplicateDetector(threshold=1.5)
        
        with pytest.raises(ValueError, match="threshold"):
            NearDuplicateDetector(threshold=0.0)
    
    def test_exact_duplicate_detected(self) -> None:
        """Test that identical text is detected as duplicate."""
        detector = create_detector(threshold=0.5)
        
        text = "This is a test document about machine learning and AI."
        detector.add("doc1", text)
        
        result = detector.query(text)
        
        assert result.is_duplicate
        assert result.similarity > 0.99
        assert "doc1" in result.matched_ids
    
    def test_near_duplicate_detected(self) -> None:
        """Test that similar text is detected as near-duplicate."""
        detector = create_detector(threshold=0.7)
        
        detector.add("doc1", "Machine learning is a subset of artificial intelligence.")
        
        result = detector.query("Machine learning is part of artificial intelligence.")
        
        assert result.is_duplicate
        assert result.similarity > 0.7
    
    def test_different_text_not_duplicate(self) -> None:
        """Test that different text is not flagged as duplicate."""
        detector = create_detector(threshold=0.8)
        
        detector.add("doc1", "The quick brown fox jumps over the lazy dog.")
        
        result = detector.query("Python is a programming language for data science.")
        
        assert not result.is_duplicate
        assert result.similarity < 0.3
    
    def test_empty_index_query(self) -> None:
        """Test query against empty index."""
        detector = create_detector()
        
        result = detector.query("Any text here")
        
        assert not result.is_duplicate
        assert result.similarity == 0.0
        assert result.matched_ids == []
    
    def test_stats(self) -> None:
        """Test statistics reporting."""
        detector = create_detector()
        
        detector.add("doc1", "First document text")
        detector.add("doc2", "Second document text")
        
        stats = detector.get_stats()
        
        assert stats.document_count == 2
        assert stats.total_shingles > 0


class TestNearDuplicateProperties:
    """Property-based tests for near-duplicate detection."""
    
    @given(st.text(min_size=50, max_size=1000))
    def test_self_similarity(self, text: str) -> None:
        """Any text should be highly similar to itself."""
        if len(text.split()) < 5:
            return  # Skip very short texts
        
        detector = create_detector(threshold=0.5)
        detector.add("original", text)
        
        result = detector.query(text)
        
        assert result.is_duplicate
        assert result.similarity > 0.95
    
    @given(
        st.text(min_size=50, max_size=500),
        st.text(min_size=50, max_size=500),
    )
    def test_symmetry(self, text1: str, text2: str) -> None:
        """Similarity should be approximately symmetric."""
        if len(text1.split()) < 5 or len(text2.split()) < 5:
            return
        
        detector1 = create_detector()
        detector1.add("doc1", text1)
        result1 = detector1.query(text2)
        
        detector2 = create_detector()
        detector2.add("doc2", text2)
        result2 = detector2.query(text1)
        
        # Allow small difference due to LSH approximation
        assert abs(result1.similarity - result2.similarity) < 0.15
```

**Acceptance Criteria:**
- [ ] Test coverage ≥ 90% for `collector_core/`
- [ ] Property-based tests for core algorithms
- [ ] Integration tests for full pipeline flows
- [ ] All tests pass on Python 3.10 and 3.11

---

### 3.3 Integration Test Suite

**Create `tests/integration/test_full_flow.py`:**

```python
"""End-to-end integration tests for complete pipeline flows."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest


@pytest.fixture
def mini_dataset(tmp_path: Path) -> dict[str, Path]:
    """Create a minimal dataset for testing."""
    # Create directory structure
    data_root = tmp_path / "data"
    data_root.mkdir()
    
    # Create targets YAML
    targets_content = """
schema_version: "0.9"
updated_utc: "2024-01-01"

companion_files:
  license_map:
    - "license_map.yaml"
  denylist:
    - "denylist.yaml"

globals:
  raw_root: "{data_root}/raw"
  combined_root: "{data_root}/combined"
  manifests_root: "{data_root}/_manifests"
  queues_root: "{data_root}/_queues"
  ledger_root: "{data_root}/_ledger"

targets:
  - id: test-green
    name: "Test GREEN Dataset"
    enabled: true
    license_profile: permissive
    license_evidence:
      spdx_hint: "MIT"
      url: "https://example.com/license"
    download:
      strategy: none

  - id: test-yellow
    name: "Test YELLOW Dataset"
    enabled: true
    license_profile: unknown
    license_evidence:
      url: "https://example.com/terms"
    download:
      strategy: none

  - id: test-red
    name: "Test RED Dataset"
    enabled: true
    license_profile: deny
    license_evidence:
      spdx_hint: "Proprietary"
    download:
      strategy: none
""".format(data_root=data_root)
    
    targets_path = tmp_path / "targets_test.yaml"
    targets_path.write_text(targets_content)
    
    # Create minimal license map
    license_map = """
schema_version: "0.9"
spdx:
  allow:
    - "MIT"
    - "Apache-2.0"
  conditional:
    - "GPL-3.0"
  deny_prefixes:
    - "Proprietary"
gating:
  unknown_spdx_bucket: "YELLOW"
  deny_spdx_bucket: "RED"
"""
    (tmp_path / "license_map.yaml").write_text(license_map)
    
    # Create minimal denylist
    denylist = """
schema_version: "1.0"
domain_patterns: []
patterns: []
"""
    (tmp_path / "denylist.yaml").write_text(denylist)
    
    return {
        "root": tmp_path,
        "data_root": data_root,
        "targets": targets_path,
    }


@pytest.mark.integration
class TestFullPipelineFlow:
    """Integration tests for complete pipeline execution."""
    
    def test_classify_creates_queues(
        self,
        mini_dataset: dict[str, Path],
        run_dc: Any,
    ) -> None:
        """Test that classify stage creates queue files."""
        result = run_dc([
            "pipeline", "fixture",
            "--targets", str(mini_dataset["targets"]),
            "--dataset-root", str(mini_dataset["data_root"]),
            "--stage", "classify",
        ], capture_output=True)
        
        assert result.returncode == 0
        
        queues_root = mini_dataset["data_root"] / "_queues"
        assert queues_root.exists()
        
        # Check queue files created
        green_queue = queues_root / "green_pipeline.jsonl"
        yellow_queue = queues_root / "yellow_pipeline.jsonl"
        red_queue = queues_root / "red_pipeline.jsonl"
        
        assert green_queue.exists() or yellow_queue.exists() or red_queue.exists()
    
    def test_classify_sorts_by_license(
        self,
        mini_dataset: dict[str, Path],
        run_dc: Any,
    ) -> None:
        """Test that targets are sorted into correct buckets."""
        run_dc([
            "pipeline", "fixture",
            "--targets", str(mini_dataset["targets"]),
            "--dataset-root", str(mini_dataset["data_root"]),
            "--stage", "classify",
        ])
        
        queues_root = mini_dataset["data_root"] / "_queues"
        
        # Load and check queues
        def load_queue(name: str) -> list[dict]:
            path = queues_root / f"{name}_pipeline.jsonl"
            if not path.exists():
                return []
            return [json.loads(line) for line in path.read_text().strip().split("\n") if line]
        
        green = load_queue("green")
        yellow = load_queue("yellow")
        red = load_queue("red")
        
        # Check sorting
        green_ids = {r["id"] for r in green}
        yellow_ids = {r["id"] for r in yellow}
        red_ids = {r["id"] for r in red}
        
        assert "test-green" in green_ids, "MIT license should be GREEN"
        assert "test-yellow" in yellow_ids, "Unknown license should be YELLOW"
        assert "test-red" in red_ids, "Proprietary license should be RED"
```

**Acceptance Criteria:**
- [ ] Integration tests cover classify → acquire → yellow_screen → merge flow
- [ ] Tests verify correct bucket sorting
- [ ] Tests run in CI
- [ ] Tests clean up temporary data

---

## Phase 4: Production Hardening

### 4.1 Add Metrics Dashboard

**Create `src/collector_core/metrics/dashboard.py`:**

Implementation should provide:
- Pipeline run metrics collection
- Prometheus export format
- Simple HTML dashboard generation
- JSON metrics export

### 4.2 Add Checkpoint/Resume Support

**Create `src/collector_core/checkpoint.py`:**

Implementation should provide:
- Checkpoint saving during long operations
- Resume from checkpoint on restart
- Checkpoint cleanup on completion
- CLI flags: `--resume`, `--checkpoint-dir`

### 4.3 Schema Version Enforcement

**Create `src/collector_core/schema_version.py`:**

Implementation should provide:
- Schema version validation
- Version compatibility checks
- CI validation script
- Migration helpers for version upgrades

---

## Phase 5: Documentation

### 5.1 API Documentation

**Set up Sphinx documentation:**

```bash
# Install sphinx
pip install sphinx sphinx-rtd-theme sphinx-autodoc-typehints

# Initialize docs
cd docs
sphinx-quickstart

# Generate API docs
sphinx-apidoc -o api ../src/collector_core
```

**Documentation structure:**

```
docs/
├── conf.py
├── index.rst
├── quickstart.rst
├── architecture.rst
├── api/
│   ├── index.rst
│   ├── collector_core.rst
│   ├── acquire.rst
│   ├── classification.rst
│   ├── merge.rst
│   └── yellow.rst
├── guides/
│   ├── adding_pipeline.rst
│   ├── custom_screener.rst
│   ├── content_checks.rst
│   └── production_deploy.rst
└── reference/
    ├── schema_reference.rst
    ├── cli_reference.rst
    └── config_reference.rst
```

### 5.2 Example Notebooks

**Create Jupyter notebooks:**

```
notebooks/
├── 01_quickstart.ipynb
├── 02_custom_pipeline.ipynb
├── 03_yellow_review.ipynb
├── 04_content_checks.ipynb
└── 05_production_deployment.ipynb
```

---

## Acceptance Criteria Summary

### Phase 1: Technical Debt
- [ ] Deprecated wrappers removed (-2,500 lines)
- [ ] Broken symlink fixed
- [ ] HTTP strategies consolidated (-800 lines)
- [ ] CI passes with no deprecated imports

### Phase 2: Features
- [ ] All domain screeners implemented with real logic
- [ ] Near-duplicate detection working
- [ ] At least 5 content checks implemented
- [ ] Integration with merge stage complete

### Phase 3: Quality
- [ ] `mypy --strict` passes
- [ ] Test coverage ≥ 90%
- [ ] Property-based tests for core algorithms
- [ ] Integration test suite complete

### Phase 4: Production
- [ ] Metrics collection and export
- [ ] Checkpoint/resume support
- [ ] Schema version enforcement
- [ ] CI validation scripts

### Phase 5: Documentation
- [ ] API reference generated
- [ ] All guides complete
- [ ] Example notebooks working
- [ ] CLI reference complete

---

## Final Validation

Run the complete validation suite:

```bash
# Type checking
mypy src/collector_core --strict

# Linting
ruff check .
ruff format --check .

# Tests with coverage
pytest --cov=collector_core --cov-report=html --cov-fail-under=90

# Schema validation
python -m tools.validate_yaml_schemas --root .

# Preflight check
python -m tools.preflight --repo-root .

# Integration tests
pytest -m integration

# Documentation build
cd docs && make html
```

All commands must pass for A-grade status.
