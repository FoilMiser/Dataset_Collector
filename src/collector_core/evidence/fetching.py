from __future__ import annotations

import html
import ipaddress
import json
import logging
import re
import socket
import threading
import urllib.parse
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from pathlib import Path
from typing import Any

import requests

from collector_core.artifact_metadata import build_artifact_metadata
from collector_core.dependencies import _try_import
from collector_core.network_utils import _with_retries
from collector_core.secrets import REDACTED, SecretStr, redact_headers
from collector_core.stability import stable_api
from collector_core.utils.hash import sha256_bytes, sha256_file
from collector_core.utils.io import write_json
from collector_core.utils.logging import utc_now
from collector_core.utils.paths import ensure_dir

from .change_detection import (
    apply_normalized_hash_fallback,
    compute_normalized_text_hash,
    resolve_evidence_change,
)

logger = logging.getLogger(__name__)
PdfReader = _try_import("pypdf", "PdfReader")

EVIDENCE_EXTENSIONS = [".html", ".pdf", ".txt", ".json"]


class EvidenceFetchCache:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._entries: dict[tuple[object, ...], _FetchCacheEntry] = {}

    def get_or_fetch(
        self,
        key: tuple[object, ...],
        fetcher: Callable[[], tuple[bytes | None, str | None, dict[str, Any]]],
    ) -> tuple[bytes | None, str | None, dict[str, Any]]:
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                entry = _FetchCacheEntry()
                self._entries[key] = entry
                should_fetch = True
            else:
                should_fetch = False
        if should_fetch:
            try:
                entry.value = fetcher()
            except Exception as exc:
                entry.value = (None, "error", {"errors": [repr(exc)], "retries": 1})
                raise
            finally:
                entry.event.set()
        else:
            entry.event.wait()
        assert entry.value is not None
        content, info, meta = entry.value
        return content, info, deepcopy(meta)


class _FetchCacheEntry:
    def __init__(self) -> None:
        self.event = threading.Event()
        self.value: tuple[bytes | None, str | None, dict[str, Any]] | None = None


def _is_blocked_ip(ip_value: str) -> bool:
    ip = ipaddress.ip_address(ip_value)
    return ip.is_private or ip.is_loopback or ip.is_link_local


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


@stable_api
def validate_evidence_url(url: str, allow_private_hosts: bool) -> tuple[bool, str | None]:
    parsed = urllib.parse.urlparse(url)
    scheme = (parsed.scheme or "").lower()
    if scheme not in {"http", "https"}:
        return False, f"unsupported_scheme:{scheme or 'missing'}"
    if not parsed.hostname:
        return False, "missing_hostname"
    if allow_private_hosts:
        return True, None
    hostname = parsed.hostname
    try:
        ip_value = ipaddress.ip_address(hostname)
    except ValueError:
        ips = _resolve_host_ips(hostname)
        if not ips:
            return False, "unresolvable_hostname"
        for ip_value in ips:
            if _is_blocked_ip(ip_value):
                return False, f"blocked_ip:{ip_value}"
        return True, None
    if _is_blocked_ip(str(ip_value)):
        return False, f"blocked_ip:{ip_value}"
    return True, None


_HTML_SCRIPT_STYLE_RE = re.compile(r"(?is)<(script|style)[^>]*>.*?</\1>")
_HTML_COMMENT_RE = re.compile(r"(?s)<!--.*?-->")
_HTML_TAG_RE = re.compile(r"(?s)<[^>]+>")


@stable_api
def html_to_text(text: str) -> str:
    cleaned = html.unescape(text or "")
    cleaned = _HTML_SCRIPT_STYLE_RE.sub(" ", cleaned)
    cleaned = _HTML_COMMENT_RE.sub(" ", cleaned)
    cleaned = _HTML_TAG_RE.sub(" ", cleaned)
    return cleaned


@stable_api
def extract_text_from_path(path: Path, evidence: dict[str, Any] | None = None) -> str:
    if not path.exists():
        if evidence is not None:
            evidence["text_extraction_failed"] = True
        return ""
    if path.suffix.lower() == ".pdf":
        if evidence is not None:
            evidence["pdf_text_extraction_failed"] = False
            evidence["text_extraction_failed"] = False
        if PdfReader is None:
            if evidence is not None:
                evidence["pdf_text_extraction_failed"] = True
                evidence["text_extraction_failed"] = True
            return ""
        try:
            reader = PdfReader(str(path))
            pages = []
            for page in list(reader.pages)[:5]:
                text = page.extract_text() or ""
                if text:
                    pages.append(text)
            extracted = "\n\n".join(pages).strip()
            if not extracted and evidence is not None:
                evidence["pdf_text_extraction_failed"] = True
                evidence["text_extraction_failed"] = True
            return extracted
        except Exception:
            logger.warning("Failed to extract text from PDF: %s", path, exc_info=True)
            if evidence is not None:
                evidence["pdf_text_extraction_failed"] = True
                evidence["text_extraction_failed"] = True
            return ""
    try:
        raw = path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        logger.warning("Failed to read text from file: %s", path, exc_info=True)
        if evidence is not None:
            evidence["text_extraction_failed"] = True
        return ""
    if evidence is not None:
        evidence["text_extraction_failed"] = False
    if path.suffix.lower() in {".html", ".htm"}:
        return html_to_text(raw)
    return raw


@stable_api
def extract_text_for_scanning(evidence: dict[str, Any]) -> str:
    saved = str(evidence.get("saved_path") or "")
    if not saved:
        return ""
    return extract_text_from_path(Path(saved), evidence)


@stable_api
def compute_file_hashes(
    path: Path, evidence: dict[str, Any] | None = None
) -> tuple[str | None, str | None]:
    raw_hash = sha256_file(path)
    extracted = extract_text_from_path(path, evidence)
    extraction_failed = bool(
        evidence
        and (evidence.get("text_extraction_failed") or evidence.get("pdf_text_extraction_failed"))
    )
    if (
        not extraction_failed
        and evidence is None
        and path.suffix.lower() == ".pdf"
        and PdfReader is None
    ):
        extraction_failed = True
    normalized_hash = None
    if not extraction_failed:
        normalized_hash = compute_normalized_text_hash(extracted)
    normalized_hash = apply_normalized_hash_fallback(
        evidence=evidence,
        raw_hash=raw_hash,
        extraction_failed=extraction_failed,
        normalized_hash=normalized_hash,
    )
    return raw_hash, normalized_hash


def _scrub_secret_values(value: Any) -> Any:
    if isinstance(value, SecretStr):
        return REDACTED
    if isinstance(value, dict):
        return {key: _scrub_secret_values(val) for key, val in value.items()}
    if isinstance(value, list):
        return [_scrub_secret_values(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_scrub_secret_values(item) for item in value)
    return value


@stable_api
def redact_headers_for_manifest(headers: dict[str, str] | None) -> dict[str, Any]:
    if not headers:
        return {}
    return _scrub_secret_values(redact_headers(headers))


@stable_api
def find_existing_evidence(manifest_dir: Path) -> Path | None:
    for ext in EVIDENCE_EXTENSIONS:
        candidate = manifest_dir / f"license_evidence{ext}"
        if candidate.exists():
            return candidate
    return None


@stable_api
def cleanup_stale_evidence(manifest_dir: Path, keep: Path | None = None) -> list[Path]:
    removed: list[Path] = []
    keep_resolved = keep.resolve() if keep else None
    for path in manifest_dir.glob("license_evidence*"):
        if path.name == "license_evidence_meta.json":
            continue
        if path.name.startswith("license_evidence.prev_"):
            continue
        if keep_resolved and path.resolve() == keep_resolved:
            continue
        if path.is_file():
            path.unlink()
            removed.append(path)
    return removed


@stable_api
def fetch_url_with_retry(
    url: str,
    *,
    user_agent: str,
    timeout_s: float | tuple[float, float] = (15.0, 60.0),
    max_retries: int = 3,
    backoff_base: float = 2.0,
    headers: dict[str, str] | None = None,
    max_bytes: int | None = None,
    allow_private_hosts: bool = False,
) -> tuple[bytes | None, str | None, dict[str, Any]]:
    """Fetch URL with retry and exponential backoff."""
    meta: dict[str, Any] = {
        "retries": 0,
        "errors": [],
        "timeout": timeout_s,
        "max_bytes": max_bytes,
    }
    allowed, reason = validate_evidence_url(url, allow_private_hosts)
    if not allowed:
        meta["blocked_url"] = url
        meta["blocked_reason"] = reason
        return None, "blocked_url", meta

    def _record_retry(attempt: int, exc: Exception) -> None:
        meta["retries"] = attempt
        meta["errors"].append({"attempt": attempt, "error": repr(exc)})

    def _fetch_once() -> tuple[bytes, str]:
        with requests.get(
            url,
            timeout=timeout_s,
            headers={"User-Agent": user_agent, **(headers or {})},
            stream=True,
        ) as r:
            r.raise_for_status()
            ctype = r.headers.get("Content-Type", "")
            final_url = r.url
            meta["final_status"] = r.status_code
            meta["final_url"] = final_url
            meta["content_type"] = ctype
            redirect_urls: list[str] = []
            for resp in r.history:
                location = resp.headers.get("Location")
                if not location:
                    continue
                redirect_urls.append(urllib.parse.urljoin(resp.url, location))
            redirect_urls.append(final_url)
            for redirect_url in redirect_urls:
                allowed, reason = validate_evidence_url(redirect_url, allow_private_hosts)
                if not allowed:
                    meta["blocked_url"] = redirect_url
                    meta["blocked_reason"] = reason
                    logger.warning(
                        "Evidence fetch blocked: url=%s blocked_url=%s reason=%s",
                        url,
                        redirect_url,
                        reason,
                    )
                    raise RuntimeError("blocked_url")
            content_length = r.headers.get("Content-Length")
            if max_bytes and content_length:
                try:
                    if int(content_length) > max_bytes:
                        meta["size_exceeded"] = True
                        meta["bytes"] = int(content_length)
                        logger.warning(
                            "Evidence fetch aborted: url=%s final_url=%s content_type=%s bytes=%s limit=%s",
                            url,
                            final_url,
                            ctype,
                            content_length,
                            max_bytes,
                        )
                        raise RuntimeError("response_too_large")
                except ValueError:
                    pass

            chunks: list[bytes] = []
            total = 0
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if not chunk:
                    continue
                total += len(chunk)
                if max_bytes and total > max_bytes:
                    meta["size_exceeded"] = True
                    meta["bytes"] = total
                    logger.warning(
                        "Evidence fetch aborted: url=%s final_url=%s content_type=%s bytes=%s limit=%s",
                        url,
                        final_url,
                        ctype,
                        total,
                        max_bytes,
                    )
                    raise RuntimeError("response_too_large")
                chunks.append(chunk)
            content = b"".join(chunks)
            meta["bytes"] = total
            logger.info(
                "Evidence fetched: url=%s final_url=%s content_type=%s bytes=%s",
                url,
                final_url,
                ctype,
                total,
            )
            return content, ctype

    try:
        content, ctype = _with_retries(
            _fetch_once,
            max_attempts=max_retries,
            backoff_base=backoff_base,
            backoff_max=60.0,
            on_retry=_record_retry,
        )
        return content, ctype, meta
    except Exception as e:
        meta["errors"].append({"attempt": meta["retries"] + 1, "error": repr(e)})
        meta["retries"] = max(meta["retries"], 1)
        if str(e) == "blocked_url":
            return None, "blocked_url", meta
        if str(e) == "response_too_large":
            return None, "response_too_large", meta

    return None, f"Failed after {meta['retries']} attempts", meta


def _build_fetch_cache_key(
    *,
    url: str,
    headers: dict[str, str] | None,
    user_agent: str,
    allow_private_hosts: bool,
    max_bytes: int,
    max_retries: int,
    backoff_base: float,
) -> tuple[object, ...]:
    headers_key = tuple(sorted((headers or {}).items()))
    return (
        url,
        headers_key,
        user_agent,
        allow_private_hosts,
        max_bytes,
        max_retries,
        backoff_base,
    )


def _fetch_url_with_cache(
    *,
    url: str,
    user_agent: str,
    timeout_s: float | tuple[float, float] = (15.0, 60.0),
    max_retries: int = 3,
    backoff_base: float = 2.0,
    headers: dict[str, str] | None = None,
    max_bytes: int | None = None,
    allow_private_hosts: bool = False,
    fetch_cache: EvidenceFetchCache | None = None,
) -> tuple[bytes | None, str | None, dict[str, Any]]:
    if fetch_cache is None:
        return fetch_url_with_retry(
            url,
            user_agent=user_agent,
            timeout_s=timeout_s,
            max_retries=max_retries,
            backoff_base=backoff_base,
            headers=headers,
            max_bytes=max_bytes,
            allow_private_hosts=allow_private_hosts,
        )
    cache_key = _build_fetch_cache_key(
        url=url,
        headers=headers,
        user_agent=user_agent,
        allow_private_hosts=allow_private_hosts,
        max_bytes=max_bytes or 0,
        max_retries=max_retries,
        backoff_base=backoff_base,
    )

    def _fetch() -> tuple[bytes | None, str | None, dict[str, Any]]:
        return fetch_url_with_retry(
            url,
            user_agent=user_agent,
            timeout_s=timeout_s,
            max_retries=max_retries,
            backoff_base=backoff_base,
            headers=headers,
            max_bytes=max_bytes,
            allow_private_hosts=allow_private_hosts,
        )

    return fetch_cache.get_or_fetch(cache_key, _fetch)


@stable_api
def snapshot_evidence(
    manifest_dir: Path,
    url: str,
    *,
    user_agent: str,
    evidence_change_policy: str = "normalized",
    cosmetic_change_policy: str = "warn_only",
    max_retries: int = 3,
    backoff_base: float = 2.0,
    headers: dict[str, str] | None = None,
    allow_private_hosts: bool = False,
    max_bytes: int = 20 * 1024 * 1024,
    fetch_cache: EvidenceFetchCache | None = None,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "url": url,
        "fetched_at_utc": utc_now(),
        "status": "skipped",
        "headers_used": redact_headers_for_manifest(headers),
    }
    if not url:
        result["status"] = "no_url"
        return result

    previous_digest = None
    previous_normalized_digest = None
    existing_path = None
    existing_meta: dict[str, Any] = {}
    existing_path = find_existing_evidence(manifest_dir)
    if existing_path:
        previous_digest = sha256_file(existing_path)
    meta_path = manifest_dir / "license_evidence_meta.json"
    if meta_path.exists():
        try:
            existing_meta = json.loads(meta_path.read_text(encoding="utf-8"))
        except Exception:
            logger.warning("Failed to read existing evidence meta from %s", meta_path, exc_info=True)
            existing_meta = {}
    previous_normalized_digest = existing_meta.get("sha256_normalized_text")
    if previous_normalized_digest is None and existing_path:
        _, previous_normalized_digest = compute_file_hashes(existing_path)

    content, info, meta = _fetch_url_with_cache(
        url=url,
        user_agent=user_agent,
        max_retries=max_retries,
        backoff_base=backoff_base,
        headers=headers,
        max_bytes=max_bytes,
        allow_private_hosts=allow_private_hosts,
        fetch_cache=fetch_cache,
    )
    result["fetch_meta"] = meta

    if content is None:
        if meta.get("size_exceeded"):
            result["status"] = "needs_manual_evidence"
            result["error"] = info
        elif info == "blocked_url":
            result["status"] = "blocked_url"
            result["error"] = meta.get("blocked_reason") or info
        elif info == "response_too_large":
            result["status"] = "needs_manual_evidence"
            result["error"] = info
        else:
            result["status"] = "error"
            result["error"] = info
        return result

    ctype = info or ""
    if meta.get("final_url"):
        result["final_url"] = meta.get("final_url")
    digest = sha256_bytes(content)
    result.update(
        {
            "status": "ok",
            "content_type": ctype,
            "sha256": digest,
            "sha256_raw_bytes": digest,
            "bytes": len(content),
            "previous_sha256": previous_digest,
            "previous_sha256_raw_bytes": previous_digest,
            "previous_sha256_normalized_text": previous_normalized_digest,
            "previous_path": str(existing_path) if existing_path else None,
        }
    )

    ext = ".html"
    if "pdf" in ctype.lower():
        ext = ".pdf"
    elif "json" in ctype.lower():
        ext = ".json"
    elif "text/plain" in ctype.lower():
        ext = ".txt"

    ensure_dir(manifest_dir)
    out_path = manifest_dir / f"license_evidence{ext}"
    temp_path = manifest_dir / f"license_evidence{ext}.part"
    cleanup_stale_evidence(manifest_dir, keep=existing_path)
    raw_changed_from_previous = bool(previous_digest and previous_digest != digest)
    history: list[dict[str, Any]] = []
    if isinstance(existing_meta.get("history"), list):
        history = list(existing_meta.get("history", []))
    previous_entry = None
    previous_renamed_path = None
    if raw_changed_from_previous and existing_path and previous_digest:
        prev_ext = existing_path.suffix
        prev_prefix = f"license_evidence.prev_{previous_digest[:8]}"
        prev_path = manifest_dir / f"{prev_prefix}{prev_ext}"
        counter = 1
        while prev_path.exists():
            prev_path = manifest_dir / f"{prev_prefix}_{counter}{prev_ext}"
            counter += 1
        # P1.2G: Handle OSError on rename
        try:
            existing_path.rename(prev_path)
        except OSError:
            pass  # Ignore rename failures, proceed with new file
        previous_renamed_path = prev_path
        previous_entry = {
            "sha256": previous_digest,
            "sha256_raw_bytes": previous_digest,
            "sha256_normalized_text": previous_normalized_digest,
            "filename": prev_path.name,
            "fetched_at_utc": existing_meta.get("fetched_at_utc"),
        }
        history.append(previous_entry)
    temp_path.write_bytes(content)
    temp_path.replace(out_path)
    saved_digest = sha256_file(out_path)
    result["saved_path"] = str(out_path)
    if not saved_digest or saved_digest != digest:
        result["status"] = "error"
        result["error"] = "Evidence file write verification failed."
    normalized_text = extract_text_for_scanning(result)
    extraction_failed = bool(
        result.get("text_extraction_failed") or result.get("pdf_text_extraction_failed")
    )
    normalized_digest = None
    if not extraction_failed:
        normalized_digest = compute_normalized_text_hash(normalized_text)
    normalized_digest = apply_normalized_hash_fallback(
        evidence=result,
        raw_hash=digest,
        extraction_failed=extraction_failed,
        normalized_hash=normalized_digest,
    )
    normalized_changed_from_previous = bool(
        previous_normalized_digest
        and normalized_digest
        and previous_normalized_digest != normalized_digest
    )
    cosmetic_change = bool(
        raw_changed_from_previous
        and not normalized_changed_from_previous
        and normalized_digest
        and previous_normalized_digest
    )
    changed_from_previous = resolve_evidence_change(
        raw_changed_from_previous,
        normalized_changed_from_previous,
        cosmetic_change,
        evidence_change_policy,
        cosmetic_change_policy,
    )
    result.update(
        {
            "sha256_normalized_text": normalized_digest,
            "raw_changed_from_previous": raw_changed_from_previous,
            "normalized_changed_from_previous": normalized_changed_from_previous,
            "cosmetic_change": cosmetic_change,
            "changed_from_previous": changed_from_previous,
        }
    )
    if previous_renamed_path:
        result["previous_renamed_path"] = str(previous_renamed_path)
    result["history"] = history
    if result["changed_from_previous"]:
        result["evidence_files_verified"] = bool(
            (previous_renamed_path and previous_renamed_path.exists())
            and out_path.exists()
            and saved_digest == digest
        )
        if not result["evidence_files_verified"]:
            result["status"] = "error"
            result["error"] = "Evidence file rename/write verification failed."

    result.update(build_artifact_metadata(written_at_utc=result.get("fetched_at_utc")))
    write_json(manifest_dir / "license_evidence_meta.json", result)
    return result


@stable_api
def fetch_evidence(
    *,
    ctx: TargetContext,
    cfg: DriverConfig,
    user_agent: str,
    max_bytes: int,
    fetch_cache: EvidenceFetchCache | None = None,
) -> EvidenceResult:
    from collector_core.pipeline_driver_base import EvidenceResult

    evidence_snapshot = {"status": "skipped", "url": ctx.evidence_url}
    evidence_text = ""
    license_change_detected = False
    no_fetch_missing_evidence = False
    if "snapshot_terms" in ctx.license_gates and not cfg.args.no_fetch:
        evidence_snapshot = snapshot_evidence(
            ctx.target_manifest_dir,
            ctx.evidence_url,
            user_agent=user_agent,
            evidence_change_policy=cfg.license_map.evidence_change_policy,
            cosmetic_change_policy=cfg.license_map.cosmetic_change_policy,
            max_retries=cfg.retry_max,
            backoff_base=cfg.retry_backoff,
            headers=cfg.headers,
            allow_private_hosts=cfg.args.allow_private_evidence_hosts,
            max_bytes=max_bytes,
            fetch_cache=fetch_cache,
        )
        evidence_text = extract_text_for_scanning(evidence_snapshot)
        license_change_detected = bool(evidence_snapshot.get("changed_from_previous"))
    elif "snapshot_terms" in ctx.license_gates and cfg.args.no_fetch:
        existing_evidence_path = find_existing_evidence(ctx.target_manifest_dir)
        if existing_evidence_path:
            evidence_snapshot = {
                "status": "ok",
                "url": ctx.evidence_url,
                "saved_path": str(existing_evidence_path),
                "fetched_at_utc": utc_now(),
                "offline_mode": True,
            }
            raw_hash, normalized_hash = compute_file_hashes(
                existing_evidence_path, evidence_snapshot
            )
            evidence_snapshot.update(
                {
                    "sha256": raw_hash,
                    "sha256_raw_bytes": raw_hash,
                    "sha256_normalized_text": normalized_hash,
                }
            )
            evidence_text = extract_text_for_scanning(evidence_snapshot)
        else:
            evidence_snapshot = {"status": "offline_missing", "url": ctx.evidence_url}
            no_fetch_missing_evidence = True
    return EvidenceResult(
        snapshot=evidence_snapshot,
        text=evidence_text,
        license_change_detected=license_change_detected,
        no_fetch_missing_evidence=no_fetch_missing_evidence,
    )


@stable_api
def fetch_evidence_batch(
    *,
    ctxs: Sequence[TargetContext],
    cfg: DriverConfig,
    user_agent: str,
    max_bytes: int,
    max_workers: int | None = None,
) -> list[EvidenceResult]:
    if not ctxs:
        return []
    fetch_cache = EvidenceFetchCache()
    worker_count = max_workers or min(8, len(ctxs))
    if worker_count <= 1:
        return [
            fetch_evidence(
                ctx=ctx,
                cfg=cfg,
                user_agent=user_agent,
                max_bytes=max_bytes,
                fetch_cache=fetch_cache,
            )
            for ctx in ctxs
        ]
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = [
            executor.submit(
                fetch_evidence,
                ctx=ctx,
                cfg=cfg,
                user_agent=user_agent,
                max_bytes=max_bytes,
                fetch_cache=fetch_cache,
            )
            for ctx in ctxs
        ]
        return [future.result() for future in futures]


if False:  # pragma: no cover - type checking
    from collector_core.pipeline_driver_base import DriverConfig, EvidenceResult, TargetContext
