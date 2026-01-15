#!/usr/bin/env python3
"""
yellow_scrubber_base.py

Shared base implementation for yellow_scrubber functionality across pipelines.
This module provides common utilities for:
  - PubChem computed-only extraction with schema validation
  - PMC OA allowlist planning
  - Field schema loading and validation
  - License map helpers

Pipeline-specific yellow_scrubber.py files should import from this module
rather than duplicating the implementation.

Issue 3.2 (v3.0): This module is being consolidated with yellow/base.py.
New code should use collector_core.yellow.base for yellow screen logic.
The utilities in this module (Pools, FieldSpec, etc.) remain stable API.

Deprecation schedule:
- v3.0: Both locations work; this module provides utilities
- v4.0: Consider consolidating remaining utilities into yellow/ package
"""

from __future__ import annotations

import dataclasses
import fnmatch
import gzip
import json
import re
import time
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from collector_core.__version__ import __schema_version__ as SCHEMA_VERSION
from collector_core.__version__ import __version__ as TOOL_VERSION
from collector_core.companion_files import (
    read_field_schemas,
    read_license_maps,
    resolve_companion_paths,
)
from collector_core.stability import stable_api
from collector_core.utils.hash import sha256_file
from collector_core.utils.http import requests, require_requests
from collector_core.utils.io import read_jsonl_list as read_jsonl
from collector_core.utils.io import read_yaml, write_json, write_jsonl_gz
from collector_core.utils.logging import utc_now
from collector_core.utils.paths import ensure_dir
from collector_core.utils.text import lower, normalize_whitespace, safe_text

# --------------------------
# Pools configuration
# --------------------------


@stable_api
@dataclasses.dataclass
class Pools:
    """Pool directories for dataset storage."""

    permissive: Path
    copyleft: Path
    quarantine: Path


@stable_api
def pools_from_targets_yaml(targets_yaml: Path, fallback: Path) -> Pools:
    """Load pool paths from targets YAML config."""
    cfg = read_yaml(targets_yaml, schema_name="targets")
    pools = cfg.get("globals", {}).get("pools", {})
    return Pools(
        permissive=Path(pools.get("permissive", fallback / "permissive")).expanduser(),
        copyleft=Path(pools.get("copyleft", fallback / "copyleft")).expanduser(),
        quarantine=Path(pools.get("quarantine", fallback / "quarantine")).expanduser(),
    )


# --------------------------
# Field Schema Loading & Validation
# --------------------------


@stable_api
@dataclasses.dataclass
class FieldSpec:
    """Specification for a field with validation rules."""

    name: str
    field_type: str
    required: bool = False
    nullable: bool = True
    validation: dict[str, Any] = dataclasses.field(default_factory=dict)


@stable_api
def load_field_schemas(paths: list[Path]) -> dict[str, dict[str, FieldSpec]]:
    """Load field schemas from field_schemas.yaml files."""
    schemas: dict[str, dict[str, FieldSpec]] = {}

    for schema_name, schema_def in read_field_schemas(paths).items():
        fields: dict[str, FieldSpec] = {}
        for field_name, field_def in schema_def.get("fields", {}).items():
            fields[field_name] = FieldSpec(
                name=field_name,
                field_type=field_def.get("type", "string"),
                required=field_def.get("required", False),
                nullable=field_def.get("nullable", True),
                validation=field_def.get("validation", {}),
            )
        schemas[schema_name] = fields

    return schemas


@stable_api
def cast_value(value: str, field_type: str, validation: dict[str, Any]) -> Any:
    """Cast a string value to the appropriate type with validation."""
    if not value or value.strip() == "":
        return None

    value = value.strip()

    try:
        if field_type == "integer":
            result = int(float(value))  # Handle "123.0" style
            if "min" in validation and result < validation["min"]:
                return None
            if "max" in validation and result > validation["max"]:
                return None
            return result

        elif field_type == "float":
            result = float(value)
            if "min" in validation and result < validation["min"]:
                return None
            if "max" in validation and result > validation["max"]:
                return None
            return result

        elif field_type == "string":
            if "max_length" in validation and len(value) > validation["max_length"]:
                value = value[: validation["max_length"]]
            if "pattern" in validation:
                if not re.match(validation["pattern"], value):
                    return None
            return value

        elif field_type == "boolean":
            return value.lower() in ("true", "1", "yes")

        else:
            return value

    except (ValueError, TypeError):
        return None


@stable_api
def validate_record(
    record: dict[str, Any], schema: dict[str, FieldSpec]
) -> tuple[bool, list[str]]:
    """Validate a record against a schema. Returns (is_valid, errors)."""
    errors: list[str] = []

    for field_name, spec in schema.items():
        value = record.get(field_name)

        if spec.required and (value is None or value == ""):
            errors.append(f"Missing required field: {field_name}")

        if value is not None and not spec.nullable and value == "":
            errors.append(f"Non-nullable field is empty: {field_name}")

    return len(errors) == 0, errors


# --------------------------
# PubChem SDF parsing
# --------------------------


@stable_api
def iter_sdf_records_from_gz(gz_path: Path) -> Iterable[str]:
    """Iterate over SDF records in a gzipped file."""
    with gzip.open(gz_path, "rt", encoding="utf-8", errors="ignore") as f:
        buf: list[str] = []
        for line in f:
            if line.rstrip("\n").strip() == "$$$$":
                if buf:
                    yield "".join(buf)
                buf = []
            else:
                buf.append(line)
        if buf:
            yield "".join(buf)


TAG_RE = re.compile(r"^>\s*<([^>]+)>\s*$")


@stable_api
def parse_sdf_tags(record: str) -> dict[str, str]:
    """Parse SDF tags from a record."""
    lines = record.splitlines()
    out: dict[str, str] = {}
    i = 0
    while i < len(lines):
        m = TAG_RE.match(lines[i].strip())
        if m:
            key = m.group(1).strip()
            i += 1
            vals: list[str] = []
            while i < len(lines):
                if TAG_RE.match(lines[i].strip()) or lines[i].strip() == "":
                    break
                vals.append(lines[i])
                i += 1
            out[key] = "\n".join(vals).strip()
        else:
            i += 1
    return out


@stable_api
def extract_pubchem_computed_only(
    quarantine_dir: Path,
    permissive_out_dir: Path,
    include_globs: list[str],
    include_fields: list[str],
    field_schema: dict[str, FieldSpec] | None,
    shard_max_rows: int,
    cid_range_size: int | None = None,
    limit_files: int | None = None,
    limit_rows: int | None = None,
    resume_state_path: Path | None = None,
) -> dict[str, Any]:
    """Extract computed-only fields from PubChem SDF files.

    Args:
        quarantine_dir: Input directory containing SDF.gz files
        permissive_out_dir: Output directory for processed shards
        include_globs: Glob patterns for input files
        include_fields: Fields to extract from SDF records
        field_schema: Optional schema for validation
        shard_max_rows: Maximum rows per output shard
        cid_range_size: Optional CID range for sharding
        limit_files: Optional limit on files to process
        limit_rows: Optional limit on rows to emit
        resume_state_path: Optional path for resume state

    Returns:
        Manifest dict with processing results
    """
    ensure_dir(permissive_out_dir / "shards")
    ensure_dir(permissive_out_dir / "_manifests")

    state: dict[str, Any] = {"processed_files": [], "last_shard_idx": 0}
    processed: set[str] = set()
    if resume_state_path and resume_state_path.exists():
        # P1.4D: Add try/except for JSON load
        try:
            state = json.loads(resume_state_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            state = {"processed_files": [], "last_shard_idx": 0}
        processed = set(state.get("processed_files") or [])

    files = sorted([p for p in quarantine_dir.glob("*.gz")])
    if include_globs:
        files = [p for p in files if any(fnmatch.fnmatch(p.name, g) for g in include_globs)]

    if limit_files:
        files = [f for f in files if f.name not in processed][:limit_files]
    else:
        files = [f for f in files if f.name not in processed]

    shard_idx = state.get("last_shard_idx", 0)
    total_rows = 0
    files_done = 0
    current_shard: list[dict[str, Any]] = []
    validation_errors = 0

    # For CID-range sharding
    cid_shards: dict[int, list[dict[str, Any]]] | None = {} if cid_range_size else None

    for gz_path in files:
        for raw_record in iter_sdf_records_from_gz(gz_path):
            tags = parse_sdf_tags(raw_record)

            record: dict[str, Any] = {}
            for f in include_fields:
                raw_val = tags.get(f, "")

                # Apply schema validation if available
                if field_schema and f in field_schema:
                    spec = field_schema[f]
                    record[f] = cast_value(raw_val, spec.field_type, spec.validation)
                else:
                    record[f] = raw_val if raw_val else None

            # Validate record
            if field_schema:
                is_valid, errors = validate_record(record, field_schema)
                if not is_valid:
                    validation_errors += 1
                    continue

            # CID-range sharding
            if cid_range_size and cid_shards is not None and "PUBCHEM_COMPOUND_CID" in record:
                cid = record.get("PUBCHEM_COMPOUND_CID")
                if cid is not None:
                    range_idx = cid // cid_range_size
                    if range_idx not in cid_shards:
                        cid_shards[range_idx] = []
                    cid_shards[range_idx].append(record)
                    total_rows += 1
            else:
                current_shard.append(record)
                total_rows += 1

                if len(current_shard) >= shard_max_rows:
                    shard_path = (
                        permissive_out_dir / "shards" / f"pubchem_computed_{shard_idx:05d}.jsonl.gz"
                    )
                    write_jsonl_gz(shard_path, current_shard)
                    current_shard = []
                    shard_idx += 1

            if limit_rows and total_rows >= limit_rows:
                break

        processed.add(gz_path.name)
        files_done += 1

        # Update resume state
        state["processed_files"] = sorted(list(processed))
        state["last_shard_idx"] = shard_idx
        if resume_state_path:
            write_json(resume_state_path, state)

        if limit_rows and total_rows >= limit_rows:
            break

    # Write remaining records
    if cid_range_size and cid_shards:
        for range_idx, records in sorted(cid_shards.items()):
            if records:
                shard_path = (
                    permissive_out_dir / "shards" / f"pubchem_cid_range_{range_idx:08d}.jsonl.gz"
                )
                write_jsonl_gz(shard_path, records)
    elif current_shard:
        shard_path = permissive_out_dir / "shards" / f"pubchem_computed_{shard_idx:05d}.jsonl.gz"
        write_jsonl_gz(shard_path, current_shard)

    manifest = {
        "task": "pubchem_computed_only_extraction",
        "tool_version": TOOL_VERSION,
        "schema_version": SCHEMA_VERSION,
        "input_dir": str(quarantine_dir),
        "output_dir": str(permissive_out_dir),
        "include_fields": include_fields,
        "schema_validated": field_schema is not None,
        "cid_range_sharding": cid_range_size is not None,
        "files_processed_now": files_done,
        "rows_emitted_now": total_rows,
        "validation_errors": validation_errors,
        "finished_at_utc": utc_now(),
    }
    write_json(
        permissive_out_dir / "_manifests" / f"extract_manifest_{int(time.time())}.json", manifest
    )
    return manifest


# --------------------------
# License Map helpers
# --------------------------


@stable_api
def load_license_map(paths: list[Path]) -> dict[str, Any]:
    """Load license maps from files."""
    return read_license_maps(paths)


@stable_api
def normalize_spdx_from_text(license_map: dict[str, Any], blob: str, spdx_hint: str = "") -> str:
    """Normalize license text to SPDX identifier."""
    hint = normalize_whitespace(safe_text(spdx_hint))
    if hint and hint.upper() not in {"MIXED", "UNKNOWN"}:
        return hint

    rules = license_map.get("normalization", {}).get("rules", [])
    b = lower(normalize_whitespace(blob))

    for rule in rules:
        needles = [lower(x) for x in (rule.get("match_any") or []) if x]
        if any(n in b for n in needles):
            return safe_text(rule.get("spdx", "UNKNOWN")) or "UNKNOWN"

    return "UNKNOWN"


@stable_api
def spdx_is_allowed(license_map: dict[str, Any], spdx: str) -> bool:
    """Check if SPDX identifier is in allowlist."""
    spdx = safe_text(spdx).strip()
    allow = license_map.get("spdx", {}).get("allow", [])
    deny_prefixes = license_map.get("spdx", {}).get("deny_prefixes", [])

    for pref in deny_prefixes:
        if spdx.startswith(pref):
            return False
    return spdx in allow


@stable_api
def restriction_phrase_hits(license_map: dict[str, Any], text: str) -> list[str]:
    """Find restriction phrases in text."""
    phrases = license_map.get("restriction_scan", {}).get("phrases", [])
    t = lower(text)
    return [p for p in phrases if p and lower(p) in t]


# --------------------------
# PMC OA allowlisting
# --------------------------

PMC_OA_LIST_URLS = [
    "https://pmc.ncbi.nlm.nih.gov/tools/openftlist/",
    "https://www.ncbi.nlm.nih.gov/pmc/tools/openftlist/",
    "https://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_file_list.txt",
]

PMC_OA_FILE_LIST_PATTERNS = [
    "oa_file_list",
    "oa-file-list",
    "oa_file",
    "filelist",
    ".csv",
    ".txt",
    ".tsv",
]


@stable_api
def fetch_text_with_fallback(
    urls: list[str], timeout_s: int = 30, user_agent_prefix: str = "corpus-scrubber"
) -> tuple[str, str]:
    """Try multiple URLs, return first successful response."""
    require_requests()

    for url in urls:
        try:
            r = requests.get(
                url,
                timeout=timeout_s,
                headers={"User-Agent": f"{user_agent_prefix}/{TOOL_VERSION}"},
            )
            r.raise_for_status()
            return r.text, url
        except requests.RequestException:
            # P1.1F: Catch specific requests exception
            continue

    raise RuntimeError(f"Failed to fetch from any URL: {urls}")


@stable_api
def download_file(
    url: str, out_path: Path, timeout_s: int = 60, user_agent_prefix: str = "corpus-scrubber"
) -> dict[str, Any]:
    """Download file from URL."""
    require_requests()
    ensure_dir(out_path.parent)

    with requests.get(
        url,
        stream=True,
        timeout=timeout_s,
        headers={"User-Agent": f"{user_agent_prefix}/{TOOL_VERSION}"},
    ) as r:
        r.raise_for_status()
        tmp = out_path.with_suffix(out_path.suffix + ".part")
        with tmp.open("wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
        tmp.replace(out_path)

    return {
        "status": "ok",
        "url": url,
        "path": str(out_path),
        "bytes": out_path.stat().st_size,
        "sha256": sha256_file(out_path),
    }


@stable_api
def detect_delimiter(lines: list[str]) -> str:
    """Detect CSV/TSV delimiter from file content."""
    if not lines:
        return ","

    first_line = lines[0]
    tab_count = first_line.count("\t")
    comma_count = first_line.count(",")

    return "\t" if tab_count > comma_count else ","


@stable_api
def find_column_index(header: list[str], patterns: list[str]) -> int | None:
    """Find column index matching any of the patterns."""
    for i, h in enumerate(header):
        h_lower = h.lower().strip()
        for pattern in patterns:
            if pattern.lower() in h_lower:
                return i
    return None


@stable_api
def plan_pmc_allowlist(
    license_map: dict[str, Any],
    out_dir: Path,
    allowed_spdx: list[str] | None = None,
    user_agent_prefix: str = "corpus-scrubber",
) -> dict[str, Any]:
    """Plan PMC allowlist with improved resilience."""
    ensure_dir(out_dir)
    ensure_dir(out_dir / "_manifests")

    plan: dict[str, Any] = {
        "task": "pmc_allowlist_plan",
        "tool_version": TOOL_VERSION,
        "schema_version": SCHEMA_VERSION,
        "oa_list_urls_tried": PMC_OA_LIST_URLS,
        "picked_list_url": None,
        "downloaded_list_path": None,
        "allowlist_rows": 0,
        "deny_rows": 0,
        "unknown_rows": 0,
        "finished_at_utc": utc_now(),
    }

    # Try to fetch OA list page
    try:
        html, used_url = fetch_text_with_fallback(
            PMC_OA_LIST_URLS[:2], user_agent_prefix=user_agent_prefix
        )
        # Atomic write to prevent corruption if interrupted
        html_path = out_dir / "pmc_openftlist.html"
        html_tmp = html_path.with_suffix(".tmp")
        html_tmp.write_text(html, encoding="utf-8")
        html_tmp.replace(html_path)
        plan["oa_list_page_url"] = used_url
    except Exception as e:
        plan["error"] = f"Failed to fetch OA list page: {e}"
        write_json(out_dir / "_manifests" / f"pmc_allowlist_plan_{int(time.time())}.json", plan)
        return plan

    # Find candidate file list URLs
    hrefs = re.findall(r'href="([^"]+)"', html, flags=re.IGNORECASE)
    candidates: list[str] = []

    for h in hrefs:
        if any(x in h.lower() for x in PMC_OA_FILE_LIST_PATTERNS):
            full_url = h if h.startswith("http") else f"https://pmc.ncbi.nlm.nih.gov{h}"
            candidates.append(full_url)

    # Add fallback direct URL
    candidates.append("https://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_file_list.txt")
    candidates = list(dict.fromkeys(candidates))[:20]
    plan["download_list_candidates"] = candidates

    if not candidates:
        plan["error"] = "No file list candidates found"
        write_json(out_dir / "_manifests" / f"pmc_allowlist_plan_{int(time.time())}.json", plan)
        return plan

    # Try to download file list
    list_path: Path | None = None
    for list_url in candidates:
        try:
            ext = ".txt"
            if ".csv" in list_url.lower():
                ext = ".csv"
            elif ".tsv" in list_url.lower():
                ext = ".tsv"

            list_path = (out_dir / "pmc_oa_list_download").with_suffix(ext)
            meta = download_file(list_url, list_path, user_agent_prefix=user_agent_prefix)
            plan["picked_list_url"] = list_url
            plan["downloaded_list_path"] = meta["path"]
            break
        except Exception:
            continue

    if not list_path or not list_path.exists():
        plan["error"] = "Failed to download any file list"
        write_json(out_dir / "_manifests" / f"pmc_allowlist_plan_{int(time.time())}.json", plan)
        return plan

    # Parse file list with robust delimiter detection
    text = list_path.read_text(encoding="utf-8", errors="ignore")
    lines = [ln for ln in text.splitlines() if ln.strip()]

    delim = detect_delimiter(lines)
    header = [h.strip() for h in lines[0].split(delim)] if lines else []
    rows = [ln.split(delim) for ln in lines[1:]]

    # Find columns with multiple pattern attempts
    lic_col = find_column_index(header, ["license", "licence"])
    file_col = find_column_index(header, ["file", "ftp", "url", "path"])
    pmcid_col = find_column_index(header, ["pmcid", "pmc", "accession"])

    plan["detected_columns"] = {
        "license": lic_col,
        "file": file_col,
        "pmcid": pmcid_col,
        "delimiter": delim,
        "header": header[:10],  # First 10 columns
    }

    allow_spdx = allowed_spdx or license_map.get("spdx", {}).get("allow", [])
    allow_rows: list[dict[str, Any]] = []
    deny = 0
    unk = 0

    for cols in rows:
        lic_text = cols[lic_col].strip() if lic_col is not None and lic_col < len(cols) else ""
        file_ref = cols[file_col].strip() if file_col is not None and file_col < len(cols) else ""
        pmcid = cols[pmcid_col].strip() if pmcid_col is not None and pmcid_col < len(cols) else ""

        blob = f"{lic_text} {file_ref}"
        spdx = normalize_spdx_from_text(license_map, blob, spdx_hint="UNKNOWN")
        hits = restriction_phrase_hits(license_map, blob)

        if hits:
            deny += 1
            continue
        if spdx == "UNKNOWN":
            unk += 1
            continue
        if spdx in allow_spdx and spdx_is_allowed(license_map, spdx):
            allow_rows.append(
                {"pmcid": pmcid, "file": file_ref, "license_text": lic_text, "resolved_spdx": spdx}
            )
        else:
            deny += 1

    plan["allowlist_rows"] = len(allow_rows)
    plan["deny_rows"] = deny
    plan["unknown_rows"] = unk

    allow_path = out_dir / "pmc_allowlist.jsonl"
    with allow_path.open("w", encoding="utf-8") as f:
        for rr in allow_rows:
            f.write(json.dumps(rr, ensure_ascii=False) + "\n")
    plan["allowlist_path"] = str(allow_path)

    write_json(out_dir / "_manifests" / f"pmc_allowlist_plan_{int(time.time())}.json", plan)
    return plan


# Export common classes and functions for pipeline-specific imports
__all__ = [
    "SCHEMA_VERSION",
    "TOOL_VERSION",
    "FieldSpec",
    "Pools",
    "cast_value",
    "detect_delimiter",
    "download_file",
    "ensure_dir",
    "extract_pubchem_computed_only",
    "fetch_text_with_fallback",
    "find_column_index",
    "iter_sdf_records_from_gz",
    "load_field_schemas",
    "load_license_map",
    "lower",
    "normalize_spdx_from_text",
    "normalize_whitespace",
    "parse_sdf_tags",
    "plan_pmc_allowlist",
    "pools_from_targets_yaml",
    "read_jsonl",
    "read_yaml",
    "require_requests",
    "resolve_companion_paths",
    "restriction_phrase_hits",
    "safe_text",
    "sha256_file",
    "spdx_is_allowed",
    "utc_now",
    "validate_record",
    "write_json",
    "write_jsonl_gz",
]
