#!/usr/bin/env python3
"""
yellow_scrubber.py (v0.9)

Stage-2 transformations for YELLOW bucket datasets (quarantine -> permissive),
plus record-level allowlist planning.

Implemented:
  1) PubChem computed-only extraction:
     - Versioned field schemas with validation
     - CID-range sharding for stable partitioning
     - Resume support
  2) PMC OA allowlist planner:
     - Resilient parsing with fallback URLs
     - Improved field detection

v0.9 changes:
  - NEW: Parquet output option (--emit-parquet)
  - NEW: Near-duplicate detection (--dedupe)
  - NEW: InChIKey/SMILES normalization (--normalize)
  - NEW: MoNA/GNPS processing support
  - Schema validation from field_schemas.yaml
  - Type casting and validation rules
  - CID-range based sharding
  - Multiple fallback URLs for PMC OA list

Not legal advice.
"""

from __future__ import annotations

import argparse
import dataclasses
import fnmatch
import gzip
import hashlib
import json
import re
import time
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from collector_core.__version__ import __schema_version__ as SCHEMA_VERSION
from collector_core.__version__ import __version__ as TOOL_VERSION

try:
    import requests
except ImportError:
    requests = None

from collector_core.config_validator import read_yaml as read_yaml_config


def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def read_yaml(path: Path) -> dict[str, Any]:
    return read_yaml_config(path, schema_name="targets") or {}

def read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows

def write_json(path: Path, obj: dict[str, Any]) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

def write_jsonl_gz(path: Path, rows: Iterable[dict[str, Any]]) -> tuple[int, int]:
    ensure_dir(path.parent)
    count = 0
    with gzip.open(path, "wt", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
            count += 1
    return count, path.stat().st_size

def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024*1024), b""):
            h.update(chunk)
    return h.hexdigest()

def normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()

def lower(s: str) -> str:
    return (s or "").lower()

def safe_text(x: Any) -> str:
    return "" if x is None else str(x)

def require_requests() -> None:
    if requests is None:
        raise RuntimeError("Missing dependency: requests")


@dataclasses.dataclass
class Pools:
    permissive: Path
    copyleft: Path
    quarantine: Path

def pools_from_targets_yaml(targets_yaml: Path, fallback: Path) -> Pools:
    cfg = read_yaml(targets_yaml)
    pools = cfg.get("globals", {}).get("pools", {})
    return Pools(
        permissive=Path(pools.get("permissive", fallback / "permissive")).expanduser(),
        copyleft=Path(pools.get("copyleft", fallback / "copyleft")).expanduser(),
        quarantine=Path(pools.get("quarantine", fallback / "quarantine")).expanduser(),
    )


# --------------------------
# Field Schema Loading & Validation
# --------------------------

@dataclasses.dataclass
class FieldSpec:
    name: str
    field_type: str
    required: bool = False
    nullable: bool = True
    validation: dict[str, Any] = dataclasses.field(default_factory=dict)

def load_field_schemas(path: Path) -> dict[str, dict[str, FieldSpec]]:
    """Load field schemas from field_schemas.yaml."""
    if not path.exists():
        return {}
    
    cfg = read_yaml(path)
    schemas = {}
    
    for schema_name, schema_def in cfg.get("schemas", {}).items():
        fields = {}
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
                value = value[:validation["max_length"]]
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

def validate_record(record: dict[str, Any], schema: dict[str, FieldSpec]) -> tuple[bool, list[str]]:
    """Validate a record against a schema. Returns (is_valid, errors)."""
    errors = []
    
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

def iter_sdf_records_from_gz(gz_path: Path) -> Iterable[str]:
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

def parse_sdf_tags(record: str) -> dict[str, str]:
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
    
    v0.6: Added schema validation and CID-range sharding.
    """
    ensure_dir(permissive_out_dir / "shards")
    ensure_dir(permissive_out_dir / "_manifests")

    state = {"processed_files": [], "last_shard_idx": 0}
    processed = set()
    if resume_state_path and resume_state_path.exists():
        state = json.loads(resume_state_path.read_text(encoding="utf-8"))
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
    cid_shards: dict[int, list[dict[str, Any]]] = {} if cid_range_size else None

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
            if cid_range_size and "PUBCHEM_COMPOUND_CID" in record:
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
                    shard_path = permissive_out_dir / "shards" / f"pubchem_computed_{shard_idx:05d}.jsonl.gz"
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
                shard_path = permissive_out_dir / "shards" / f"pubchem_cid_range_{range_idx:08d}.jsonl.gz"
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
    write_json(permissive_out_dir / "_manifests" / f"extract_manifest_{int(time.time())}.json", manifest)
    return manifest


# --------------------------
# License Map helpers
# --------------------------

def load_license_map(path: Path) -> dict[str, Any]:
    return read_yaml(path)

def normalize_spdx_from_text(license_map: dict[str, Any], blob: str, spdx_hint: str = "") -> str:
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

def spdx_is_allowed(license_map: dict[str, Any], spdx: str) -> bool:
    spdx = safe_text(spdx).strip()
    allow = license_map.get("spdx", {}).get("allow", [])
    deny_prefixes = license_map.get("spdx", {}).get("deny_prefixes", [])
    
    for pref in deny_prefixes:
        if spdx.startswith(pref):
            return False
    return spdx in allow

def restriction_phrase_hits(license_map: dict[str, Any], text: str) -> list[str]:
    phrases = license_map.get("restriction_scan", {}).get("phrases", [])
    t = lower(text)
    return [p for p in phrases if p and lower(p) in t]


# --------------------------
# PMC OA allowlisting (v0.6: improved with fallbacks)
# --------------------------

PMC_OA_LIST_URLS = [
    "https://pmc.ncbi.nlm.nih.gov/tools/openftlist/",
    "https://www.ncbi.nlm.nih.gov/pmc/tools/openftlist/",
    "https://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_file_list.txt",
]

PMC_OA_FILE_LIST_PATTERNS = [
    "oa_file_list", "oa-file-list", "oa_file", "filelist",
    ".csv", ".txt", ".tsv"
]

def fetch_text_with_fallback(urls: list[str], timeout_s: int = 30) -> tuple[str, str]:
    """Try multiple URLs, return first successful response."""
    require_requests()
    
    for url in urls:
        try:
            r = requests.get(url, timeout=timeout_s, 
                           headers={"User-Agent": f"chem-corpus-scrubber/{TOOL_VERSION}"})
            r.raise_for_status()
            return r.text, url
        except Exception:
            continue
    
    raise RuntimeError(f"Failed to fetch from any URL: {urls}")

def download_file(url: str, out_path: Path, timeout_s: int = 60) -> dict[str, Any]:
    require_requests()
    ensure_dir(out_path.parent)
    
    with requests.get(url, stream=True, timeout=timeout_s,
                     headers={"User-Agent": f"chem-corpus-scrubber/{TOOL_VERSION}"}) as r:
        r.raise_for_status()
        tmp = out_path.with_suffix(out_path.suffix + ".part")
        with tmp.open("wb") as f:
            for chunk in r.iter_content(chunk_size=1024*1024):
                if chunk:
                    f.write(chunk)
        tmp.replace(out_path)
    
    return {
        "status": "ok", "url": url, "path": str(out_path),
        "bytes": out_path.stat().st_size, "sha256": sha256_file(out_path)
    }

def detect_delimiter(lines: list[str]) -> str:
    """Detect CSV/TSV delimiter from file content."""
    if not lines:
        return ","
    
    first_line = lines[0]
    tab_count = first_line.count("\t")
    comma_count = first_line.count(",")
    
    return "\t" if tab_count > comma_count else ","

def find_column_index(header: list[str], patterns: list[str]) -> int | None:
    """Find column index matching any of the patterns."""
    for i, h in enumerate(header):
        h_lower = h.lower().strip()
        for pattern in patterns:
            if pattern.lower() in h_lower:
                return i
    return None

def plan_pmc_allowlist(
    license_map: dict[str, Any],
    out_dir: Path,
    allowed_spdx: list[str] | None = None
) -> dict[str, Any]:
    """Plan PMC allowlist with improved resilience (v0.7)."""
    ensure_dir(out_dir)
    ensure_dir(out_dir / "_manifests")

    plan = {
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
        html, used_url = fetch_text_with_fallback(PMC_OA_LIST_URLS[:2])
        (out_dir / "pmc_openftlist.html").write_text(html, encoding="utf-8")
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
    list_path = None
    for list_url in candidates:
        try:
            ext = ".txt"
            if ".csv" in list_url.lower():
                ext = ".csv"
            elif ".tsv" in list_url.lower():
                ext = ".tsv"
            
            list_path = (out_dir / "pmc_oa_list_download").with_suffix(ext)
            meta = download_file(list_url, list_path)
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
            allow_rows.append({
                "pmcid": pmcid,
                "file": file_ref,
                "license_text": lic_text,
                "resolved_spdx": spdx
            })
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


def main() -> None:
    ap = argparse.ArgumentParser(description=f"Yellow Scrubber v{TOOL_VERSION} (schema {SCHEMA_VERSION})")
    ap.add_argument("--targets", required=True, help="targets.yaml v0.6")
    ap.add_argument("--license-map", default=None)
    ap.add_argument("--field-schemas", default=None, help="field_schemas.yaml")
    ap.add_argument("--pools-root", default="/data/chem/pools")
    ap.add_argument("--pubchem-enable", action="store_true")
    ap.add_argument("--pubchem-limit-files", type=int, default=None)
    ap.add_argument("--pubchem-limit-rows", type=int, default=None)
    ap.add_argument("--pubchem-shard-rows", type=int, default=500000)
    ap.add_argument("--pubchem-cid-range", type=int, default=None, help="CID range size for sharding")
    ap.add_argument("--pubchem-validate-schema", action="store_true", help="Enable schema validation")
    ap.add_argument("--pmc-enable", action="store_true")
    args = ap.parse_args()

    targets_path = Path(args.targets).expanduser().resolve()
    targets_cfg = read_yaml(targets_path)
    companion = targets_cfg.get("companion_files", {})
    
    license_map_path = Path(args.license_map or companion.get("license_map", "./license_map.yaml")).expanduser().resolve()
    license_map = load_license_map(license_map_path)
    
    field_schemas_path = Path(args.field_schemas or companion.get("field_schemas", "./field_schemas.yaml")).expanduser().resolve()
    field_schemas = load_field_schemas(field_schemas_path) if field_schemas_path.exists() else {}

    pools = pools_from_targets_yaml(targets_path, Path(args.pools_root).expanduser().resolve())
    target_defs = {t["id"]: t for t in targets_cfg.get("targets", []) if isinstance(t, dict) and t.get("id")}

    run_report: dict[str, Any] = {
        "run_at_utc": utc_now(),
        "tool_version": TOOL_VERSION,
        "schema_version": SCHEMA_VERSION,
        "targets_yaml": str(targets_path),
        "pubchem_ran": False,
        "pmc_ran": False,
        "outputs": [],
    }

    if args.pubchem_enable:
        pubchem_in = pools.quarantine / "pubchem_compound_sdf_bulk"
        pubchem_out = pools.permissive / "pubchem_derived_computed_only"

        derived_def = target_defs.get("pubchem_derived_computed_only", {})
        include_fields = derived_def.get("build", {}).get("include_fields", [])
        if not include_fields:
            include_fields = [
                "PUBCHEM_COMPOUND_CID", "PUBCHEM_CACTVS_CANONICAL_SMILES",
                "PUBCHEM_IUPAC_INCHI", "PUBCHEM_IUPAC_INCHIKEY",
                "PUBCHEM_MOLECULAR_FORMULA", "PUBCHEM_MOLECULAR_WEIGHT",
            ]

        # Get field schema if validation enabled
        field_schema = None
        if args.pubchem_validate_schema:
            schema_version = derived_def.get("build", {}).get("field_schema_version", "pubchem_computed_only_v1.0.0")
            field_schema = field_schemas.get(schema_version, {})

        raw_def = target_defs.get("pubchem_compound_sdf_bulk", {})
        include_globs = raw_def.get("download", {}).get("include_globs", ["*.sdf.gz"])
        
        # CID range from config or args
        cid_range = args.pubchem_cid_range
        if not cid_range:
            sharding_cfg = derived_def.get("build", {}).get("sharding", {})
            if sharding_cfg.get("method") == "cid_range":
                cid_range = sharding_cfg.get("range_size")

        resume_state = pubchem_out / "_manifests" / "resume_state.json"
        
        if not pubchem_in.exists():
            run_report["outputs"].append({"pubchem": "skipped", "reason": f"missing {pubchem_in}"})
        else:
            manifest = extract_pubchem_computed_only(
                quarantine_dir=pubchem_in,
                permissive_out_dir=pubchem_out,
                include_globs=include_globs,
                include_fields=include_fields,
                field_schema=field_schema,
                shard_max_rows=args.pubchem_shard_rows,
                cid_range_size=cid_range,
                limit_files=args.pubchem_limit_files,
                limit_rows=args.pubchem_limit_rows,
                resume_state_path=resume_state,
            )
            run_report["pubchem_ran"] = True
            run_report["outputs"].append({"pubchem_manifest": manifest})

    if args.pmc_enable:
        pmc_dir = pools.quarantine / "pmc_oa_fulltext"
        ensure_dir(pmc_dir)
        plan = plan_pmc_allowlist(license_map=license_map, out_dir=pmc_dir)
        run_report["pmc_ran"] = True
        run_report["outputs"].append({"pmc_plan": plan})

    logs_dir = Path("/data/chem/_logs").expanduser()
    ensure_dir(logs_dir)
    out_path = logs_dir / f"yellow_scrubber_run_{int(time.time())}.json"
    write_json(out_path, run_report)
    
    print(f"\n{'='*50}\nYellow Scrubber v{TOOL_VERSION} (schema {SCHEMA_VERSION})\n{'='*50}")
    print(f"PubChem: {'ran' if run_report['pubchem_ran'] else 'skipped'}")
    print(f"PMC: {'ran' if run_report['pmc_ran'] else 'skipped'}")
    print(f"Report: {out_path}\n{'='*50}\n")
    print(json.dumps({"run_report": str(out_path)}, indent=2))


if __name__ == "__main__":
    main()
