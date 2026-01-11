#!/usr/bin/env python3
"""
yellow_scrubber.py (chem pipeline v0.9)

Stage-2 transformations for YELLOW bucket datasets (quarantine -> permissive),
plus record-level allowlist planning for the chemistry domain.

This module imports shared implementation from collector_core.yellow_scrubber_base,
providing chem-specific defaults and user-agent strings.

Not legal advice.
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any

from collector_core.__version__ import __version__ as VERSION
from collector_core.yellow_scrubber_base import (
    SCHEMA_VERSION,
    TOOL_VERSION,
    FieldSpec,
    Pools,
    cast_value,
    detect_delimiter,
    ensure_dir,
    extract_pubchem_computed_only,
    find_column_index,
    iter_sdf_records_from_gz,
    load_field_schemas,
    load_license_map,
    lower,
    normalize_spdx_from_text,
    normalize_whitespace,
    parse_sdf_tags,
    pools_from_targets_yaml,
    read_jsonl,
    read_yaml,
    require_requests,
    resolve_companion_paths,
    restriction_phrase_hits,
    safe_text,
    sha256_file,
    spdx_is_allowed,
    utc_now,
    validate_record,
    write_json,
    write_jsonl_gz,
)
from collector_core.yellow_scrubber_base import (
    download_file as _download_file,
)
from collector_core.yellow_scrubber_base import (
    fetch_text_with_fallback as _fetch_text_with_fallback,
)
from collector_core.yellow_scrubber_base import (
    plan_pmc_allowlist as _plan_pmc_allowlist,
)

# Re-export for backward compatibility
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

# Chem-specific user agent prefix
_USER_AGENT_PREFIX = "chem-corpus-scrubber"


def fetch_text_with_fallback(urls: list[str], timeout_s: int = 30) -> tuple[str, str]:
    """Chem-specific wrapper with appropriate user agent."""
    return _fetch_text_with_fallback(urls, timeout_s, user_agent_prefix=_USER_AGENT_PREFIX)


def download_file(url: str, out_path: Path, timeout_s: int = 60) -> dict[str, Any]:
    """Chem-specific wrapper with appropriate user agent."""
    return _download_file(url, out_path, timeout_s, user_agent_prefix=_USER_AGENT_PREFIX)


def plan_pmc_allowlist(
    license_map: dict[str, Any], out_dir: Path, allowed_spdx: list[str] | None = None
) -> dict[str, Any]:
    """Chem-specific wrapper with appropriate user agent."""
    return _plan_pmc_allowlist(
        license_map, out_dir, allowed_spdx, user_agent_prefix=_USER_AGENT_PREFIX
    )


def main() -> None:
    """CLI entry point for chem yellow scrubber."""
    ap = argparse.ArgumentParser(
        description=f"Yellow Scrubber v{TOOL_VERSION} (schema {SCHEMA_VERSION})"
    )
    ap.add_argument("--targets", required=True, help="targets.yaml v0.6")
    ap.add_argument("--license-map", default=None)
    ap.add_argument("--field-schemas", default=None, help="field_schemas.yaml")
    ap.add_argument("--pools-root", default="/data/chem/pools")
    ap.add_argument("--pubchem-enable", action="store_true")
    ap.add_argument("--pubchem-limit-files", type=int, default=None)
    ap.add_argument("--pubchem-limit-rows", type=int, default=None)
    ap.add_argument("--pubchem-shard-rows", type=int, default=500000)
    ap.add_argument(
        "--pubchem-cid-range", type=int, default=None, help="CID range size for sharding"
    )
    ap.add_argument(
        "--pubchem-validate-schema", action="store_true", help="Enable schema validation"
    )
    ap.add_argument("--pmc-enable", action="store_true")
    args = ap.parse_args()

    targets_path = Path(args.targets).expanduser().resolve()
    targets_cfg = read_yaml(targets_path)
    companion = targets_cfg.get("companion_files", {})

    license_map_value = args.license_map if args.license_map else companion.get("license_map")
    license_map_paths = resolve_companion_paths(
        targets_path, license_map_value, "./license_map.yaml"
    )
    license_map = load_license_map(license_map_paths)

    field_schemas_value = (
        args.field_schemas if args.field_schemas else companion.get("field_schemas")
    )
    field_schemas_paths = resolve_companion_paths(
        targets_path, field_schemas_value, "./field_schemas.yaml"
    )
    field_schemas = load_field_schemas(field_schemas_paths) if field_schemas_paths else {}

    pools = pools_from_targets_yaml(targets_path, Path(args.pools_root).expanduser().resolve())
    target_defs = {
        t["id"]: t for t in targets_cfg.get("targets", []) if isinstance(t, dict) and t.get("id")
    }

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
                "PUBCHEM_COMPOUND_CID",
                "PUBCHEM_CACTVS_CANONICAL_SMILES",
                "PUBCHEM_IUPAC_INCHI",
                "PUBCHEM_IUPAC_INCHIKEY",
                "PUBCHEM_MOLECULAR_FORMULA",
                "PUBCHEM_MOLECULAR_WEIGHT",
            ]

        # Get field schema if validation enabled
        field_schema = None
        if args.pubchem_validate_schema:
            schema_version = derived_def.get("build", {}).get(
                "field_schema_version", "pubchem_computed_only_v1.0.0"
            )
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

    print(f"\n{'=' * 50}\nYellow Scrubber v{TOOL_VERSION} (schema {SCHEMA_VERSION})\n{'=' * 50}")
    print(f"PubChem: {'ran' if run_report['pubchem_ran'] else 'skipped'}")
    print(f"PMC: {'ran' if run_report['pmc_ran'] else 'skipped'}")
    print(f"Report: {out_path}\n{'=' * 50}\n")
    print(json.dumps({"run_report": str(out_path)}, indent=2))


if __name__ == "__main__":
    main()
