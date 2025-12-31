"""GitHub Advisory Database normalization worker (stub).

Parses GHSA YAML advisories into flattened JSONL rows that align to
`field_schemas.yaml` (schema: github_advisory_v1.0.0).
"""
from __future__ import annotations

import json
import yaml
from pathlib import Path
from typing import Dict, Iterable
import gzip


def load_yaml(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def iter_advisories(root: Path) -> Iterable[dict]:
    for yaml_path in root.rglob("*.yml"):
        advisory = load_yaml(yaml_path)
        affects = advisory.get("affected", {}) or {}
        yield {
            "ghsa_id": advisory.get("id"),
            "package_name": affects.get("package", {}).get("name"),
            "ecosystem": affects.get("package", {}).get("ecosystem"),
            "severity": (advisory.get("database_specific", {}) or {}).get("severity"),
            "summary": advisory.get("summary"),
            "vulnerable_ranges": _join_versions((affects.get("ranges") or [])),
            "patched_versions": _join_versions((affects.get("versions") or [])),
            "references": _join_refs(advisory.get("references", [])),
            "cve_ids": ",".join(advisory.get("aliases", []) or []),
        }


def _join_versions(entries: Iterable[dict]) -> str:
    vals = []
    for entry in entries or []:
        if isinstance(entry, str):
            vals.append(entry)
            continue
        version = entry.get("introduced") or entry.get("fixed") or entry.get("last_affected")
        if version:
            vals.append(version)
    return "\n".join(vals)


def _join_refs(refs: Iterable[dict]) -> str:
    urls = []
    for ref in refs or []:
        url = ref.get("url") or ""
        if url:
            urls.append(url)
    return "\n".join(sorted(set(urls)))


def write_jsonl_gz(path: Path, rows: Iterable[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def run(advisory_root: Path, output_path: Path) -> None:
    write_jsonl_gz(output_path, iter_advisories(advisory_root))


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Normalize GitHub Advisory Database into JSONL")
    parser.add_argument("--input", required=True, type=Path, help="Path to GHSA repo root")
    parser.add_argument("--output", required=True, type=Path, help="Output JSONL.GZ path")
    args = parser.parse_args()
    run(args.input, args.output)
