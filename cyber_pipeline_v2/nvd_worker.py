"""NVD CVE 2.0 normalization worker.

Parses NVD CVE 2.0 JSON feeds into normalized JSONL rows aligned to
`field_schemas.yaml` (schema: nvd_cve_v2.0.0). Reads a downloaded NVD 2.0
JSON.gz feed and flattens it into a JSONL stream suitable for the catalog.
"""
from __future__ import annotations

import gzip
import json
from collections.abc import Iterable
from pathlib import Path


def load_feed(path: Path) -> dict:
    with gzip.open(path, "rt", encoding="utf-8") as f:
        return json.load(f)


def iter_records(feed: dict) -> Iterable[dict]:
    for item in feed.get("vulnerabilities", []):
        cve = item.get("cve", {})
        metrics = (cve.get("metrics", {}) or {}).get("cvssMetricV31", [])
        metric = metrics[0] if metrics else {}
        yield {
            "cve_id": cve.get("id"),
            "published": cve.get("published"),
            "last_modified": cve.get("lastModified"),
            "cvss_base_score": (metric.get("cvssData") or {}).get("baseScore"),
            "cvss_vector": (metric.get("cvssData") or {}).get("vectorString"),
            "cwes": ",".join([w.get("value", "") for w in cve.get("weaknesses", []) if w.get("value")]),
            "cpe_matches": _flatten_cpes(cve.get("configurations", [])),
            "description": _primary_description(cve),
            "references": _flatten_refs(cve.get("references", [])),
            "known_exploited": False,
        }


def _flatten_cpes(configurations: Iterable[dict]) -> str:
    cpes = []
    for cfg in configurations or []:
        for node in cfg.get("nodes", []):
            for match in node.get("cpeMatch", []):
                uri = match.get("criteria") or match.get("cpe23Uri")
                if uri:
                    cpes.append(uri)
    return "\n".join(sorted(set(cpes)))


def _primary_description(cve: dict) -> str:
    descriptions = cve.get("descriptions", [])
    if not descriptions:
        return ""
    # Prefer English description
    for desc in descriptions:
        if desc.get("lang") == "en":
            return desc.get("value", "")
    return descriptions[0].get("value", "")


def _flatten_refs(refs: Iterable[dict]) -> str:
    urls = []
    for ref in refs or []:
        for url in ref.get("url", "").split():
            if url:
                urls.append(url)
    return "\n".join(sorted(set(urls)))


def write_jsonl_gz(path: Path, rows: Iterable[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def run(feed_path: Path, output_path: Path) -> None:
    feed = load_feed(feed_path)
    write_jsonl_gz(output_path, iter_records(feed))


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Normalize NVD CVE 2.0 feed into JSONL")
    parser.add_argument("--input", required=True, type=Path, help="Path to NVD CVE 2.0 .json.gz feed")
    parser.add_argument("--output", required=True, type=Path, help="Output JSONL.GZ path")
    args = parser.parse_args()
    run(args.input, args.output)
