"""STIX bundle normalization worker.

Convert STIX 2.x bundles into:
- text summaries of objects and relationships for training corpora
- optional graph JSONL (nodes/edges) for downstream graph-aware models

Integrate this worker in the yellow pipeline once ready; keep raw bundles for
auditability.
"""
from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, List
import json
import gzip


def load_bundle(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def summarize_objects(bundle: dict) -> Iterable[dict]:
    """Yield simplified STIX object summaries suitable for text corpora."""
    for obj in bundle.get("objects", []):
        yield {
            "stix_id": obj.get("id"),
            "object_type": obj.get("type"),
            "name": obj.get("name"),
            "description": obj.get("description"),
            "relationships": _stringify_relationships(bundle, obj.get("id")),
            "external_references": _stringify_references(obj.get("external_references", [])),
        }


def _stringify_relationships(bundle: dict, source_id: str | None) -> str:
    rels: list[str] = []
    if not source_id:
        return ""
    for rel in bundle.get("objects", []):
        if rel.get("type") != "relationship":
            continue
        if rel.get("source_ref") == source_id:
            rels.append(f"{source_id} -> {rel.get('target_ref')} ({rel.get('relationship_type')})")
    return "\n".join(rels)


def _stringify_references(refs: list[dict]) -> str:
    lines = []
    for ref in refs or []:
        name = ref.get("source_name") or ""
        url = ref.get("url") or ""
        if name or url:
            lines.append(f"{name}: {url}".strip())
    return "\n".join(lines)


def write_jsonl_gz(path: Path, rows: Iterable[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def run(input_bundle: Path, output_path: Path) -> None:
    bundle = load_bundle(input_bundle)
    summaries = summarize_objects(bundle)
    write_jsonl_gz(output_path, summaries)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Summarize STIX 2.x bundles into JSONL")
    parser.add_argument("--input", required=True, type=Path, help="Path to STIX bundle JSON")
    parser.add_argument("--output", required=True, type=Path, help="Output JSONL.GZ path")
    args = parser.parse_args()
    run(args.input, args.output)
