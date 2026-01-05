#!/usr/bin/env python3
"""
mesh_worker.py (v0.1)

Normalizes 3D assets (STL/OBJ/GLB/GLTF/PLY/STEP), validates meshes,
and emits structured metadata compatible with mesh_record_v1.0.0.

Features:
- Loads meshes with trimesh (required), optional thumbnail rendering and point clouds.
- Computes vertex/face counts, bbox, surface area, approximate volume (if watertight).
- Exports normalized meshes (triangulated) to output_dir, preserving relative paths.
- Emits JSONL metadata; optional Parquet when pyarrow is installed.

Not legal advice.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from pathlib import Path
from typing import Any

from collector_core.dependencies import _try_import, requires

np = _try_import("numpy")
trimesh = _try_import("trimesh")
Image = _try_import("PIL.Image")
pa = _try_import("pyarrow")
pq = _try_import("pyarrow.parquet")


ALLOWED_EXTS = {".stl", ".obj", ".ply", ".glb", ".gltf", ".step", ".stp"}


def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def load_mesh(path: Path) -> trimesh.Trimesh | None:
    missing = requires("trimesh", trimesh, install="pip install trimesh")
    if missing:
        raise RuntimeError(missing)
    try:
        return trimesh.load(path, force="mesh")
    except Exception:
        return None


def render_thumbnail(mesh: trimesh.Trimesh, out_path: Path) -> Path | None:
    if Image is None:
        return None
    try:
        scene = mesh.scene()
        png = scene.save_image(resolution=(512, 512), visible=True)
        ensure_dir(out_path.parent)
        with out_path.open("wb") as f:
            f.write(png)
        return out_path
    except Exception:
        return None


def sample_point_cloud(mesh: trimesh.Trimesh, n_points: int) -> np.ndarray | None:
    try:
        pts, _ = trimesh.sample.sample_surface(mesh, n_points)
        return pts
    except Exception:
        return None


def export_mesh(mesh: trimesh.Trimesh, dest: Path) -> Path | None:
    ensure_dir(dest.parent)
    try:
        mesh.export(dest)
        return dest
    except Exception:
        return None


def compute_metadata(
    source_id: str,
    src_path: Path,
    norm_path: Path,
    mesh: trimesh.Trimesh,
    sha_original: str,
    sha_norm: str | None,
    license_spdx: str,
    attribution: str | None,
    creator: str | None,
    thumbnail: Path | None,
    point_cloud_path: Path | None,
) -> dict[str, Any]:
    bounds = mesh.bounds if mesh.bounds is not None else None
    bbox_min = ",".join(map(str, bounds[0].tolist())) if bounds is not None else None
    bbox_max = ",".join(map(str, bounds[1].tolist())) if bounds is not None else None
    surface_area = float(mesh.area) if mesh.area is not None else None
    volume = float(mesh.volume) if mesh.is_volume else None

    return {
        "source_id": source_id,
        "record_id": src_path.stem,
        "source_url": "",
        "license_spdx": license_spdx,
        "license_url": "",
        "attribution_text": attribution,
        "creator": creator,
        "original_format": src_path.suffix.lower().lstrip("."),
        "normalized_format": norm_path.suffix.lower().lstrip("."),
        "vertex_count": int(mesh.vertices.shape[0]) if mesh.vertices is not None else None,
        "face_count": int(mesh.faces.shape[0]) if mesh.faces is not None else None,
        "bbox_min": bbox_min,
        "bbox_max": bbox_max,
        "surface_area": surface_area,
        "volume_closed": volume,
        "unit_guess": "meter",
        "scale_applied": 1.0,
        "sha256_original": sha_original,
        "sha256_normalized": sha_norm,
        "has_texture": bool(mesh.visual.kind == "texture") if mesh.visual else None,
        "thumbnail_path": str(thumbnail) if thumbnail else None,
        "point_cloud_path": str(point_cloud_path) if point_cloud_path else None,
        "tags": "",
    }


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def write_parquet(path: Path, rows: list[dict[str, Any]]) -> None:
    missing = requires("pyarrow", pa, install="pip install pyarrow")
    if missing or pq is None:
        raise RuntimeError(missing or "missing dependency: pyarrow.parquet (install: pip install pyarrow)")
    ensure_dir(path.parent)
    table = pa.Table.from_pylist(rows)
    pq.write_table(table, path)


def process_file(
    path: Path,
    output_root: Path,
    source_id: str,
    license_spdx: str,
    attribution: str | None,
    creator: str | None,
    generate_thumbnails: bool,
    generate_point_clouds: bool,
) -> tuple[dict[str, Any], dict[str, Any]]:
    sha_original = sha256_file(path)
    mesh = load_mesh(path)
    if mesh is None:
        return {}, {"status": "error", "path": str(path), "error": "unparsable_mesh"}

    mesh = mesh.as_triangles()
    relative = path.relative_to(path.parents[1]) if len(path.parents) > 1 else Path(path.name)
    norm_path = output_root / "normalized" / relative.with_suffix(".glb")
    exported = export_mesh(mesh, norm_path)
    sha_norm = sha256_file(norm_path) if exported and norm_path.exists() else None

    thumb_path = None
    if generate_thumbnails:
        thumb_path = render_thumbnail(mesh, output_root / "thumbnails" / relative.with_suffix(".png"))

    pc_path = None
    if generate_point_clouds:
        pts = sample_point_cloud(mesh, 2048)
        if pts is not None:
            pc_path = output_root / "point_clouds" / relative.with_suffix(".npy")
            ensure_dir(pc_path.parent)
            np.save(pc_path, pts)

    metadata = compute_metadata(
        source_id,
        path,
        norm_path if exported else path,
        mesh,
        sha_original,
        sha_norm,
        license_spdx,
        attribution,
        creator,
        thumb_path,
        pc_path,
    )
    return metadata, {"status": "ok", "path": str(path), "normalized_path": str(norm_path)}


def discover_files(root: Path) -> list[Path]:
    paths: list[Path] = []
    for p in root.rglob("*"):
        if p.suffix.lower() in ALLOWED_EXTS and p.is_file():
            paths.append(p)
    return paths


def main() -> None:
    ap = argparse.ArgumentParser(description="Mesh worker for 3D corpus (v0.1)")
    ap.add_argument("--targets", required=False, help="targets.yaml (optional; used for defaults)")
    ap.add_argument("--target-id", required=True, help="Target ID (for source_id field)")
    ap.add_argument("--input-dir", required=True, help="Directory with downloaded meshes")
    ap.add_argument("--output-dir", required=True, help="Directory for normalized meshes/metadata")
    ap.add_argument("--license-spdx", default="CC0-1.0", help="License to apply to outputs")
    ap.add_argument("--attribution", default=None, help="Attribution text if required")
    ap.add_argument("--creator", default=None, help="Creator/author if available")
    ap.add_argument("--emit-parquet", action="store_true", help="Emit parquet alongside JSONL (requires pyarrow)")
    ap.add_argument("--generate-thumbnails", action="store_true", help="Render thumbnails (requires PIL)")
    ap.add_argument("--generate-point-clouds", action="store_true", help="Emit sampled point clouds (requires numpy)")
    args = ap.parse_args()

    if trimesh is None or np is None:
        sys.exit("trimesh and numpy are required. Install with: pip install trimesh numpy")

    input_dir = Path(args.input_dir).expanduser().resolve()
    output_dir = Path(args.output_dir).expanduser().resolve()
    ensure_dir(output_dir)

    files = discover_files(input_dir)
    if not files:
        sys.exit("No mesh files found in input directory.")

    metadata_rows: list[dict[str, Any]] = []
    results: list[dict[str, Any]] = []

    for path in files:
        md, res = process_file(
            path,
            output_dir,
            args.target_id,
            args.license_spdx,
            args.attribution,
            args.creator,
            args.generate_thumbnails,
            args.generate_point_clouds,
        )
        if md:
            metadata_rows.append(md)
        results.append(res)

    jsonl_path = output_dir / "mesh_records.jsonl"
    write_jsonl(jsonl_path, metadata_rows)
    if args.emit_parquet:
        try:
            write_parquet(output_dir / "mesh_records.parquet", metadata_rows)
        except Exception as e:
            print(f"Parquet export failed: {e}", file=sys.stderr)

    manifest = {
        "target_id": args.target_id,
        "input_dir": str(input_dir),
        "output_dir": str(output_dir),
        "generated_at": utc_now(),
        "records": len(metadata_rows),
        "results": results,
    }
    (output_dir / "_manifests").mkdir(parents=True, exist_ok=True)
    (output_dir / "_manifests" / "mesh_worker_manifest.json").write_text(
        json.dumps(manifest, indent=2), encoding="utf-8"
    )
    print(json.dumps({"records": len(metadata_rows), "jsonl": str(jsonl_path)}, indent=2))


if __name__ == "__main__":
    main()
