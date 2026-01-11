#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
export PYTHONPATH="${REPO_ROOT}/src"

WORKDIR="$(mktemp -d)"
DATASET_ROOT="${WORKDIR}/dataset_root"
FIXTURES_DIR="${REPO_ROOT}/src/tools/fixtures"
TEMP_FIXTURES="${WORKDIR}/fixtures"

cleanup() {
  rm -rf "${WORKDIR}"
}
trap cleanup EXIT

mkdir -p "${TEMP_FIXTURES}"
cp "${FIXTURES_DIR}/targets_minimal.yaml" "${TEMP_FIXTURES}/targets_minimal.yaml"
cp "${FIXTURES_DIR}/license_map_minimal.yaml" "${TEMP_FIXTURES}/license_map_minimal.yaml"
cp "${FIXTURES_DIR}/denylist_minimal.yaml" "${TEMP_FIXTURES}/denylist_minimal.yaml"

python -m tools.patch_targets \
  --targets "${TEMP_FIXTURES}/targets_minimal.yaml" \
  --dataset-root "${DATASET_ROOT}" \
  --output "${TEMP_FIXTURES}/targets_minimal_patched.yaml"

python regcomp_pipeline_v2/pipeline_driver.py \
  --targets "${TEMP_FIXTURES}/targets_minimal_patched.yaml" \
  --license-map "${TEMP_FIXTURES}/license_map_minimal.yaml" \
  --no-fetch \
  --quiet

python regcomp_pipeline_v2/acquire_worker.py \
  --queue "${DATASET_ROOT}/_queues/green_download.jsonl" \
  --targets-yaml "${TEMP_FIXTURES}/targets_minimal_patched.yaml" \
  --bucket green \
  --raw-root "${DATASET_ROOT}/raw" \
  --manifests-root "${DATASET_ROOT}/_manifests" \
  --logs-root "${DATASET_ROOT}/_logs" \
  --limit-targets 1 \
  --workers 1

python regcomp_pipeline_v2/acquire_worker.py \
  --queue "${DATASET_ROOT}/_queues/yellow_pipeline.jsonl" \
  --targets-yaml "${TEMP_FIXTURES}/targets_minimal_patched.yaml" \
  --bucket yellow \
  --raw-root "${DATASET_ROOT}/raw" \
  --manifests-root "${DATASET_ROOT}/_manifests" \
  --logs-root "${DATASET_ROOT}/_logs" \
  --limit-targets 1 \
  --workers 1

python regcomp_pipeline_v2/merge_worker.py \
  --targets "${TEMP_FIXTURES}/targets_minimal_patched.yaml" \
  --dataset-root "${DATASET_ROOT}"

python -m collector_core.pipeline_cli \
  --pipeline-id regcomp \
  catalog-builder \
  --targets "${TEMP_FIXTURES}/targets_minimal_patched.yaml" \
  --output "${DATASET_ROOT}/_catalogs/catalog.json"
