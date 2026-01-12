#!/usr/bin/env bash
#
# run_pipeline.sh (v2.0)
#
# Deprecated compatibility shim for the agri_circular pipeline.
# Use: dc run --pipeline agri_circular --stage <stage>
# Removal target: v3.0.
#
set -euo pipefail

YELLOW='\033[0;33m'
RED='\033[0;31m'
NC='\033[0m'

TARGETS=""
EXECUTE=""
STAGE=""
EXTRA_ARGS=()

usage() {
  cat << 'EOM'
Deprecated pipeline wrapper (v2)

Required:
  --targets FILE          Path to targets YAML
  --stage STAGE           Stage to run: classify, acquire, yellow_screen, merge, catalog, review

Options:
  --execute               Perform actions (default is dry-run/plan only)
  --                      Pass remaining args directly to the stage command
  -h, --help              Show this help

Notes:
  - This shim no longer resolves queue paths automatically.
  - Provide stage arguments after -- (for example: --queue, --bucket, --targets-yaml).
EOM
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --targets) TARGETS="$2"; shift 2 ;;
    --stage) STAGE="$2"; shift 2 ;;
    --execute) EXECUTE="--execute"; shift ;;
    --) shift; EXTRA_ARGS+=("$@"); break ;;
    -h|--help) usage; exit 0 ;;
    *) EXTRA_ARGS+=("$1"); shift ;;
  esac
done

if [[ -z "$TARGETS" || -z "$STAGE" ]]; then
  echo -e "${RED}--targets and --stage are required${NC}"
  usage
  exit 1
fi

if [[ ! -f "$TARGETS" ]]; then
  echo -e "${RED}targets file not found: $TARGETS${NC}"
  exit 1
fi

echo -e "${YELLOW}[deprecated] run_pipeline.sh is deprecated; use 'dc run --pipeline agri_circular --stage <stage>' instead. Removal target: v3.0.${NC}" >&2

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
export PYTHONPATH="${REPO_ROOT}:${PYTHONPATH:-}"

case "$STAGE" in
  classify)
    NO_FETCH=""
    if [[ -z "$EXECUTE" ]]; then
      NO_FETCH="--no-fetch"
    fi
    python -m collector_core.dc_cli pipeline agri_circular -- --targets "$TARGETS" $NO_FETCH "${EXTRA_ARGS[@]}"
    ;;
  acquire)
    python -m collector_core.dc_cli run --pipeline agri_circular --stage acquire -- --targets-yaml "$TARGETS" $EXECUTE "${EXTRA_ARGS[@]}"
    ;;
  yellow_screen)
    python -m collector_core.dc_cli run --pipeline agri_circular --stage yellow_screen -- --targets "$TARGETS" $EXECUTE "${EXTRA_ARGS[@]}"
    ;;
  merge)
    python -m collector_core.dc_cli run --pipeline agri_circular --stage merge -- --targets "$TARGETS" $EXECUTE "${EXTRA_ARGS[@]}"
    ;;
  catalog)
    python -m collector_core.dc_cli catalog-builder --pipeline agri_circular -- --targets "$TARGETS" "${EXTRA_ARGS[@]}"
    ;;
  review)
    python -m collector_core.dc_cli review-queue --pipeline agri_circular -- --targets "$TARGETS" "${EXTRA_ARGS[@]}"
    ;;
  *)
    echo -e "${RED}Unknown stage: $STAGE${NC}"
    usage
    exit 1
    ;;
esac
