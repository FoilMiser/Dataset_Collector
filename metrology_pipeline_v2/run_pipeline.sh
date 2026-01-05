#!/usr/bin/env bash
#
# run_pipeline.sh (v2.0)
#
# Wrapper script for the Metrology Corpus Pipeline v2. It follows the
# docs/PIPELINE_V2_REWORK_PLAN.md stage order.
#
# Usage examples:
#   ./run_pipeline.sh --targets targets_metrology.yaml --stage classify
#   ./run_pipeline.sh --targets targets_metrology.yaml --stage acquire_green --execute
#
set -euo pipefail

# Interpreter: python

VERSION="2.0"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

TARGETS=""
STAGE="all"
EXECUTE=""
LIMIT_TARGETS=""
LIMIT_FILES=""
WORKERS="4"

usage() {
  cat << EOF
Metrology Corpus Pipeline v${VERSION}

Required:
  --targets FILE          Path to targets_metrology.yaml

Options:
  --execute               Perform actions (default is dry-run/plan only)
  --stage STAGE           Stage to run: all, classify, acquire_green, acquire_yellow, \
                          screen_yellow, merge, catalog, review
  --limit-targets N       Limit number of queue rows processed
  --limit-files N         Limit files per target during acquisition
  --workers N             Parallel workers for acquisition (default: 4)
  -h, --help              Show this help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --targets) TARGETS="$2"; shift 2 ;;
    --stage) STAGE="$2"; shift 2 ;;
    --execute) EXECUTE="--execute"; shift ;;
    --limit-targets) LIMIT_TARGETS="$2"; shift 2 ;;
    --limit-files) LIMIT_FILES="$2"; shift 2 ;;
    --workers) WORKERS="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo -e "${RED}Unknown option: $1${NC}"; usage; exit 1 ;;
  esac
done

if [[ -z "$TARGETS" ]]; then
  echo -e "${RED}Error: --targets is required${NC}"
  usage
  exit 1
fi
if [[ ! -f "$TARGETS" ]]; then
  echo -e "${RED}Error: targets file not found: $TARGETS${NC}"
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export PYTHONPATH="${SCRIPT_DIR}/..:${PYTHONPATH:-}"

QUEUES_ROOT="$(python - <<'PY'
import sys
from pathlib import Path
from collector_core.config_validator import read_yaml
cfg = read_yaml(Path(sys.argv[1]), schema_name="targets") or {}
print((cfg.get('globals', {}) or {}).get('queues_root', '/data/metrology/_queues'))
PY
"$TARGETS")"

LIMIT_TARGETS_ARG=""; [[ -n "$LIMIT_TARGETS" ]] && LIMIT_TARGETS_ARG="--limit-targets $LIMIT_TARGETS"
LIMIT_FILES_ARG=""; [[ -n "$LIMIT_FILES" ]] && LIMIT_FILES_ARG="--limit-files $LIMIT_FILES"

echo -e "${BLUE}Metrology Corpus Pipeline v${VERSION}${NC}"
echo -e "Targets: ${GREEN}${TARGETS}${NC}"
echo -e "Stage:   ${GREEN}${STAGE}${NC}"
echo -e "Mode:    ${GREEN}$([[ -n \"$EXECUTE\" ]] && echo EXECUTE || echo DRY-RUN)${NC}"
echo ""

run_classify() {
  echo -e "${YELLOW}[classify]${NC} Emitting queues..."
  python "${SCRIPT_DIR}/pipeline_driver.py" --targets "$TARGETS"
  echo -e "${GREEN}[classify] done${NC}"
}

run_review() {
  local queue="${QUEUES_ROOT}/yellow_pipeline.jsonl"
  echo -e "${BLUE}[review]${NC} Showing first 50 YELLOW queue rows from ${queue}"
  python "${SCRIPT_DIR}/review_queue.py" --queue "${queue}" list --limit 50 || true
}

run_acquire() {
  local bucket="$1"
  local queue_file=""
  case "$bucket" in
    green) queue_file="green_download.jsonl" ;;
    yellow) queue_file="yellow_pipeline.jsonl" ;;
    *) queue_file="${bucket}.jsonl" ;;
  esac
  local queue="${QUEUES_ROOT}/${queue_file}"
  if [[ ! -f "$queue" ]]; then
    echo -e "${RED}[acquire_${bucket}] queue not found: ${queue}${NC}"
    exit 1
  fi
  echo -e "${YELLOW}[acquire_${bucket}]${NC} Processing queue ${queue}"
  python "${SCRIPT_DIR}/acquire_worker.py" \
    --queue "$queue" \
    --targets-yaml "$TARGETS" \
    --bucket "$bucket" \
    --workers "$WORKERS" \
    $LIMIT_TARGETS_ARG \
    $LIMIT_FILES_ARG \
    $EXECUTE
  echo -e "${GREEN}[acquire_${bucket}] done${NC}"
}

run_screen_yellow() {
  local queue="${QUEUES_ROOT}/yellow_pipeline.jsonl"
  if [[ ! -f "$queue" ]]; then
    echo -e "${RED}[screen_yellow] queue not found: ${queue}${NC}"
    exit 1
  fi
  echo -e "${YELLOW}[screen_yellow]${NC} Screening YELLOW payloads"
  python "${SCRIPT_DIR}/yellow_screen_worker.py" \
    --targets "$TARGETS" \
    --queue "$queue" \
    $EXECUTE
  echo -e "${GREEN}[screen_yellow] done${NC}"
}

run_merge() {
  echo -e "${YELLOW}[merge]${NC} Combining GREEN + screened YELLOW shards"
  python "${SCRIPT_DIR}/merge_worker.py" --targets "$TARGETS" $EXECUTE
  echo -e "${GREEN}[merge] done${NC}"
}


run_catalog() {
  echo -e "${YELLOW}[catalog]${NC} Building global catalog"
  python "${SCRIPT_DIR}/catalog_builder.py" --targets "$TARGETS"
  echo -e "${GREEN}[catalog] done${NC}"
}

case "$STAGE" in
  all)
    run_classify
    run_acquire green
    run_acquire yellow
    run_screen_yellow
    run_merge
    run_catalog
    ;;
  classify) run_classify ;;
  acquire_green) run_acquire green ;;
  acquire_yellow) run_acquire yellow ;;
  screen_yellow) run_screen_yellow ;;
  merge) run_merge ;;
  catalog) run_catalog ;;
  review) run_review ;;
  *) echo -e "${RED}Unknown stage: ${STAGE}${NC}"; usage; exit 1 ;;
esac

echo -e "${BLUE}Pipeline complete.${NC}"
