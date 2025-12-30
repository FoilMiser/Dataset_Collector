#!/usr/bin/env bash
#
# run_pipeline.sh (v2.0)
#
# Wrapper script for the Math Corpus Pipeline v2. It follows the
# docs/PIPELINE_V2_REWORK_PLAN.md stage order.
#
# Usage examples:
#   ./run_pipeline.sh --targets targets_math.yaml --stage classify
#   ./run_pipeline.sh --targets targets_math.yaml --stage acquire_green --execute
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
EXECUTE=""
STAGE="all"
LIMIT_TARGETS=""
LIMIT_FILES=""
WORKERS="4"

usage() {
  cat << 'EOM'
Math Corpus Pipeline v${VERSION}

Required:
  --targets FILE          Path to targets_math.yaml

Options:
  --execute               Perform actions (default is dry-run/plan only)
  --stage STAGE           Stage to run: all, classify, acquire_green, acquire_yellow, \
                          screen_yellow, merge, catalog, review
  --limit-targets N       Limit number of queue rows processed
  --limit-files N         Limit files per target during acquisition
  --workers N             Parallel workers for acquisition (default: 4)
  -h, --help              Show this help
EOM
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
  echo -e "${RED}--targets is required${NC}"
  usage
  exit 1
fi

if [[ ! -f "$TARGETS" ]]; then
  echo -e "${RED}targets file not found: $TARGETS${NC}"
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
QUEUES_ROOT=$(python - << PY
import yaml
cfg = yaml.safe_load(open("${TARGETS}")) or {}
print(cfg.get("globals", {}).get("queues_root", "/data/math/_queues"))
PY
)
CATALOGS_ROOT=$(python - << PY
import yaml
cfg = yaml.safe_load(open("${TARGETS}")) or {}
print(cfg.get("globals", {}).get("catalogs_root", "/data/math/_catalogs"))
PY
)
LIMIT_TARGETS_ARG=""; [[ -n "$LIMIT_TARGETS" ]] && LIMIT_TARGETS_ARG="--limit-targets $LIMIT_TARGETS"
LIMIT_FILES_ARG=""; [[ -n "$LIMIT_FILES" ]] && LIMIT_FILES_ARG="--limit-files $LIMIT_FILES"

run_classify() {
  echo -e "${BLUE}== Stage: classify ==${NC}"
  local no_fetch=""
  if [[ -z "$EXECUTE" ]]; then
    no_fetch="--no-fetch"
  fi
  python "$SCRIPT_DIR/pipeline_driver.py" --targets "$TARGETS" $no_fetch
}

run_review() {
  local queue_file="$QUEUES_ROOT/yellow_pipeline.jsonl"
  echo -e "${BLUE}== Stage: review ==${NC}"
  python "$SCRIPT_DIR/review_queue.py" --queue "$queue_file" list --limit 50 || true
}

run_acquire() {
  local bucket="$1"
  local queue_file="$QUEUES_ROOT/${bucket}_download.jsonl"
  if [[ "$bucket" == "yellow" ]]; then
    queue_file="$QUEUES_ROOT/yellow_pipeline.jsonl"
  fi
  if [[ ! -f "$queue_file" ]]; then
    echo -e "${RED}Queue not found: $queue_file${NC}"
    exit 1
  fi
  echo -e "${BLUE}== Stage: acquire_${bucket} ==${NC}"
  python "$SCRIPT_DIR/acquire_worker.py" \
    --queue "$queue_file" \
    --targets-yaml "$TARGETS" \
    --bucket "$bucket" \
    --workers "$WORKERS" \
    $EXECUTE \
    $LIMIT_TARGETS_ARG \
    $LIMIT_FILES_ARG
}

run_screen_yellow() {
  local queue_file="$QUEUES_ROOT/yellow_pipeline.jsonl"
  if [[ ! -f "$queue_file" ]]; then
    echo -e "${RED}Queue not found: $queue_file${NC}"
    exit 1
  fi
  echo -e "${BLUE}== Stage: screen_yellow ==${NC}"
  python "$SCRIPT_DIR/yellow_screen_worker.py" \
    --targets "$TARGETS" \
    --queue "$queue_file" \
    $EXECUTE
}

run_merge() {
  echo -e "${BLUE}== Stage: merge ==${NC}"
  python "$SCRIPT_DIR/merge_worker.py" --targets "$TARGETS" $EXECUTE
}


run_catalog() {
  echo -e "${BLUE}== Stage: catalog ==${NC}"
  python "$SCRIPT_DIR/catalog_builder.py" --targets "$TARGETS" --output "${CATALOGS_ROOT}/catalog.json"
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
  *) echo -e "${RED}Unknown stage: $STAGE${NC}"; usage; exit 1 ;;
esac
