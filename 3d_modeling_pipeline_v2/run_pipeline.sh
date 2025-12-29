#!/usr/bin/env bash
#
# run_pipeline.sh (v2.0)
#
# Wrapper script for the 3D Modeling Pipeline v2. It follows the
# 3D_MODELING_PIPELINE_V1_TO_V2_PLAN.md stage order.
#
# Usage examples:
#   ./run_pipeline.sh --targets targets_3d.yaml --stage classify
#   ./run_pipeline.sh --targets targets_3d.yaml --stage acquire_green --execute
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
  cat << EOF
3D Modeling Pipeline v${VERSION}

Required:
  --targets FILE          Path to targets_3d.yaml

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
  case $1 in
    --targets)
      TARGETS="$2"
      shift 2
      ;;
    --execute)
      EXECUTE="--execute"
      shift
      ;;
    --stage)
      STAGE="$2"
      shift 2
      ;;
    --limit-targets)
      LIMIT_TARGETS="$2"
      shift 2
      ;;
    --limit-files)
      LIMIT_FILES="$2"
      shift 2
      ;;
    --workers)
      WORKERS="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo -e "${RED}Unknown option: $1${NC}"
      usage
      exit 1
      ;;
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
QUEUES_ROOT=$(python -c "import yaml; print(yaml.safe_load(open('$TARGETS'))['globals'].get('queues_root', '/data/3d/_queues'))")
CATALOGS_ROOT=$(python -c "import yaml; print(yaml.safe_load(open('$TARGETS'))['globals'].get('catalogs_root', '/data/3d/_catalogs'))")
LIMIT_TARGETS_ARG=""; [[ -n "$LIMIT_TARGETS" ]] && LIMIT_TARGETS_ARG="--limit-targets $LIMIT_TARGETS"
LIMIT_FILES_ARG=""; [[ -n "$LIMIT_FILES" ]] && LIMIT_FILES_ARG="--limit-files $LIMIT_FILES"

echo -e "${BLUE}======================================${NC}"
echo -e "${BLUE}3D Modeling Pipeline v${VERSION}${NC}"
echo -e "${BLUE}======================================${NC}"
echo ""
echo -e "Targets:  ${GREEN}$TARGETS${NC}"
echo -e "Stage:    ${GREEN}$STAGE${NC}"
echo -e "Mode:     ${GREEN}$([ -n "$EXECUTE" ] && echo "EXECUTE" || echo "DRY-RUN")${NC}"
echo ""

run_classify() {
  echo -e "${YELLOW}[Stage: Classify]${NC} Generating queues and evidence snapshots..."
  python "$SCRIPT_DIR/pipeline_driver.py" \
    --targets "$TARGETS" \
    $( [ -n "$EXECUTE" ] || echo "--no-fetch" )
  echo -e "${GREEN}[Stage: Classify] Complete${NC}"
  echo ""
}

run_acquire() {
  local bucket="$1"
  echo -e "${YELLOW}[Stage: Acquire ${bucket}]${NC} Downloading targets..."
  local queue_file="${QUEUES_ROOT}/${bucket}_download.jsonl"
  if [[ "$bucket" == "yellow" ]]; then
    queue_file="${QUEUES_ROOT}/yellow_pipeline.jsonl"
  fi
  if [[ ! -f "$queue_file" ]]; then
    echo -e "${RED}Queue file not found: $queue_file${NC}"
    echo "Run classify stage first."
    return 1
  fi
  python "$SCRIPT_DIR/acquire_worker.py" \
    --queue "$queue_file" \
    --bucket "$bucket" \
    --targets-yaml "$TARGETS" \
    --workers "$WORKERS" \
    --verify-sha256 \
    $EXECUTE \
    $LIMIT_TARGETS_ARG \
    $LIMIT_FILES_ARG
  echo -e "${GREEN}[Stage: Acquire ${bucket}] Complete${NC}"
  echo ""
}

run_screen_yellow() {
  echo -e "${YELLOW}[Stage: Screen Yellow]${NC} Screening YELLOW targets..."
  local queue_file="${QUEUES_ROOT}/yellow_pipeline.jsonl"
  if [[ ! -f "$queue_file" ]]; then
    echo -e "${RED}Queue file not found: $queue_file${NC}"
    echo "Run classify stage first."
    return 1
  fi
  python "$SCRIPT_DIR/yellow_screen_worker.py" \
    --targets "$TARGETS" \
    --queue "$queue_file" \
    $EXECUTE
  echo -e "${GREEN}[Stage: Screen Yellow] Complete${NC}"
  echo ""
}

run_merge() {
  echo -e "${YELLOW}[Stage: Merge]${NC} Merging GREEN + screened YELLOW..."
  python "$SCRIPT_DIR/merge_worker.py" \
    --targets "$TARGETS" \
    $EXECUTE
  echo -e "${GREEN}[Stage: Merge] Complete${NC}"
  echo ""
}


run_catalog() {
  echo -e "${YELLOW}[Stage: Catalog]${NC} Building global catalog..."
  python "$SCRIPT_DIR/catalog_builder.py" \
    --targets "$TARGETS" \
    --output "$CATALOGS_ROOT/global_catalog.json"
  echo -e "${GREEN}[Stage: Catalog] Complete${NC}"
  echo ""
}

run_review() {
  local queue_file="${QUEUES_ROOT}/yellow_pipeline.jsonl"
  echo -e "${YELLOW}[Stage: Review]${NC} Listing YELLOW queue..."
  python "$SCRIPT_DIR/review_queue.py" --queue "$queue_file" list --limit 50 || true
  echo ""
}

case "$STAGE" in
  classify)
    run_classify
    ;;
  acquire_green)
    run_acquire "green"
    ;;
  acquire_yellow)
    run_acquire "yellow"
    ;;
  screen_yellow)
    run_screen_yellow
    ;;
  merge)
    run_merge
    ;;
  catalog)
    run_catalog
    ;;
  review)
    run_review
    ;;
  all)
    run_classify
    run_acquire "green"
    run_acquire "yellow"
    run_screen_yellow
    run_merge
    run_catalog
    ;;
  *)
    echo -e "${RED}Unknown stage: $STAGE${NC}"
    usage
    exit 1
    ;;
esac
