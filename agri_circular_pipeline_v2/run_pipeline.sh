#!/usr/bin/env bash
#
# run_pipeline.sh (v2.0)
#
# Wrapper script for the Agri Circular Pipeline v2. Follows the staged layout
# documented in agri_circular_pipeline_v2_adaptation.md.
#
# Usage examples:
#   ./run_pipeline.sh --targets targets_agri_circular.yaml --stage classify
#   ./run_pipeline.sh --targets targets_agri_circular.yaml --stage acquire_green --execute
#   ./run_pipeline.sh --targets targets_agri_circular.yaml --stage all --execute --workers 4
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
Agri Circular Pipeline v${VERSION}

Required:
  --targets FILE          Path to targets_agri_circular.yaml

Options:
  --execute               Perform actions (default is dry-run/plan only)
  --stage STAGE           Stage to run: all, classify, acquire_green, acquire_yellow, \
                          screen_yellow, merge, difficulty, catalog, review
  --limit-targets N       Limit number of queue rows processed (acquire stages)
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

queue_root() {
  python - << PY
import yaml, pathlib
cfg = yaml.safe_load(pathlib.Path("$TARGETS").read_text()) or {}
print((cfg.get("globals", {}) or {}).get("queues_root", "/data/agri_circular/_queues"))
PY
}

catalog_root() {
  python - << PY
import yaml, pathlib
cfg = yaml.safe_load(pathlib.Path("$TARGETS").read_text()) or {}
print((cfg.get("globals", {}) or {}).get("catalogs_root", "/data/agri_circular/_catalogs"))
PY
}

print_header() {
  echo -e "${BLUE}======================================${NC}"
  echo -e "${BLUE}Agri Circular Pipeline v${VERSION}${NC}"
  echo -e "${BLUE}======================================${NC}"
  echo ""
  echo -e "Targets:  ${GREEN}$TARGETS${NC}"
  echo -e "Stage:    ${GREEN}$STAGE${NC}"
  echo -e "Mode:     ${GREEN}$([ -n "$EXECUTE" ] && echo "EXECUTE" || echo "DRY-RUN")${NC}"
  echo ""
}

run_classify() {
  echo -e "${YELLOW}[Stage: classify]${NC} Generating queues and license evidence..."
  python "$SCRIPT_DIR/pipeline_driver.py" \
    --targets "$TARGETS" \
    $([ -z "$EXECUTE" ] && echo "--no-fetch")
  echo -e "${GREEN}[Stage: classify] Complete${NC}\n"
}

run_review() {
  echo -e "${YELLOW}[Stage: review]${NC} Listing pending YELLOW items..."
  local queues_root
  queues_root=$(queue_root)
  python "$SCRIPT_DIR/review_queue.py" --queue "${queues_root}/yellow_pipeline.jsonl" list --limit 50 || true
  echo ""
}

run_acquire() {
  local bucket="$1"
  local queue_file
  queue_file="$(queue_root)/${bucket}_pipeline.jsonl"
  if [[ "$bucket" == "green" ]]; then
    queue_file="$(queue_root)/green_download.jsonl"
  fi
  if [[ ! -f "$queue_file" ]]; then
    echo -e "${RED}Queue file not found: $queue_file${NC}"
    echo "Run classify stage first."
    return 1
  fi
  echo -e "${YELLOW}[Stage: acquire_${bucket}]${NC} Processing queue ${queue_file}..."
  args=("$SCRIPT_DIR/acquire_worker.py" --queue "$queue_file" --bucket "$bucket" --targets-yaml "$TARGETS" --workers "$WORKERS")
  [[ -n "$EXECUTE" ]] && args+=("--execute")
  [[ -n "$LIMIT_TARGETS" ]] && args+=(--limit-targets "$LIMIT_TARGETS")
  [[ -n "$LIMIT_FILES" ]] && args+=(--limit-files "$LIMIT_FILES")
  python "${args[@]}"
  echo -e "${GREEN}[Stage: acquire_${bucket}] Complete${NC}\n"
}

run_screen_yellow() {
  local queue_file
  queue_file="$(queue_root)/yellow_pipeline.jsonl"
  if [[ ! -f "$queue_file" ]]; then
    echo -e "${RED}Queue file not found: $queue_file${NC}"
    echo "Run classify stage first."
    return 1
  fi
  echo -e "${YELLOW}[Stage: screen_yellow]${NC} Screening YELLOW acquisitions..."
  python "$SCRIPT_DIR/yellow_screen_worker.py" --targets "$TARGETS" --queue "$queue_file" ${EXECUTE}
  echo -e "${GREEN}[Stage: screen_yellow] Complete${NC}\n"
}

run_merge() {
  echo -e "${YELLOW}[Stage: merge]${NC} Merging GREEN + screened YELLOW..."
  python "$SCRIPT_DIR/merge_worker.py" --targets "$TARGETS" ${EXECUTE}
  echo -e "${GREEN}[Stage: merge] Complete${NC}\n"
}

run_difficulty() {
  echo -e "${YELLOW}[Stage: difficulty]${NC} Assigning difficulty and final shards..."
  python "$SCRIPT_DIR/difficulty_worker.py" --targets "$TARGETS" ${EXECUTE}
  echo -e "${GREEN}[Stage: difficulty] Complete${NC}\n"
}

run_catalog() {
  echo -e "${YELLOW}[Stage: catalog]${NC} Building catalog..."
  local catalogs_root
  catalogs_root=$(catalog_root)
  python "$SCRIPT_DIR/catalog_builder.py" --targets "$TARGETS" --output "${catalogs_root}/catalog_v2.json"
  echo -e "${GREEN}[Stage: catalog] Complete${NC}\n"
}

print_header

case "$STAGE" in
  all)
    run_classify
    run_acquire green
    run_acquire yellow
    run_screen_yellow
    run_merge
    run_difficulty
    run_catalog
    ;;
  classify)
    run_classify
    ;;
  acquire_green)
    run_acquire green
    ;;
  acquire_yellow)
    run_acquire yellow
    ;;
  screen_yellow)
    run_screen_yellow
    ;;
  merge)
    run_merge
    ;;
  difficulty)
    run_difficulty
    ;;
  catalog)
    run_catalog
    ;;
  review)
    run_review
    ;;
  *)
    echo -e "${RED}Unknown stage: ${STAGE}${NC}"
    usage
    exit 1
    ;;
esac
