#!/usr/bin/env bash
#
# run_pipeline.sh (v2.0)
#
# Wrapper script for the Cyber Corpus Pipeline v2. It follows the
# v2 stage order used by math_pipeline_v2.
#
# Usage examples:
#   ./run_pipeline.sh --targets targets_cyber.yaml --stage classify
#   ./run_pipeline.sh --targets targets_cyber.yaml --stage acquire_green --execute
#   ./run_pipeline.sh --targets targets_cyber.yaml --stage difficulty --execute
#
set -euo pipefail

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
Cyber Corpus Pipeline v${VERSION}

Required:
  --targets FILE          Path to targets_cyber.yaml

Options:
  --execute               Perform actions (default is dry-run/plan only)
  --stage STAGE           Stage to run: all, classify, review, acquire_green, acquire_yellow, \
                          screen_yellow, merge, difficulty, catalog
  --limit-targets N       Limit number of queue rows processed
  --limit-files N         Limit files per target during acquisition
  --workers N             Parallel workers for acquisition (default: 4)
  -h, --help              Show this help
EOF
}

# Parse arguments
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
      LIMIT_TARGETS="--limit-targets $2"
      shift 2
      ;;
    --limit-files)
      LIMIT_FILES="--limit-files $2"
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

cfg_value() {
  local key="$1"
  local default="$2"
  python - "$TARGETS" "$key" "$default" <<'PY'
import sys, yaml
path = sys.argv[1]
key = sys.argv[2]
default = sys.argv[3]
cfg = yaml.safe_load(open(path, "r", encoding="utf-8")) or {}
print((cfg.get("globals", {}) or {}).get(key, default))
PY
}

queues_root=$(cfg_value "queues_root" "/data/cyber/_queues")
catalogs_root=$(cfg_value "catalogs_root" "/data/cyber/_catalogs")

echo -e "${BLUE}Cyber Corpus Pipeline v${VERSION}${NC}"
echo -e "Targets: ${GREEN}${TARGETS}${NC}"
echo -e "Stage:   ${GREEN}${STAGE}${NC}"
echo -e "Mode:    ${GREEN}$([ -n "${EXECUTE}" ] && echo "EXECUTE" || echo "DRY-RUN")${NC}"
echo ""

run_classify() {
  echo -e "${YELLOW}[Stage: classify]${NC}"
  python "${SCRIPT_DIR}/pipeline_driver.py" --targets "${TARGETS}" $([ -z "${EXECUTE}" ] && echo "--no-fetch")
}

run_review() {
  echo -e "${YELLOW}[Stage: review]${NC}"
  python "${SCRIPT_DIR}/yellow_scrubber.py" --targets "${TARGETS}" --limit 50 || true
}

run_acquire() {
  local bucket="$1"
  local queue_file="${queues_root}/${bucket}_download.jsonl"
  if [[ ! -f "${queue_file}" ]]; then
    echo -e "${RED}Queue file not found: ${queue_file}${NC}"
    echo "Run classify first."
    return 1
  fi
  echo -e "${YELLOW}[Stage: acquire_${bucket}]${NC}"
  python "${SCRIPT_DIR}/acquire_worker.py" \
    --queue "${queue_file}" \
    --targets-yaml "${TARGETS}" \
    --bucket "${bucket}" \
    --workers "${WORKERS}" \
    ${EXECUTE} ${LIMIT_TARGETS} ${LIMIT_FILES}
}

run_screen_yellow() {
  local queue_file="${queues_root}/yellow_pipeline.jsonl"
  if [[ ! -f "${queue_file}" ]]; then
    echo -e "${RED}YELLOW queue not found: ${queue_file}${NC}"
    echo "Run classify first."
    return 1
  fi
  echo -e "${YELLOW}[Stage: screen_yellow]${NC}"
  python "${SCRIPT_DIR}/yellow_screen_worker.py" \
    --targets "${TARGETS}" \
    --queue "${queue_file}" \
    ${EXECUTE}
}

run_merge() {
  echo -e "${YELLOW}[Stage: merge]${NC}"
  python "${SCRIPT_DIR}/merge_worker.py" --targets "${TARGETS}" ${EXECUTE}
}

run_difficulty() {
  echo -e "${YELLOW}[Stage: difficulty]${NC}"
  python "${SCRIPT_DIR}/difficulty_worker.py" --targets "${TARGETS}" ${EXECUTE}
}

run_catalog() {
  echo -e "${YELLOW}[Stage: catalog]${NC}"
  mkdir -p "${catalogs_root}"
  python "${SCRIPT_DIR}/catalog_builder.py" \
    --targets "${TARGETS}" \
    --output "${catalogs_root}/global_catalog.json"
}

case "${STAGE}" in
  all)
    run_classify
    run_review
    run_acquire "green"
    run_acquire "yellow"
    run_screen_yellow
    run_merge
    run_difficulty
    run_catalog
    ;;
  classify) run_classify ;;
  review) run_review ;;
  acquire_green) run_acquire "green" ;;
  acquire_yellow) run_acquire "yellow" ;;
  screen_yellow) run_screen_yellow ;;
  merge) run_merge ;;
  difficulty) run_difficulty ;;
  catalog) run_catalog ;;
  *)
    echo -e "${RED}Unknown stage: ${STAGE}${NC}"
    usage
    exit 1
    ;;
esac
