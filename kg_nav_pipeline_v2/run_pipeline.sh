#!/usr/bin/env bash
#
# run_pipeline.sh (kg_nav v2)
# Wrapper for the KG + Literature Navigation Pipeline v2 stages.
#
# Stage order (parity with math_pipeline_v2):
#   classify -> acquire_green -> acquire_yellow -> screen_yellow -> merge -> difficulty -> catalog
# Optional: review (list YELLOW queue)
#
set -euo pipefail

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
KG + Literature Navigation Pipeline v${VERSION}

Required:
  --targets FILE          Path to targets_kg_nav.yaml

Options:
  --execute               Perform actions (default: dry-run/plan only)
  --stage STAGE           Stage to run: all, classify, acquire_green, acquire_yellow, \\
                          screen_yellow, merge, difficulty, catalog, review
  --limit-targets N       Limit number of queue rows processed (acquire stages)
  --limit-files N         Limit files per target during acquisition
  --workers N             Parallel workers for acquisition (default: 4)
  -h, --help              Show this help
EOF
  exit 0
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --targets) TARGETS="$2"; shift 2 ;;
    --stage) STAGE="$2"; shift 2 ;;
    --execute) EXECUTE="--execute"; shift ;;
    --limit-targets) LIMIT_TARGETS="$2"; shift 2 ;;
    --limit-files) LIMIT_FILES="$2"; shift 2 ;;
    --workers) WORKERS="$2"; shift 2 ;;
    -h|--help) usage ;;
    *) echo -e "${RED}Unknown option: $1${NC}"; usage ;;
  esac
done

if [[ -z "$TARGETS" ]]; then
  echo -e "${RED}Error: --targets is required${NC}"
  usage
fi
if [[ ! -f "$TARGETS" ]]; then
  echo -e "${RED}Error: targets file not found: $TARGETS${NC}"
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

QUEUES_ROOT="$(python - <<'PY'\nimport yaml,sys\ncfg=yaml.safe_load(open(sys.argv[1],encoding='utf-8')) or {}\nprint((cfg.get('globals',{}) or {}).get('queues_root','/data/kg_nav/_queues'))\nPY\n"$TARGETS")"

LIMIT_TARGETS_ARG=""; [[ -n "$LIMIT_TARGETS" ]] && LIMIT_TARGETS_ARG="--limit-targets $LIMIT_TARGETS"
LIMIT_FILES_ARG=""; [[ -n "$LIMIT_FILES" ]] && LIMIT_FILES_ARG="--limit-files $LIMIT_FILES"

echo -e "${BLUE}KG + Literature Navigation Pipeline v${VERSION}${NC}"
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
  local queue="${QUEUES_ROOT}/${bucket}_download.jsonl"
  if [[ "$bucket" == "yellow" ]]; then
    queue="${QUEUES_ROOT}/yellow_pipeline.jsonl"
  fi
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

run_difficulty() {
  echo -e "${YELLOW}[difficulty]${NC} Assigning difficulty and writing final shards"
  python "${SCRIPT_DIR}/difficulty_worker.py" --targets "$TARGETS" $EXECUTE
  echo -e "${GREEN}[difficulty] done${NC}"
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
    run_difficulty
    run_catalog
    ;;
  classify) run_classify ;;
  acquire_green) run_acquire green ;;
  acquire_yellow) run_acquire yellow ;;
  screen_yellow) run_screen_yellow ;;
  merge) run_merge ;;
  difficulty) run_difficulty ;;
  catalog) run_catalog ;;
  review) run_review ;;
  *) echo -e "${RED}Unknown stage: ${STAGE}${NC}"; usage ;;
esac

echo -e "${BLUE}Pipeline complete.${NC}"
