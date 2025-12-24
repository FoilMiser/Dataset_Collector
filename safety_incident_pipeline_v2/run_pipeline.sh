#!/usr/bin/env bash
#
# run_pipeline.sh (v2.0)
#
# Wrapper script for the Safety Incident Pipeline v2. It follows the
# SAFETY_INCIDENT_PIPELINE_V2_ADAPTATION_PLAN.md stage order.
#
# Usage examples:
#   ./run_pipeline.sh --targets targets_safety_incident.yaml --stage classify
#   ./run_pipeline.sh --targets targets_safety_incident.yaml --stage acquire_green --execute
#   ./run_pipeline.sh --targets targets_safety_incident.yaml --stage difficulty --execute
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
  cat << 'EOF'
Safety Incident Pipeline v${VERSION}

Required:
  --targets FILE          Path to targets_safety_incident.yaml

Options:
  --execute               Perform actions (default is dry-run/plan only)
  --stage STAGE           Stage to run: all, classify, acquire_green, acquire_yellow, \
                          screen_yellow, merge, difficulty, catalog, review
  --limit-targets N       Limit number of queue rows processed
  --limit-files N         Limit files per target during acquisition
  --workers N             Parallel workers for acquisition (default: 4)
  -h, --help              Show this help
EOF
}

# Read a value from the targets YAML using python
get_yaml_value() {
  local key="$1"
  python - << PY
import yaml, pathlib
path = pathlib.Path("${TARGETS}").expanduser()
cfg = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
value = cfg
for part in "${key}".split('.'):
    value = value.get(part, {}) if isinstance(value, dict) else {}
print(value if value else "")
PY
}

queues_root() { get_yaml_value "globals.queues_root"; }
catalogs_root() { get_yaml_value "globals.catalogs_root"; }

# Parse arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --targets)
      TARGETS="$2"; shift 2;;
    --execute)
      EXECUTE="--execute"; shift;;
    --stage)
      STAGE="$2"; shift 2;;
    --limit-targets)
      LIMIT_TARGETS="$2"; shift 2;;
    --limit-files)
      LIMIT_FILES="$2"; shift 2;;
    --workers)
      WORKERS="$2"; shift 2;;
    -h|--help)
      usage; exit 0;;
    *)
      echo -e "${RED}Unknown argument: $1${NC}"; usage; exit 1;;
  esac
done

if [[ -z "${TARGETS}" ]]; then
  echo -e "${RED}--targets is required${NC}"
  usage
  exit 1
fi
if [[ ! -f "${TARGETS}" ]]; then
  echo -e "${RED}targets file not found: ${TARGETS}${NC}"
  exit 1
fi

QUEUES_ROOT="$(queues_root)"
CATALOGS_ROOT="$(catalogs_root)"
GREEN_QUEUE="${QUEUES_ROOT:-/data/safety/_queues}/green_download.jsonl"
YELLOW_QUEUE="${QUEUES_ROOT:-/data/safety/_queues}/yellow_pipeline.jsonl"
CATALOG_OUT="${CATALOGS_ROOT:-/data/safety/_catalogs}/catalog.json"
LIMIT_TARGETS_ARG=""; [[ -n "$LIMIT_TARGETS" ]] && LIMIT_TARGETS_ARG="--limit-targets $LIMIT_TARGETS"
LIMIT_FILES_ARG=""; [[ -n "$LIMIT_FILES" ]] && LIMIT_FILES_ARG="--limit-files $LIMIT_FILES"

run_classify() {
  echo -e "${BLUE}[*] Classify targets${NC}"
  python pipeline_driver.py --targets "${TARGETS}"
  echo -e "${GREEN}[✓] Queues written to ${QUEUES_ROOT:-/data/safety/_queues}${NC}"
}

run_acquire_green() {
  echo -e "${BLUE}[*] Acquire GREEN targets${NC}"
  python acquire_worker.py --queue "${GREEN_QUEUE}" --targets-yaml "${TARGETS}" --bucket green ${LIMIT_TARGETS_ARG} ${LIMIT_FILES_ARG} --workers "${WORKERS}" ${EXECUTE}
}

run_acquire_yellow() {
  echo -e "${BLUE}[*] Acquire YELLOW targets${NC}"
  python acquire_worker.py --queue "${YELLOW_QUEUE}" --targets-yaml "${TARGETS}" --bucket yellow ${LIMIT_TARGETS_ARG} ${LIMIT_FILES_ARG} --workers "${WORKERS}" ${EXECUTE}
}

run_screen_yellow() {
  echo -e "${BLUE}[*] Screen YELLOW targets${NC}"
  python yellow_screen_worker.py --targets "${TARGETS}" --queue "${YELLOW_QUEUE}" ${EXECUTE}
}

run_merge() {
  echo -e "${BLUE}[*] Merge GREEN + screened YELLOW${NC}"
  python merge_worker.py --targets "${TARGETS}" ${EXECUTE}
}

run_difficulty() {
  echo -e "${BLUE}[*] Final screen + difficulty assignment${NC}"
  python difficulty_worker.py --targets "${TARGETS}" ${EXECUTE}
}

run_catalog() {
  echo -e "${BLUE}[*] Build catalog${NC}"
  mkdir -p "$(dirname "${CATALOG_OUT}")"
  python catalog_builder.py --targets "${TARGETS}" --output "${CATALOG_OUT}"
  echo -e "${GREEN}[✓] Catalog written to ${CATALOG_OUT}${NC}"
}

run_review() {
  echo -e "${BLUE}[*] Review YELLOW queue${NC}"
  python review_queue.py --queue "${YELLOW_QUEUE}" list --limit 50 || true
}

case "${STAGE}" in
  all)
    run_classify
    run_acquire_green
    run_acquire_yellow
    run_screen_yellow
    run_merge
    run_difficulty
    run_catalog
    ;;
  classify) run_classify ;;
  acquire_green) run_acquire_green ;;
  acquire_yellow) run_acquire_yellow ;;
  screen_yellow) run_screen_yellow ;;
  merge) run_merge ;;
  difficulty) run_difficulty ;;
  catalog) run_catalog ;;
  review) run_review ;;
  *)
    echo -e "${RED}Unknown stage: ${STAGE}${NC}"
    usage
    exit 1
    ;;
 esac
