#!/usr/bin/env bash
#
# run_pipeline.sh (v2.0)
#
# Wrapper script for the NLP Corpus Pipeline v2.
#
# Usage examples:
#   ./run_pipeline.sh --targets targets_nlp.yaml --stage classify
#   ./run_pipeline.sh --targets targets_nlp.yaml --stage acquire_green --execute
#   ./run_pipeline.sh --targets targets_nlp.yaml --stage difficulty --execute
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
  cat << EOM
NLP Corpus Pipeline v${VERSION}

Required:
  --targets FILE          Path to targets_nlp.yaml

Options:
  --execute               Perform actions (default is dry-run/plan only)
  --stage STAGE           Stage to run: all, classify, acquire_green, acquire_yellow, \
                          screen_yellow, merge, difficulty, catalog
  --limit-targets N       Limit number of queue rows processed
  --limit-files N         Limit files per target during acquisition
  --workers N             Parallel workers for acquisition (default: 4)
  -h, --help              Show this help
EOM
}

log() {
  echo -e "${BLUE}[pipeline]${NC} $1"
}

warn() {
  echo -e "${YELLOW}[warn]${NC} $1"
}

fail() {
  echo -e "${RED}[error]${NC} $1"
  exit 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
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
      warn "Unknown arg: $1"
      shift
      ;;
  esac
done

if [[ -z "$TARGETS" ]]; then
  fail "--targets is required"
fi

if [[ ! -f "$TARGETS" ]]; then
  fail "targets file not found: $TARGETS"
fi

run_classify() {
  log "Stage: classify"
  python3 pipeline_driver.py --targets "$TARGETS" ${EXECUTE} \
    ${LIMIT_TARGETS:+--limit-targets "$LIMIT_TARGETS"}
}

run_acquire_green() {
  log "Stage: acquire_green"
  python3 acquire_worker.py --queue "/data/nlp/_queues/green_download.jsonl" \
    --targets-yaml "$TARGETS" --bucket green --workers "$WORKERS" ${EXECUTE} \
    ${LIMIT_TARGETS:+--limit-targets "$LIMIT_TARGETS"} \
    ${LIMIT_FILES:+--limit-files "$LIMIT_FILES"}
}

run_acquire_yellow() {
  log "Stage: acquire_yellow"
  python3 acquire_worker.py --queue "/data/nlp/_queues/yellow_pipeline.jsonl" \
    --targets-yaml "$TARGETS" --bucket yellow --workers "$WORKERS" ${EXECUTE} \
    ${LIMIT_TARGETS:+--limit-targets "$LIMIT_TARGETS"} \
    ${LIMIT_FILES:+--limit-files "$LIMIT_FILES"}
}

run_screen_yellow() {
  log "Stage: screen_yellow"
  python3 yellow_screen_worker.py --targets "$TARGETS" --queue "/data/nlp/_queues/yellow_pipeline.jsonl" ${EXECUTE}
}

run_merge() {
  log "Stage: merge"
  python3 merge_worker.py --targets "$TARGETS" ${EXECUTE}
}

run_difficulty() {
  log "Stage: difficulty"
  python3 difficulty_worker.py --targets "$TARGETS" ${EXECUTE}
}

run_catalog() {
  log "Stage: catalog"
  python3 catalog_builder.py --targets "$TARGETS" --output "/data/nlp/_catalogs/catalog.json"
}

case "$STAGE" in
  all)
    run_classify
    run_acquire_green
    run_acquire_yellow
    run_screen_yellow
    run_merge
    run_difficulty
    run_catalog
    ;;
  classify)
    run_classify
    ;;
  acquire_green)
    run_acquire_green
    ;;
  acquire_yellow)
    run_acquire_yellow
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
  *)
    fail "Unknown stage: $STAGE"
    ;;
esac

log "Done."
