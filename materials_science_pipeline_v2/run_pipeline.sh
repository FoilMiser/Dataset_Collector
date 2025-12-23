#!/usr/bin/env bash
#
# run_pipeline.sh (v2.0)
#
# Wrapper script for the Materials Science Corpus Pipeline v2. It follows the
# v2 stage order (classify → acquire → screen_yellow → merge → difficulty → catalog).
#
# Usage examples:
#   ./run_pipeline.sh --targets targets_materials.yaml --stage classify
#   ./run_pipeline.sh --targets targets_materials.yaml --stage acquire_green --execute
#   ./run_pipeline.sh --targets targets_materials.yaml --stage difficulty --execute
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
Materials Science Corpus Pipeline v${VERSION}

Required:
  --targets FILE          Path to targets_materials.yaml

Options:
  --execute               Perform actions (default is dry-run/plan only)
  --stage STAGE           Stage to run: all, classify, acquire_green, acquire_yellow, \
                          screen_yellow, merge, difficulty, catalog
  --limit-targets N       Limit number of queue rows processed
  --limit-files N         Limit files per target during acquisition
  --workers N             Parallel workers for acquisition (default: 4)
  -h, --help              Show this help
