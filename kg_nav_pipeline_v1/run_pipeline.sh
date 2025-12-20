#!/usr/bin/env bash
#
# run_pipeline.sh (v1.0)
#
# Orchestrator for the KG + literature navigation pipeline.
#
# Usage:
#   ./run_pipeline.sh --targets targets.yaml                    # Dry-run classify
#   ./run_pipeline.sh --targets targets.yaml --stage download   # Download GREEN targets
#   ./run_pipeline.sh --targets targets.yaml --execute          # Run all enabled stages
#
# Stages:
#   all       - classify -> download -> nav (placeholder) -> catalog
#   classify  - generate queues + evidence snapshots
#   review    - list YELLOW items awaiting signoff
#   download  - download GREEN targets
#   nav       - run navigation episode builder scaffold
#   catalog   - build global catalog

set -euo pipefail

VERSION="1.0"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Defaults
TARGETS=""
EXECUTE=""
STAGE="all"
LIMIT_TARGETS=""
LIMIT_FILES=""
WORKERS="4"

usage() {
    cat << EOF
KG Navigation Pipeline v${VERSION}

Required:
  --targets FILE          Path to targets.yaml

Options:
  --execute               Actually download/process (default is dry-run where supported)
  --stage STAGE           Stage: all, classify, review, download, nav, catalog
  --limit-targets N       Limit number of targets (download stage)
  --limit-files N         Limit files per target (download stage)
  --workers N             Parallel download workers (default: 4)
  -h, --help              Show this help
