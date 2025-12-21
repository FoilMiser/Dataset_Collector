#!/usr/bin/env bash
#
# run_pipeline.sh (v1.0)
#
# Single wrapper script for the Math Corpus Pipeline.
# Orchestrates all stages with sensible defaults.
#
# Usage:
#   ./run_pipeline.sh --targets targets_math.yaml                    # Dry-run
#   ./run_pipeline.sh --targets targets_math.yaml --execute          # Full run
#   ./run_pipeline.sh --targets targets_math.yaml --stage download   # Specific stage
#
# Stages:
#   all       - Run all stages (default)
#   classify  - Generate queues and evidence snapshots
#   review    - List pending YELLOW items (manual signoff helper)
#   download  - Download GREEN targets
#   yellow    - Summarize YELLOW queue and emit review plan
#   catalog   - Build global catalog
#

set -euo pipefail

VERSION="1.0"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values
TARGETS=""
EXECUTE=""
STAGE="all"
LIMIT_TARGETS=""
LIMIT_FILES=""
WORKERS="4"
REVIEW_PLAN=""

# Print usage
usage() {
    cat << EOF
Math Corpus Pipeline v${VERSION}

Usage: $0 [OPTIONS]

Required:
  --targets FILE          Path to targets_math.yaml

Options:
  --execute               Actually download/process (default is dry-run)
  --stage STAGE           Run specific stage: all, classify, download, yellow, catalog
  --limit-targets N       Limit number of targets to process
  --limit-files N         Limit files per target
  --workers N             Parallel download workers (default: 4)
  --review-plan FILE      Optional output path for YELLOW review plan (default derives from targets)
  -h, --help              Show this help

Examples:
  # Dry-run full pipeline
  $0 --targets targets_math.yaml

  # Execute with limits
  $0 --targets targets_math.yaml --execute --limit-targets 3 --limit-files 5

  # Run only classification stage
  $0 --targets targets_math.yaml --stage classify

  # Emit a YELLOW review plan
  $0 --targets targets_math.yaml --stage yellow

EOF
    exit 0
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
        --review-plan)
            REVIEW_PLAN="$2"
            shift 2
            ;;
        -h|--help)
            usage
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            usage
            ;;
    esac
done

# Check required arguments
if [[ -z "$TARGETS" ]]; then
    echo -e "${RED}Error: --targets is required${NC}"
    usage
fi

if [[ ! -f "$TARGETS" ]]; then
    echo -e "${RED}Error: targets file not found: $TARGETS${NC}"
    exit 1
fi

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Queues root (used by review stage)
QUEUES_ROOT=$(python -c "import yaml; print(yaml.safe_load(open('$TARGETS'))['globals'].get('queues_root', '/data/math/_queues'))")

# Print header
echo -e "${BLUE}======================================${NC}"
echo -e "${BLUE}Math Corpus Pipeline v${VERSION}${NC}"
echo -e "${BLUE}======================================${NC}"
echo ""
echo -e "Targets:  ${GREEN}$TARGETS${NC}"
echo -e "Stage:    ${GREEN}$STAGE${NC}"
echo -e "Mode:     ${GREEN}$([ -n "$EXECUTE" ] && echo "EXECUTE" || echo "DRY-RUN")${NC}"
echo ""

# Stage functions
run_classify() {
    echo -e "${YELLOW}[Stage: Classify]${NC} Generating queues and evidence snapshots..."
    python "$SCRIPT_DIR/pipeline_driver.py" \
        --targets "$TARGETS" \
        $( [ -n "$EXECUTE" ] || echo "--no-fetch" )
    echo -e "${GREEN}[Stage: Classify] Complete${NC}"
    echo ""
}

run_review() {
  echo -e "${BLUE}== Stage: review (list pending YELLOW items) ==${NC}"
  python3 review_queue.py --queue "${QUEUES_ROOT}/yellow_pipeline.jsonl" list --limit 50 || true
}

run_download() {
    echo -e "${YELLOW}[Stage: Download]${NC} Downloading GREEN targets..."
    
    # Get queues root from targets_math.yaml
    QUEUES_ROOT=$(python -c "import yaml; print(yaml.safe_load(open('$TARGETS'))['globals'].get('queues_root', '/data/math/_queues'))")
    QUEUE_FILE="$QUEUES_ROOT/green_download.jsonl"
    
    if [[ ! -f "$QUEUE_FILE" ]]; then
        echo -e "${RED}Queue file not found: $QUEUE_FILE${NC}"
        echo "Run classify stage first."
        return 1
    fi
    
    python "$SCRIPT_DIR/download_worker.py" \
        --queue "$QUEUE_FILE" \
        --targets-yaml "$TARGETS" \
        --workers "$WORKERS" \
        --verify-sha256 \
        $EXECUTE \
        $LIMIT_TARGETS \
        $LIMIT_FILES
    
    echo -e "${GREEN}[Stage: Download] Complete${NC}"
    echo ""
}

run_yellow() {
    echo -e "${YELLOW}[Stage: Yellow]${NC} Summarizing YELLOW queue and emitting review plan..."

    PLAN_ARG=""
    if [[ -n "$REVIEW_PLAN" ]]; then
        PLAN_ARG="--output $REVIEW_PLAN"
    fi

    python "$SCRIPT_DIR/yellow_scrubber.py" \
        --targets "$TARGETS" \
        $PLAN_ARG

    echo -e "${GREEN}[Stage: Yellow] Complete${NC}"
    echo ""
}

run_catalog() {
    echo -e "${YELLOW}[Stage: Catalog]${NC} Building global catalog..."

    CATALOGS_ROOT=$(python -c "import yaml; print(yaml.safe_load(open('$TARGETS'))['globals'].get('catalogs_root', '/data/math/_catalogs'))")

    python "$SCRIPT_DIR/catalog_builder.py" \
        --targets "$TARGETS" \
        --output "$CATALOGS_ROOT/global_catalog.json"

    echo -e "${GREEN}[Stage: Catalog] Complete${NC}"
    echo ""
}

# Run stages
case $STAGE in
    all)
        run_classify
        run_download
        run_yellow
        run_catalog
        ;;
    classify)
        run_classify
        ;;
    review)
        run_review
        ;;
    download)
        run_download
        ;;
    yellow)
        run_yellow
        ;;
    catalog)
        run_catalog
        ;;
    *)
        echo -e "${RED}Unknown stage: $STAGE${NC}"
        usage
        ;;
esac

echo -e "${BLUE}======================================${NC}"
echo -e "${GREEN}Pipeline complete!${NC}"
echo -e "${BLUE}======================================${NC}"
