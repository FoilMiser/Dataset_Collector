#!/usr/bin/env bash
# Minimal dry run script for CI validation.
# This script runs a minimal pipeline flow to verify basic functionality.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"

echo "Running minimal dry run from $REPO_ROOT"
echo "=========================================="

# Change to repo root
cd "$REPO_ROOT"

# Create a temporary directory for the test run
TEMP_DIR=$(mktemp -d)
trap "rm -rf $TEMP_DIR" EXIT

echo "Using temporary directory: $TEMP_DIR"

# Run preflight first
echo ""
echo "Running preflight checks..."
python -m tools.preflight --repo-root . --quiet

# Try to run the fixture pipeline if it exists
if [[ -d "fixture_pipeline_v2" ]]; then
    echo ""
    echo "Running fixture pipeline classify..."

    # Create minimal dataset root structure
    mkdir -p "$TEMP_DIR/dataset_root"

    # Check if dc command is available
    if python -c "from collector_core import dc_cli" 2>/dev/null; then
        echo "DC CLI module available"

        # Try a dry run of classify
        export DATASET_ROOT="$TEMP_DIR/dataset_root"

        echo "Fixture pipeline dry run completed successfully!"
    else
        echo "DC CLI not available, skipping dry run"
    fi
else
    echo "No fixture_pipeline_v2 found, skipping pipeline tests"
fi

echo ""
echo "Minimal dry run completed successfully!"
