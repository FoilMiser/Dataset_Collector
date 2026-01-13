# CLI Reference

This document describes all CLI commands provided by the Dataset Collector package.

After installing with `pip install -e .`, these commands are available in your PATH.

## Main Commands

### dc

Unified CLI entry point that routes to various subcommands.

```bash
dc <stage> [options]
```

**Stages:**
- `acquire` - Run the acquisition stage
- `merge` - Run the merge stage
- `yellow_screen` - Run the yellow screen filtering stage
- `review-queue` - Manage the review queue
- `catalog-builder` - Build catalog from pipeline outputs

**Example:**
```bash
dc acquire --queue queue.jsonl --bucket green --execute
dc merge --targets targets.yaml --execute
```

### dc-pipeline

Run a complete pipeline with all stages.

```bash
dc-pipeline <pipeline-id> [options]
```

**Options:**
- `--stages` - Comma-separated list of stages to run
- `--execute` - Actually execute operations (default: dry-run)
- `--strict` - Exit with error on any stage failure

### dc-review

Manage the review queue for manual review of targets.

```bash
dc-review [options]
```

**Options:**
- `--list` - List all pending reviews
- `--approve <target-id>` - Approve a target
- `--reject <target-id>` - Reject a target
- `--notes <text>` - Add review notes

### dc-catalog

Build catalog from pipeline outputs.

```bash
dc-catalog [options]
```

**Options:**
- `--targets <path>` - Path to targets.yaml
- `--output <path>` - Output path for catalog
- `--format` - Output format (json, yaml, csv)

## Validation Commands

### dc-preflight

Run preflight validation checks before pipeline execution.

```bash
dc-preflight [options]
```

**Options:**
- `--repo-root <path>` - Repository root (default: current directory)
- `--pipeline-map <path>` - Path to pipeline map YAML
- `--pipelines <list>` - Specific pipelines to check
- `--strict` - Treat warnings as errors
- `--warn-disabled` - Also check disabled targets
- `--verbose` - Enable verbose output

**Example:**
```bash
dc-preflight --repo-root . --strict
```

### dc-validate-repo

Validate repository configuration including targets files.

```bash
dc-validate-repo [options]
```

**Options:**
- `--repo-root <path>` - Repository root (default: current directory)
- `--output <path>` - Output path for validation report
- `--strict` - Exit with error on warnings

### dc-validate-yaml-schemas

Validate YAML files against their JSON schemas.

```bash
dc-validate-yaml-schemas [paths...]
```

**Options:**
- `--schema <name>` - Schema to validate against (targets, license_map, etc.)

### dc-check-constraints

Check output constraints and data integrity.

```bash
dc-check-constraints [options]
```

### dc-validate-output-contract

Validate pipeline outputs against the output contract.

```bash
dc-validate-output-contract [options]
```

**Options:**
- `--manifest-dir <path>` - Path to manifests directory
- `--strict` - Fail on any contract violation

### dc-validate-pipeline-specs

Validate pipeline specification files.

```bash
dc-validate-pipeline-specs [options]
```

### dc-validate-metrics-outputs

Validate metrics output files.

```bash
dc-validate-metrics-outputs [options]
```

## Maintenance Commands

### dc-sync-wrappers

Synchronize pipeline wrapper scripts to latest template.

```bash
dc-sync-wrappers [options]
```

### dc-clean-repo-tree

Clean up generated files and caches from the repository.

```bash
dc-clean-repo-tree [options]
```

**Options:**
- `--dry-run` - Show what would be deleted
- `--force` - Skip confirmation

### dc-touch-updated-utc

Update the `updated_utc` field in YAML files to current date.

```bash
dc-touch-updated-utc <file>...
```

### dc-make-release-zip

Create a release ZIP archive.

```bash
dc-make-release-zip [options]
```

**Options:**
- `--output <path>` - Output ZIP file path
- `--include-data` - Include data directories

### dc-init-layout

Initialize a new pipeline layout with required directories.

```bash
dc-init-layout <pipeline-name> [options]
```

### dc-generate-pipeline

Generate a new pipeline from template.

```bash
dc-generate-pipeline <pipeline-name> [options]
```

**Options:**
- `--domain <name>` - Domain type (code, biology, etc.)
- `--template <name>` - Template to use

### dc-migrate-pipeline-structure

Migrate existing pipeline to new structure.

```bash
dc-migrate-pipeline-structure <pipeline-dir> [options]
```

### dc-update-wrapper-deprecations

Update deprecated wrapper patterns in pipeline files.

```bash
dc-update-wrapper-deprecations [options]
```

### dc-build-natural-corpus

Build natural language corpus from collected data.

```bash
dc-build-natural-corpus [options]
```

**Options:**
- `--input <path>` - Input data directory
- `--output <path>` - Output corpus path
- `--format` - Output format

## Common Options

Most commands support these common options:

- `--help` - Show help message
- `--verbose` / `-v` - Enable verbose output
- `--quiet` / `-q` - Suppress non-error output
- `--log-level <level>` - Set logging level (DEBUG, INFO, WARNING, ERROR)
- `--log-format <format>` - Set log format (text, json)

## Exit Codes

| Code | Meaning |
| --- | --- |
| 0 | Success |
| 1 | General error or validation failure |
| 2 | Invalid arguments |

## See Also

- [Environment Variables](environment-variables.md)
- [Pipeline Runtime Contract](pipeline_runtime_contract.md)
- [Quickstart Guide](quickstart.md)
