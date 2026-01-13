# Dataset Collector Quickstart Guide

This guide covers the canonical way to install, run, and extend Dataset Collector v3.0.

## Table of Contents

1. [Installation](#installation)
2. [Running a Pipeline](#running-a-pipeline)
3. [Adding a Target](#adding-a-target)
4. [Adding a Pipeline/Domain Handler](#adding-a-pipelinedomain-handler)
5. [Policy Semantics](#policy-semantics)

---

## Installation

### Install Modes

Dataset Collector supports several installation modes:

#### Minimal Install (Core Only)

```bash
pip install -r requirements.constraints.txt
pip install -e .
```

This installs the core package without optional dependencies.

#### Full Install with All Extras

```bash
pip install -r requirements.constraints.txt
pip install -e ".[all]"
```

#### With Specific Extras

```bash
# Observability (OpenTelemetry, Prometheus)
pip install -e ".[observability]"

# Async downloads (aiohttp, httpx)
pip install -e ".[async]"

# Development tools
pip install -r requirements-dev.constraints.txt
```

#### Domain-Specific Dependencies

As of v3.0, domain requirements are centralized in `pipelines/requirements/`:

```bash
# Install base + domain requirements
pip install -r requirements.constraints.txt
pip install -e .
pip install -r pipelines/requirements/math.txt
pip install -r pipelines/requirements/physics.txt
```

See `pipelines/requirements/README.md` for the full domain mapping.

---

## Running a Pipeline

### Using the Unified CLI (`dc`)

The `dc` CLI is the canonical way to run all pipeline operations:

```bash
# List available pipelines
dc --list-pipelines

# Run classification stage
dc pipeline math -- --targets pipelines/targets/targets_math.yaml --stage classify

# Run acquisition for GREEN bucket
dc run --pipeline math --stage acquire --dataset-root /data/math -- \
    --queue /data/math/_queues/green_pipeline.jsonl \
    --bucket green \
    --execute

# Run acquisition for YELLOW bucket
dc run --pipeline math --stage acquire --dataset-root /data/math -- \
    --queue /data/math/_queues/yellow_pipeline.jsonl \
    --bucket yellow \
    --execute

# Run yellow screening
dc run --pipeline math --stage yellow_screen --dataset-root /data/math -- \
    --queue /data/math/_queues/yellow_pipeline.jsonl \
    --targets pipelines/targets/targets_math.yaml \
    --execute

# Run merge
dc run --pipeline math --stage merge --dataset-root /data/math -- \
    --targets pipelines/targets/targets_math.yaml \
    --execute

# Build catalog
dc catalog-builder --pipeline math -- \
    --targets pipelines/targets/targets_math.yaml \
    --output /data/math/_catalogs/catalog.json
```

### Key CLI Options

- `--pipeline <domain>`: Pipeline domain (e.g., math, physics, chem)
- `--stage <stage>`: Stage to run (acquire, merge, yellow_screen)
- `--dataset-root <path>`: Override the dataset root directory
- `--allow-data-root`: Allow /data defaults (disabled by default)
- `--execute`: Actually perform operations (default is dry-run)

### Environment Variables

- `DATASET_ROOT` or `DATASET_COLLECTOR_ROOT`: Default dataset root
- `DC_PROFILE`: Configuration profile (development, production)

---

## Adding a Target

### 1. Edit the Targets YAML

Targets are defined in `pipelines/targets/targets_<domain>.yaml`:

```yaml
targets:
  - id: my_new_dataset
    name: My New Dataset
    enabled: true
    bucket: GREEN  # or YELLOW for manual review, RED to block

    # License information
    license_profile: permissive  # permissive, copyleft, quarantine
    license_evidence_url: https://example.com/license

    # Download configuration
    download:
      strategy: http  # http, git, zenodo, figshare, s3, etc.
      urls:
        - https://example.com/data.jsonl.gz
      sha256: abc123...  # Optional checksum

    # Routing metadata
    routing:
      subject: math
      domain: algebra
      category: datasets
      level: 3
```

### 2. Validate the Target

Run the preflight checker:

```bash
python -m tools.preflight --pipelines math_pipeline_v2
```

### 3. Run Classification

```bash
dc pipeline math -- --targets pipelines/targets/targets_math.yaml --stage classify
```

This emits queue files to `_queues/` based on bucket classification.

### Target Fields Reference

| Field | Required | Description |
|-------|----------|-------------|
| `id` | Yes | Unique identifier |
| `name` | Yes | Human-readable name |
| `enabled` | No | Whether to process (default: true) |
| `bucket` | Yes | Safety bucket: GREEN, YELLOW, RED |
| `license_profile` | Yes | License category |
| `license_evidence_url` | Yes | URL with license terms |
| `download.strategy` | Yes | Download method |
| `download.urls` | Yes | Download URLs |
| `routing` | No | Routing metadata |

---

## Adding a Pipeline/Domain Handler

### Option 1: Register in pipeline_specs_registry.py

For most domains, add a `PipelineSpec` in `src/collector_core/pipeline_specs_registry.py`:

```python
register_pipeline(
    PipelineSpec(
        domain="my_domain",
        name="My Domain Pipeline",
        targets_yaml="targets_my_domain.yaml",
        routing_keys=["my_domain_routing"],
        routing_confidence_keys=["my_domain_routing"],
        default_routing={
            "subject": "my_domain",
            "domain": "misc",
            "category": "misc",
            "level": 5,
            "granularity": "target",
        },
    )
)
```

### Option 2: Custom Yellow Screen Handler

For domain-specific screening logic, create a handler in `src/collector_core/yellow/domains/`:

```python
# src/collector_core/yellow/domains/my_domain.py
from collector_core.yellow.base import (
    DomainContext,
    FilterDecision,
    standard_filter,
    standard_transform,
)


def filter_record(raw: dict, ctx: DomainContext) -> FilterDecision:
    # Add domain-specific filtering logic
    decision = standard_filter(raw, ctx)

    # Custom checks
    if not decision.allow:
        return decision

    # Domain-specific validation
    if some_domain_specific_check(raw):
        return FilterDecision(allow=False, reason="domain_specific_rejection")

    return decision


def transform_record(
    raw: dict,
    ctx: DomainContext,
    decision: FilterDecision,
    *,
    license_profile: str,
) -> dict | None:
    return standard_transform(raw, ctx, decision, license_profile=license_profile)
```

Then register it in `pipeline_specs_registry.py`:

```python
register_pipeline(
    PipelineSpec(
        domain="my_domain",
        ...
        yellow_screen_module="yellow_screen_my_domain",
    )
)
```

### Domain Screener Outputs

Domain screeners return a `FilterDecision` with structured metadata that is preserved in the
canonical record under `screening`. When a record is allowed, the screener adds:

- `screening.domain`: domain identifier (e.g., `nlp`, `cyber`).
- `screening.quality.score`: floating score in `[0, 1]` representing domain-specific quality.
- `screening.quality.signals`: normalized signals such as language detection, CVE counts, or
  relation coverage used to compute the score.

Rejected records still include the standard `reason` in the pitch ledger and may provide
extra signals to aid manual review.

---

## Policy Semantics

### Safety Buckets: GREEN / YELLOW / RED

Dataset Collector uses a three-bucket safety model:

| Bucket | Meaning | Merge Eligibility |
|--------|---------|-------------------|
| **GREEN** | Safe to collect and merge automatically | Immediate |
| **YELLOW** | Requires manual review before merge | After signoff |
| **RED** | Do not collect or merge | Never |

### Classification Rules

Targets are classified based on:

1. **Explicit bucket** in targets YAML
2. **Denylist matches** (domain, publisher, content patterns)
3. **License profile** (permissive, copyleft, unknown, deny)
4. **Content checks** (PII, secrets, dual-use content)

### Evidence Change Policy

When license evidence changes since the last signoff:

1. **Detection**: Hash comparison (raw and normalized)
2. **Action**: Target demoted to YELLOW re-review queue
3. **Blocking**: Merge blocked until re-approved

Configure via `globals.evidence_policy` in targets YAML:

```yaml
globals:
  evidence_policy:
    comparison: normalized  # raw, normalized, either
    cosmetic_handling: warn_only  # warn_only, treat_as_changed
    demote_on_change: true
    block_merge_on_change: true
```

### Denylist Governance

The denylist (`configs/common/denylist.yaml`) blocks or flags sources:

```yaml
domain_patterns:
  - domain: sci-hub.se
    severity: hard_red
    rationale: Copyright circumvention site
    link: https://example.com/policy

publisher_patterns:
  - publisher: Example Publisher
    severity: force_yellow
    rationale: Requires license review
```

Severity levels:
- `hard_red`: Block collection entirely
- `force_yellow`: Flag for manual review

### Override Mechanism

Exceptions can be granted with documentation:

```python
from collector_core.policy_override import create_override, OverrideType

override = create_override(
    target_id="my_target",
    override_type=OverrideType.DENYLIST_EXCEPTION,
    justification="This specific dataset is released under CC0 despite publisher",
    reference_link="https://github.com/org/repo/issues/123",
    approved_by="reviewer@example.com",
)
```

All overrides are recorded in the audit ledger for compliance review.

---

## Next Steps

- See [Architecture](architecture.md) for system design details
- See [Troubleshooting](troubleshooting.md) for common issues
- See [Adding a New Pipeline](adding-new-pipeline.md) for detailed instructions
