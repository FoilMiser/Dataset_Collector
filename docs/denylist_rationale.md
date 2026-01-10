# Denylist rationale (Dataset Collector v2)

This document captures the provenance and high-level rationale for denylist entries.

## Why denylists exist

Denylists enforce hard exclusions for sources that explicitly prohibit model training,
redistribution, or related downstream usage. They also support conservative blocking
for domains or publishers with clear restrictions in their public terms.

## Denylist categories

### License denylists

Located in `configs/common/license_denylist.yaml`:
- SPDX identifiers that prohibit training or redistribution
- Restriction phrases that indicate incompatible terms
- Publisher-specific exclusions

### Domain denylists

Located in `configs/common/domain_denylist.yaml`:
- Domains with known restrictive terms of service
- Paywalled or subscription-only content sources
- Sites that have explicitly opted out of AI training

## How the denylist is enforced

1. **Classification stage**: `pipeline_driver.py` checks targets against denylists
2. **YELLOW bucket routing**: Denied targets route to RED bucket
3. **Merge stage**: Denylist hits are recorded in output metadata
4. **Audit trail**: All denylist decisions are logged with rationale

## How to update

When adding a denylist entry, include:

- A short rationale (e.g., explicit "no AI training" clause).
- A stable URL where the restriction is documented.
- The date the restriction was reviewed.

If a policy URL is unavailable, leave the `link` field blank and include the rationale
inline in the denylist entry.

### Example entry format

```yaml
- pattern: "example.com"
  type: domain
  rationale: "Explicit prohibition on AI training in robots.txt"
  link: "https://example.com/robots.txt"
  reviewed: "2024-01-15"
```

## Testing denylist changes

Run the license denylist enforcement tests:

```bash
python -m pytest tests/test_regcomp_license_denylist_enforcement.py -v
```

## Review workflow

1. Identify potential denylist candidate
2. Document the restriction source (TOS, license, explicit statement)
3. Add entry to appropriate denylist file
4. Run tests to verify no regressions
5. Submit PR with rationale in description
