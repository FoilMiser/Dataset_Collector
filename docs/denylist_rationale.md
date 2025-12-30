# Denylist rationale (Dataset Collector v2)

This document captures the provenance and high-level rationale for denylist entries.

## Why denylists exist

Denylists enforce hard exclusions for sources that explicitly prohibit model training,
redistribution, or related downstream usage. They also support conservative blocking
for domains or publishers with clear restrictions in their public terms.

## How to update

When adding a denylist entry, include:

- A short rationale (e.g., explicit "no AI training" clause).
- A stable URL where the restriction is documented.
- The date the restriction was reviewed.

If a policy URL is unavailable, leave the `link` field blank and include the rationale
inline in the denylist entry.
