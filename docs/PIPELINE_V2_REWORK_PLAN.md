# Pipeline V2 Stage Flow (Rework Plan Summary)

This document captures the expected **Dataset Collector v2** stage order and
responsibilities. It exists to align per-pipeline drivers and README guidance
with a single canonical description.

## Stage order

1. **classify** — identify targets and apply license profiles.
2. **acquire** — fetch source artifacts and capture evidence.
3. **yellow_screen** — isolate potential restrictions for manual review.
4. **merge** — consolidate GREEN sources into the combined corpus.
5. **catalog** — produce summary metadata and outputs.

## Related docs

- Output structure: `docs/output_contract.md`

## Notes

- YELLOW items must be manually reviewed and approved before merging.
- Each pipeline should point to this doc when describing the stage order.
