# Earth pipeline v2 notes

This directory adapts the v2 pipeline shape from `math_pipeline_v2` to the Earth Systems domain. See `../../EARTH_PIPELINE_V2_ADAPTATION_PLAN.md` for the authoritative adaptation requirements.

Key deltas from math → earth:
- Default roots under `/data/earth` and Earth-specific queue names.
- Routing defaults to `subject=earth` with optional `earth_routing` alias in targets.
- User agent set to `earth-corpus-pipeline/{VERSION}` during evidence fetching.
- New companion files: `../pipelines/targets/targets_earth.yaml` and Earth schemas/license map.

Future improvements live in `todo.txt`.
