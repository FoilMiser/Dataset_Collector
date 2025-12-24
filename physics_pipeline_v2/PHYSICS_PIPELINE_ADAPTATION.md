# Physics pipeline v2 adaptation notes

This folder is derived from the v2 pipeline engineering in `math_pipeline_v2`,
then specialized to Physics. For the authoritative plan and checklist, see:

- `PHYSICS_PIPELINE_V2_ADAPTATION_PLAN.md` at the repo root.

Key deltas from math v2:

- `targets_physics.yaml` contains the physics/astro/materials inventory.
- `difficulties_physics.yaml` defines the physics difficulty rubric + routing map.
- `pipeline_driver.py` accepts `physics_routing` and defaults routing subject to `physics`.
- Default roots point to `/data/physics/...`.

Stage ordering and strict pitch behavior are identical to math v2.

Not legal advice.
