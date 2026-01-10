# Claude Code Checklist — Dataset Collector repo (fixes + refactors)

_Last updated: 2026-01-09_

This checklist is written for **Claude Code** to apply directly in the repo. It focuses on eliminating the biggest maintenance risks (duplicated per-pipeline boilerplate), making pipeline behavior more configuration-driven, and adding the missing regression coverage.

> Source context: this plan is derived from the repository assessment provided by the user (see downloaded assessment file in this chat).

---

## Ground rules for Claude Code

- **Keep output contracts stable**: do not change schemas or emitted file layouts unless explicitly called out and covered by tests.
- **Prefer spec-driven behavior**: `collector_core/pipeline_specs_registry.py` + `PipelineSpec` should remain the source of truth.
- **Make changes in small commits** (one checklist item per commit when possible).
- After each major step run:
  - `python -m ruff format .`
  - `python -m ruff check .`
  - `python -m pytest -q`

---

## Registered pipeline specs (source of truth)

Use this table when filling in wrapper constants or verifying defaults.

| domain | prefix | targets_yaml | yellow_screen_module | custom_workers |
| --- | --- | --- | --- | --- |
| 3d_modeling | 3d | targets_3d.yaml |  | mesh_worker |
| agri_circular | agri_circular | targets_agri_circular.yaml |  |  |
| biology | bio | targets_biology.yaml |  |  |
| chem | chem | targets_chem.yaml | yellow_screen_chem |  |
| code | code | targets_code.yaml |  | code_worker |
| cyber | cyber | targets_cyber.yaml |  | nvd_worker,stix_worker,advisory_worker |
| earth | earth | targets_earth.yaml |  |  |
| econ_stats_decision_adaptation | econ | targets_econ_stats_decision_v2.yaml | yellow_screen_econ |  |
| engineering | engineering | targets_engineering.yaml |  |  |
| kg_nav | kg_nav | targets_kg_nav.yaml | yellow_screen_kg_nav |  |
| logic | logic | targets_logic.yaml |  |  |
| materials_science | matsci | targets_materials.yaml |  |  |
| math | math | targets_math.yaml |  |  |
| metrology | metrology | targets_metrology.yaml |  |  |
| nlp | nlp | targets_nlp.yaml | yellow_screen_nlp |  |
| physics | physics | targets_physics.yaml |  |  |
| regcomp | regcomp | targets_regcomp.yaml |  |  |
| safety_incident | safety | targets_safety_incident.yaml | yellow_screen_safety |  |
---

## P0 — High-priority fixes (do these first)

### [ ] 1) Replace all per-pipeline `pipeline_driver.py` subclasses with the factory

**Problem:** Every `*_pipeline_v2/pipeline_driver.py` defines a small `BasePipelineDriver` subclass. This is duplicated across **18** pipelines and contradicts the existing `collector_core/pipeline_factory.py`.

**Goal:** Each pipeline’s `pipeline_driver.py` becomes a thin wrapper that calls the factory by domain.

**Pipelines:** all `*_pipeline_v2` directories.

**How**
1. For each pipeline directory `X_pipeline_v2/`, rewrite `pipeline_driver.py` to:

```py
#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from collector_core.pipeline_factory import get_pipeline_driver  # noqa: E402

DOMAIN = "<domain>"

if __name__ == "__main__":
    get_pipeline_driver(DOMAIN).main()
```

2. Delete the per-pipeline driver subclass and any per-pipeline routing blocks (routing is already in `PipelineSpec`).
3. Ensure `collector_core/pipeline_specs_registry.py` contains correct routing keys/default routing for every domain used.

**Acceptance**
- Running `python X_pipeline_v2/pipeline_driver.py --help` works.
- Running `python X_pipeline_v2/pipeline_driver.py --targets X_pipeline_v2/targets_*.yaml --execute` behaves identically.

---

### [ ] 2) Replace per-pipeline `acquire_worker.py` with a call into `collector_core.generic_workers.main_acquire`

**Problem:** Every pipeline ships a near-identical `acquire_worker.py` with hard-coded defaults like `/data/bio/raw`.

**Goal:** Move default roots to `PipelineSpec.get_default_roots()` (already implemented) and eliminate duplicated scripts.

**How**
1. Confirm `collector_core/generic_workers.py` exports `main_acquire(domain: str)`.
2. For every pipeline `X_pipeline_v2/acquire_worker.py`, rewrite to:

```py
#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from collector_core.generic_workers import main_acquire  # noqa: E402

DOMAIN = "<domain>"

if __name__ == "__main__":
    main_acquire(DOMAIN)
```

3. Remove all per-pipeline `RootsDefaults(...)` constants.

**Acceptance**
- Defaults still point at the same `/data/<prefix>/...` locations (prefix comes from `PipelineSpec.prefix`).
- `pytest` passes.

---

### [ ] 3) Normalize *all* `yellow_scrubber.py` to use `collector_core.yellow_review_helpers.make_main`

**Problem:** `yellow_scrubber.py` exists in multiple duplicated patterns:
- 735-line copies (chem/materials_science/regcomp)
- ~183–184-line partial duplicates
- Some already-correct 21-line wrappers
- Two pipelines missing the file entirely

**Goal:** Every pipeline uses the same wrapper style.

**Pipelines needing rewrite:**
- 735-line versions: `chem_pipeline_v2`, `materials_science_pipeline_v2`, `regcomp_pipeline_v2`
- ~183–184-line versions: `3d_modeling`, `agri_circular`, `biology`, `code`, `earth`, `econ_stats_decision_adaptation`, `engineering`, `kg_nav`, `logic`, `math`
- Missing: `metrology`, `safety_incident`
- Already OK (leave as-is, but verify args): `cyber`, `nlp`, `physics`

**How**
1. For each pipeline, set:
   - `domain_name` = spec.domain (e.g. `"biology"`)
   - `domain_prefix` = spec.prefix (e.g. `"bio"`)
   - `targets_yaml_name` = spec.targets_yaml (e.g. `"targets_biology.yaml"`)
2. Rewrite each `yellow_scrubber.py` to the minimal wrapper:

```py
#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from collector_core.pipeline_spec import get_pipeline_spec  # noqa: E402
from collector_core.yellow_review_helpers import make_main  # noqa: E402

DOMAIN = "<domain>"

if __name__ == "__main__":
    spec = get_pipeline_spec(DOMAIN)
    assert spec is not None, f"Unknown domain: {DOMAIN}"
    make_main(domain_name=spec.domain, domain_prefix=spec.prefix, targets_yaml_name=spec.targets_yaml)
```

3. Add `yellow_scrubber.py` for `metrology_pipeline_v2` and `safety_incident_pipeline_v2` using the same wrapper.

**Acceptance**
- `python X_pipeline_v2/yellow_scrubber.py --help` works for every pipeline.
- `tests/test_yellow_review_helpers.py` still passes.
- No pipeline contains a >100-line `yellow_scrubber.py` anymore.

---

### [ ] 4) Make `yellow_screen_worker.py` spec-dispatched and remove hard-coded module selection

**Problem:** Some pipelines call `collector_core.yellow_screen_standard`, others call specialized modules; selection is duplicated across pipelines.

**Goal:** Centralize module dispatch in core, based on `PipelineSpec.yellow_screen_module`.

**How**
1. Add a new core helper, e.g. `collector_core/yellow_screen_dispatch.py`:

- It should:
  - Load spec via `get_pipeline_spec(domain)`
  - If `spec.yellow_screen_module` is set, import `collector_core.<module>` and call its `main(...)`
  - Else call `collector_core.yellow_screen_standard.main(...)`
  - Use `default_yellow_roots(spec.prefix)` for defaults.

2. Rewrite every pipeline `yellow_screen_worker.py` to:

```py
#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from collector_core.yellow_screen_dispatch import main as yellow_main  # noqa: E402

DOMAIN = "<domain>"

if __name__ == "__main__":
    yellow_main(DOMAIN)
```

**Acceptance**
- All pipelines continue to run the correct specialized screener (chem/econ/kg_nav/nlp/safety) when configured in specs.
- No pipeline hard-codes `/data/...` paths for yellow screening defaults.

---

## P1 — Quality + consistency improvements

### [ ] 5) Add a small generator/sync tool to keep pipeline wrappers consistent

**Problem:** Even with wrappers, consistency drifts over time across 18+ directories.

**Goal:** Create `tools/sync_pipeline_wrappers.py` that can overwrite wrapper files from templates using registered `PipelineSpec`s.

**How**
1. Implement a script that:
   - Reads domains via `collector_core.pipeline_spec.list_pipelines()`
   - Writes:
     - `pipeline_driver.py`
     - `acquire_worker.py`
     - `yellow_screen_worker.py`
     - `yellow_scrubber.py`
   - Adds a header comment like `# AUTO-GENERATED; DO NOT EDIT MANUALLY`
2. Update `tools/generate_pipeline.py` to:
   - Register the new pipeline in `collector_core/pipeline_specs_registry.py`
   - Call the sync tool to create wrappers
3. Add a CI check (optional but recommended):
   - Run the sync tool in “check” mode to verify wrappers are up to date.

**Acceptance**
- Adding a pipeline becomes “add spec + targets yaml + run generator”.
- Wrapper drift becomes impossible without CI failing.

---

### [ ] 6) Update docs to reflect spec-driven entrypoints

**Files likely needing edits**
- `README.md`
- `docs/adding-new-pipeline.md`
- `docs/yellow_review_workflow.md`
- Each pipeline README under `*_pipeline_v2/README.md`

**How**
- Replace guidance that suggests editing per-pipeline driver subclasses.
- Prefer:
  - `python -m collector_core.dc_cli --list-pipelines`
  - `python -m collector_core.dc_cli pipeline <domain> ...`
  - `python -m collector_core.dc_cli run --pipeline <domain> --stage acquire|yellow_screen|merge ...`
- If per-pipeline scripts remain, mark them as wrappers and point to `dc_cli` as the “real” API.

**Acceptance**
- A new user can run any pipeline using only the unified CLI + `targets.yaml`.
- Docs do not contradict the existence of `pipeline_factory.py` / specs.

---

### [ ] 7) Add regression tests for evidence change detection

**Problem:** Evidence-change detection is important but not directly tested for “cosmetic-only” changes vs real text/terms changes.

**How**
1. Add tests (suggested new file): `tests/test_license_evidence_change_detection.py`
2. Cover:
   - Only timestamp/date changes → should NOT flip classification / should not mark “evidence changed” if normalization ignores it
   - Added/removed restriction keywords (“no ai”, “no machine learning”) → should be detected
   - HTML formatting-only changes → should be ignored if normalization strips it

**Acceptance**
- Tests validate both “raw bytes changed” and “normalized text changed” behaviors (as intended).
- No network calls.

---

### [ ] 8) Stop swallowing exceptions silently in core paths

**Problem:** At least:
- `collector_core/pipeline_driver_base.py`
- `collector_core/review_queue.py`

contain patterns like:

```py
except Exception:
    return {}
```

**How**
- Replace with logging at WARNING level including exception info (`logger.warning(..., exc_info=True)`).
- If the call site requires “best-effort”, keep returning `{}` but make it observable.
- Add tests verifying a warning is emitted for the error path.

**Acceptance**
- Failures are visible in logs without breaking existing “best-effort” behavior.

---

## P2 — Optional / larger refactors (do after the above lands cleanly)

### [ ] 9) Split monolithic modules (keep behavior identical)

Candidates:
- `collector_core/pipeline_driver_base.py` (~2000 lines)
- `collector_core/acquire_strategies.py` (~1500 lines)
- `collector_core/merge.py` (~1000 lines)

**How**
- Extract cohesive submodules (e.g., `evidence.py`, `routing.py`, `queue_io.py`, `strategies/http.py`, etc.).
- Keep public imports stable (re-export from old modules) to avoid churn.

**Acceptance**
- Zero behavioral change; test suite remains green.

---

### [ ] 10) Wire `tools/strategy_registry.py` into strategy validation + preflight

**Goal**
- Ensure each strategy declares required fields + external tool dependencies and that errors are actionable *before* long downloads start.

**How**
- Add a validation step in the acquire stage that:
  - checks required keys for each target’s `download.strategy`
  - checks required external tools exist (`git`, `aria2c`, etc.) and provides a clear error
- Prefer a shared helper that both `preflight.py` and acquire code can call.

---

### [ ] 11) Decide what to do about `rate_limit` config in targets YAML

**Observation**
- Some `targets_*.yaml` include `resolvers: ... rate_limit: ...`, but the codebase does not appear to consume it (schema also doesn’t validate it).

**Options**
1. **Implement** rate limiting for resolver callers (e.g., GitHub API) and validate in schema.
2. **Remove** `rate_limit` blocks from targets YAML/docs until it is supported, to avoid configuration placebo.

**Acceptance**
- No “dead” YAML config remains undocumented/unimplemented.

---

## Quick “done” definition

This checklist is considered successfully implemented when:

- Boilerplate across `*_pipeline_v2/` is reduced to small wrappers (no copy/paste logic).
- Routing + defaults come from `PipelineSpec`s.
- Yellow review tooling is uniform across all pipelines.
- Core error paths log warnings instead of silently swallowing failures.
- Tests cover evidence-change detection and wrapper behavior.