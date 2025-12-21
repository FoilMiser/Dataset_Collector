# agri_circular_pipeline_v1.2 — difficulty routing upgrade

This note describes how to upgrade your current **agri_circular_pipeline_v1** into an
**agri_circular_pipeline_v1.2** that matches the *math_pipeline_v1.2* difficulty-aware routing architecture.

The goal: downloaded datasets automatically land in a stable curriculum-style layout:

`pools/{pool}/{subject}/d{level:02d}/{domain}/{category}/{target_id}/`

This plan follows the same “multi-domain difficulty schema v2” approach described in your schema upgrade note. fileciteturn0file0

---

## 1) Add the new difficulty map file

Add this file to the pipeline repo root:

- `difficulties_agri_circular.yaml` (schema v2.0)

Key expectations (mirrors math v1.2):
- `globals.folder_layout` defines the output directory template.
- `subjects.agri_circular.domains.*.categories.*.level` defines **default/min/max**.
- `source_overrides` can map target IDs to routes until targets have explicit `routing:` metadata.
- `rule_sets.global.keyword_rules` provides fallback routing if nothing else is available.

---

## 2) Update targets.yaml companion_files

In `targets.yaml` (and/or `targets_agri_circular.yaml`), add:

```yaml
companion_files:
  license_map: "./license_map.yaml"
  field_schemas: "./field_schemas.yaml"
  denylist: "./denylist.yaml"
  difficulties_map: "./difficulties_agri_circular.yaml"
```

### Recommended (optional) — add per-target `routing:` blocks

To make routing deterministic (and reduce dependence on keyword rules), add this to each target:

```yaml
routing:
  subject: agri_circular
  domain: environmental_monitoring
  category: water_discharge_npdes_dmr
  level: 6
  granularity: target
```

If you already have a legacy field (e.g., `agri_routing:`), keep it temporarily and have the driver map it into `routing:`.

---

## 3) Update field_schemas.yaml (queue record routing fields)

Port the schema block from math v1.2 into **agri** `field_schemas.yaml`:

- `queue_record_routing_v2.0.0`

Fields to add:
- `routing_subject` (required)
- `routing_domain`
- `routing_category`
- `routing_level` (1–10)
- `routing_granularity` (target|record|chunk)
- `routing_confidence`
- `routing_reason`

This keeps routing subject-agnostic so other domains can reuse the same pipeline later.

---

## 4) Update pipeline_driver.py to emit routing_* fields

### What to add (match math v1.2 behavior)

Add a helper:

- `resolve_routing_fields(target: dict) -> dict`

Logic:
1. Prefer `target["routing"]`
2. Else fall back to a legacy block if present (e.g., `target["agri_routing"]`)
3. Else return `{subject: "agri_circular", domain: None, category: None, level: None, granularity: "target"}`

Then, when emitting each queue record, include:

```python
"routing_subject": routing["subject"],
"routing_domain": routing["domain"],
"routing_category": routing["category"],
"routing_level": routing["level"],
"routing_granularity": routing["granularity"],
"routing_confidence": routing.get("confidence"),
"routing_reason": routing.get("reason"),
```

Tip: you can leave existing driver behavior untouched otherwise; this is an additive metadata upgrade.

---

## 5) Update download_worker.py to be difficulty-aware

Port the “difficulty routing” parts from math v1.2 into agri’s `download_worker.py`.

### A) Load difficulty config
- Add CLI arg: `--difficulty-yaml`
- If not provided, load `targets.yaml -> companion_files.difficulties_map`

### B) Resolve route
Add (or copy) functions:
- `_default_route(diff_cfg)`
- `_match_keyword_rules(blob, rules, fallback)`
- `resolve_route(row, diff_cfg)`

Resolution order (same as math v1.2):
1. `routing_*` fields on the queue row
2. `source_overrides` keyed by subject + target_id
3. `rule_sets.global.keyword_rules`
4. `rule_sets.subjects.agri_circular.keyword_rules`
5. defaults

### C) Resolve output dir
Replace (or bypass) `resolve_pool_dir()` and instead compute:

- `out_dir = resolve_output_dir(ctx, pool_name, route, target_id)`

Where `resolve_output_dir` implements:
- token substitution using `globals.folder_layout`
- `sanitize_path_segments` to keep paths safe on Windows

---

## 6) Wiring the Windows/WSL destination root (optional but nice)

Math v1.2 allows the difficulty YAML to “own” the destination roots.

If you want this behavior, add logic like:

- when `--pools-root` is not provided (or is default),
- and difficulty YAML has `globals.destination_root_*`,
- then set base pools root to:

`{destination_root}/pools/`

So running under WSL can naturally write to:

`/mnt/e/AI-Research/datasets/Natural/agri_circular/pools/...`

---

## 7) Quick test procedure

1. **Driver dry run**
   - Run `pipeline_driver.py` and confirm queue JSONL rows now contain `routing_*`.
2. **Downloader dry run**
   - Run `download_worker.py` without `--execute`.
   - Confirm planned output directories include `dXX/domain/category/target_id`.
3. **Execute a small subset**
   - `--limit-targets 1 --execute`
   - Verify manifests are written to the routed folder.

---

## 8) Suggested next improvements (v1.3+)

- Add a post-download “chunk router” to refine difficulty within each category’s `min/max`.
- Add more `source_overrides` as you add targets, then gradually replace them with explicit `routing:` blocks inside targets.yaml.
- Add a few more high-signal keyword rules for your most common sources (USDA, NRCS, USGS, FAO, World Bank, etc.).

---

## Deliverables produced

- `difficulties_agri_circular.yaml`
- This document: `AGRI_CIRCULAR_PIPELINE_v1.2_ADAPTATION.md`
