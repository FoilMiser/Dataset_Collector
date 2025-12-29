# Repo-wide refactor: **Collector + License Screener** pipelines (and moving difficulty sorting to a separate pipeline)

This document is a **domain-agnostic** version of the `math_pipeline_v2` refactor plan. It is written so you can apply the *same* update pattern to **every** `*_pipeline_v2/` folder in this repository.

**New division of responsibilities**

- **Pipeline A — Collector (this refactor, per-domain):** *Acquire → capture license evidence → license screen → content screen → emit canonical shards + ledgers + catalog.*
- **Pipeline B — Sorter (separate, shared across domains):** *Read canonical shards → topic/difficulty routing → curriculum folder outputs → training manifests.*

> **Rule of thumb:** If it changes how you *train* (difficulty, curriculum, routing), it belongs in the sorter.  
> If it changes whether you can *legally and safely keep* data (license, evidence, basic content screening), it belongs in the collector.

---

## 0) Applies to which folders?

This applies to **every** pipeline folder that currently contains:

- `pipeline_driver.py`
- `acquire_worker.py`
- `yellow_screen_worker.py`
- `difficulty_worker.py`
- `merge_worker.py`
- `catalog_builder.py`
- `review_queue.py`
- `targets_*.yaml`, `license_map.yaml`, `denylist.yaml`, and `difficulties_*.yaml`

### Pipelines and their targets files (current repo)
| Pipeline folder | Targets YAML | Difficulty YAML (moved out) |
|---|---|---|
| `math_pipeline_v2/` | `targets_math.yaml` | `difficulties_math.yaml` |
| `chem_pipeline_v2/` | `targets_chem.yaml` | `difficulties_chem.yaml` |
| `physics_pipeline_v2/` | `targets_physics.yaml` | `difficulties_physics.yaml` |
| `biology_pipeline_v2/` | `targets_biology.yaml` | `difficulties_biology.yaml` |
| `earth_pipeline_v2/` | `targets_earth.yaml` | `difficulties_earth.yaml` |
| `engineering_pipeline_v2/` | `targets_engineering.yaml` | `difficulties_engineering.yaml` |
| `materials_science_pipeline_v2/` | `targets_materials.yaml` | `difficulties_materials.yaml` |
| `metrology_pipeline_v2/` | `targets_metrology.yaml` | `difficulties_metrology.yaml` |
| `nlp_pipeline_v2/` | `targets_nlp.yaml` | `difficulties_nlp.yaml` |
| `code_pipeline_v2/` | `targets_code.yaml` | `difficulties_code.yaml` |
| `cyber_pipeline_v2/` | `targets_cyber.yaml` | `difficulties_cyber.yaml` |
| `agri_circular_pipeline_v2/` | `targets_agri_circular.yaml` | `difficulties_agri_circular.yaml` |
| `3d_modeling_pipeline_v2/` | `targets_3d.yaml` | `difficulties_3d.yaml` |
| `kg_nav_pipeline_v2/` | `targets_kg_nav.yaml` | `difficulties_kg_nav.yaml` |
| `logic_pipeline_v2/` | `targets_logic.yaml` | `difficulties_logic.yaml` |
| `regcomp_pipeline_v2/` | `targets_regcomp.yaml` | `difficulties_regcomp.yaml` |
| `safety_incident_pipeline_v2/` | `targets_safety_incident.yaml` | `difficulties_safety_incident.yaml` |
| `econ_stats_decision_adaptation_pipeline_v2/` | `targets_econ_stats_decision_v2.yaml` | `difficulties_econ_stats_decision.yaml` |

---

## 1) New scope: what stays vs. what moves out

### Keep in each Collector pipeline
1. **Targets parsing**
2. **Manifest creation** per target (resolved license, evidence locations, acquisition config, target metadata)
3. **Queue creation** (GREEN / YELLOW)
4. **Acquisition** (HF datasets, HTTP/FTP/Zenodo, GitHub releases, domain-specific fetchers)
5. **License evidence snapshots** (dataset card, README/LICENSE, terms pages, etc.)
6. **License screening / pool assignment**
7. **Record-level content screening** (denylist / PII / restriction phrases / minimum quality)
8. **Canonical JSONL shard writing**
9. **Ledgering + per-target evaluation report**
10. **Collector catalog** (counts, bytes, hashes, top reject reasons)

### Move out into the separate Sorter pipeline (shared)
- `difficulty_worker.py` and all `difficulties_*.yaml`
- Any “final curriculum folder structure” logic
- Regex trees for routing by topic/difficulty
- Heavy chunking and structural parsing beyond “make text readable”
- Global near-duplicate detection across the entire corpus (keep only *cheap* per-target dedupe in collector)

---

## 2) Standardized collector filesystem layout (per pipeline run)

Replace the current `raw + screened_yellow + combined + final(difficulty)` model with:

```
<DATA_ROOT>/<DOMAIN>/
  raw/
    green/<target_id>/...
    yellow/<target_id>/...
  evidence/
    <target_id>/...
  manifests/
    <target_id>/
      manifest.json
      evaluation.json
  queues/
    green_queue.jsonl
    yellow_queue.jsonl
    yellow_queue_approved.jsonl        # output of manual review step
  screened/
    green/
      shards/
        shard_00000.jsonl.gz
        shard_00001.jsonl.gz
    yellow/
      shards/
        shard_00000.jsonl.gz
  ledger/
    collector_ledger.jsonl
  catalog/
    collector_catalog.json
```

**Notes**
- The collector outputs only **two pools**: `screened/green` and `screened/yellow`.
- The sorter reads `screened/*/shards/*.jsonl.gz` and produces curriculum outputs elsewhere.

---

## 3) Targets YAML changes (collector-only schema)

Each domain’s `targets_<domain>.yaml` currently includes fields intended for difficulty routing. For the collector, keep only what you need to:

- acquire
- prove license permission
- run basic content screening
- emit canonical records

### 3.1 Update `globals`

**Remove from collector config** (or ignore if present):
- `screened_yellow_root` (replace with `screened_root` + pool subdirs)
- `combined_root`
- `final_root`
- any `difficulty_*` fields

**Add / keep:**
- `raw_root`
- `screened_root`
- `evidence_root`
- `manifests_root`
- `queues_root`
- `ledger_root`
- `require_yellow_signoff` (manual gate)
- `sharding` (size + compression)
- `screening` (minimal rules)

Example (Windows-friendly):

```yaml
globals:
  raw_root: "E:/AI-Research/datasets/Natural/<domain>/raw"
  screened_root: "E:/AI-Research/datasets/Natural/<domain>/screened"
  evidence_root: "E:/AI-Research/datasets/Natural/<domain>/evidence"
  manifests_root: "E:/AI-Research/datasets/Natural/<domain>/_manifests"
  queues_root: "E:/AI-Research/datasets/Natural/<domain>/_queues"
  ledger_root: "E:/AI-Research/datasets/Natural/<domain>/_ledger"

  require_yellow_signoff: true

  sharding:
    max_shard_bytes: 268435456   # 256 MiB
    compress: true              # jsonl.gz

  screening:
    text_field_candidates: ["text", "content", "body", "article", "document"]
    min_chars: 200
    max_chars: 200000
    denylist_yaml: "./denylist.yaml"
    restriction_phrase_scan: true
    pii_basic_scan: true
    drop_if_non_english: false
```

### 3.2 Simplify each `target`

Keep:
- `id`, `name`, `enabled`, `priority`
- `license_profile` (declared license, evidence URLs/paths, restrictions)
- `resolver` + acquisition config (`download`, `build`, etc.)
- optional, small overrides:
  - `screening.text_fields` (if dataset uses nonstandard columns)
  - `record_filters` (explicit and minimal)

Remove / ignore in collector:
- `routing`, `topic_routing`, `difficulty_routing`, etc.
- category trees intended for curriculum folder output
- anything that assumes `final/` output

Minimal target shape:

```yaml
- id: <string>
  name: <string>
  enabled: true
  priority: 50

  license_profile:
    declared_spdx: "CC-BY-4.0"
    terms_url: "https://..."
    license_url: "https://..."
    evidence:
      - type: "url"
        url: "https://..."
      - type: "repo_file"
        path: "LICENSE"
    pool_hint: "GREEN"  # optional; else computed via license_map.yaml

  resolver: "huggingface_datasets"
  download:
    dataset_id: "org/name"
    splits: ["train"]
```

---

## 4) Code refactor pattern (apply to each pipeline folder)

### 4.1 Move out difficulty sorting (collector must not do it)
- Remove (or relocate to a new sorter repo/folder):
  - `difficulty_worker.py`
  - `difficulties_<domain>.yaml`

Update any docs and scripts that reference `final/` outputs.

### 4.2 Replace `yellow_screen_worker.py` with a unified `screen_worker.py`
Goal: one screening worker that can screen **both** GREEN and approved YELLOW.

**New worker behavior**
- Input: a queue (`green_queue.jsonl` or `yellow_queue_approved.jsonl`)
- Output: canonical shards under `screened/<pool>/shards/*.jsonl.gz`
- Always writes `manifests/<target_id>/evaluation.json` with:
  - records seen / kept
  - top reject reasons
  - pool decision + resolved license
  - evidence snapshot paths
- Appends to `ledger/collector_ledger.jsonl` (idempotent writes preferred)

**Implementation guidance**
- Start by copying your existing `yellow_screen_worker.py` → `screen_worker.py`.
- Remove any logic that assumes “yellow only”.
- Add a `--pool {green,yellow}` argument (or infer from queue item).
- Ensure GREEN also gets content screening (even if license is trusted).

> Keep `yellow_scrubber.py` / domain scrubbers only if they are strictly *pre-screen normalization*.  
> Anything that resembles “topic routing” or “difficulty” belongs in the sorter.

### 4.3 Keep acquisition as-is (but treat domain-specific fetchers as acquisition helpers)
`acquire_worker.py` is the correct abstraction. If you have extra workers like:
- `pmc_worker.py` (PubMed Central parsing)
- `nvd_worker.py`, `advisory_worker.py`, `stix_worker.py` (cyber sources)
- `mesh_worker.py` (3D assets)
- `code_worker.py` (repo/code packaging)
…keep them, but position them as **acquisition helpers** that output into `raw/<pool>/<target_id>/...` plus evidence snapshots when relevant.

Acquisition should not “understand” datasets beyond:
- where data is stored on disk
- what evidence was captured
- basic checksums/size metadata

### 4.4 Keep `review_queue.py` as the *only* manual step
For YELLOW pools:
- `review_queue.py` reads `yellow_queue.jsonl` + evidence snapshots
- you approve targets
- it outputs `yellow_queue_approved.jsonl`

**Collector must never auto-promote YELLOW → GREEN.**

### 4.5 Remove or demote `merge_worker.py`
Collectors should not need a “combined” stage. Prefer:
- `catalog/collector_catalog.json` lists shard paths and counts
- sorter reads shards via catalog or glob

If you keep `merge_worker.py`, restrict it to creating an **index** (not physically rewriting one giant file).

### 4.6 Simplify `catalog_builder.py` to be collector-only
Collector catalog should report:

Per target:
- pool (GREEN/YELLOW)
- resolved license SPDX + confidence
- acquisition type
- evidence snapshot paths
- #records acquired / kept
- bytes kept
- shard count
- top reject reasons
- manifest hash

Global totals:
- totals by pool
- totals by license class (CC-BY, MIT, PD, etc.)

Remove difficulty counts and anything assuming `final/dXX/...`.

---

## 5) Canonical record contract (Collector → Sorter)

Collectors must emit **one canonical JSONL schema** that all sorters can trust.

### 5.1 Base schema (domain-agnostic)
```json
{
  "id": "sha256:....",
  "text": "....",
  "source": {
    "target_id": "example_target",
    "resolver": "huggingface_datasets",
    "dataset": "org/name",
    "split": "train",
    "row_id": "12345",
    "url": null
  },
  "license": {
    "declared_spdx": "CC-BY-4.0",
    "resolved_spdx": "CC-BY-4.0",
    "pool": "GREEN",
    "evidence_dir": "E:/.../evidence/example_target/"
  },
  "meta": {
    "lang": "en",
    "doc_type": "text",
    "title": null,
    "created": null
  }
}
```

### 5.2 Optional extensions (use sparingly)
If a domain requires non-text payloads (e.g., 3D meshes, code archives), keep the **training input** as `text`, and attach non-text assets via metadata:

```json
{
  "meta": {
    "assets": [
      {"type": "mesh", "path": "raw/.../model.obj", "sha256": "..."},
      {"type": "code", "path": "raw/.../repo.tar.gz", "sha256": "..."}
    ]
  }
}
```

The sorter can decide whether to use assets, generate captions, etc. The collector’s job is only to keep them traceable and license-auditable.

---

## 6) Minimal screening rules (collector-safe, predictable)

Collector screening should be explainable and consistent across domains.

Recommended minimum checks:
1. Text exists (using `text_field_candidates` or per-target `text_fields`)
2. Length bounds (`min_chars`, `max_chars`)
3. Restriction phrase scan (“not for redistribution”, “no AI training”, “non-commercial only”, etc.)
4. PII basic scan (emails/phones/SSN patterns) — conservative
5. Denylist scan (`denylist.yaml`)
6. Light dedupe (optional): within a single target run (normalized text hash)

Everything else (topic routing, difficulty estimation, deep chunking, global near-dup) belongs in the sorter.

---

## 7) License screening and pool assignment (GREEN / YELLOW / RED)

Use `license_map.yaml` + per-target `license_profile` to classify.

### GREEN examples (typical)
- CC0 / Public Domain
- CC-BY (attribution is OK; keep attribution bundle)
- MIT / BSD / Apache-2.0 (when applicable)

### YELLOW examples
- license unclear or missing in dataset card
- custom terms pages that require reading
- ambiguous “no ML / no AI training” language
- evidence exists but confidence is below your threshold

### RED (skip entirely)
- All rights reserved
- Non-commercial-only (NC)
- No-derivatives (ND) for text corpora
- Explicit “no AI training” restrictions
- Anything failing your legal allowlist

**Write the decision to:**
- `manifests/<target_id>/manifest.json`
- `manifests/<target_id>/evaluation.json`
- each canonical record’s `license.pool`

---

## 8) Orchestration: simplest runnable sequence (JupyterLab-friendly)

Per pipeline folder:

```bash
python pipeline_driver.py --targets targets_<domain>.yaml --out-queues <...> --out-manifests <...>
python acquire_worker.py  --targets targets_<domain>.yaml --queue <queues>/green_queue.jsonl  --execute
python acquire_worker.py  --targets targets_<domain>.yaml --queue <queues>/yellow_queue.jsonl --execute
python review_queue.py    --targets targets_<domain>.yaml --queue <queues>/yellow_queue.jsonl --out <queues>/yellow_queue_approved.jsonl
python screen_worker.py   --targets targets_<domain>.yaml --queue <queues>/green_queue.jsonl  --pool green  --execute
python screen_worker.py   --targets targets_<domain>.yaml --queue <queues>/yellow_queue_approved.jsonl --pool yellow --execute
python catalog_builder.py --targets targets_<domain>.yaml --output <catalog>/collector_catalog.json
```

**Collector outputs are the end of this pipeline.** The sorter starts from `screened/*/shards`.

---

## 9) Repo implementation strategy (to update every pipeline consistently)

### Option A — Minimal change (duplicate code per pipeline)
- Apply the edits above separately inside each `*_pipeline_v2/` folder.
- Pros: lowest up-front refactor risk.
- Cons: duplicated fixes across pipelines.

### Option B — Recommended (shared `collector_core/` with thin wrappers)
Create a repo-level package, e.g.:

```
collector_core/
  __init__.py
  io.py
  license.py
  screening.py
  sharding.py
  evidence.py
  manifest.py
  catalog.py
```

Then each pipeline keeps small wrappers:
- `pipeline_driver.py` calls `collector_core.manifest + collector_core.queue`
- `acquire_worker.py` uses `collector_core.evidence` + its domain fetchers
- `screen_worker.py` uses `collector_core.screening + collector_core.sharding`

This makes “update every pipeline” a **one-change** job later.

---

## 10) Migration from existing outputs

If you already have:
- `raw/`
- `screened_yellow/`
- `combined/`
- `final/`

Recommended migration:
1. Keep `raw/` as-is (no re-download)
2. Re-run the new `screen_worker.py` for GREEN and approved YELLOW to produce `screened/<pool>/shards`
3. Ignore `combined/` and `final/` for collector going forward
4. Start the sorter pipeline from `screened/`

---

## 11) Smoke test checklist (do before scaling)

Pick **1 small GREEN target** and **1 small YELLOW target**.

Validate:
- [ ] queues generate
- [ ] evidence snapshots appear under `evidence/<target_id>/`
- [ ] acquisition lands under `raw/<pool>/<target_id>/`
- [ ] screening writes shards under `screened/<pool>/shards/`
- [ ] `evaluation.json` has sane reject reasons
- [ ] `collector_catalog.json` lists correct totals
- [ ] reruns are idempotent (no duplicate shards / ledger spam)

---

## 12) What the Sorter pipeline should assume

Sorter should assume:
- canonical JSONL records with stable `id`, `text`, `source`, `license`, `meta`
- pools are already separated (green/yellow)
- licenses are already vetted to your process standard
- evidence directories exist for audits

Sorter then:
- optionally chunks text
- estimates topic/difficulty
- writes curriculum outputs
- builds training-ready manifests
