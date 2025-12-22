# Chem pipeline v2 adaptation plan (chem_pipeline_v1 ➜ chem_pipeline_v2)

This document describes how to adapt **`chem_pipeline_v1`** into a **`chem_pipeline_v2`** that matches the *behavior and stage flow* of **`math_pipeline_v2`**.

## Target behavior (match math_pipeline_v2)

Chem v2 should follow the same six-stage flow and produce the same style of artifacts:

1. **Classify** targets + snapshot license evidence: `pipeline_driver.py`
2. **Acquire** GREEN and YELLOW into raw pools: `acquire_worker.py`
3. **Screen YELLOW** into canonical JSONL shards with strict pitch behavior: `yellow_screen_worker.py`
4. **Merge** canonical GREEN + screened YELLOW into combined shards (dedupe on `content_sha256`): `merge_worker.py`
5. **Final screen + difficulty assignment** into `d01..d10` shard folders: `difficulty_worker.py`
6. **Catalog** over the new layout: `catalog_builder.py`

Where “canonical record” means a JSON object shaped like:

```json
{
  "record_id": "...",
  "text": "...",
  "source": {
    "target_id": "...",
    "origin": "...",
    "source_url": "...",
    "license_spdx": "...",
    "license_profile": "permissive|copyleft|quarantine",
    "license_evidence": "...",
    "retrieved_at_utc": "..."
  },
  "routing": {"subject": "chem", "domain": "...", "category": "...", "level": 1},
  "hash": {"content_sha256": "..."}
}
```

> In v2, license pools are always `{permissive|copyleft|quarantine}` and the folder layout always segregates by license pool.

---

## Current repo reality

From the provided zips:

- **chem v1** (`chem_pipeline_v1/`) is the older flow:
  - `pipeline_driver.py` emits GREEN/YELLOW/RED queues under `_queues/`.
  - `download_worker.py` downloads GREEN to `pools/{pool}/{target_id}`.
  - `yellow_scrubber.py` performs chemistry-specific “safe extraction / filtering” to promote subsets.
  - `pmc_worker.py` downloads+chunks allowlisted PMC OA.
  - `catalog_builder.py` builds a large “global catalog”.

- **math v2** (`math_pipeline_v2/`) is the new flow we’re matching:
  - `acquire_worker.py` downloads to `raw/{green|yellow}/{license_pool}/{target_id}`.
  - `yellow_screen_worker.py` converts YELLOW raw artifacts → canonical sharded JSONL.
  - `merge_worker.py` merges canonical GREEN JSONL + screened YELLOW shards.
  - `difficulty_worker.py` does final filter + difficulty shard writing.
  - `catalog_builder.py` summarizes the new layout.

The main adaptation is **re-homing chemistry-specific transforms into the v2 stage boundaries**, without reintroducing a separate “yellow_scrubber stage” that breaks the math v2 flow.

---

## Target directory layout (chem v2)

Mirror the math v2 layout, but under `/data/chem/`:

```
/data/chem/
  raw/
    green/{license_pool}/{target_id}/...
    yellow/{license_pool}/{target_id}/...
  screened_yellow/{license_pool}/shards/*.jsonl.gz
  combined/{license_pool}/shards/*.jsonl.gz
  final/{license_pool}/d01..d10/shards/*.jsonl.gz
  _ledger/*.jsonl
  _pitches/*.jsonl
  _queues/*.jsonl
  _manifests/{target_id}/*
  _catalogs/*.json
```

Key rule for v2 compatibility:

- Anything that reaches **merge** must already be **canonical JSONL**.
  - In math v2, merge reads canonical JSONL from `raw/green/**.jsonl*`.
  - For chem v2, either (a) ensure GREEN targets acquire as JSONL, or (b) treat non-JSONL “extractable” sources as YELLOW so they are canonicalized during **Screen YELLOW**.

---

## Recommended chem_pipeline_v2 repo skeleton

Start from the math v2 layout and rename math-specific files:

```
chem_pipeline_v2/
  README.md
  requirements.txt
  run_pipeline.sh

  pipeline_driver.py
  acquire_worker.py
  yellow_screen_worker.py
  merge_worker.py
  difficulty_worker.py
  catalog_builder.py
  review_queue.py

  targets_chem.yaml
  difficulties_chem.yaml

  license_map.yaml
  field_schemas.yaml
  denylist.yaml

  (optional legacy / reference)
  pmc_worker.py
  yellow_scrubber.py
```

Notes:

- **Keep** `review_queue.py` as-is (it’s already present in math v2 and chem v1).
- If you keep `pmc_worker.py` and `yellow_scrubber.py` for reference, mark them *legacy* and/or integrate their logic into `yellow_screen_worker.py` (preferred).

---
## Implementation strategy

The fastest, lowest-risk path is:

1. **Copy `math_pipeline_v2/` as the starting point** for `chem_pipeline_v2/`.
2. Replace math-specific configuration + naming with chemistry equivalents.
3. Port or re-home the chemistry-specific functionality from v1 into the v2 scripts **without adding extra stages**.

Concretely:

- Treat **math v2 scripts as the contract** (CLI flags, stage outputs, ledgers).
- Add chemistry capabilities by extending `acquire_worker.py` and `yellow_screen_worker.py` (plugins/modes), not by bringing back a standalone “yellow_scrubber stage”.

---

## File-level mapping (v1 ➜ v2)

| v1 file | v2 equivalent | Action |
| --- | --- | --- |
| `pipeline_driver.py` | `pipeline_driver.py` | Base off math v2; add `chem_routing` support and remove math-only output fields. |
| `download_worker.py` | `acquire_worker.py` | Replace; port missing download strategies (Figshare, GitHub releases) and chem-friendly options (integrity checks, retry defaults). |
| `yellow_scrubber.py` | `yellow_screen_worker.py` | Re-home chem transforms as **screen plugins** (PubChem computed-only, PMC allowlist, MoNA/GNPS record-level filters). |
| `pmc_worker.py` | `yellow_screen_worker.py` plugin (or helper module) | Fold “PMC download+chunk” into YELLOW screening for the PMC target(s). |
| `catalog_builder.py` (large) | `catalog_builder.py` (v2 summary) | Use v2 summary catalog; optionally add token/statistics later. |
| `run_pipeline.sh` (v1) | `run_pipeline.sh` (v2 stages) | Replace with v2 stage names: `classify|acquire_green|acquire_yellow|screen_yellow|merge|difficulty|catalog`. |
| `targets.yaml` | `targets_chem.yaml` | Rewrite globals to v2 roots; migrate targets; keep companion files. |
| `difficulties_math.yaml` | `difficulties_chem.yaml` | New rubric + subject/domain/category map for chemistry. |

---

## Step 1 — Create the chem_pipeline_v2 skeleton

Start from the math v2 tree and rename configs:

- Copy these files from `math_pipeline_v2/`:
  - `pipeline_driver.py`
  - `acquire_worker.py`
  - `yellow_screen_worker.py`
  - `merge_worker.py`
  - `difficulty_worker.py`
  - `catalog_builder.py`
  - `review_queue.py`
  - `license_map.yaml`, `denylist.yaml`, `field_schemas.yaml`
  - `run_pipeline.sh`, `requirements.txt`, `todo.txt`

- Add chem-specific files:
  - `targets_chem.yaml`
  - `difficulties_chem.yaml`

- Keep optional helpers if you want them as standalone debug tools:
  - `pmc_worker.py` (but do *not* make it a required stage)
  - `yellow_scrubber.py` (but treat as deprecated; see Appendix)

---

## Step 2 — Migrate targets.yaml ➜ targets_chem.yaml (v2 globals + chem inventory)

### 2.1 Globals: map v1 roots to v2 roots

In **math v2**, `targets_math.yaml -> globals` is the source of truth for roots.

Update chem to use these keys (same semantics):

- `raw_root`
- `screened_yellow_root`
- `combined_root`
- `final_root`
- `ledger_root`
- `pitches_root`
- `manifests_root`
- `queues_root`
- `catalogs_root`
- `logs_root`

**Recommended defaults (WSL/Linux):**

```yaml
globals:
  raw_root: /data/chem/raw
  screened_yellow_root: /data/chem/screened_yellow
  combined_root: /data/chem/combined
  final_root: /data/chem/final
  ledger_root: /data/chem/_ledger
  pitches_root: /data/chem/_pitches
  manifests_root: /data/chem/_manifests
  queues_root: /data/chem/_queues
  catalogs_root: /data/chem/_catalogs
  logs_root: /data/chem/_logs
```

### 2.2 Companion files

Keep the v2 pattern used in math:

```yaml
companion_files:
  license_map: ./license_map.yaml
  field_schemas: ./field_schemas.yaml
  denylist: ./denylist.yaml
  difficulties_map: ./difficulties_chem.yaml
```

### 2.3 Target entries: what carries over unchanged

Most per-target fields in v1 can be copied directly:

- `id`, `name`, `enabled`, `priority`
- `license_profile`, `license_evidence`, `data_type`
- `download` (but validate strategy-specific fields; see Step 4)
- `text_processing` (still useful for PMC chunking plugins)
- `split_group_id` (still emitted by `pipeline_driver.py`)

### 2.4 Targets that need a structural change

Chem v1 contains targets that are expressed as **“build/derived”** targets (e.g., PubChem raw SDF ➜ derived computed-only JSONL). Math v2 doesn’t have a dedicated “build stage”, so you have two options:

**Option A (recommended): express the derived output as a YELLOW screening plugin**

- Keep the *raw* acquisition target (`pubchem_compound_sdf_bulk`) as YELLOW (record-level).
- Move the computed-only extraction into `yellow_screen_worker.py` as a plugin.
- The plugin writes canonical JSONL shards into `screened_yellow/permissive/shards/...`.

**Option B: treat derived targets as their own acquisition targets**

- Only works if you can acquire the derived JSONL directly from a source.
- (For PubChem computed-only, you typically *cannot*; it’s a transform.)

For chemistry, **Option A** is the correct fit.

---

## Step 3 — Update pipeline_driver.py for chemistry routing

Math v2 `pipeline_driver.py` already supports a **generic** `routing` object and can fall back to `math_routing`. To make chem v2 consistent:

### 3.1 Add `chem_routing` support

Update `resolve_routing_fields(target: dict)` to consider, in this order:

1. `target["routing"]`
2. `target["chem_routing"]`
3. `target["math_routing"]` (legacy/back-compat)

And ensure the resolved object includes:

- `subject` default: `chem`
- `domain` default: `misc`
- `category` default: `misc`
- `level` default: from target or `globals.screening.default_level` (or 5)
- `granularity` default: `target`

### 3.2 Stop emitting math-only convenience keys

Math v2 emits duplicated keys like `math_domain`, `math_category`, etc. In chem v2, either:

- **(preferred)** emit only the generic `routing_*` keys + keep `routing` as the canonical field, or
- emit `chem_domain`, `chem_category`, ... instead of `math_*`.

Downstream v2 workers (`yellow_screen_worker.py`, `difficulty_worker.py`) rely on `routing` (or can be made to), so you don’t need math-specific duplicates.

### 3.3 Queue file names

Keep the **same queue names** as math v2 (downstream scripts expect these names):

- `_queues/green_download.jsonl`
- `_queues/yellow_pipeline.jsonl`
- `_queues/red_rejected.jsonl`

---

## Step 4 — Update acquire_worker.py for chemistry acquisition

Math v2 acquisition is intentionally minimal. Chem v1 supports additional acquisition patterns that matter in chemistry:

### 4.1 Port missing download strategies

Add to `STRATEGY_HANDLERS` (from chem v1 `download_worker.py`):

- `figshare`
- `github_release`

These should write payloads into:

- `raw/{green|yellow}/{license_pool}/{target_id}/...`

and write `download_manifest.json` under that target directory (matching v2 behavior).

### 4.2 Keep v2 run controls

Retain the v2 flags and semantics:

- Dry-run by default; only download when `--execute`.
- Respect `--overwrite`.
- Respect limits: `--limit-targets`, `--limit-files`, `--max-bytes-per-target`.
- Keep `acquire_done.json` marker under `_manifests/{target_id}/`.

### 4.3 Strategy field normalization

Chem v1 targets sometimes use:

- `download.urls` (list) vs `download.url` (string)
- `ftp.base_url` + `include_globs`

Math v2’s handlers cover some of this already. Ensure the chem-extended handlers accept the chem v1 shapes.

### 4.4 Integrity hooks (optional but useful)

Chem v1 supports:

- size verification
- sha256 computation
- Zenodo md5 verification

Math v2 already includes `--verify-sha256` and `--verify-zenodo-md5`; keep those and allow per-target overrides if you want.

---

## Step 5 — Update yellow_screen_worker.py to handle chemistry formats

This is the most important re-home.

Math v2 `yellow_screen_worker.py` assumes raw YELLOW is already JSONL/JSONL.gz and applies:

- strict parsing
- content hashing
- sharding
- `yellow_passed.jsonl` / `yellow_pitched.jsonl`
- done markers

Chemistry needs additional **input parsers** and **record-level licensing transforms**.

### 5.1 Keep the v2 screening contract

No matter the input format, *every screening plugin must output the same canonical JSONL record schema* used by math v2, and must write:

- `screened_yellow/{license_pool}/shards/yellow_00000.jsonl.gz`
- `_ledger/yellow_passed.jsonl`
- `_ledger/yellow_pitched.jsonl`
- `_manifests/{target_id}/yellow_done.json` (or whatever marker v2 uses)

### 5.2 Add a plugin/mode mechanism (recommended)

Add per-target `yellow_screen` overrides in `targets_chem.yaml`, e.g.:

```yaml
yellow_screen:
  mode: pubchem_computed_only
  input_glob: "*.sdf.gz"
  output_pool: permissive
  field_schema_version: pubchem_computed_only_v1.0.0
  include_fields: ["PUBCHEM_COMPOUND_CID", "PUBCHEM_IUPAC_INCHIKEY", ...]
  sharding:
    method: cid_range
    range_size: 1000000
```

Then implement `MODE_HANDLERS = {"jsonl": ..., "pubchem_computed_only": ..., "pmc_oa": ..., ...}`.

If `yellow_screen.mode` is not set, default to the existing math v2 behavior (`jsonl`).

### 5.3 Re-home chem v1 transforms as screening plugins

#### (A) PubChem computed-only extraction

Source in v1: `yellow_scrubber.py::extract_pubchem_computed_only()`.

v2 plugin behavior:

- Input: `raw/yellow/quarantine/pubchem_compound_sdf_bulk/*.sdf.gz`
- Parse SDF tags.
- Keep *only* the allowlisted computed fields.
- Validate/cast using `field_schemas.yaml` (optional but recommended).
- Output canonical JSONL records.
- Route output to `permissive` pool (computed-only derived).

Pitch cases:

- missing CID
- schema validation failure (if strict)
- record too empty / no usable fields

#### (B) PMC OA allowlist + download + chunk

Source in v1:

- allowlist planning: `yellow_scrubber.py` (PMC OA list parsing)
- download+chunk: `pmc_worker.py`

v2 plugin behavior:

- Input: `raw/yellow/quarantine/pmc_oa_fulltext/...` (e.g., the OA list or cached tarballs)
- Produce canonical text chunks as JSONL records.
- Attach per-record license metadata if available; otherwise route conservatively to `quarantine`.

Important constraint: PMC OA is **mixed licensing**. To stay safe:

- default to `license_profile: quarantine`
- only promote to `permissive` when the record has strong evidence of a permissive license (e.g., CC-BY, CC0, or public domain) and you capture that evidence in `source`.

#### (C) MoNA / GNPS record-level filtering

Chem v1 has record-level filtering hooks for MoNA/GNPS.

v2 plugin behavior:

- Input: `raw/yellow/quarantine/{mona|gnps}/...`
- Emit only records with valid per-record license/attribution fields.
- Attach the record-level license evidence.
- Route pool based on resolved record license; otherwise pitch.

### 5.4 Routing fields in screened records

Math v2 screening populates routing from:

- the queue row (ideal)
- or `raw["routing"]` / `raw["math_routing"]`

Update it to also consider `raw["chem_routing"]`, and always set:

```json
"routing": {"subject": "chem", "domain": ..., "category": ..., "level": ...}
```

### 5.5 What about GREEN canonicalization?

In math v2, `merge_worker.py` assumes **GREEN raw contains canonical JSONL already** (it just reads `**/*.jsonl*`).

For chem v2, keep the same assumption to match behavior:

- Mark sources GREEN only when they already arrive as JSONL/JSONL.gz in the desired schema, **or**
- treat them as YELLOW so the screening stage converts them.

If you later want “screen green too”, do it as an *enhancement* (do not change v2 semantics now).

---

## Step 6 — Merge worker (merge_worker.py)

`merge_worker.py` can stay identical to math v2.

Checklist:

- Ensure canonical records include `hash.content_sha256`.
- Ensure `source.license_profile` is set correctly so `route_pool()` places records into the right `{permissive|copyleft|quarantine}` combined folder.

Outputs:

- `combined/{license_pool}/shards/combined_00000.jsonl.gz`
- `_ledger/combined_index.jsonl`

---

## Step 7 — Difficulty assignment (difficulty_worker.py + difficulties_chem.yaml)

Math v2 assigns difficulty using a YAML rubric and a per-record `routing` field.

For chem v2:

1. Create `difficulties_chem.yaml` with:
   - scale 1–10
   - `subjects.chem.domains.*.categories.*.level.default/min/max`
   - defaults: subject=chem, level=5
2. Update `targets_chem.yaml -> companion_files.difficulties_map`.
3. Ensure records carry `routing.domain` + `routing.category`.

Outputs:

- `final/{license_pool}/d01..d10/shards/final_00000.jsonl.gz`
- `_ledger/final_index.jsonl`

---

## Step 8 — Catalog builder (catalog_builder.py)

Use the math v2 `catalog_builder.py` as-is (it summarizes by stage and pool).

If you miss v1’s heavyweight catalog features (token estimates, per-dataset stats), add them later as optional catalog “extensions” (do not block v2 parity on this).

---

## Step 9 — run_pipeline.sh orchestration

Math v2 expects these stages:

- `classify`
- `acquire_green`
- `acquire_yellow`
- `screen_yellow`
- `merge`
- `difficulty`
- `catalog`
- `all`

Update chem’s wrapper accordingly.

Key differences from chem v1:

- Remove standalone `yellow` and `pmc` stages.
- Replace `download` with `acquire_green` / `acquire_yellow`.
- Replace `catalog` to point to the v2 catalog output (e.g., `/data/chem/_catalogs/catalog.json`).

---

## Step 10 — Migration checklist (do this in order)

1. **Create `chem_pipeline_v2/` from math v2** skeleton.
2. Add `targets_chem.yaml` with v2 `globals` roots + chem target inventory.
3. Add `difficulties_chem.yaml` and wire it in via `companion_files`.
4. Patch `pipeline_driver.py`:
   - resolve `chem_routing`
   - emit routing fields consistently
5. Patch `acquire_worker.py`:
   - add `figshare` + `github_release` strategies
   - normalize chem-style `download` fields
6. Patch `yellow_screen_worker.py`:
   - add mode/plugin support
   - implement `pubchem_computed_only` and `pmc_oa` (at minimum)
   - optionally implement MoNA/GNPS record-level screening
7. Keep `merge_worker.py`, `difficulty_worker.py`, `catalog_builder.py` unchanged unless you must add `chem_routing` fallback.
8. Update `README.md` to describe the chem v2 flow and the directory layout.

---

## Step 11 — Test plan (parity + safety)

Run tests in increasing cost order:

### 11.1 Classify only (dry run)

- Run `pipeline_driver.py` to generate `_queues/*.jsonl` and `_manifests/{target_id}/evaluation.json`.
- Confirm YELLOW targets are correctly marked as `queue_bucket=YELLOW`.

### 11.2 Acquire (execute) on 1–2 tiny targets

- Choose a small HTTP dataset and a small GitHub release.
- Confirm outputs land in `raw/green/...` or `raw/yellow/...`.
- Confirm each has `download_manifest.json`.

### 11.3 Screen YELLOW (execute) on one plugin

- Start with PubChem on a tiny subset:
  - use `--limit-files 1` and/or `--max-bytes-per-target`.
- Confirm:
  - `screened_yellow/permissive/shards/yellow_00000.jsonl.gz` exists
  - `_ledger/yellow_passed.jsonl` contains entries
  - `_ledger/yellow_pitched.jsonl` contains rejected records

### 11.4 Merge + difficulty

- Ensure `combined/.../shards/*.jsonl.gz` and `final/.../dXX/shards/*.jsonl.gz` are produced.
- Confirm `final_index.jsonl` exists.

### 11.5 Spot-check safety invariants

- No record in `permissive` pool has an incompatible license.
- PMC records remain in `quarantine` unless per-record evidence supports promotion.
- Content hashes exist and are stable.

---

## Appendix A — Minimal targets_chem.yaml header template

```yaml
schema_version: "0.8"
updated_utc: "2025-12-21T00:00:00Z"

globals:
  raw_root: /data/chem/raw
  screened_yellow_root: /data/chem/screened_yellow
  combined_root: /data/chem/combined
  final_root: /data/chem/final
  ledger_root: /data/chem/_ledger
  pitches_root: /data/chem/_pitches
  manifests_root: /data/chem/_manifests
  queues_root: /data/chem/_queues
  catalogs_root: /data/chem/_catalogs
  logs_root: /data/chem/_logs

  require_yellow_signoff: true

  sharding:
    max_records_per_shard: 50000
    compression: gzip

  screening:
    strict_json: true
    min_text_chars: 32

companion_files:
  license_map: ./license_map.yaml
  field_schemas: ./field_schemas.yaml
  denylist: ./denylist.yaml
  difficulties_map: ./difficulties_chem.yaml

# targets:
#   - id: ...
```

---

## Appendix B — Minimal difficulties_chem.yaml skeleton

```yaml
schema_version: "2.0"
updated_utc: "2025-12-21T00:00:00Z"

globals:
  default_subject: chem
  default_domain: misc
  default_category: misc
  default_level: 5
  folder_layout: final/{license_pool}/d{level:02d}/{subject}/{domain}/{category}

rubric:
  scale:
    min: 1
    max: 10
    name: "Difficulty 1–10"
  levels:
    1: {label: Foundations, description: "Atoms, molecules, units, simple stoichiometry.", signals: ["single-step", "definitions"]}
    5: {label: Undergraduate core, description: "P-chem/ochem/analytical core concepts.", signals: ["multi-step", "equilibria", "mechanisms"]}
    10: {label: Research frontier, description: "Specialized papers / advanced theory.", signals: ["research", "dense jargon"]}

subjects:
  chem:
    name: Chemistry
    domains:
      general_chem:
        name: General chemistry
        categories:
          stoichiometry:
            level: {default: 2, min: 1, max: 3}
          thermodynamics:
            level: {default: 5, min: 4, max: 6}
      organic:
        name: Organic chemistry
        categories:
          reactions_mechanisms:
            level: {default: 6, min: 5, max: 7}
```

---

## Appendix C — Legacy scripts (keep, but don’t make them stages)

- `yellow_scrubber.py`: keep as a reference implementation for chem-specific parsing and transformations; migrate required behavior into `yellow_screen_worker.py` plugins.
- `pmc_worker.py`: keep as a debug tool; production flow should call it from within YELLOW screening (or share its helper functions).
- `download_worker.py`: replaced by `acquire_worker.py`.

