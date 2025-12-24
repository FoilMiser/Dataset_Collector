# Safety Incident Pipeline v2 Adaptation Plan (from `safety_incident_pipeline_v1`)

Goal: create `safety_incident_pipeline_v2` that behaves like **`math_pipeline_v2`**:
- **Classify** targets + snapshot license evidence → emit GREEN/YELLOW/RED queues
- **Acquire** GREEN + YELLOW into `raw/green/...` and `raw/yellow/...`
- **Screen YELLOW** into canonical, privacy-safe JSONL shards + pass/pitch ledgers
- **Merge** canonical GREEN + screened YELLOW into combined shards with lightweight dedupe
- **Difficulty**: final light screen + difficulty assignment → write `final/{license_pool}/d01..d10/...` shards + ledger
- **Catalog**: summarize counts/bytes/ledgers for reproducibility

This doc is written as an implementation checklist you can hand to Codex.

---

## 1) What “same behavior as `math_pipeline_v2`” means

### Stage order and outputs
Mirror the math v2 stage contract:

| Stage | Inputs | Outputs | “Behavior” to match |
|---|---|---|---|
| `classify` | `targets_*.yaml` + evidence URLs | `_manifests/{target_id}/...`, `_queues/green_download.jsonl`, `_queues/yellow_pipeline.jsonl`, `_queues/red_rejected.jsonl`, `_queues/run_summary.json` | License normalization + restriction phrase scan + denylist → conservative GREEN/YELLOW/RED |
| `acquire_green` | green queue | `raw/green/{license_pool}/{target_id}/...` + per-target download manifest | Dry-run by default; `--execute` writes |
| `acquire_yellow` | yellow queue | `raw/yellow/{license_pool}/{target_id}/...` | Same as above |
| `screen_yellow` | raw yellow pool | `screened_yellow/{license_pool}/shards/*.jsonl.gz`, `_ledger/yellow_pass.jsonl`, `_pitches/yellow_pitch.jsonl`, `_manifests/{target_id}/screen_yellow_done.json` | “Anything unclear is pitched” + privacy gates |
| `merge` | raw green (canonical records) + screened yellow | `combined/{license_pool}/shards/*.jsonl.gz`, `_ledger/combined_index.jsonl`, `_ledger/merge_summary.json` | Dedupe on `content_sha256` |
| `difficulty` | combined shards + difficulties map | `final/{license_pool}/d01..d10/shards/*.jsonl.gz`, `_ledger/difficulty_index.jsonl`, `_ledger/difficulty_summary.json` | Rules-first routing → fallback heuristic |
| `catalog` | ledgers + shard dirs | `_catalogs/*` (or `_ledger/*` summaries) | One place to audit what was produced |

### Directory layout
Adopt the math v2 layout (keep `/data/safety` root to stay consistent with v1):

```
/data/safety/
  raw/
    green/{license_pool}/{target_id}/...
    yellow/{license_pool}/{target_id}/...
  screened_yellow/{license_pool}/shards/*.jsonl.gz
  combined/{license_pool}/shards/*.jsonl.gz
  final/{license_pool}/d01..d10/shards/*.jsonl.gz
  _ledger/*.jsonl
  _pitches/*.jsonl
  _queues/*.jsonl
  _manifests/{target_id}/...
```

---

## 2) v1 → v2 stage mapping

`safety_incident_pipeline_v1` stages today (from `run_pipeline.sh`):
- `classify` (OK)
- `review` (manual helper; keep as **standalone**, not in main v2 stage list)
- `download` (GREEN download)
- `yellow` (YELLOW transformations)
- `pmc` (optional)
- `catalog`

Map to math v2 semantics:

| v1 stage | v2 stage(s) | Notes |
|---|---|---|
| `classify` | `classify` | Replace v1 `pipeline_driver.py` with math v2 driver behavior (adds `queue_bucket`, `output_pool`, `routing_*` fields). |
| `download` | `acquire_green` | Replace/rename `download_worker.py` → `acquire_worker.py` (math v2 interface). Keep extra strategies as optional extension. |
| `yellow` | `acquire_yellow` + `screen_yellow` | Split: first download raw YELLOW artifacts, then canonicalize/redact into shards. |
| (none) | `merge` | New stage (copy from math v2). |
| (none) | `difficulty` | New stage (copy from math v2 + safety difficulties map). |
| `catalog` | `catalog` | Update catalog builder to read the new roots + ledgers. |
| `review` | (unchanged helper) | Keep `review_queue.py` exactly as-is; call it manually. |
| `pmc` | optional helper | Keep only if you still use PMC targets; not part of core v2 run order. |

---

## 3) Repository/file changes to create `safety_incident_pipeline_v2`

### 3.1 Copy-over baseline v2 workers from math
Start `safety_incident_pipeline_v2/` by cloning these from `math_pipeline_v2/`:

- `pipeline_driver.py`  ✅ (then remove math-specific routing fields or generalize)
- `acquire_worker.py`   ✅ (then add missing strategies if safety targets need them)
- `yellow_screen_worker.py` ✅ (replace extraction + gating logic with safety logic)
- `merge_worker.py`     ✅ (path defaults `/data/safety/...`)
- `difficulty_worker.py` ✅ (path defaults + heuristics)
- `catalog_builder.py`  ✅ (path defaults + include safety schemas)
- `review_queue.py`     ✅ (can keep the v1 version; compatible)
- `requirements.txt`    ✅ (merge; keep optional deps gated)
- `run_pipeline.sh`     ✅ (make it actually call the workers; stage list identical to math v2)

Keep from v1 (domain-specific):
- `field_schemas_safety_incident.yaml`
- `license_map.yaml`
- `denylist.yaml`

### 3.2 Replace the v1 globals schema in targets
Create a new `targets_safety_incident.yaml` (or keep name `targets.yaml` but match v2 keys) with math v2-compatible globals:

```yaml
companion_files:
  license_map: ./license_map.yaml
  field_schemas: ./field_schemas_safety_incident.yaml
  denylist: ./denylist.yaml
  difficulties_map: ./difficulties_safety_incident.yaml

globals:
  raw_root: /data/safety/raw
  screened_yellow_root: /data/safety/screened_yellow
  combined_root: /data/safety/combined
  final_root: /data/safety/final
  ledger_root: /data/safety/_ledger
  pitches_root: /data/safety/_pitches
  manifests_root: /data/safety/_manifests
  queues_root: /data/safety/_queues

  sharding:
    max_records_per_shard: 50000
    compression: gzip

  screening:
    min_chars: 200
    max_chars: 12000
```

**Important:** remove v1-only keys like `storage_root`, `staging_root`, `pools: ...` and instead use `raw_root` + `{license_pool}` subfolders.

### 3.3 Introduce *generic routing* for difficulty + folder layout
For each target, add a `routing:` block (preferred) and optionally `safety_routing:` for backwards-compat.

Example (structured incident tables):
```yaml
- id: phmsa_pipeline_incidents
  ...
  license_profile: permissive
  routing:
    subject: safety_incident
    domain: incident_tables
    category: pipelines_hazmat
    level: 3
    granularity: target
    confidence: 0.9
    reason: "Structured public dataset; minimal narrative"
```

Example (investigation narrative reports):
```yaml
routing:
  subject: safety_incident
  domain: incident_reports
  category: investigations_root_cause
  level: 6
  granularity: target
  confidence: 0.8
  reason: "Multi-factor investigations; recommendations; causal graphs"
```

---

## 4) Safety v2 data model (canonical record)

To match the merge/difficulty workers, **both GREEN and screened-YELLOW must produce JSONL records** with:

- `text` (string) — the model training text
- `hash.content_sha256` — stable dedupe key
- `source` dict — includes `target_id`, `license_profile`, `license_evidence`, `retrieved_at_utc`, `url(s)`
- `routing` dict — `{subject, domain, category, level?}` for difficulty assignment
- optional `meta` — dates, coarse location, severity, modality, etc.

Recommended shape (compatible with your existing schema):

```json
{
  "record_id": "phmsa_pipeline_incidents:row:00001234",
  "schema_version": "incident_event_row_v1.0.0",
  "text": "...",
  "meta": {...},
  "routing": {"subject":"safety_incident","domain":"incident_tables","category":"pipelines_hazmat"},
  "source": {"target_id":"phmsa_pipeline_incidents","license_profile":"permissive","spdx":"US-PUBLIC-DOMAIN","url":"..."},
  "hash": {"content_sha256":"..."}
}
```

**Key constraint:** `merge_worker.py` reads `raw/green/**/.jsonl*` and `screened_yellow/**/.jsonl*`. If GREEN targets produce PDFs/XLSX only, the merge stage will ignore them. See §6.3 for how to handle this safely.

---

## 5) Implementing `yellow_screen_worker.py` for safety incidents

Start from math v2’s worker, but swap the core “screen/extract” logic.

### 5.1 Inputs
- `targets_safety_incident.yaml` (for roots + per-target rules)
- `raw/yellow/{pool}/{target_id}/...` downloaded artifacts

### 5.2 Outputs (must match math v2 expectations)
- `screened_yellow/{pool}/shards/screened_yellow_00000.jsonl.gz`
- `_ledger/yellow_pass.jsonl` (index of accepted records)
- `_pitches/yellow_pitch.jsonl` (anything unclear is pitched + reason)
- `_manifests/{target_id}/screen_yellow_done.json` (idempotency)

### 5.3 Safety-specific gates (defaults should be conservative)
Implement gates as ordered “fail-closed” checks:

1) **License gate**  
   - Only process YELLOW targets that have a review decision OR explicit `yellow_screen.allow: true`.  
   - Otherwise pitch *all* records with reason `yellow_requires_signoff`.

2) **PII redaction & location coarsening**  
   Apply to narrative sources and any field with people/addresses:
   - Remove emails/phones, exact street addresses, IDs
   - Replace personal names with placeholders only if confident (avoid false positives)
   - Coarsen location to state/province/country (keep `location_general` only)
   - Drop victim/patient names, witness statements, direct quotes that identify individuals

3) **Sensitive content filters**  
   - Drop records containing explicit personal medical details tied to a person
   - Drop records with precise coordinates, full addresses, or unique incident IDs that are linkable *unless* the source is expressly public + policy-approved

4) **Text extraction rules**  
   - PDFs: extract text; remove headers/footers; chunk by sections (“Findings”, “Recommendations”)
   - HTML: strip nav; keep article body
   - CSV/XLSX: render rows into compact “event row narratives” (controlled template) for training

5) **Schema validation**  
   Validate against `field_schemas_safety_incident.yaml`:
   - `incident_report_chunk_v1.0.0` for narrative chunks
   - `incident_event_row_v1.0.0` for row-form events

6) **Hashing + routing**  
   - Compute `content_sha256` from normalized `text` + key meta fields
   - Attach `routing` based on target routing plus per-record overrides (if available)

### 5.4 “Anything unclear is pitched” examples
Pitch a record when:
- the source has ambiguous third-party content rights (e.g., embedded copyrighted images or quotes)
- extraction yields mostly boilerplate/navigation
- PII redaction cannot confidently remove identifiers
- record is too short/too long outside screening bounds

---

## 6) Making GREEN compatible with merge/difficulty

### 6.1 The math v2 contract you must satisfy
`merge_worker.py` only merges JSONL records found under:
- `raw/green/{pool}/{target_id}/**/*.jsonl*`

So GREEN must yield canonical JSONL records.

### 6.2 Recommended policy for safety data
To keep safety conservative (and avoid silent “green ignored”):
- **GREEN only for sources that are already structured + low-PII** (or can be rendered into de-identified rows deterministically).
- Put narrative investigation reports in **YELLOW** even if public-domain, because privacy + third-party attachments often exist.

### 6.3 Two implementation options (pick one)
**Option A (closest to math v2):** green sources are already JSONL  
- Prefer HF datasets, pre-exported CSV→JSONL sources, or repositories that ship `.jsonl(.gz)`.

**Option B (practical for safety):** add a *green normalization hook*  
Keep stage list the same but extend `acquire_worker.py`:
- If target has `download.emit_canonical_jsonl: true`, then after download:
  - convert CSV/XLSX to canonical event rows (`incident_event_row_v1.0.0`)
  - write `raw/green/{pool}/{target_id}/records.jsonl.gz`
This preserves the stage structure while satisfying merge’s expectation.

---

## 7) Difficulty mapping for safety incidents (1–10)

### 7.1 Define a safety rubric (analogous to math v2)
Create `difficulties_safety_incident.yaml` with a 1–10 rubric tuned to safety engineering:

**Level 1 — Safety basics**  
Simple definitions, PPE, signage, “what happened” summaries.

**Level 2 — Operational procedures**  
SOPs, checklists, basic hazard categories, simple compliance reminders.

**Level 3 — Simple incident datasets & summaries**  
Structured tables, single-cause narratives, basic rates/trends.

**Level 4 — Standard investigations**  
Timeline + contributing factors, 5-Whys, fishbone, basic regulatory interpretation.

**Level 5 — Safety management systems**  
SMS/PSM basics, audit findings, barrier language, moderate technical depth.

**Level 6 — Engineering analysis**  
Fault tree/event tree intro, reliability concepts, human factors, multi-factor causality.

**Level 7 — Systems safety methods**  
STPA/STAMP intro, bow-tie at depth, safety cases, complex multi-system interactions.

**Level 8 — Quantitative risk & advanced standards**  
PRA, Bayesian/Monte Carlo, IEC/NFPA/ISO standards at depth, SIL allocation logic.

**Level 9 — Advanced compliance + technical modeling**  
Cross-regime compliance, complex legal/engineering interplay, dense modeling papers.

**Level 10 — Research frontier**  
State-of-the-art methods, large-scale simulation, formal verification/safety proofs.

### 7.2 Domain/category mapping (defaults)
Use the same `subjects/domains/categories` structure as math:

```yaml
subjects:
  safety_incident:
    domains:
      incident_tables:
        categories:
          pipelines_hazmat: { level: { default: 3, min: 2, max: 4 }, notes: "Row-form incidents; mostly descriptive." }
          aviation_occurrence: { default: 4, min: 3, max: 5 }
          rail_occurrence: { default: 4, min: 3, max: 5 }
          maritime_occurrence: { default: 4, min: 3, max: 5 }
      incident_reports:
        categories:
          investigations_root_cause: { default: 6, min: 5, max: 7 }
          lessons_learned: { default: 5, min: 4, max: 6 }
          safety_bulletins_alerts: { default: 4, min: 3, max: 5 }
      regulations:
        categories:
          us_cfr_osha: { default: 6, min: 5, max: 7 }
          us_cfr_dot_phmsa: { default: 6, min: 5, max: 7 }
          eu_aviation_easa: { default: 7, min: 6, max: 8 }
      standards:
        categories:
          iso_management_systems: { default: 7, min: 6, max: 8 }
          iec_functional_safety: { default: 8, min: 7, max: 9 }
          nfpa_process_safety: { default: 8, min: 7, max: 9 }
      methods:
        categories:
          hazop_jha_jsa: { default: 5, min: 4, max: 6 }
          fault_tree_event_tree: { default: 6, min: 5, max: 7 }
          stpa_stamp_fram: { default: 7, min: 6, max: 9 }
          human_factors_hfacs: { default: 6, min: 5, max: 8 }
      research:
        categories:
          quantitative_risk_analysis: { default: 9, min: 8, max: 10 }
          safety_culture_org: { default: 7, min: 6, max: 9 }
```

### 7.3 How difficulty gets assigned (match math v2)
Use the same precedence as `math_pipeline_v2/difficulty_worker.py`:

1) If record already has `difficulty.level` → keep it  
2) Else if `routing.subject/domain/category` is present → `level_from_routing()`  
3) Else fallback heuristic (length-based). For safety, **extend heuristic** with keyword boosts:

- If text contains `STPA`, `STAMP`, `IEC 61508`, `Bayesian`, `Monte Carlo`, `fault tree`, `event tree`, `SIL`, `PRA` → bump +1 to +2 (cap at 10)
- If it’s a short bulletin/recall notice → bias toward 3–4

(You can implement this as an optional `heuristic_level_safety()` wrapper.)

### 7.4 Folder placement in final
Match math v2: `final/{license_pool}/d{level:02d}/shards/*.jsonl.gz`  
If you want richer hierarchy, reuse math v2’s `folder_layout` idea (optional):

`final/{license_pool}/d{level:02d}/{subject}/{domain}/{category}/shards/*.jsonl.gz`

---

## 8) Update `catalog_builder.py` to match v2

Catalog should:
- walk `raw/`, `screened_yellow/`, `combined/`, `final/` by `{license_pool}`
- aggregate bytes + record counts from shards
- summarize ledgers (`yellow_pass`, `yellow_pitch`, `combined_index`, `difficulty_index`)
- write a single `catalog.json` (and optionally per-stage CSVs)

Keep it simple: you mostly need an auditable *inventory* and links to the ledgers.

---

## 9) Testing/validation checklist (do this before scaling)

1) **Classify stage**
- Ensure `_queues/green_download.jsonl` and `_queues/yellow_pipeline.jsonl` include:
  - `queue_bucket`
  - `license_profile`
  - `output_pool`
  - `routing_*` fields (or at least `routing`)

2) **Acquire stages**
- Verify downloads land in `raw/{bucket}/{pool}/{target_id}/`
- Ensure per-target manifests are written

3) **Screen YELLOW**
- Confirm pitch behavior: uncertain licensing/PII → goes to `_pitches/yellow_pitch.jsonl`
- Confirm pass ledgers record `content_sha256` and output shard path

4) **Merge**
- Confirm dedupe reduces duplicates and `combined_index.jsonl` maps hashes → shards

5) **Difficulty**
- Confirm routing-based assignment works (spot-check 20 records)
- Confirm outputs appear in `final/{pool}/dXX/`

---

## 10) Suggested “first targets” to validate difficulty mapping

Use a small test set that covers the rubric:

- **d03**: PHMSA incident tables (pipelines) rendered as event-row text
- **d04–d05**: basic safety bulletins/alerts (short narrative)
- **d06**: CSB investigation report chunks (Findings/Recommendations)
- **d07–d08**: STPA or IEC functional safety guide excerpts (if license allows)
- **d09–d10**: a handful of open-access PRA/quant risk analysis papers (clear TDM terms)

---

## 11) Minimal file list for `safety_incident_pipeline_v2`

```
safety_incident_pipeline_v2/
  acquire_worker.py
  yellow_screen_worker.py
  merge_worker.py
  difficulty_worker.py
  catalog_builder.py
  pipeline_driver.py
  review_queue.py
  run_pipeline.sh
  targets_safety_incident.yaml
  difficulties_safety_incident.yaml
  field_schemas_safety_incident.yaml
  license_map.yaml
  denylist.yaml
  requirements.txt
  README.md
```

---

## 12) Quick “diff summary” (what you’ll actually change from v1)

- Replace `globals.storage_root/pools/...` with v2 roots (`raw_root`, `screened_yellow_root`, `combined_root`, `final_root`, `ledger_root`, `pitches_root`)
- Split v1 `yellow` stage into `acquire_yellow` + `screen_yellow`
- Add missing stages: `merge` and `difficulty`
- Introduce `difficulties_safety_incident.yaml` and ensure `routing` exists on targets and records
- Ensure GREEN yields canonical JSONL records (or add a green normalization hook)

