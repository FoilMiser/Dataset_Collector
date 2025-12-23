# kg_nav_pipeline_v1 → kg_nav_pipeline_v2 adaptation plan (match math_pipeline_v2)  
## Expanded difficulty-mapping specification

This document upgrades **kg_nav_pipeline_v1** into **kg_nav_pipeline_v2** so it has the *same pipeline behavior* and *artifact contract* as **math_pipeline_v2**, while preserving KG/Lit-navigation specifics (computed-only extraction, PII scrubbing, and navigation-episode synthesis).

The key “v2 parity” goal: **same stage ordering, same folder layout, same ledgers/manifests/queues conventions**, and **same difficulty assignment precedence**:
> **existing difficulty** → **routing-based default** → **heuristic fallback**.

---

## 0) North-star behavior to match (math_pipeline_v2)

Replicate these invariants from `math_pipeline_v2/`:

1. **Classify first, always**  
   `pipeline_driver.py` reads `targets_*.yaml`, snapshots license evidence, resolves SPDX, scans restriction phrases, applies denylist, and emits:
   - `_queues/green_download.jsonl`
   - `_queues/yellow_pipeline.jsonl`
   - `_queues/red_rejected.jsonl`
   - `_manifests/{target_id}/evaluation.json` (+ evidence snapshots)

2. **Acquire is bucketed**  
   `acquire_worker.py` reads a queue JSONL and writes payloads to:
   - `raw/green/{license_pool}/{target_id}/...`
   - `raw/yellow/{license_pool}/{target_id}/...`
   + `_manifests/{target_id}/acquire_{bucket}_done.json`

3. **YELLOW gets screened & canonicalized before merging**  
   `yellow_screen_worker.py` outputs:
   - `screened_yellow/{license_pool}/shards/yellow_shard_00000.jsonl.gz`
   - `_ledger/yellow_passed.jsonl`
   - `_ledger/yellow_pitched.jsonl`
   + `_manifests/{target_id}/yellow_screen_done.json`

4. **Merge canonical GREEN + screened YELLOW**  
   `merge_worker.py` outputs:
   - `combined/{license_pool}/shards/combined_00000.jsonl.gz`
   - `_ledger/combined_index.jsonl`

5. **Final screening + difficulty bucketing**  
   `difficulty_worker.py` outputs:
   - `final/{license_pool}/d01..d10/{subject}/{domain}/{category}/shards/final_00000.jsonl.gz`
   - `_ledger/final_index.jsonl`
   - `_pitches/final_pitched.jsonl` (optional)

6. **Catalog stage** summarizes the v2 layout and stats.  
   `catalog_builder.py` emits `_catalogs/*.json`.

> **KG-nav v2 must keep these same stage names, artifacts, and folder conventions.**

---

## 1) Create `kg_nav_pipeline_v2/` as a v2-skeleton clone

Start by copying `math_pipeline_v2/` into `kg_nav_pipeline_v2/` and then replace/extend domain logic:

### 1.1 Required v2 file set
- `pipeline_driver.py` (ported from math v2; default subject = `kg_nav`)
- `acquire_worker.py` (ported + KG-specific download strategies)
- `yellow_screen_worker.py` (**NEW/real** for kg-nav canonicalization + strict pitch)
- `merge_worker.py` (ported as-is)
- `difficulty_worker.py` (ported but extended: kg-nav difficulty + optional episode synthesis)
- `catalog_builder.py` (ported)
- `review_queue.py`, `yellow_scrubber.py` (ported)

New companions:
- `targets_kg_nav.yaml` (v2 globals + routing contract)
- `difficulties_kg_nav.yaml` (expanded 1–10 mapping; **see §8**)

### 1.2 Retire v1-only stages
Do not keep these as pipeline stages in v2 (fold into v2 workers or keep as libraries only):
- `download_worker.py` → replaced by `acquire_worker.py`
- `kg_worker.py`, `pii_scrub_worker.py`, `nav_episode_builder.py` → folded into `yellow_screen_worker.py` and `difficulty_worker.py` (or imported as helpers)

---

## 2) Update `targets.yaml` → `targets_kg_nav.yaml` (v2 globals + routing)

### 2.1 V2 globals (match math v2 keys)
Use the same `globals:` key names used by math v2, e.g.:

```yaml
globals:
  raw_root: /data/kg_nav/raw
  screened_yellow_root: /data/kg_nav/screened_yellow
  combined_root: /data/kg_nav/combined
  final_root: /data/kg_nav/final
  ledger_root: /data/kg_nav/_ledger
  pitches_root: /data/kg_nav/_pitches
  manifests_root: /data/kg_nav/_manifests
  queues_root: /data/kg_nav/_queues
  catalogs_root: /data/kg_nav/_catalogs
  logs_root: /data/kg_nav/_logs

  sharding:
    max_records_per_shard: 50000
    compression: gzip

  screening:
    min_chars: 200
    max_chars: 12000
    text_field_candidates: [text]
    record_license_field_candidates: [license, license_spdx]
    require_record_license: false
    allow_spdx: [CC0-1.0, CC-BY-4.0, MIT, Apache-2.0, CC-BY-SA-4.0]
    deny_phrases: ["noai", "no tdm", "no machine learning"]

  require_yellow_signoff: false
```

### 2.2 Add v2 routing blocks (critical for difficulty)
Add to every target:

```yaml
routing:
  subject: kg_nav
  domain: scientific_kg
  category: openalex_minimal_graph
  level: null
  granularity: target
```

> **Important:** In kg-nav, you will also generate *episode records* during/after merge; episode records must carry routing too (domain/category by *task type*), not just “source target”.

### 2.3 Companion files contract
Match math v2’s `companion_files` pattern so the v2 workers can load difficulty config:

```yaml
companion_files:
  license_map: ./license_map.yaml
  field_schemas: ./field_schemas.yaml
  denylist: ./denylist.yaml
  difficulties_map: ./difficulties_kg_nav.yaml
```

---

## 3) Make `pipeline_driver.py` v2-identical (subject defaults only)

Port `math_pipeline_v2/pipeline_driver.py` and adjust:
- default `subject` to `kg_nav`
- keep denylist semantics (`hard_red`, `force_yellow`)
- keep “terms/evidence changed since last run → downgrade to YELLOW” behavior

This ensures queue formation behaves identically to math v2.

---

## 4) Replace `download_worker.py` with v2 `acquire_worker.py` (kg-nav strategies)

### 4.1 Adopt v2 output layout
Write payloads to:
- `raw/green/{pool}/{target_id}/...`
- `raw/yellow/{pool}/{target_id}/...`

### 4.2 Add kg-nav download strategies (from v1 targets)
Your v1 kg-nav targets include things like:
- Wikidata dumps (HTTP)
- OpenAlex snapshot (S3 or HTTP)
- Crossref public data file (often requester-pays S3)
- OpenCitations COCI (HTTP/Zenodo/etc.)
- ORCID public data file (often limited/terms-sensitive)
- DataCite
- MeSH
- “CommonPile scholarly slice” (HF / existing ingestion)

Port these strategy handlers into `acquire_worker.py` (math v2 already has several; add the missing ones):
- `figshare`
- `s3_sync`
- `aws_requester_pays`
- `torrent` (only if you’re confident this stays within your policy/terms constraints)

### 4.3 Canonicalization timing for GREEN targets
To preserve **math v2 merge simplicity**, GREEN sources should become **canonical JSONL** *before* merge.

Recommended: implement “acquire+extract” strategies for GREEN targets that:
1) download the upstream snapshot/dump
2) compute/derive only allowed fields
3) output canonical `*.jsonl.gz` in the target folder

This keeps `merge_worker.py` unchanged.

---

## 5) Implement `yellow_screen_worker.py` for KG (strict pitch + canonicalization)

`yellow_screen_worker.py` is the v2 “hard gate”: anything suspicious/nonconforming is pitched, not merged.

### 5.1 Output contract (must match math v2)
- Passed: `screened_yellow/{pool}/shards/yellow_shard_XXXXX.jsonl.gz`
- Ledgers:
  - `_ledger/yellow_passed.jsonl` (one row per accepted record)
  - `_ledger/yellow_pitched.jsonl` (one row per rejected record; include `reason`)
- `_manifests/{target_id}/yellow_screen_done.json`

### 5.2 Adapter-based parsing (kg-specific)
Implement `adapter:` specified per target:
- `wikidata_truthy_edges`
- `openalex_minimal_graph`
- `crossref_minimal_graph`
- `opencitations_coci_edges`
- `orcid_scrub_minimal` (**PII-focused**)
- `nlm_mesh_minimal`

Each adapter must emit canonical records with:
- stable `record_id`
- `routing` (subject/domain/category)
- `hash.content_sha256` for dedupe
- `source` fields containing SPDX/evidence

---

## 6) Merge worker (keep identical)

`merge_worker.py` should be a straight port from math v2:
- input: canonical GREEN + screened YELLOW shards
- dedupe: `hash.content_sha256`
- output: `combined/{pool}/shards/*.jsonl.gz` + `_ledger/combined_index.jsonl`

---

## 7) Where difficulty is applied in kg-nav v2 (two supported modes)

You can keep strict parity while choosing *what* “final” contains:

### Mode A (recommended): **final = navigation episodes**
- `combined/` contains canonical graph records (nodes/edges/minimal entities).
- `difficulty_worker.py` synthesizes **navigation episodes** from the combined graph and writes them to `final/` by difficulty.

This matches the *stage order* of math v2 while producing the most useful kg-nav training data.

### Mode B: **final = canonical graph records sorted by difficulty**
- Use difficulty to sort graph records (usually low difficulty).
- Produce episodes in a separate optional tool.
This is less aligned with your v1 intent (which includes `kg_navigation_episodes`), so Mode A is usually better.

---

# 8) Difficulty mapping (expanded, concrete spec)

This is the main requested expansion.

## 8.1 Difficulty precedence (must match math v2)

In `difficulty_worker.py`, keep math v2’s precedence chain:

1) If record already has `difficulty.level` (or `metadata.difficulty.level`) → **keep it**
2) Else if `routing.subject/domain/category` matches `difficulties_kg_nav.yaml` → **use routing default**
3) Else → **heuristic fallback**  
   (math uses text length; kg-nav should use *episode/graph structure features*.)

> The output difficulty object should mirror math v2’s style:
```json
"difficulty": { "level": 6, "method": "routing", "confidence": 0.7 }
```

## 8.2 Define what “difficulty” means for KG navigation

KG-nav difficulty is **not** “math complexity”; it is “navigation/grounding complexity”:

- **Graph reasoning depth:** hop count, join count, branching factor
- **Disambiguation load:** entity candidate set sizes, need to choose among near-duplicates
- **Cross-namespace mapping:** DOI↔OpenAlex↔Wikidata↔ROR↔ORCID conversions
- **Constraint complexity:** time slicing, venue filters, concept filters, author-affiliation windows
- **Provenance burden:** explaining why an answer is supported, reconciling conflicting sources
- **Robustness to missingness:** fallbacks when edges are absent

**Key principle:** avoid using protected text for difficulty; use structural metadata.

## 8.3 Episode record schema (what the worker should produce)

For Mode A, ensure episode records include machine-readable metadata used for scoring:

```json
{
  "record_id": "navep_...",
  "text": "Prompt...\n\nAnswer...",
  "episode": {
    "prompt": "...",
    "answer": "...",
    "evidence": [ {"type":"edge", "src":"...", "rel":"...", "dst":"..."} ],
    "tools": [ {"name":"graph_lookup", "args": {...}} ]
  },
  "metadata": {
    "task_type": "citation_chain",
    "hop_count": 4,
    "join_count": 2,
    "branch_factor": 3,
    "candidate_counts": { "paper": 12, "author": 1, "org": 4 },
    "crosswalk_steps": 2,
    "constraint_count": 2,
    "constraints": ["year_range", "venue_filter"],
    "requires_reconciliation": false,
    "provenance_steps": 3
  },
  "routing": { "subject":"kg_nav", "domain":"citation_navigation", "category":"multi_hop_citation_chain" }
}
```

You can store the full structured object under `payload` instead of `episode` if you prefer; just keep metadata fields stable.

## 8.4 `difficulties_kg_nav.yaml` (match math v2 schema)

Start from math v2’s difficulty YAML schema:

- `schema_version`
- `updated_utc`
- `globals` (folder layout)
- `rubric.levels` (1..10 labels/descriptions/signals)
- `subjects.kg_nav.domains.<domain>.categories.<category>.level.default`
- optionally `rule_sets` for keyword-based routing (optional)

### Minimal skeleton (must load with math v2 loader)
```yaml
schema_version: "2.0"
updated_utc: "2025-12-22T00:00:00Z"

globals:
  default_subject: kg_nav
  default_domain: misc
  default_category: misc
  default_level: 5
  folder_layout: "final/{license_pool}/d{level:02d}/{subject}/{domain}/{category}"
  sanitize_path_segments: true

rubric:
  scale: { name: "Difficulty 1–10", min: 1, max: 10 }
  levels:
    1: { label: "Single-hop lookup", description: "...", signals: [...] }
    2: { label: "Hygiene + lookup", description: "...", signals: [...] }
    ...
    10:{ label: "Audit-grade multi-branch navigation", description: "...", signals: [...] }

subjects:
  kg_nav:
    domains:
      citation_navigation:
        categories:
          direct_citation_lookup: { level: { default: 3 } }
          multi_hop_citation_chain: { level: { default: 6 } }
          influence_pathfinding: { level: { default: 8 } }
      identifier_resolution:
        categories:
          doi_normalization: { level: { default: 2 } }
          id_crosswalk:      { level: { default: 4 } }
          entity_disambiguation: { level: { default: 5 } }
      provenance_grounding:
        categories:
          single_source_proof: { level: { default: 4 } }
          multi_source_reconciliation: { level: { default: 9 } }
```

> This exact nesting is what `math_pipeline_v2/difficulty_worker.py` expects when it calls `level_from_routing()`.

## 8.5 Concrete 1–10 rubric tailored to KG navigation

Use these “meaning anchors” for the scale (make them explicit in the YAML `rubric.levels`):

### d01 — Single-hop lookup
- One entity lookup, no ambiguity, minimal evidence (1 edge)
- Example: “What is the ROR for ‘University of X’?” with unique match.

### d02 — Hygiene + normalization
- Validate/normalize identifier (DOI format, ORCID checksum-style checks), single-hop retrieval
- Example: “Normalize this DOI and return its canonical form and OpenAlex ID.”

### d03 — 1–2 hop navigation
- Simple citation/author/org lookup with 1–2 hops, small evidence set (≤2)
- Example: “Given DOI, list the venue and year (via OpenAlex).”

### d04 — 2 hop + crosswalk OR light provenance
- Either a crosswalk step (DOI→OpenAlex) or a short provenance explanation (2–3 steps)
- Example: “Given paper DOI, find authors’ ORCIDs if available.”

### d05 — Multi-hop with modest disambiguation
- 3–4 hops, moderate ambiguity (candidate set 5–10), needs disambiguation by a single constraint
- Example: “Find the paper titled X by author Y in year Z; provide DOI.”

### d06 — Multi-hop chain with joins
- 4–5 hops, join across entity types (paper↔author↔paper), evidence 3–5
- Example: “Find a 3-step citation chain from Paper A to Paper B.”

### d07 — Constrained navigation + time slicing
- Adds constraint complexity: time windows, venue/concept filters, affiliation time slicing
- Example: “Find institutions an author was affiliated with between 2016–2018.”

### d08 — Cross-namespace joins + robust reasoning
- 2+ crosswalk steps, higher branching, evidence ≥5, handling missing edges with fallbacks
- Example: “From a Wikidata QID, find related OpenAlex works and the COCI citation neighborhood.”

### d09 — Multi-source reconciliation / provenance audit
- Conflicts across sources must be reconciled; provenance chain must be explicit and consistent
- Example: “Crossref vs OpenAlex disagree on venue/year—explain and decide.”

### d10 — Audit-grade multi-branch navigation plan
- Long, branching, agentic plan; multiple subgoals; ambiguous entities; must provide a traceable proof
- Example: “Identify the intellectual lineage of a concept across 5+ hops with disambiguations and provenance.”

## 8.6 How to map episode metadata → a level (heuristic fallback)

Math v2 uses `heuristic_level(text)` based on length.  
For kg-nav, implement a new `heuristic_level(record)` based on metadata:

### Feature set (recommended)
- `hop_count` (H)
- `join_count` (J)
- `branch_factor` (B)
- `evidence_count` (E)
- `crosswalk_steps` (X)
- `constraint_count` (C)
- `max_candidate_count` (K) (max of candidate_counts)
- `requires_reconciliation` (R ∈ {0,1})
- `provenance_steps` (P)

### Simple, deterministic scoring function
Use an integer score then clamp to 1..10:

```text
level = 2
level += max(0, H - 1)          # 1 hop doesn't add; each additional hop adds
level += min(2, J)              # joins matter, cap contribution
level += 1 if B >= 3 else 0
level += 1 if E >= 4 else 0
level += 1 if X >= 2 else 0
level += 1 if C >= 2 else 0
level += 1 if K >= 6 else 0
level += 2 if R else 0          # reconciliation is expensive
level += 1 if P >= 4 else 0
level = clamp(level, 1, 10)
```

Return `{level, method:"structural", confidence:0.55}`.

### Why this works
- It’s **stable** (no dependence on prompt length).
- It aligns with the rubric anchors (d06+ implies multi-hop + joins; d09 implies reconciliation).
- It’s **tunable** via thresholds without changing the pipeline contract.

## 8.7 Routing-based mapping: how to set `routing.domain/category` for episodes

To make routing mapping meaningful, set episode routing based on `metadata.task_type`:

### Suggested domain/category taxonomy (practical & comprehensive)

**identifier_resolution**
- `doi_normalization` (d02)
- `id_crosswalk` (d04)
- `entity_disambiguation` (d05)

**citation_navigation**
- `direct_citation_lookup` (d03)
- `multi_hop_citation_chain` (d06)
- `influence_pathfinding` (d08)

**affiliation_navigation**
- `org_lookup_ror` (d03)
- `author_affiliation_time_slice` (d07)

**concept_navigation**
- `mesh_concept_lookup` (d03)
- `concept_lineage_wikidata` (d08)

**provenance_grounding**
- `single_source_proof` (d04)
- `multi_source_reconciliation` (d09)

**graph_query_planning**
- `multi_step_plan` (d08)
- `audit_trace_plan` (d10)

> Put these into `difficulties_kg_nav.yaml` so `level_from_routing()` can return a default for the majority of episodes.

## 8.8 How to handle mixed tasks (multi-label episodes)

Some episodes combine task types (e.g., “crosswalk + citation chain + reconciliation”). Use one of:

**Option 1 (simple): choose the dominant category**  
Pick the category with the highest expected default level; set that in routing.

**Option 2 (recommended): add `routing.category_secondary[]`**  
Keep primary routing for foldering, but preserve extra labels in metadata for analysis.

Difficulty should be **max(primary_default, heuristic_level)** to avoid under-rating mixed episodes.

## 8.9 How to keep the folder layout identical to math v2

In `difficulties_kg_nav.yaml` keep:

```yaml
globals:
  folder_layout: "final/{license_pool}/d{level:02d}/{subject}/{domain}/{category}"
```

Then in the kg-nav v2 `difficulty_worker.py`, route output to:

```
final/{pool}/d{level:02d}/kg_nav/{domain}/{category}/shards/final_00000.jsonl.gz
```

This matches math v2 semantics while allowing richer routing.

## 8.10 Worked examples (expected outcomes)

### Example A — DOI normalization (easy)
Metadata:
- H=1, J=0, X=0, C=0, K=1, R=0, P=1  
Routing: `identifier_resolution / doi_normalization` → default **2**  
Heuristic ≈ 2–3 → **final=2**

### Example B — 4-hop citation chain with join
Metadata:
- H=4, J=2, B=3, E=4, X=1, C=1, K=3, R=0, P=3  
Routing: `citation_navigation / multi_hop_citation_chain` → default **6**  
Heuristic: ~7 → **final=7** (max of routing + heuristic)

### Example C — reconcile Crossref vs OpenAlex disagreement
Metadata:
- H=3, J=1, X=2, E=5, R=1, P=5  
Routing: `provenance_grounding / multi_source_reconciliation` → default **9**  
Heuristic: 9–10 → **final=9 or 10** depending on thresholds

---

## 9) What to change in `difficulty_worker.py` (kg-nav v2)

Start from `math_pipeline_v2/difficulty_worker.py` and make these changes:

1) Keep `level_from_routing()` exactly the same (it already supports `subjects/domains/categories/level/default`).
2) Replace `heuristic_level(text)` with `heuristic_level(record)`:
   - if record has `metadata.hop_count`, etc. use structural scoring
   - else fallback to text-length heuristic as last resort
3) When the record is an episode, compute:
   - `final_level = max(level_from_routing, heuristic_level)` (optional but recommended)
4) Keep output paths consistent with `folder_layout`.

You still output:
- `final/.../shards/*.jsonl.gz`
- `_ledger/final_index.jsonl`

---

## 10) Calibration & validation (make difficulty mapping “real”)

### 10.1 Calibration dataset
Create a small “gold” set (e.g., 200 episodes) and hand-label levels 1–10.
Then tune:
- routing defaults in YAML
- heuristic thresholds/weights

### 10.2 Unit tests (fast)
Add tests for:
- routing lookup returns expected default
- heuristic scoring matches known cases
- `existing difficulty` overrides routing/heuristic
- mixed-task episodes choose max(routing, heuristic) if enabled

### 10.3 Monitoring in catalog
Extend `catalog_builder.py` stats to include:
- counts per difficulty level
- distribution by `metadata.task_type`
- rate of “method=routing” vs “method=structural” vs “method=existing”
- pitch rates for episodes (if you pitch at difficulty stage)

---

## 11) Minimal critical path (what you implement first)

To get a working kg-nav v2 with rich difficulty mapping:

1. Copy `math_pipeline_v2/` → `kg_nav_pipeline_v2/`
2. Create `targets_kg_nav.yaml` with v2 globals + routing
3. Port KG download strategies into `acquire_worker.py`
4. Implement adapter-based `yellow_screen_worker.py`
5. Implement episode synthesis in `difficulty_worker.py` (Mode A) or keep as separate tool
6. Create `difficulties_kg_nav.yaml` with:
   - rubric 1–10
   - domains/categories taxonomy
   - routing defaults
7. Add heuristic structural scoring + tests

---

## Appendix A — Suggested `difficulties_kg_nav.yaml` starter (short)

Below is a compact starter you can expand:

```yaml
schema_version: "2.0"
updated_utc: "2025-12-22T00:00:00Z"

globals:
  default_subject: kg_nav
  default_domain: misc
  default_category: misc
  default_level: 5
  folder_layout: "final/{license_pool}/d{level:02d}/{subject}/{domain}/{category}"
  sanitize_path_segments: true

rubric:
  scale: { name: "Difficulty 1–10", min: 1, max: 10 }
  levels:
    1: { label: "Single-hop lookup", description: "One hop, no ambiguity.", signals: ["H=1", "low evidence"] }
    2: { label: "Normalization", description: "ID hygiene + single hop.", signals: ["normalize DOI/ORCID"] }
    3: { label: "Simple navigation", description: "1–2 hops, small evidence.", signals: ["H<=2"] }
    4: { label: "Crosswalk/proof", description: "Cross-namespace mapping OR light provenance.", signals: ["X>=1", "P>=2"] }
    5: { label: "Disambiguation", description: "Moderate candidate sets + constraint.", signals: ["K>=5", "C>=1"] }
    6: { label: "Multi-hop chain", description: "4–5 hops, joins.", signals: ["H>=4", "J>=1"] }
    7: { label: "Constraints + time", description: "Time slicing/filters.", signals: ["C>=2", "time window"] }
    8: { label: "Crosswalk joins", description: "Multiple crosswalks + branching.", signals: ["X>=2", "B>=3"] }
    9: { label: "Reconciliation", description: "Multi-source conflict resolution.", signals: ["R=true"] }
    10:{ label: "Audit-grade plan", description: "Long multi-branch plan with trace.", signals: ["branching", "P>=5"] }

subjects:
  kg_nav:
    domains:
      identifier_resolution:
        categories:
          doi_normalization: { level: { default: 2 } }
          id_crosswalk: { level: { default: 4 } }
          entity_disambiguation: { level: { default: 5 } }

      citation_navigation:
        categories:
          direct_citation_lookup: { level: { default: 3 } }
          multi_hop_citation_chain: { level: { default: 6 } }
          influence_pathfinding: { level: { default: 8 } }

      affiliation_navigation:
        categories:
          org_lookup_ror: { level: { default: 3 } }
          author_affiliation_time_slice: { level: { default: 7 } }

      provenance_grounding:
        categories:
          single_source_proof: { level: { default: 4 } }
          multi_source_reconciliation: { level: { default: 9 } }

      graph_query_planning:
        categories:
          multi_step_plan: { level: { default: 8 } }
          audit_trace_plan: { level: { default: 10 } }
```

---

If you want, I can also generate a concrete `targets_kg_nav.yaml` v2 draft (mirroring your v1 targets like `wikidata_dumps`, `openalex_snapshot`, `opencitations_coci`, etc.) with routing categories already populated to match this difficulty map.
